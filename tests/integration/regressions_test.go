package integration

import (
	"bytes"
	"fmt"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	pb "streamforge/api/gen"
	"streamforge/internal/domain"
	"testing"
	"time"
)

func TestTopicPathsCannotAlias(t *testing.T) {
	c := newCluster(t, 1)
	for _, name := range []string{"Orders", "orders.", "CON", "con", "con.retry", "lpt1.events"} {
		if _, e := c.client.Bootstrap.CreateTopic(c.ctx, &pb.CreateTopicRequest{Name: name, Partitions: 1}); status.Code(e) != codes.InvalidArgument {
			t.Fatalf("unsafe physical topic accepted: %q: %v", name, e)
		}
	}
	topic := c.topic(t, 1)
	for _, payload := range []string{"first", "second"} {
		if _, e := c.client.Publish(c.ctx, &pb.PublishRequest{Topic: topic, Payload: []byte(payload)}); e != nil {
			t.Fatal(e)
		}
	}
	c.restart(t, 0)
	f, e := c.client.Fetch(c.ctx, &pb.FetchRequest{Topic: topic, Limit: 10})
	if e != nil || len(f.Messages) != 2 || string(f.Messages[0].Payload) != "first" || string(f.Messages[1].Payload) != "second" || f.NextOffset != 2 {
		t.Fatal("safe-topic payloads/offsets did not survive restart", f, e)
	}
}

func TestReservedHeadersRejectedThroughRPCAndBatch(t *testing.T) {
	c := newCluster(t, 1)
	topic := c.topic(t, 1)
	for _, key := range []string{"sf.original_topic", "sf.attempt", "sf.retry_job", "SF.Attempt"} {
		r := &pb.PublishRequest{Topic: topic, Headers: map[string]string{key: "forged"}}
		if _, e := c.client.Bootstrap.Publish(c.ctx, r); status.Code(e) != codes.InvalidArgument {
			t.Fatalf("reserved header accepted on Publish: %s: %v", key, e)
		}
		batch := &pb.PublishBatchRequest{Messages: []*pb.PublishRequest{{Topic: topic, Payload: []byte("valid prefix")}, r}}
		if _, e := c.client.Bootstrap.PublishBatch(c.ctx, batch); status.Code(e) != codes.InvalidArgument {
			t.Fatalf("reserved header accepted on PublishBatch: %s: %v", key, e)
		}
	}
	f, e := c.client.Fetch(c.ctx, &pb.FetchRequest{Topic: topic, Limit: 1})
	if e != nil || f.NextOffset != 0 {
		t.Fatal("invalid-header operations must not append", f, e)
	}
}

func TestRetryPreservesFullUserHeaderAllowance(t *testing.T) {
	c := newCluster(t, 1)
	topic := c.topic(t, 1)
	headers := make(map[string]string)
	for i := 0; i < 32; i++ {
		headers[fmt.Sprintf("user-%d", i)] = fmt.Sprintf("value-%d", i)
	}
	source, e := c.client.Publish(c.ctx, &pb.PublishRequest{Topic: topic, Payload: []byte("poison"), Headers: headers})
	if e != nil {
		t.Fatal(e)
	}
	current := source
	for attempt := 1; attempt <= 3; attempt++ {
		job, e := c.client.Fail(c.ctx, &pb.FailRequest{Topic: current.Topic, Offset: current.Offset, Error: "processing failure"})
		if e != nil || job.Attempt != int32(attempt) {
			t.Fatal(job, e)
		}
		var delivered *pb.Message
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			f, e := c.client.Fetch(c.ctx, &pb.FetchRequest{Topic: job.TargetTopic, Limit: 100})
			if e != nil {
				t.Fatal(e)
			}
			for _, m := range f.Messages {
				if m.Headers["sf.retry_job"] == job.JobId {
					delivered = m
				}
			}
			if delivered != nil {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		if delivered == nil {
			t.Fatalf("attempt %d with 32 user headers never dispatched", attempt)
		}
		for key, value := range headers {
			if delivered.Headers[key] != value {
				t.Fatalf("user header lost on attempt %d: %s", attempt, key)
			}
		}
		if len(delivered.Headers) != 39 || delivered.Headers["sf.original_id"] != source.Id || delivered.Headers["sf.original_topic"] != topic || delivered.Headers["sf.attempt"] != fmt.Sprint(attempt) || !bytes.Equal(delivered.Payload, source.Payload) {
			t.Fatal("retry provenance/payload changed", delivered.Headers)
		}
		current = delivered
	}
	if current.Topic != topic+".dlq" {
		t.Fatal("full-header source did not reach DLQ")
	}
}

func TestLargeMessagesFitTransportAndFetch(t *testing.T) {
	c := newCluster(t, 1)
	c.configs[0].MaxMessage = domain.MaxPayloadBytes
	c.configs[0].SegmentBytes = 32 << 20
	c.restart(t, 0)
	topic := c.topic(t, 1)
	for _, size := range []int{5 << 20, domain.MaxPayloadBytes} {
		payload := bytes.Repeat([]byte("x"), size)
		r := &pb.PublishRequest{Topic: topic, Payload: payload, Key: bytes.Repeat([]byte("k"), 4096)}
		var published *pb.Message
		if size == domain.MaxPayloadBytes {
			batch, e := c.client.Bootstrap.PublishBatch(c.ctx, &pb.PublishBatchRequest{Messages: []*pb.PublishRequest{r}})
			if e != nil || len(batch.Messages) != 1 {
				t.Fatal("maximum payload batch rejected", e)
			}
			published = batch.Messages[0]
		} else {
			var e error
			published, e = c.client.Publish(c.ctx, r)
			if e != nil {
				t.Fatal("large network publish rejected", e)
			}
		}
		f, e := c.client.Fetch(c.ctx, &pb.FetchRequest{Topic: topic, Offset: published.Offset, Limit: 1})
		if e != nil || len(f.Messages) != 1 || !bytes.Equal(f.Messages[0].Payload, payload) || !bytes.Equal(f.Messages[0].Key, r.Key) {
			t.Fatal("acknowledged record must be fetchable", size, e)
		}
	}
	if _, e := c.client.Bootstrap.Publish(c.ctx, &pb.PublishRequest{Topic: topic, Payload: make([]byte, domain.MaxPayloadBytes+1)}); status.Code(e) != codes.InvalidArgument {
		t.Fatal("over-limit payload accepted", e)
	}
	c.restart(t, 0)
	f, e := c.client.Fetch(c.ctx, &pb.FetchRequest{Topic: topic, Offset: 1, Limit: 1})
	if e != nil || len(f.Messages) != 1 || len(f.Messages[0].Payload) != domain.MaxPayloadBytes || f.NextOffset != 2 {
		t.Fatal("maximum payload did not survive restart", e)
	}
}
