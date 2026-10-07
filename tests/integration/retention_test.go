package integration

import (
	"bytes"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	pb "streamforge/api/gen"
	"testing"
	"time"
)

func TestAutomaticRetentionPinsDurableRetries(t *testing.T) {
	c := newCluster(t, 1)
	c.configs[0].RetentionSegments = 2
	c.configs[0].RetentionInterval = 20 * time.Millisecond
	c.configs[0].RetryBase = time.Hour
	c.configs[0].RetryMax = time.Hour
	c.restart(t, 0)
	topic := c.topic(t, 1)
	source, e := c.client.Publish(c.ctx, &pb.PublishRequest{Topic: topic, Payload: bytes.Repeat([]byte("p"), 200)})
	if e != nil {
		t.Fatal(e)
	}
	job, e := c.client.Fail(c.ctx, &pb.FailRequest{Topic: topic, Offset: source.Offset, Error: "delayed failure"})
	if e != nil {
		t.Fatal(e)
	}
	for i := 0; i < 20; i++ {
		if _, e := c.client.Publish(c.ctx, &pb.PublishRequest{Topic: topic, Payload: bytes.Repeat([]byte("x"), 200)}); e != nil {
			t.Fatal(e)
		}
	}
	if e := c.brokers[0].ApplyRetention(c.ctx); e != nil {
		t.Fatal(e)
	}
	f, e := c.client.Fetch(c.ctx, &pb.FetchRequest{Topic: topic, Limit: 1})
	if e != nil || len(f.Messages) != 1 || f.EarliestOffset != 0 {
		t.Fatal("pending retry source was deleted", e)
	}
	if e := c.brokers[0].ProcessRetries(c.ctx, time.Now().Add(2*time.Hour)); e != nil {
		t.Fatal(e)
	}
	retry, e := c.client.Fetch(c.ctx, &pb.FetchRequest{Topic: job.TargetTopic, Limit: 1})
	if e != nil || len(retry.Messages) != 1 || retry.Messages[0].Headers["sf.original_id"] != source.Id {
		t.Fatal("pinned source did not dispatch", e)
	}
	deadline := time.Now().Add(3 * time.Second)
	trimmed := false
	for time.Now().Before(deadline) {
		latest, e := c.client.Fetch(c.ctx, &pb.FetchRequest{Topic: topic, Start: pb.StartPosition_EARLIEST, Limit: 10})
		if e != nil {
			t.Fatal(e)
		}
		if latest.EarliestOffset > 0 {
			trimmed = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !trimmed {
		t.Fatal("retention worker did not remove inactive unpinned segments")
	}
	if _, e := c.client.Fetch(c.ctx, &pb.FetchRequest{Topic: topic, Limit: 1}); status.Code(e) != codes.OutOfRange {
		t.Fatal("expired offset must explicitly fail", e)
	}
	c.restart(t, 0)
	m, e := c.client.Publish(c.ctx, &pb.PublishRequest{Topic: topic, Payload: []byte("after restart")})
	if e != nil || m.Offset != 21 {
		t.Fatal("retention/restart reset next offset", m, e)
	}
}
