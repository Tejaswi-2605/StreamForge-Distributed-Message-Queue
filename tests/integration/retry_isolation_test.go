package integration

import (
	pb "streamforge/api/gen"
	"streamforge/internal/domain"
	"testing"
	"time"
)

func TestBrokenRetryDoesNotBlockSamePass(t *testing.T) {
	c := newCluster(t, 1)
	c.configs[0].RetryBase = time.Hour
	c.configs[0].RetryMax = time.Hour
	c.restart(t, 0)
	topic := c.topic(t, 1)
	source, e := c.client.Publish(c.ctx, &pb.PublishRequest{Topic: topic, Payload: []byte("healthy retry")})
	if e != nil {
		t.Fatal(e)
	}
	// Simulate legacy damaged job metadata that refers to a missing source.
	job, e := c.client.Fail(c.ctx, &pb.FailRequest{Topic: topic, Offset: source.Offset, Error: "recoverable"})
	if e != nil {
		t.Fatal(e)
	}
	_, e = c.brokers[0].Store.Schedule(c.ctx, domain.RetryRecord{ID: "broken-job", Topic: topic, Offset: 999, Attempt: 1, Due: time.Now().Add(-time.Hour).UnixNano()}, topic+".retry")
	if e != nil {
		t.Fatal(e)
	}
	if e = c.brokers[0].ProcessRetries(c.ctx, time.Now().Add(2*time.Hour)); e == nil {
		t.Fatal("broken retry must be reported")
	}
	fetched, e := c.client.Fetch(c.ctx, &pb.FetchRequest{Topic: job.TargetTopic, Limit: 10})
	if e != nil || len(fetched.Messages) != 1 || string(fetched.Messages[0].Payload) != "healthy retry" {
		t.Fatal("broken job blocked healthy dispatch", fetched, e)
	}
	jobs, e := c.brokers[0].Store.Due(c.ctx, 0, time.Now().Add(2*time.Hour), 32)
	if e != nil || len(jobs) != 1 || jobs[0].ID != "broken-job" {
		t.Fatal("healthy completion or failed job preservation is incorrect", jobs, e)
	}
}
