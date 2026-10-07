package broker

import (
	"fmt"
	pb "streamforge/api/gen"
	"streamforge/internal/config"
	"testing"
)

func TestPublishHeaderTrustBoundary(t *testing.T) {
	b := &Broker{C: config.Default()}
	r := &pb.PublishRequest{Topic: "orders", Headers: make(map[string]string)}
	for i := 0; i < 32; i++ {
		r.Headers[fmt.Sprintf("user-%d", i)] = "value"
	}
	if e := b.validatePublish(r, false); e != nil {
		t.Fatal("full user allowance rejected", e)
	}
	for _, key := range []string{"sf.original_topic", "sf.attempt", "sf.unknown", "SF.Attempt"} {
		one := &pb.PublishRequest{Topic: "orders", Headers: map[string]string{key: "forged"}}
		if e := b.validatePublish(one, false); e == nil {
			t.Fatalf("producer-controlled reserved key accepted: %s", key)
		}
	}
	for _, key := range []string{"sf.original_id", "sf.original_topic", "sf.original_partition", "sf.original_offset", "sf.attempt", "sf.last_error", "sf.retry_job"} {
		r.Headers[key] = "trusted"
	}
	if e := b.validatePublish(r, true); e != nil {
		t.Fatal("trusted provenance reduced the user allowance", e)
	}
	r.Headers["user-32"] = "excess"
	if e := b.validatePublish(r, true); e == nil {
		t.Fatal("too many user headers accepted on internal path")
	}
}
