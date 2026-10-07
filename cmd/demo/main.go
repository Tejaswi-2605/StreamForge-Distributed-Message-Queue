package main

import (
	"context"
	"flag"
	"fmt"
	pb "streamforge/api/gen"
	"streamforge/internal/cli"
	"streamforge/internal/client"
	"time"
)

func wait(ctx context.Context, c *client.Client, topic string, p int32, attempt int32) (*pb.Message, error) {
	tick := time.NewTicker(25 * time.Millisecond)
	defer tick.Stop()
	for {
		f, e := c.Fetch(ctx, &pb.FetchRequest{Topic: topic, Partition: p, Limit: 100})
		if e == nil {
			for _, m := range f.Messages {
				if m.Headers["sf.attempt"] == fmt.Sprint(attempt) {
					return m, nil
				}
			}
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-tick.C:
		}
	}
}
func run() error {
	addr := flag.String("broker", "127.0.0.1:9001", "bootstrap")
	flag.Parse()
	c, _, cancel, e := cli.Connect(*addr)
	defer cancel()
	if e != nil {
		return e
	}
	defer c.Close()
	ctx, done := context.WithTimeout(context.Background(), 90*time.Second)
	defer done()
	topic := fmt.Sprintf("demo.%d", time.Now().UnixNano())
	t, e := c.Bootstrap.CreateTopic(ctx, &pb.CreateTopicRequest{Name: topic, Partitions: 3})
	if e != nil {
		return e
	}
	if e = cli.Print(t); e != nil {
		return e
	}
	var sent []*pb.Message
	for i := 0; i < 6; i++ {
		r := &pb.PublishRequest{Topic: topic, Payload: []byte(fmt.Sprintf("order-%d", i))}
		if i < 3 {
			r.Key = []byte("customer-42")
		}
		m, e := c.Publish(ctx, r)
		if e != nil {
			return e
		}
		sent = append(sent, m)
		fmt.Printf("published partition=%d offset=%d keyed=%t\n", m.Partition, m.Offset, i < 3)
	}
	if sent[0].Partition != sent[1].Partition || sent[1].Partition != sent[2].Partition {
		return fmt.Errorf("key partition mismatch")
	}
	aReq := &pb.MemberRequest{Topic: topic, Group: topic, Member: "A"}
	bReq := &pb.MemberRequest{Topic: topic, Group: topic, Member: "B"}
	if _, e = c.Bootstrap.JoinConsumerGroup(ctx, aReq); e != nil {
		return e
	}
	b, e := c.Bootstrap.JoinConsumerGroup(ctx, bReq)
	if e != nil {
		return e
	}
	a, e := c.Bootstrap.Heartbeat(ctx, aReq)
	if e != nil {
		return e
	}
	fmt.Printf("generation=%d A=%v B=%v\n", a.Generation, ids(a), ids(b))
	if len(a.Partitions) != 2 || len(b.Partitions) != 1 {
		return fmt.Errorf("assignment mismatch")
	}
	count := 0
	for _, assignment := range []*pb.Assignment{a, b} {
		member := "A"
		if assignment == b {
			member = "B"
		}
		for _, p := range assignment.Partitions {
			f, e := c.Fetch(ctx, &pb.FetchRequest{Topic: topic, Partition: p.Id, Limit: 100, Group: topic, Member: member, Generation: assignment.Generation, Start: pb.StartPosition_EARLIEST})
			if e != nil {
				return e
			}
			count += len(f.Messages)
			if len(f.Messages) > 0 {
				if e = c.Commit(ctx, &pb.CommitRequest{Topic: topic, Partition: p.Id, Group: topic, Member: member, Generation: assignment.Generation, NextOffset: f.Messages[len(f.Messages)-1].Offset + 1}); e != nil {
					return e
				}
			}
		}
	}
	if count != 6 {
		return fmt.Errorf("fetch count %d", count)
	}
	if _, e = c.Bootstrap.LeaveConsumerGroup(ctx, bReq); e != nil {
		return e
	}
	if _, e = c.Bootstrap.LeaveConsumerGroup(ctx, aReq); e != nil {
		return e
	}
	a, e = c.Bootstrap.JoinConsumerGroup(ctx, aReq)
	if e != nil {
		return e
	}
	for _, p := range a.Partitions {
		f, e := c.Fetch(ctx, &pb.FetchRequest{Topic: topic, Partition: p.Id, Group: topic, Member: "A", Generation: a.Generation, Limit: 100, Start: pb.StartPosition_COMMITTED})
		if e != nil {
			return e
		}
		if len(f.Messages) != 0 {
			return fmt.Errorf("resume re-delivered committed data")
		}
	}
	_, e = c.Bootstrap.LeaveConsumerGroup(ctx, aReq)
	if e != nil {
		return e
	}
	current := sent[0]
	var retryCount int
	for i := 0; i < 32; i++ {
		r, e := c.Fail(ctx, &pb.FailRequest{Topic: current.Topic, Partition: current.Partition, Offset: current.Offset, Error: "deterministic demo failure"})
		if e != nil {
			return e
		}
		current, e = wait(ctx, c, r.TargetTopic, current.Partition, r.Attempt)
		if e != nil {
			return e
		}
		fmt.Printf("attempt=%d target=%s original_id=%s\n", r.Attempt, r.TargetTopic, current.Headers["sf.original_id"])
		if r.TargetTopic == topic+".dlq" {
			break
		}
		retryCount++
		if i == 31 {
			return fmt.Errorf("DLQ not reached")
		}
	}
	if current.Headers["sf.original_id"] != sent[0].Id {
		return fmt.Errorf("provenance mismatch")
	}
	return cli.Print(map[string]interface{}{"published": 6, "fetched": count, "consumer_resume": "passed", "rebalanced": "A owns all 3 after B leaves", "retry_deliveries": retryCount, "dlq": "verified", "topic": topic})
}
func ids(a *pb.Assignment) []int32 {
	var out []int32
	for _, p := range a.Partitions {
		out = append(out, p.Id)
	}
	return out
}
func main() { cli.Main(run) }
