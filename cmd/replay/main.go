package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"flag"
	"fmt"
	"hash"
	"io"
	"os"
	pb "streamforge/api/gen"
	"streamforge/internal/cli"
	"streamforge/internal/replay"
	"strings"
	"time"
)

func run() error {
	addr := flag.String("broker", "127.0.0.1:9001", "bootstrap")
	path := flag.String("file", "", "NDJSON or .gz")
	topic := flag.String("topic", "events.raw", "existing empty topic")
	limit := flag.Int("limit", 10000, "record limit; 0 unlimited")
	group := flag.String("group", fmt.Sprintf("replay.%d", time.Now().UnixNano()), "exclusive verification consumer group")
	flag.Parse()
	if *path == "" || *limit < 0 {
		return fmt.Errorf("--file is required and --limit must be nonnegative")
	}
	f, e := os.Open(*path)
	if e != nil {
		return e
	}
	defer f.Close()
	var r io.Reader = f
	if strings.HasSuffix(*path, ".gz") {
		g, e := gzip.NewReader(f)
		if e != nil {
			return e
		}
		defer g.Close()
		r = g
	}
	c, _, cancel, e := cli.Connect(*addr)
	defer cancel()
	if e != nil {
		return e
	}
	defer c.Close()
	ctx, done := context.WithTimeout(context.Background(), 30*time.Minute)
	defer done()
	t, e := c.Bootstrap.DescribeTopic(ctx, &pb.TopicRequest{Topic: *topic})
	if e != nil {
		return e
	}
	for _, p := range t.Partitions {
		v, e := c.Fetch(ctx, &pb.FetchRequest{Topic: *topic, Partition: p.Id, Limit: 1, Start: pb.StartPosition_LATEST})
		if e != nil {
			return e
		}
		if v.NextOffset != 0 {
			return fmt.Errorf("replay verification requires an empty topic")
		}
	}
	expected := make([]int64, len(t.Partitions))
	digests := make([]hash.Hash, len(t.Partitions))
	for p := range digests {
		digests[p] = sha256.New()
	}
	s, e := replay.Stream(ctx, r, *limit, func(ctx context.Context, event replay.Event, b []byte) error {
		m, e := c.Publish(ctx, &pb.PublishRequest{Topic: *topic, Key: []byte(event.Repo.Name), Payload: b})
		if e != nil {
			return e
		}
		if m.Partition < 0 || int(m.Partition) >= len(expected) || m.Offset != expected[m.Partition] {
			return fmt.Errorf("unexpected published partition/offset")
		}
		replay.AppendDigest(digests[m.Partition], m.Offset, []byte(event.Repo.Name), b)
		expected[m.Partition]++
		return nil
	})
	if e != nil {
		return e
	}
	consumed := 0
	consumeStart := time.Now()
	member := &pb.MemberRequest{Topic: *topic, Group: *group, Member: "replay-verifier"}
	assignment, e := c.Bootstrap.JoinConsumerGroup(ctx, member)
	if e != nil {
		return e
	}
	defer c.Bootstrap.LeaveConsumerGroup(ctx, member)
	checkExclusive := func(a *pb.Assignment) error {
		if len(a.Partitions) != len(t.Partitions) || len(a.ActiveMembers) != 1 {
			return fmt.Errorf("replay verification requires exclusive group ownership")
		}
		return nil
	}
	if e := checkExclusive(assignment); e != nil {
		return e
	}
	lastHeartbeat := time.Now()
	for _, p := range t.Partitions {
		var off int64
		readDigest := sha256.New()
		for {
			if time.Since(lastHeartbeat) >= 250*time.Millisecond {
				assignment, e = c.Bootstrap.Heartbeat(ctx, member)
				if e != nil {
					return e
				}
				if e := checkExclusive(assignment); e != nil {
					return e
				}
				lastHeartbeat = time.Now()
			}
			v, e := c.Fetch(ctx, &pb.FetchRequest{Topic: *topic, Partition: p.Id, Offset: off, Limit: 100, Group: *group, Member: member.Member, Generation: assignment.Generation})
			if e != nil {
				return e
			}
			if len(v.Messages) == 0 {
				break
			}
			for _, m := range v.Messages {
				if m.Partition != p.Id || m.Offset != off || off >= expected[p.Id] {
					return fmt.Errorf("unexpected replay offset %d/%d", p.Id, m.Offset)
				}
				replay.AppendDigest(readDigest, m.Offset, m.Key, m.Payload)
				consumed++
				off = m.Offset + 1
			}
			if e := c.Commit(ctx, &pb.CommitRequest{Topic: *topic, Partition: p.Id, Group: *group, Member: member.Member, Generation: assignment.Generation, NextOffset: off}); e != nil {
				return e
			}
		}
		if off != expected[p.Id] || !bytes.Equal(readDigest.Sum(nil), digests[p.Id].Sum(nil)) {
			return fmt.Errorf("replay payload/key/offset digest mismatch on partition %d", p.Id)
		}
		committed, e := c.Bootstrap.GetCommittedOffset(ctx, &pb.OffsetRequest{Topic: *topic, Partition: p.Id, Group: *group})
		if e != nil || committed.NextOffset != expected[p.Id] || (off > 0 && !committed.Exists) {
			return fmt.Errorf("replay committed-offset verification failed: %v", e)
		}
		resume, e := c.Fetch(ctx, &pb.FetchRequest{Topic: *topic, Partition: p.Id, Limit: 1, Start: pb.StartPosition_COMMITTED, Group: *group, Member: member.Member, Generation: assignment.Generation})
		if e != nil || len(resume.Messages) != 0 {
			return fmt.Errorf("replay committed resume failed: %v", e)
		}
	}
	elapsed := time.Since(consumeStart).Seconds()
	if consumed != s.Published {
		return fmt.Errorf("replay readback mismatch")
	}
	return cli.Print(struct {
		Ingestion      replay.Stats `json:"ingestion"`
		Consumed       int          `json:"consumed"`
		ConsumeSeconds float64      `json:"consume_seconds"`
		ConsumeRate    float64      `json:"consume_records_per_second"`
		Group          string       `json:"consumer_group"`
		Verification   string       `json:"verification"`
		CommittedLag   int64        `json:"committed_lag"`
	}{s, consumed, elapsed, float64(consumed) / elapsed, *group, "SHA-256 payload/key/offset equality per partition and committed group resume", 0})
}
func main() { cli.Main(run) }
