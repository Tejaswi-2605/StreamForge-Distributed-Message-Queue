package main

import (
	"flag"
	"fmt"
	pb "streamforge/api/gen"
	"streamforge/internal/cli"
)

func run() error {
	addr := flag.String("broker", "127.0.0.1:9001", "bootstrap")
	topic := flag.String("topic", "orders", "topic")
	group := flag.String("group", "demo", "group")
	member := flag.String("member", "consumer-1", "member")
	from := flag.String("from", "committed", "earliest|latest|committed|explicit")
	offset := flag.Int64("offset", 0, "explicit offset")
	limit := flag.Int("limit", 100, "per partition")
	fail := flag.Bool("fail", false, "schedule retry instead of successful processing")
	flag.Parse()
	if *limit < 1 || *limit > 1000 || *offset < 0 {
		return fmt.Errorf("--limit must be 1..1000 and --offset must be nonnegative")
	}
	starts := map[string]pb.StartPosition{"earliest": pb.StartPosition_EARLIEST, "latest": pb.StartPosition_LATEST, "committed": pb.StartPosition_COMMITTED, "explicit": pb.StartPosition_EXPLICIT}
	start, ok := starts[*from]
	if !ok {
		return fmt.Errorf("invalid --from")
	}
	c, ctx, cancel, e := cli.Connect(*addr)
	defer cancel()
	if e != nil {
		return e
	}
	defer c.Close()
	r := &pb.MemberRequest{Group: *group, Topic: *topic, Member: *member}
	a, e := c.Bootstrap.JoinConsumerGroup(ctx, r)
	if e != nil {
		return e
	}
	defer c.Bootstrap.LeaveConsumerGroup(ctx, r)
	if e = cli.Print(a); e != nil {
		return e
	}
	for _, p := range a.Partitions {
		f, e := c.Fetch(ctx, &pb.FetchRequest{Topic: *topic, Partition: p.Id, Offset: *offset, Limit: int32(*limit), Start: start, Group: *group, Member: *member, Generation: a.Generation})
		if e != nil {
			return e
		}
		for _, m := range f.Messages {
			if *fail {
				if _, e = c.Fail(ctx, &pb.FailRequest{Topic: *topic, Partition: p.Id, Offset: m.Offset, Error: "demo processing failure"}); e != nil {
					return e
				}
			} else {
				if e = cli.Print(m); e != nil {
					return e
				}
			}
			if e = c.Commit(ctx, &pb.CommitRequest{Topic: *topic, Partition: p.Id, NextOffset: m.Offset + 1, Group: *group, Member: *member, Generation: a.Generation}); e != nil {
				return e
			}
		}
	}
	return nil
}
func main() { cli.Main(run) }
