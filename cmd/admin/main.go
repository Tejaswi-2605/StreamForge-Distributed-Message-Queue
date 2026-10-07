package main

import (
	"flag"
	"fmt"
	"os"
	pb "streamforge/api/gen"
	"streamforge/internal/cli"
)

func run() error {
	if len(os.Args) < 2 {
		return fmt.Errorf("usage: admin create-topic|list-topics|describe-topic [--topic name] [--partitions 3] [--broker host:port]")
	}
	cmd := os.Args[1]
	f := flag.NewFlagSet(cmd, flag.ContinueOnError)
	addr := f.String("broker", "127.0.0.1:9001", "bootstrap broker")
	topic := f.String("topic", "", "topic name")
	n := f.Int("partitions", 3, "partition count")
	if e := f.Parse(os.Args[2:]); e != nil {
		return e
	}
	c, ctx, cancel, e := cli.Connect(*addr)
	defer cancel()
	if e != nil {
		return e
	}
	defer c.Close()
	var v interface{}
	switch cmd {
	case "create-topic":
		v, e = c.Bootstrap.CreateTopic(ctx, &pb.CreateTopicRequest{Name: *topic, Partitions: int32(*n)})
	case "list-topics":
		v, e = c.Bootstrap.ListTopics(ctx, &pb.Empty{})
	case "describe-topic":
		v, e = c.Bootstrap.DescribeTopic(ctx, &pb.TopicRequest{Topic: *topic})
	default:
		return fmt.Errorf("unknown admin command %q", cmd)
	}
	if e != nil {
		return e
	}
	return cli.Print(v)
}
func main() { cli.Main(run) }
