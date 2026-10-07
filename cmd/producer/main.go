package main

import (
	"flag"
	"fmt"
	pb "streamforge/api/gen"
	"streamforge/internal/cli"
)

func run() error {
	addr := flag.String("broker", "127.0.0.1:9001", "bootstrap broker")
	topic := flag.String("topic", "orders", "topic")
	key := flag.String("key", "", "key")
	msg := flag.String("message", "hello", "payload")
	p := flag.Int("partition", -1, "explicit partition; -1 auto")
	flag.Parse()
	if *p < -1 || *p > 1023 {
		return fmt.Errorf("--partition must be -1 (automatic) or 0..1023")
	}
	c, ctx, cancel, e := cli.Connect(*addr)
	defer cancel()
	if e != nil {
		return e
	}
	defer c.Close()
	r := &pb.PublishRequest{Topic: *topic, Key: []byte(*key), Payload: []byte(*msg)}
	if *p >= 0 {
		v := int32(*p)
		r.Partition = &v
	}
	v, e := c.Publish(ctx, r)
	if e != nil {
		return e
	}
	return cli.Print(v)
}
func main() { cli.Main(run) }
