package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"os"
	"sort"
	pb "streamforge/api/gen"
	"streamforge/internal/cli"
	"streamforge/internal/logstore"
	"time"
)

type Result struct {
	Mode                                                                                               string `json:"mode"`
	Clock                                                                                              string `json:"clock"`
	Messages, PayloadBytes, Partitions, Brokers                                                        int
	Durability                                                                                         string
	PublishSeconds, FetchSeconds, EndToEndSeconds, PublishPerSecond, FetchPerSecond, EndToEndPerSecond float64
	AverageMS, P50MS, P95MS, P99MS                                                                     float64
	PayloadTotalBytes                                                                                  int64
	LogBytes                                                                                           int64 `json:"log_bytes,omitempty"`
}

func run() error {
	mode := flag.String("mode", "api", "api|raw")
	addr := flag.String("broker", "127.0.0.1:9001", "bootstrap")
	n := flag.Int("messages", 10000, "measured messages")
	size := flag.Int("size", 100, "payload bytes")
	partitions := flag.Int("partitions", 3, "partitions")
	durability := flag.String("durability", "fsync", "raw: write|fsync; API: declared server mode")
	flag.Parse()
	if *n < 1 || *n > 500000 || *size < 1 || *size > 1<<20 || *partitions < 1 || *partitions > 1024 || (*durability != "write" && *durability != "fsync") {
		return fmt.Errorf("invalid workload")
	}
	payload := bytes.Repeat([]byte("x"), *size)
	lat := make([]float64, *n)
	res := Result{Mode: *mode, Clock: monotonicName(), Messages: *n, PayloadBytes: *size, Partitions: *partitions, Durability: *durability, PayloadTotalBytes: int64(*n) * int64(*size)}
	offsets := make([]int64, *partitions)
	if *mode == "raw" {
		dir, e := os.MkdirTemp("", "streamforge-bench-")
		if e != nil {
			return e
		}
		defer os.RemoveAll(dir)
		var logs []*logstore.Log
		defer func() {
			for _, l := range logs {
				l.Close()
			}
		}()
		for i := 0; i < *partitions; i++ {
			l, e := logstore.Open(fmt.Sprintf("%s/%d", dir, i), logstore.Options{SegmentBytes: 16 << 20, IndexInterval: 64, MaxRecord: (1 << 20) + (16 << 10), Sync: *durability == "fsync"})
			if e != nil {
				return e
			}
			logs = append(logs, l)
		}
		for i := 0; i < 100; i++ {
			p := i % len(logs)
			if _, e := logs[p].Append(&pb.Message{Payload: payload}); e != nil {
				return e
			}
			offsets[p]++
		}
		start := monotonicNow()
		for i := 0; i < *n; i++ {
			t := monotonicNow()
			p := i % len(logs)
			m, e := logs[p].Append(&pb.Message{Id: "benchmark", Payload: payload, Partition: int32(p)})
			if e != nil {
				return e
			}
			if m.Offset != offsets[p] {
				return fmt.Errorf("offset invariant")
			}
			offsets[p]++
			lat[i] = monotonicSeconds(t) * 1000
		}
		res.PublishSeconds = monotonicSeconds(start)
		begin := monotonicNow()
		count := 0
		for _, l := range logs {
			earliest, next := l.Bounds()
			for off := earliest; off < next; {
				ms, e := l.Read(off, 100, 4<<20)
				if e != nil {
					return e
				}
				if len(ms) == 0 {
					return fmt.Errorf("empty read")
				}
				for _, m := range ms {
					if !bytes.Equal(m.Payload, payload) {
						return fmt.Errorf("readback mismatch")
					}
					off = m.Offset + 1
					count++
				}
			}
			res.LogBytes += l.Size()
		}
		res.FetchSeconds = monotonicSeconds(begin)
		if count != *n+100 {
			return fmt.Errorf("count mismatch")
		}
		res.Brokers = 0
	} else if *mode == "api" {
		c, _, cancel, e := cli.Connect(*addr)
		defer cancel()
		if e != nil {
			return e
		}
		defer c.Close()
		ctx, done := context.WithTimeout(context.Background(), 30*time.Minute)
		defer done()
		topic := fmt.Sprintf("bench.%d", time.Now().UnixNano())
		t, e := c.Bootstrap.CreateTopic(ctx, &pb.CreateTopicRequest{Name: topic, Partitions: int32(*partitions)})
		if e != nil {
			return e
		}
		owners := make(map[int32]bool)
		for _, p := range t.Partitions {
			owners[p.OwnerId] = true
		}
		res.Brokers = len(owners)
		reqs := make([]*pb.PublishRequest, *partitions)
		apis := make([]pb.StreamForgeClient, *partitions)
		for i, p := range t.Partitions {
			v := p.Id
			reqs[i] = &pb.PublishRequest{Topic: topic, Partition: &v, Payload: payload}
			apis[i], e = c.At(p.OwnerAddress)
			if e != nil {
				return e
			}
		}
		for i := 0; i < 100; i++ {
			p := i % len(reqs)
			if _, e = apis[p].Publish(ctx, reqs[p]); e != nil {
				return e
			}
			offsets[p]++
		}
		start := monotonicNow()
		for i := 0; i < *n; i++ {
			p := i % len(reqs)
			ts := monotonicNow()
			m, e := apis[p].Publish(ctx, reqs[p])
			if e != nil {
				return e
			}
			lat[i] = monotonicSeconds(ts) * 1000
			if m.Offset != offsets[p] {
				return fmt.Errorf("offset invariant")
			}
			offsets[p]++
		}
		res.PublishSeconds = monotonicSeconds(start)
		begin := monotonicNow()
		count := 0
		for i, api := range apis {
			off := int64(0)
			for off < offsets[i] {
				f, e := api.Fetch(ctx, &pb.FetchRequest{Topic: topic, Partition: int32(i), Offset: off, Limit: 100})
				if e != nil {
					return e
				}
				if len(f.Messages) == 0 {
					return fmt.Errorf("empty fetch")
				}
				for _, m := range f.Messages {
					if m.Offset != off || !bytes.Equal(m.Payload, payload) {
						return fmt.Errorf("readback mismatch")
					}
					off++
					count++
				}
			}
		}
		res.FetchSeconds = monotonicSeconds(begin)
		if count != *n+100 {
			return fmt.Errorf("readback count mismatch")
		}
	} else {
		return fmt.Errorf("--mode must be api or raw")
	}
	var sum float64
	for _, v := range lat {
		sum += v
	}
	sort.Float64s(lat)
	q := func(p float64) float64 { return lat[int(float64(len(lat)-1)*p)] }
	res.AverageMS = sum / float64(*n)
	res.P50MS = q(.50)
	res.P95MS = q(.95)
	res.P99MS = q(.99)
	res.EndToEndSeconds = res.PublishSeconds + res.FetchSeconds
	res.PublishPerSecond = float64(*n) / res.PublishSeconds
	res.FetchPerSecond = float64(*n+100) / res.FetchSeconds
	res.EndToEndPerSecond = float64(*n) / res.EndToEndSeconds
	return cli.Print(res)
}
func main() { cli.Main(run) }
