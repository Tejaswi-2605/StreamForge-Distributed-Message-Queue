// Package client discovers persisted owners and routes explicit partitions.
// Connections are bounded by the cluster's broker count and must be closed.
package client

import (
	"context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
	pb "streamforge/api/gen"
	"streamforge/internal/domain"
	"sync"
	"sync/atomic"
)

type Client struct {
	Bootstrap pb.StreamForgeClient
	mu        sync.Mutex
	conns     map[string]*grpc.ClientConn
	rr        atomic.Uint64
}

func Dial(address string) (*Client, error) {
	c := &Client{conns: make(map[string]*grpc.ClientConn)}
	v, e := c.At(address)
	if e != nil {
		return nil, e
	}
	c.Bootstrap = v
	return c, nil
}
func (c *Client) At(address string) (pb.StreamForgeClient, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	conn := c.conns[address]
	if conn == nil {
		v, e := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(domain.MaxResponseBytes), grpc.MaxCallSendMsgSize(domain.MaxRequestBytes)))
		if e != nil {
			return nil, e
		}
		conn = v
		c.conns[address] = v
	}
	return pb.NewStreamForgeClient(conn), nil
}
func (c *Client) Owner(ctx context.Context, topic string, p int32) (pb.StreamForgeClient, error) {
	t, e := c.Bootstrap.DescribeTopic(ctx, &pb.TopicRequest{Topic: topic})
	if e != nil {
		return nil, e
	}
	if p < 0 || int(p) >= len(t.Partitions) {
		return nil, domain.ErrInvalid
	}
	return c.At(t.Partitions[p].OwnerAddress)
}
func (c *Client) Publish(ctx context.Context, r *pb.PublishRequest) (*pb.Message, error) {
	if r == nil {
		return nil, domain.ErrInvalid
	}
	t, e := c.Bootstrap.DescribeTopic(ctx, &pb.TopicRequest{Topic: r.Topic})
	if e != nil {
		return nil, e
	}
	if len(t.Partitions) == 0 {
		return nil, domain.ErrInvalid
	}
	out := proto.Clone(r).(*pb.PublishRequest)
	if out.Partition == nil {
		p := int32(0)
		if len(r.Key) > 0 {
			p = int32(domain.KeyPartition(r.Key, len(t.Partitions)))
		} else {
			p = int32((c.rr.Add(1) - 1) % uint64(len(t.Partitions)))
		}
		out.Partition = &p
	}
	p := *out.Partition
	if p < 0 || int(p) >= len(t.Partitions) {
		return nil, domain.ErrInvalid
	}
	api, e := c.At(t.Partitions[p].OwnerAddress)
	if e != nil {
		return nil, e
	}
	return api.Publish(ctx, out)
}
func (c *Client) Fetch(ctx context.Context, r *pb.FetchRequest) (*pb.FetchResponse, error) {
	if r == nil {
		return nil, domain.ErrInvalid
	}
	api, e := c.Owner(ctx, r.Topic, r.Partition)
	if e != nil {
		return nil, e
	}
	return api.Fetch(ctx, r)
}
func (c *Client) Commit(ctx context.Context, r *pb.CommitRequest) error {
	api, e := c.Owner(ctx, r.Topic, r.Partition)
	if e != nil {
		return e
	}
	_, e = api.CommitOffset(ctx, r)
	return e
}
func (c *Client) Fail(ctx context.Context, r *pb.FailRequest) (*pb.RetryResponse, error) {
	api, e := c.Owner(ctx, r.Topic, r.Partition)
	if e != nil {
		return nil, e
	}
	return api.FailMessage(ctx, r)
}
func (c *Client) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, v := range c.conns {
		v.Close()
	}
	c.conns = make(map[string]*grpc.ClientConn)
}
