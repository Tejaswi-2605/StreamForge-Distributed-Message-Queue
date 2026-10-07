package broker

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	gm "google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	pb "streamforge/api/gen"
	"streamforge/internal/config"
	"streamforge/internal/domain"
	"streamforge/internal/logstore"
	"streamforge/internal/metadata"
	"streamforge/internal/metrics"
	"streamforge/internal/retry"
)

type Broker struct {
	pb.UnimplementedStreamForgeServer
	C           config.Config
	Store       *metadata.Store
	Metrics     *metrics.Metrics
	mu          sync.Mutex
	retentionMu sync.Mutex
	logs        map[string]*logstore.Log
	closed      bool
	rr          atomic.Uint64
	slots       chan struct{}
}

func New(ctx context.Context, c config.Config, s *metadata.Store) (*Broker, error) {
	if e := c.Validate(); e != nil {
		return nil, e
	}
	b := &Broker{C: c, Store: s, Metrics: new(metrics.Metrics), logs: make(map[string]*logstore.Log), slots: make(chan struct{}, c.MaxInflight)}
	topics, e := s.List(ctx)
	if e != nil {
		return nil, e
	}
	for _, t := range topics {
		for _, p := range t.Partitions {
			if p.Owner.ID == c.ID {
				if _, e = b.log(t.Name, p.ID); e != nil {
					b.Close()
					return nil, e
				}
			}
		}
	}
	return b, nil
}
func (b *Broker) log(topic string, p int) (*logstore.Log, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil, fmt.Errorf("broker closed")
	}
	k := fmt.Sprintf("%s/%d", topic, p)
	if l := b.logs[k]; l != nil {
		return l, nil
	}
	if !domain.ValidTopicName(topic) || p < 0 {
		return nil, domain.ErrInvalid
	}
	l, e := logstore.Open(filepath.Join(b.C.DataDir, "topics", topic, fmt.Sprintf("partition-%d", p)), logstore.Options{SegmentBytes: int64(b.C.SegmentBytes), IndexInterval: b.C.IndexInterval, MaxRecord: b.C.MaxMessage + domain.RecordHeadroom, Sync: b.C.Sync})
	if e != nil {
		return nil, e
	}
	b.logs[k] = l
	return l, nil
}
func (b *Broker) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil
	}
	b.closed = true
	var errs []error
	for _, l := range b.logs {
		errs = append(errs, l.Close())
	}
	return errors.Join(errs...)
}
func (b *Broker) Interceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	start := time.Now()
	select {
	case b.slots <- struct{}{}:
		defer func() { <-b.slots }()
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	default:
		return nil, status.Error(codes.ResourceExhausted, "broker inflight limit reached")
	}
	if e := ctx.Err(); e != nil {
		return nil, status.FromContextError(e).Err()
	}
	v, e := handler(ctx, req)
	b.Metrics.Observe(time.Since(start), e)
	if e != nil {
		slog.Warn("RPC failed", "broker_id", b.C.ID, "operation", info.FullMethod, "code", status.Code(e).String())
	}
	return v, e
}
func rpcError(e error) error {
	if e == nil {
		return nil
	}
	if status.Code(e) != codes.Unknown {
		return e
	}
	switch {
	case errors.Is(e, domain.ErrInvalid):
		return status.Error(codes.InvalidArgument, e.Error())
	case errors.Is(e, domain.ErrNotFound):
		return status.Error(codes.NotFound, e.Error())
	case errors.Is(e, domain.ErrExists):
		return status.Error(codes.AlreadyExists, e.Error())
	case errors.Is(e, domain.ErrFenced):
		return status.Error(codes.FailedPrecondition, e.Error())
	case errors.Is(e, domain.ErrOffset):
		return status.Error(codes.OutOfRange, e.Error())
	case errors.Is(e, domain.ErrRecordTooLarge):
		return status.Error(codes.ResourceExhausted, e.Error())
	case errors.Is(e, context.Canceled), errors.Is(e, context.DeadlineExceeded):
		return status.FromContextError(e).Err()
	default:
		slog.Error("broker operation failed", "error", e)
		return status.Error(codes.Unavailable, "storage or coordination unavailable; inspect broker logs")
	}
}
func wireTopic(t domain.Topic) *pb.Topic {
	out := &pb.Topic{Name: t.Name}
	for _, p := range t.Partitions {
		out.Partitions = append(out.Partitions, &pb.Partition{Id: int32(p.ID), OwnerId: int32(p.Owner.ID), OwnerAddress: p.Owner.Address})
	}
	return out
}
func (b *Broker) CreateTopic(ctx context.Context, r *pb.CreateTopicRequest) (*pb.Topic, error) {
	if r == nil {
		return nil, rpcError(domain.ErrInvalid)
	}
	t, e := b.Store.Create(ctx, r.Name, int(r.Partitions))
	if e != nil {
		return nil, rpcError(e)
	}
	return wireTopic(t), nil
}
func (b *Broker) ListTopics(ctx context.Context, _ *pb.Empty) (*pb.Topics, error) {
	ts, e := b.Store.List(ctx)
	if e != nil {
		return nil, rpcError(e)
	}
	out := new(pb.Topics)
	for _, t := range ts {
		out.Topics = append(out.Topics, wireTopic(t))
	}
	return out, nil
}
func (b *Broker) DescribeTopic(ctx context.Context, r *pb.TopicRequest) (*pb.Topic, error) {
	t, e := b.Store.Describe(ctx, r.GetTopic())
	if e != nil {
		return nil, rpcError(e)
	}
	return wireTopic(t), nil
}
func (b *Broker) owned(ctx context.Context, topic string, p int) (*logstore.Log, error) {
	t, e := b.Store.Describe(ctx, topic)
	if e != nil {
		return nil, e
	}
	if p < 0 || p >= len(t.Partitions) {
		return nil, domain.ErrInvalid
	}
	owner := t.Partitions[p].Owner
	if owner.ID != b.C.ID {
		_ = grpc.SetTrailer(ctx, gm.Pairs("owner-address", owner.Address, "owner-id", strconv.Itoa(owner.ID)))
		return nil, status.Errorf(codes.FailedPrecondition, "partition owned by broker %d at %s", owner.ID, owner.Address)
	}
	return b.log(topic, p)
}
func (b *Broker) validatePublish(r *pb.PublishRequest, internal bool) error {
	headroom := 8 << 10
	if internal {
		headroom = 12 << 10
	}
	if r == nil || !domain.ValidTopicName(r.Topic) || len(r.Payload) > b.C.MaxMessage || len(r.Key) > 4096 || proto.Size(r) > b.C.MaxMessage+headroom {
		return domain.ErrInvalid
	}
	userHeaders := 0
	for k, v := range r.Headers {
		if len(k) == 0 || len(k) > 128 || len(v) > 1024 {
			return domain.ErrInvalid
		}
		if strings.HasPrefix(strings.ToLower(k), "sf.") {
			if !internal {
				return domain.ErrInvalid
			}
			switch k {
			case "sf.original_id", "sf.original_topic", "sf.original_partition", "sf.original_offset", "sf.attempt", "sf.last_error", "sf.retry_job":
			default:
				return domain.ErrInvalid
			}
		} else {
			userHeaders++
		}
	}
	if userHeaders > 32 {
		return domain.ErrInvalid
	}
	return nil
}
func newID() (string, error) {
	var id [16]byte
	_, e := rand.Read(id[:])
	return hex.EncodeToString(id[:]), e
}
func (b *Broker) Publish(ctx context.Context, r *pb.PublishRequest) (*pb.Message, error) {
	return b.publish(ctx, r, false)
}

// Only the retry worker may supply trusted provenance. No RPC or batch path
// can opt into this private mode, which preserves the full user header allowance.
func (b *Broker) publish(ctx context.Context, r *pb.PublishRequest, internal bool) (out *pb.Message, err error) {
	defer func() {
		if err != nil {
			b.Metrics.PublishErrors.Add(1)
		}
	}()
	if e := b.validatePublish(r, internal); e != nil {
		return nil, rpcError(e)
	}
	if e := ctx.Err(); e != nil {
		return nil, rpcError(e)
	}
	t, e := b.Store.Describe(ctx, r.Topic)
	if e != nil {
		return nil, rpcError(e)
	}
	var p int
	if r.Partition != nil {
		p = int(*r.Partition)
	} else if len(r.Key) > 0 {
		p = domain.KeyPartition(r.Key, len(t.Partitions))
	} else {
		p = int((b.rr.Add(1) - 1) % uint64(len(t.Partitions)))
	}
	l, e := b.owned(ctx, r.Topic, p)
	if e != nil {
		return nil, rpcError(e)
	}
	id, e := newID()
	if e != nil {
		return nil, rpcError(e)
	}
	m, e := l.Append(&pb.Message{Id: id, Topic: r.Topic, Partition: int32(p), TimestampUnixNano: time.Now().UnixNano(), Key: r.Key, Payload: r.Payload, Headers: r.Headers})
	if e != nil {
		return nil, rpcError(e)
	}
	b.Metrics.Published.Add(1)
	b.Metrics.Bytes.Add(int64(proto.Size(m) + 24))
	return m, nil
}

// Batch is ordered, bounded and deliberately non-atomic. On error an already
// acknowledged prefix may exist; inspect offsets before retrying the whole batch.
func (b *Broker) PublishBatch(ctx context.Context, r *pb.PublishBatchRequest) (*pb.Messages, error) {
	if r == nil || len(r.Messages) == 0 || len(r.Messages) > b.C.MaxBatch || proto.Size(r) > domain.MaxRequestBytes {
		return nil, rpcError(domain.ErrInvalid)
	}
	for _, m := range r.Messages {
		if e := b.validatePublish(m, false); e != nil {
			return nil, rpcError(e)
		}
	}
	out := new(pb.Messages)
	for _, m := range r.Messages {
		v, e := b.Publish(ctx, m)
		if e != nil {
			return nil, e
		}
		out.Messages = append(out.Messages, v)
	}
	return out, nil
}
func (b *Broker) Fetch(ctx context.Context, r *pb.FetchRequest) (out *pb.FetchResponse, err error) {
	defer func() {
		if err != nil {
			b.Metrics.FetchErrors.Add(1)
		}
	}()
	if r == nil || r.Limit < 1 || int(r.Limit) > b.C.MaxFetch {
		return nil, rpcError(domain.ErrInvalid)
	}
	l, e := b.owned(ctx, r.Topic, int(r.Partition))
	if e != nil {
		return nil, rpcError(e)
	}
	if r.Group != "" {
		if e = b.Store.Check(ctx, r.Group, r.Topic, r.Member, r.Generation, int(r.Partition)); e != nil {
			return nil, rpcError(e)
		}
	}
	earliest, next := l.Bounds()
	offset := r.Offset
	switch r.Start {
	case pb.StartPosition_EXPLICIT:
	case pb.StartPosition_EARLIEST:
		offset = earliest
	case pb.StartPosition_LATEST:
		offset = next
	case pb.StartPosition_COMMITTED:
		if r.Group == "" {
			return nil, rpcError(domain.ErrInvalid)
		}
		o, e := b.Store.Offset(ctx, r.Group, r.Topic, int(r.Partition))
		if e != nil {
			return nil, rpcError(e)
		}
		if o.Exists {
			offset = o.Next
		} else {
			offset = earliest
		}
	default:
		return nil, rpcError(domain.ErrInvalid)
	}
	msgs, e := l.Read(offset, int(r.Limit), domain.FetchBytes)
	if e != nil {
		return nil, rpcError(e)
	}
	b.Metrics.Fetched.Add(int64(len(msgs)))
	return &pb.FetchResponse{Messages: msgs, EarliestOffset: earliest, NextOffset: next}, nil
}
func (b *Broker) CommitOffset(ctx context.Context, r *pb.CommitRequest) (*pb.Empty, error) {
	if r == nil {
		return nil, rpcError(domain.ErrInvalid)
	}
	l, e := b.owned(ctx, r.Topic, int(r.Partition))
	if e != nil {
		return nil, rpcError(e)
	}
	earliest, next := l.Bounds()
	if r.NextOffset < earliest || r.NextOffset > next {
		return nil, rpcError(domain.ErrOffset)
	}
	if e = b.Store.Commit(ctx, r.Group, r.Topic, r.Member, r.Generation, int(r.Partition), r.NextOffset); e != nil {
		return nil, rpcError(e)
	}
	b.Metrics.Commits.Add(1)
	return &pb.Empty{}, nil
}
func (b *Broker) GetCommittedOffset(ctx context.Context, r *pb.OffsetRequest) (*pb.OffsetResponse, error) {
	if r == nil {
		return nil, rpcError(domain.ErrInvalid)
	}
	o, e := b.Store.Offset(ctx, r.Group, r.Topic, int(r.Partition))
	if e != nil {
		return nil, rpcError(e)
	}
	return &pb.OffsetResponse{NextOffset: o.Next, Exists: o.Exists}, nil
}
func (b *Broker) membership(ctx context.Context, r *pb.MemberRequest, action string) (*pb.Assignment, error) {
	if r == nil {
		return nil, rpcError(domain.ErrInvalid)
	}
	a, e := b.Store.Membership(ctx, r.Group, r.Topic, r.Member, action)
	if e != nil {
		return nil, rpcError(e)
	}
	out := &pb.Assignment{Generation: a.Generation, ActiveMembers: a.Members}
	for _, p := range a.Partitions {
		out.Partitions = append(out.Partitions, &pb.Partition{Id: int32(p.ID), OwnerId: int32(p.Owner.ID), OwnerAddress: p.Owner.Address})
	}
	return out, nil
}
func (b *Broker) JoinConsumerGroup(ctx context.Context, r *pb.MemberRequest) (*pb.Assignment, error) {
	return b.membership(ctx, r, "join")
}
func (b *Broker) Heartbeat(ctx context.Context, r *pb.MemberRequest) (*pb.Assignment, error) {
	return b.membership(ctx, r, "heartbeat")
}
func (b *Broker) LeaveConsumerGroup(ctx context.Context, r *pb.MemberRequest) (*pb.Empty, error) {
	_, e := b.membership(ctx, r, "leave")
	return &pb.Empty{}, e
}
func (b *Broker) FailMessage(ctx context.Context, r *pb.FailRequest) (*pb.RetryResponse, error) {
	// Serialize reading/scheduling a source with retention's pin snapshot and
	// removal; ordinary appends/fetches continue under their partition locks.
	b.retentionMu.Lock()
	defer b.retentionMu.Unlock()
	if r == nil {
		return nil, rpcError(domain.ErrInvalid)
	}
	l, e := b.owned(ctx, r.Topic, int(r.Partition))
	if e != nil {
		return nil, rpcError(e)
	}
	msgs, e := l.Read(r.Offset, 1, b.C.MaxMessage+domain.RecordHeadroom)
	if e != nil {
		return nil, rpcError(e)
	}
	if len(msgs) != 1 {
		return nil, rpcError(domain.ErrOffset)
	}
	m := msgs[0]
	root := r.Topic
	attempt := 1
	if v := m.Headers["sf.original_topic"]; v != "" {
		root = v
		n, e := strconv.Atoi(m.Headers["sf.attempt"])
		if e != nil || n < 0 || n > 30 {
			return nil, rpcError(domain.ErrInvalid)
		}
		attempt = n + 1
	}
	if !domain.ValidTopicName(root) || len(root) > 122 {
		return nil, rpcError(domain.ErrInvalid)
	}
	t, e := b.Store.Describe(ctx, root)
	if e != nil {
		return nil, rpcError(e)
	}
	target := root + ".retry"
	due := time.Now().Add(retry.Delay(attempt, b.C.RetryBase, b.C.RetryMax))
	if attempt > b.C.MaxRetries {
		target = root + ".dlq"
		due = time.Now()
	}
	_, e = b.Store.Create(ctx, target, len(t.Partitions))
	if e != nil && !errors.Is(e, domain.ErrExists) {
		return nil, rpcError(e)
	}
	dest, e := b.Store.Describe(ctx, target)
	if e != nil {
		return nil, rpcError(e)
	}
	if len(dest.Partitions) != len(t.Partitions) {
		return nil, status.Error(codes.FailedPrecondition, "retry topic partition count differs")
	}
	id, e := newID()
	if e != nil {
		return nil, rpcError(e)
	}
	j, e := b.Store.Schedule(ctx, domain.RetryRecord{ID: id, Topic: r.Topic, Partition: int(r.Partition), Offset: r.Offset, Attempt: attempt, Due: due.UnixNano(), Error: metadata.SafeError(r.Error)}, target)
	if e != nil {
		return nil, rpcError(e)
	}
	return &pb.RetryResponse{TargetTopic: target, Attempt: int32(j.Attempt), DueUnixNano: j.Due, JobId: j.ID}, nil
}
func (b *Broker) RunRetries(ctx context.Context) {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			workCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			if e := b.ProcessRetries(workCtx, time.Now()); e != nil && ctx.Err() == nil {
				slog.Error("retry dispatch failed", "broker_id", b.C.ID, "error", e)
			}
			cancel()
		}
	}
}

// One retry loop per broker. Payload append precedes completing the durable job;
// a crash between these steps can duplicate delivery, preserving at-least-once.
func (b *Broker) ProcessRetries(ctx context.Context, now time.Time) error {
	jobs, e := b.Store.Due(ctx, b.C.ID, now, 32)
	if e != nil {
		return e
	}
	var failures []error
	for _, j := range jobs {
		if e := ctx.Err(); e != nil {
			return errors.Join(append(failures, e)...)
		}
		if e := b.dispatchRetry(ctx, j); e != nil {
			failures = append(failures, fmt.Errorf("retry job %s: %w", j.ID, e))
		}
	}
	return errors.Join(failures...)
}

func (b *Broker) dispatchRetry(ctx context.Context, j domain.RetryRecord) error {
	l, e := b.owned(ctx, j.Topic, j.Partition)
	if e != nil {
		return e
	}
	msgs, e := l.Read(j.Offset, 1, b.C.MaxMessage+domain.RecordHeadroom)
	if e != nil {
		return e
	}
	if len(msgs) != 1 {
		return domain.ErrOffset
	}
	m := msgs[0]
	root := m.Headers["sf.original_topic"]
	if root == "" {
		root = m.Topic
	}
	headers := make(map[string]string)
	for k, v := range m.Headers {
		headers[k] = v
	}
	if headers["sf.original_id"] == "" {
		headers["sf.original_id"] = m.Id
		headers["sf.original_topic"] = m.Topic
		headers["sf.original_partition"] = strconv.Itoa(int(m.Partition))
		headers["sf.original_offset"] = strconv.FormatInt(m.Offset, 10)
	}
	headers["sf.attempt"] = strconv.Itoa(j.Attempt)
	headers["sf.last_error"] = j.Error
	headers["sf.retry_job"] = j.ID
	target := root + ".retry"
	if j.Attempt > b.C.MaxRetries {
		target = root + ".dlq"
	}
	p := int32(j.Partition)
	_, e = b.publish(ctx, &pb.PublishRequest{Topic: target, Partition: &p, Key: m.Key, Payload: m.Payload, Headers: headers}, true)
	if e != nil {
		return e
	}
	if e = b.Store.Complete(ctx, j.ID); e != nil {
		return e
	}
	if j.Attempt > b.C.MaxRetries {
		b.Metrics.DLQ.Add(1)
	} else {
		b.Metrics.Retries.Add(1)
	}
	return nil
}
