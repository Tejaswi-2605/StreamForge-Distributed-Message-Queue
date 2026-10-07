package integration

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"net"
	"net/url"
	"os"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	pb "streamforge/api/gen"
	"streamforge/internal/broker"
	"streamforge/internal/client"
	"streamforge/internal/config"
	"streamforge/internal/domain"
	"streamforge/internal/metadata"
	"streamforge/internal/server"
)

type cluster struct {
	ctx     context.Context
	brokers []*broker.Broker
	servers []*server.Server
	stores  []*metadata.Store
	configs []config.Config
	client  *client.Client
	suffix  string
}

func freeAddr(t *testing.T) string {
	t.Helper()
	l, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	a := l.Addr().String()
	l.Close()
	return a
}
func newCluster(t *testing.T, n int) *cluster {
	t.Helper()
	if os.Getenv("STREAMFORGE_INTEGRATION") != "1" {
		t.Skip("set STREAMFORGE_INTEGRATION=1 with real PostgreSQL and Redis")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)
	dsn := os.Getenv("POSTGRES_DSN")
	if dsn == "" {
		dsn = "postgres://streamforge@127.0.0.1:55432/streamforge?sslmode=disable"
	}
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "127.0.0.1:56379"
	}
	admin, e := pgxpool.New(ctx, dsn)
	if e != nil {
		t.Fatal(e)
	}
	suffix := fmt.Sprintf("test_%d", time.Now().UnixNano())
	ident := pgx.Identifier{suffix}.Sanitize()
	if _, e = admin.Exec(ctx, "CREATE SCHEMA "+ident); e != nil {
		admin.Close()
		t.Fatal(e)
	}
	t.Cleanup(func() {
		clean, c := context.WithTimeout(context.Background(), 10*time.Second)
		defer c()
		_, _ = admin.Exec(clean, "DROP SCHEMA "+ident+" CASCADE")
		admin.Close()
	})
	u, e := url.Parse(dsn)
	if e != nil {
		t.Fatal(e)
	}
	q := u.Query()
	q.Set("search_path", suffix)
	u.RawQuery = q.Encode()
	dsn = u.String()
	c := &cluster{ctx: ctx, suffix: suffix}
	var owners []domain.Broker
	for i := 0; i < n; i++ {
		owners = append(owners, domain.Broker{ID: i, Address: freeAddr(t)})
	}
	defer func() {
		t.Cleanup(func() {
			if c.client != nil {
				c.client.Close()
			}
			for _, s := range c.servers {
				if s != nil {
					s.Stop()
				}
			}
			for _, s := range c.stores {
				s.Close()
			}
		})
	}()
	for i := 0; i < n; i++ {
		conf := config.Default()
		conf.ID = i
		conf.Listen = owners[i].Address
		conf.Metrics = freeAddr(t)
		conf.Brokers = owners
		conf.DataDir = t.TempDir()
		conf.PostgresDSN = dsn
		conf.RedisAddr = redisAddr
		conf.LeaseTTL = 30 * time.Second
		conf.SegmentBytes = 512
		conf.RetryBase = 10 * time.Millisecond
		conf.RetryMax = 20 * time.Millisecond
		conf.MaxRetries = 2
		s, e := metadata.Open(ctx, dsn, redisAddr, conf.LeaseTTL, owners)
		if e != nil {
			t.Fatal(e)
		}
		c.stores = append(c.stores, s)
		b, e := broker.New(ctx, conf, s)
		if e != nil {
			t.Fatal(e)
		}
		c.brokers = append(c.brokers, b)
		srv, e := server.Start(ctx, b)
		if e != nil {
			t.Fatal(e)
		}
		c.servers = append(c.servers, srv)
		c.configs = append(c.configs, conf)
	}
	c.client, e = client.Dial(owners[0].Address)
	if e != nil {
		t.Fatal(e)
	}
	return c
}
func (c *cluster) topic(t *testing.T, n int) string {
	t.Helper()
	name := fmt.Sprintf("topic.%s", c.suffix)
	if _, e := c.client.Bootstrap.CreateTopic(c.ctx, &pb.CreateTopicRequest{Name: name, Partitions: int32(n)}); e != nil {
		t.Fatal(e)
	}
	return name
}
func (c *cluster) restart(t *testing.T, id int) {
	t.Helper()
	c.servers[id].Stop()
	b, e := broker.New(c.ctx, c.configs[id], c.stores[id])
	if e != nil {
		t.Fatal(e)
	}
	s, e := server.Start(c.ctx, b)
	if e != nil {
		t.Fatal(e)
	}
	c.brokers[id] = b
	c.servers[id] = s
}
func TestProducerConsumerPersistenceRestart(t *testing.T) {
	c := newCluster(t, 1)
	topic := c.topic(t, 3)
	for i := 0; i < 30; i++ {
		p := int32(i % 3)
		m, e := c.client.Publish(c.ctx, &pb.PublishRequest{Topic: topic, Partition: &p, Payload: []byte(fmt.Sprint(i))})
		if e != nil || m.Offset != int64(i/3) {
			t.Fatalf("%v %v", m, e)
		}
	}
	c.restart(t, 0)
	for p := int32(0); p < 3; p++ {
		f, e := c.client.Fetch(c.ctx, &pb.FetchRequest{Topic: topic, Partition: p, Limit: 100})
		if e != nil || len(f.Messages) != 10 {
			t.Fatalf("recovery: %v %v", f, e)
		}
		for i, m := range f.Messages {
			if m.Offset != int64(i) || string(m.Payload) != fmt.Sprint(i*3+int(p)) {
				t.Fatal("recovery data mismatch")
			}
		}
		m, e := c.client.Publish(c.ctx, &pb.PublishRequest{Topic: topic, Partition: &p, Payload: []byte("restart")})
		if e != nil || m.Offset != 10 {
			t.Fatalf("continuity %v %v", m, e)
		}
	}
	t.Log("3 partitions recovered 30 payloads; each continued at offset 10")
}
func TestMultiBrokerRouting(t *testing.T) {
	c := newCluster(t, 3)
	topic := c.topic(t, 6)
	for p := int32(0); p < 6; p++ {
		if p%3 != 0 {
			_, e := c.client.Bootstrap.Publish(c.ctx, &pb.PublishRequest{Topic: topic, Partition: &p, Payload: []byte("wrong")})
			if status.Code(e) != codes.FailedPrecondition {
				t.Fatal(e)
			}
		}
		m, e := c.client.Publish(c.ctx, &pb.PublishRequest{Topic: topic, Partition: &p, Payload: []byte(fmt.Sprint(p))})
		if e != nil || m.Partition != p || m.Offset != 0 {
			t.Fatal(m, e)
		}
		f, e := c.client.Fetch(c.ctx, &pb.FetchRequest{Topic: topic, Partition: p, Limit: 1})
		if e != nil || len(f.Messages) != 1 || !bytes.Equal(f.Messages[0].Payload, m.Payload) {
			t.Fatal(f, e)
		}
	}
	for i := 0; i < 3; i++ {
		c.restart(t, i)
	}
	for p := int32(0); p < 6; p++ {
		m, e := c.client.Publish(c.ctx, &pb.PublishRequest{Topic: topic, Partition: &p, Payload: []byte("after")})
		if e != nil || m.Offset != 1 {
			t.Fatal(m, e)
		}
	}
	t.Log("3 network brokers; owners 0,1,2,0,1,2; redirect and routed read/write/restart passed")
}
func TestGroupsRebalanceCommitResume(t *testing.T) {
	c := newCluster(t, 3)
	topic := c.topic(t, 4)
	for p := int32(0); p < 4; p++ {
		if _, e := c.client.Publish(c.ctx, &pb.PublishRequest{Topic: topic, Partition: &p, Payload: []byte("hello")}); e != nil {
			t.Fatal(e)
		}
	}
	group := c.suffix
	rA := &pb.MemberRequest{Topic: topic, Group: group, Member: "A"}
	rB := &pb.MemberRequest{Topic: topic, Group: group, Member: "B"}
	old, e := c.client.Bootstrap.JoinConsumerGroup(c.ctx, rA)
	if e != nil {
		t.Fatal(e)
	}
	b, e := c.client.Bootstrap.JoinConsumerGroup(c.ctx, rB)
	if e != nil {
		t.Fatal(e)
	}
	a, e := c.client.Bootstrap.Heartbeat(c.ctx, rA)
	if e != nil {
		t.Fatal(e)
	}
	if fmt.Sprint(partIDs(a)) != "[0 2]" || fmt.Sprint(partIDs(b)) != "[1 3]" {
		t.Fatal(a, b)
	}
	if e = c.client.Commit(c.ctx, &pb.CommitRequest{Topic: topic, Group: group, Member: "A", Partition: 0, Generation: old.Generation, NextOffset: 1}); status.Code(e) != codes.FailedPrecondition {
		t.Fatal("stale generation accepted", e)
	}
	for _, as := range []*pb.Assignment{a, b} {
		m := "A"
		if as == b {
			m = "B"
		}
		for _, p := range as.Partitions {
			if e = c.client.Commit(c.ctx, &pb.CommitRequest{Topic: topic, Group: group, Member: m, Partition: p.Id, Generation: as.Generation, NextOffset: 1}); e != nil {
				t.Fatal(e)
			}
		}
	}
	if e = c.client.Commit(c.ctx, &pb.CommitRequest{Topic: topic, Group: group, Member: "A", Partition: 1, Generation: a.Generation, NextOffset: 1}); status.Code(e) != codes.FailedPrecondition {
		t.Fatal("wrong owner accepted", e)
	}
	_, e = c.client.Bootstrap.LeaveConsumerGroup(c.ctx, rB)
	if e != nil {
		t.Fatal(e)
	}
	a, e = c.client.Bootstrap.Heartbeat(c.ctx, rA)
	if e != nil || len(a.Partitions) != 4 {
		t.Fatal(a, e)
	}
	for i := 0; i < 3; i++ {
		c.restart(t, i)
	}
	for _, p := range a.Partitions {
		f, e := c.client.Fetch(c.ctx, &pb.FetchRequest{Topic: topic, Partition: p.Id, Group: group, Member: "A", Generation: a.Generation, Start: pb.StartPosition_COMMITTED, Limit: 10})
		if e != nil || len(f.Messages) != 0 {
			t.Fatal("resume", f, e)
		}
	}
	other := &pb.MemberRequest{Topic: topic, Group: group + ".other", Member: "C"}
	o, e := c.client.Bootstrap.JoinConsumerGroup(c.ctx, other)
	if e != nil {
		t.Fatal(e)
	}
	f, e := c.client.Fetch(c.ctx, &pb.FetchRequest{Topic: topic, Partition: 0, Group: other.Group, Member: "C", Generation: o.Generation, Start: pb.StartPosition_COMMITTED, Limit: 10})
	if e != nil || len(f.Messages) != 1 {
		t.Fatal("independent group", f, e)
	}
	t.Log("A=[0 2], B=[1 3]; leave -> A=[0 1 2 3]; persisted next=1; independent group reads offset 0")
}
func partIDs(a *pb.Assignment) []int32 {
	var out []int32
	for _, p := range a.Partitions {
		out = append(out, p.Id)
	}
	return out
}
func TestRedisLeaseExpiry(t *testing.T) {
	c := newCluster(t, 1)
	c.stores[0].TTL = time.Second
	topic := c.topic(t, 3)
	group := c.suffix
	r := &pb.MemberRequest{Topic: topic, Group: group, Member: "expired"}
	a, e := c.client.Bootstrap.JoinConsumerGroup(c.ctx, r)
	if e != nil {
		t.Fatal(e)
	}
	time.Sleep(1100 * time.Millisecond)
	if _, e = c.client.Bootstrap.Heartbeat(c.ctx, r); status.Code(e) != codes.FailedPrecondition {
		t.Fatal("expired heartbeat", e)
	}
	r.Member = "replacement"
	b, e := c.client.Bootstrap.JoinConsumerGroup(c.ctx, r)
	if e != nil || len(b.ActiveMembers) != 1 || b.Generation <= a.Generation || len(b.Partitions) != 3 {
		t.Fatal(b, e)
	}
	t.Log("actual Redis TTL expired; replacement exclusively owns all 3 partitions")
}
func TestRedisRestartLossFencesMembership(t *testing.T) {
	c := newCluster(t, 1)
	topic := c.topic(t, 1)
	r := &pb.MemberRequest{Topic: topic, Group: c.suffix, Member: "A"}
	a, e := c.client.Bootstrap.JoinConsumerGroup(c.ctx, r)
	if e != nil {
		t.Fatal(e)
	}
	key := "sf:lease:" + r.Group + ":" + topic + ":" + r.Member
	if e = c.stores[0].Redis.Del(c.ctx, key).Err(); e != nil {
		t.Fatal(e)
	}
	if _, e = c.client.Bootstrap.Heartbeat(c.ctx, r); status.Code(e) != codes.FailedPrecondition {
		t.Fatal(e)
	}
	b, e := c.client.Bootstrap.JoinConsumerGroup(c.ctx, r)
	if e != nil || b.Generation <= a.Generation {
		t.Fatal(b, e)
	}
	t.Log("Redis lease-loss simulation fenced old generation; rejoin increased generation")
}
func TestRetryDLQAndRestart(t *testing.T) {
	c := newCluster(t, 3)
	topic := c.topic(t, 3)
	p := int32(1)
	source, e := c.client.Publish(c.ctx, &pb.PublishRequest{Topic: topic, Partition: &p, Payload: []byte("poison")})
	if e != nil {
		t.Fatal(e)
	}
	current := source
	for attempt := 1; attempt <= 3; attempt++ {
		r, e := c.client.Fail(c.ctx, &pb.FailRequest{Topic: current.Topic, Partition: p, Offset: current.Offset, Error: "safe demo error"})
		if e != nil {
			t.Fatal(e)
		}
		again, e := c.client.Fail(c.ctx, &pb.FailRequest{Topic: current.Topic, Partition: p, Offset: current.Offset, Error: "again"})
		if e != nil || r.JobId != again.JobId {
			t.Fatal("schedule not idempotent", r, again, e)
		}
		if attempt == 1 {
			c.restart(t, 1)
		}
		deadline := time.Now().Add(5 * time.Second)
		found := false
		for time.Now().Before(deadline) {
			f, e := c.client.Fetch(c.ctx, &pb.FetchRequest{Topic: r.TargetTopic, Partition: p, Limit: 100})
			if e == nil {
				for _, m := range f.Messages {
					if m.Headers["sf.attempt"] == fmt.Sprint(attempt) {
						current = m
						found = true
						break
					}
				}
			}
			if found {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		if !found {
			t.Fatal("retry not dispatched", attempt)
		}
		if !bytes.Equal(current.Payload, source.Payload) || current.Headers["sf.original_id"] != source.Id || current.Headers["sf.original_offset"] != "0" {
			t.Fatal("provenance lost")
		}
		if attempt == 3 && r.TargetTopic != topic+".dlq" {
			t.Fatal("missing DLQ")
		}
	}
	t.Log("2 delayed retry deliveries then DLQ; job recovery and original provenance verified")
}
func TestAPIValidationAndBatch(t *testing.T) {
	c := newCluster(t, 1)
	topic := c.topic(t, 3)
	for _, r := range []*pb.CreateTopicRequest{{Name: ""}, {Name: "../bad", Partitions: 3}, {Name: "x", Partitions: 0}, {Name: "y", Partitions: -1}, {Name: "z", Partitions: 1025}} {
		if _, e := c.client.Bootstrap.CreateTopic(c.ctx, r); status.Code(e) != codes.InvalidArgument {
			t.Fatal(r, e)
		}
	}
	if _, e := c.client.Bootstrap.CreateTopic(c.ctx, &pb.CreateTopicRequest{Name: topic, Partitions: 3}); status.Code(e) != codes.AlreadyExists {
		t.Fatal(e)
	}
	for _, r := range []*pb.PublishRequest{{Topic: "missing", Payload: []byte("x")}, {Topic: topic, Payload: make([]byte, (1<<20)+1)}, {Topic: topic, Partition: protoInt(3)}} {
		if _, e := c.client.Bootstrap.Publish(c.ctx, r); e == nil {
			t.Fatal("invalid publish accepted")
		}
	}
	batch := &pb.PublishBatchRequest{Messages: []*pb.PublishRequest{{Topic: topic, Partition: protoInt(0), Payload: []byte("a")}, {Topic: topic, Partition: protoInt(0), Payload: []byte("b")}}}
	v, e := c.client.Bootstrap.PublishBatch(c.ctx, batch)
	if e != nil || len(v.Messages) != 2 || v.Messages[1].Offset != 1 {
		t.Fatal(v, e)
	}
	for _, r := range []*pb.FetchRequest{{Topic: topic, Limit: 0}, {Topic: topic, Limit: 1001}, {Topic: topic, Partition: 3, Limit: 1}, {Topic: topic, Offset: 3, Limit: 1}} {
		if _, e := c.client.Fetch(c.ctx, r); e == nil {
			t.Fatal("invalid fetch accepted")
		}
	}
	for _, start := range []pb.StartPosition{pb.StartPosition_EARLIEST, pb.StartPosition_LATEST} {
		f, e := c.client.Fetch(c.ctx, &pb.FetchRequest{Topic: topic, Limit: 10, Start: start})
		if e != nil {
			t.Fatal(e)
		}
		want := 2
		if start == pb.StartPosition_LATEST {
			want = 0
		}
		if len(f.Messages) != want {
			t.Fatal(f)
		}
	}
}
func protoInt(v int32) *int32 { return &v }
func TestConcurrentPublishFetch(t *testing.T) {
	c := newCluster(t, 3)
	topic := c.topic(t, 3)
	var wg sync.WaitGroup
	errs := make(chan error, 12)
	for worker := 0; worker < 9; worker++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			p := int32(w % 3)
			for i := 0; i < 100; i++ {
				if _, e := c.client.Publish(c.ctx, &pb.PublishRequest{Topic: topic, Partition: &p, Payload: []byte(fmt.Sprintf("%d:%d", w, i))}); e != nil {
					errs <- e
					return
				}
			}
		}(worker)
	}
	for p := int32(0); p < 3; p++ {
		wg.Add(1)
		go func(p int32) {
			defer wg.Done()
			for i := 0; i < 25; i++ {
				if _, e := c.client.Fetch(c.ctx, &pb.FetchRequest{Topic: topic, Partition: p, Limit: 100}); e != nil {
					errs <- e
					return
				}
			}
		}(p)
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Fatal(e)
	}
	for p := int32(0); p < 3; p++ {
		off := int64(0)
		seen := map[string]bool{}
		for off < 300 {
			f, e := c.client.Fetch(c.ctx, &pb.FetchRequest{Topic: topic, Partition: p, Offset: off, Limit: 100})
			if e != nil {
				t.Fatal(e)
			}
			for _, m := range f.Messages {
				if m.Offset != off || seen[string(m.Payload)] {
					t.Fatal("duplicate or unordered")
				}
				seen[string(m.Payload)] = true
				off++
			}
		}
	}
	t.Log("900 concurrent publishes plus 75 fetches; unique contiguous offsets and immutable payloads")
}
func TestDeterministicStress(t *testing.T) {
	c := newCluster(t, 3)
	topic := c.topic(t, 6)
	rng := rand.New(rand.NewSource(73461))
	expected := make([][][]byte, 6)
	group := c.suffix
	member := &pb.MemberRequest{Topic: topic, Group: group, Member: "stable"}
	a, e := c.client.Bootstrap.JoinConsumerGroup(c.ctx, member)
	if e != nil {
		t.Fatal(e)
	}
	commits := make([]int64, 6)
	for op := 0; op < 2500; op++ {
		p := int32(rng.Intn(6))
		switch rng.Intn(7) {
		case 0, 1:
			payload := []byte(fmt.Sprintf("seed-73461/op-%d", op))
			m, e := c.client.Publish(c.ctx, &pb.PublishRequest{Topic: topic, Partition: &p, Payload: payload})
			if e != nil || m.Offset != int64(len(expected[p])) {
				t.Fatal(m, e)
			}
			expected[p] = append(expected[p], payload)
		case 2:
			off := int64(0)
			if len(expected[p]) > 0 {
				off = int64(rng.Intn(len(expected[p])))
			}
			f, e := c.client.Fetch(c.ctx, &pb.FetchRequest{Topic: topic, Partition: p, Offset: off, Limit: 10})
			if e != nil {
				t.Fatal(e)
			}
			for i, m := range f.Messages {
				if m.Offset != off+int64(i) || !bytes.Equal(m.Payload, expected[p][m.Offset]) {
					t.Fatal("readback invariant")
				}
			}
		case 3:
			next := int64(len(expected[p]))
			if e = c.client.Commit(c.ctx, &pb.CommitRequest{Topic: topic, Partition: p, Group: group, Member: member.Member, Generation: a.Generation, NextOffset: next}); e != nil {
				t.Fatal(e)
			}
			commits[p] = next
		case 4:
			a, e = c.client.Bootstrap.Heartbeat(c.ctx, member)
			if e != nil {
				t.Fatal(e)
			}
		case 5:
			api, e := c.client.Owner(c.ctx, topic, p)
			if e != nil {
				t.Fatal(e)
			}
			payloads := [][]byte{[]byte(fmt.Sprintf("batch-%d-a", op)), []byte(fmt.Sprintf("batch-%d-b", op))}
			result, e := api.PublishBatch(c.ctx, &pb.PublishBatchRequest{Messages: []*pb.PublishRequest{{Topic: topic, Partition: &p, Payload: payloads[0]}, {Topic: topic, Partition: &p, Payload: payloads[1]}}})
			if e != nil {
				t.Fatal(e)
			}
			for i, m := range result.Messages {
				if m.Offset != int64(len(expected[p])) {
					t.Fatal("batch offset invariant")
				}
				expected[p] = append(expected[p], payloads[i])
			}
		case 6:
			r := &pb.MemberRequest{Topic: topic, Group: group, Member: "temporary"}
			b, e := c.client.Bootstrap.JoinConsumerGroup(c.ctx, r)
			if e != nil {
				t.Fatal(e)
			}
			fresh, e := c.client.Bootstrap.Heartbeat(c.ctx, member)
			if e != nil {
				t.Fatal(e)
			}
			seen := map[int32]bool{}
			for _, as := range []*pb.Assignment{b, fresh} {
				for _, part := range as.Partitions {
					if seen[part.Id] {
						t.Fatal("overlapping assignment")
					}
					seen[part.Id] = true
				}
			}
			if len(seen) != 6 {
				t.Fatal("missing assignment")
			}
			if _, e = c.client.Bootstrap.LeaveConsumerGroup(c.ctx, r); e != nil {
				t.Fatal(e)
			}
			a, e = c.client.Bootstrap.Heartbeat(c.ctx, member)
			if e != nil {
				t.Fatal(e)
			}
		}
		if op%250 == 0 {
			a, e = c.client.Bootstrap.Heartbeat(c.ctx, member)
			if e != nil {
				t.Fatal(e)
			}
		}
	}
	for i := 0; i < 3; i++ {
		c.restart(t, i)
	}
	a, e = c.client.Bootstrap.Heartbeat(c.ctx, member)
	if e != nil {
		t.Fatal(e)
	}
	total := 0
	for p := int32(0); p < 6; p++ {
		for off := int64(0); off < int64(len(expected[p])); {
			f, e := c.client.Fetch(c.ctx, &pb.FetchRequest{Topic: topic, Partition: p, Offset: off, Limit: 100})
			if e != nil || len(f.Messages) == 0 {
				t.Fatal(e)
			}
			for _, m := range f.Messages {
				if m.Offset != off || !bytes.Equal(m.Payload, expected[p][off]) {
					t.Fatal("stress recovery invariant")
				}
				off++
				total++
			}
		}
		o, e := c.client.Bootstrap.GetCommittedOffset(c.ctx, &pb.OffsetRequest{Topic: topic, Partition: p, Group: group})
		if e != nil || o.NextOffset != commits[p] {
			t.Fatal("commit invariant", o, e)
		}
	}
	sort.Slice(a.Partitions, func(i, j int) bool { return a.Partitions[i].Id < a.Partitions[j].Id })
	if len(a.Partitions) != 6 {
		t.Fatal("assignment invariant")
	}
	t.Logf("seed=73461 operations=2500 recovered_messages=%d; offsets, payloads, commits, ownership verified", total)
}
