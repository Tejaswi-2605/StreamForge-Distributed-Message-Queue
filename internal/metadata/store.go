package metadata

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"streamforge/internal/domain"
	"streamforge/migrations"
)

type Store struct {
	Pool  *pgxpool.Pool
	Redis *redis.Client
	TTL   time.Duration
}

func Open(ctx context.Context, dsn, redisAddr string, ttl time.Duration, brokers []domain.Broker) (*Store, error) {
	p, e := pgxpool.New(ctx, dsn)
	if e != nil {
		return nil, e
	}
	r := redis.NewClient(&redis.Options{Addr: redisAddr, ReadTimeout: 2 * time.Second, WriteTimeout: 2 * time.Second, MaxRetries: 0})
	s := &Store{Pool: p, Redis: r, TTL: ttl}
	if e = p.Ping(ctx); e != nil {
		s.Close()
		return nil, e
	}
	if e = r.Ping(ctx).Err(); e != nil {
		s.Close()
		return nil, e
	}
	// Serialize concurrent broker migrations and reject inconsistent cluster lists.
	tx, e := p.Begin(ctx)
	if e != nil {
		s.Close()
		return nil, e
	}
	defer tx.Rollback(ctx)
	fail := func(err error) (*Store, error) { _ = tx.Rollback(ctx); s.Close(); return nil, err }
	if _, e = tx.Exec(ctx, "SELECT pg_advisory_xact_lock(7346100)"); e == nil {
		_, e = tx.Exec(ctx, migrations.Initial)
	}
	if e != nil {
		return fail(e)
	}
	for _, b := range brokers {
		_, e = tx.Exec(ctx, "INSERT INTO brokers(id,address) VALUES($1,$2) ON CONFLICT(id) DO NOTHING", b.ID, b.Address)
		if e != nil {
			return fail(e)
		}
	}
	rows, e := tx.Query(ctx, "SELECT id,address FROM brokers ORDER BY id")
	if e != nil {
		return fail(e)
	}
	var actual []domain.Broker
	for rows.Next() {
		var b domain.Broker
		if e = rows.Scan(&b.ID, &b.Address); e != nil {
			break
		}
		actual = append(actual, b)
	}
	rows.Close()
	if e == nil {
		e = rows.Err()
	}
	if e != nil {
		return fail(e)
	}
	if len(actual) != len(brokers) {
		return fail(fmt.Errorf("cluster broker list differs from persisted metadata"))
	}
	for i, b := range actual {
		if b != brokers[i] {
			return fail(fmt.Errorf("cluster broker address differs from persisted metadata"))
		}
	}
	if e = tx.Commit(ctx); e != nil {
		return fail(e)
	}
	return s, nil
}
func (s *Store) Close() { s.Redis.Close(); s.Pool.Close() }
func (s *Store) Create(ctx context.Context, name string, n int) (domain.Topic, error) {
	if !domain.ValidTopicName(name) || n < 1 || n > 1024 {
		return domain.Topic{}, domain.ErrInvalid
	}
	tx, e := s.Pool.Begin(ctx)
	if e != nil {
		return domain.Topic{}, e
	}
	defer tx.Rollback(ctx)
	_, e = tx.Exec(ctx, "INSERT INTO topics(name) VALUES($1)", name)
	if e != nil {
		var pe *pgconn.PgError
		if errors.As(e, &pe) && pe.Code == "23505" {
			return domain.Topic{}, domain.ErrExists
		}
		return domain.Topic{}, e
	}
	var count int
	if e = tx.QueryRow(ctx, "SELECT count(*) FROM brokers").Scan(&count); e != nil {
		return domain.Topic{}, e
	}
	for i := 0; i < n; i++ {
		if _, e = tx.Exec(ctx, "INSERT INTO partitions(topic,id,owner_id) VALUES($1,$2,$3)", name, i, i%count); e != nil {
			return domain.Topic{}, e
		}
	}
	if e = tx.Commit(ctx); e != nil {
		return domain.Topic{}, e
	}
	return s.Describe(ctx, name)
}
func (s *Store) Describe(ctx context.Context, name string) (domain.Topic, error) {
	if !domain.ValidTopicName(name) {
		return domain.Topic{}, domain.ErrInvalid
	}
	t := domain.Topic{Name: name}
	rows, e := s.Pool.Query(ctx, "SELECT p.id,b.id,b.address FROM partitions p JOIN brokers b ON b.id=p.owner_id WHERE p.topic=$1 ORDER BY p.id", name)
	if e != nil {
		return t, e
	}
	defer rows.Close()
	for rows.Next() {
		var p domain.Partition
		if e = rows.Scan(&p.ID, &p.Owner.ID, &p.Owner.Address); e != nil {
			return t, e
		}
		t.Partitions = append(t.Partitions, p)
	}
	if e = rows.Err(); e != nil {
		return t, e
	}
	if len(t.Partitions) == 0 {
		return t, domain.ErrNotFound
	}
	return t, nil
}
func (s *Store) List(ctx context.Context) ([]domain.Topic, error) {
	rows, e := s.Pool.Query(ctx, "SELECT name FROM topics ORDER BY name")
	if e != nil {
		return nil, e
	}
	var names []string
	for rows.Next() {
		var n string
		if e = rows.Scan(&n); e != nil {
			break
		}
		names = append(names, n)
	}
	rows.Close()
	if e != nil {
		return nil, e
	}
	if e = rows.Err(); e != nil {
		return nil, e
	}
	var out []domain.Topic
	for _, n := range names {
		t, e := s.Describe(ctx, n)
		if e != nil {
			return nil, e
		}
		out = append(out, t)
	}
	return out, nil
}
func leaseKey(group, topic, member string) string {
	return "sf:lease:" + group + ":" + topic + ":" + member
}

type groupState struct {
	Generation int64
	Members    []string
}

// groupTx holds a PostgreSQL row lock while pruning Redis-expired members,
// computing the assignment, and fencing commits. No distributed lock in Redis.
func (s *Store) groupTx(ctx context.Context, group, topic, member, action string, fn func(pgx.Tx, groupState) error) error {
	if !domain.ValidName(group) || !domain.ValidTopicName(topic) || !domain.ValidName(member) {
		return domain.ErrInvalid
	}
	tx, e := s.Pool.Begin(ctx)
	if e != nil {
		return e
	}
	defer tx.Rollback(ctx)
	if action == "join" {
		_, e = tx.Exec(ctx, "INSERT INTO consumer_groups(name,topic) VALUES($1,$2) ON CONFLICT DO NOTHING", group, topic)
		if e != nil {
			return e
		}
	}
	var gen int64
	e = tx.QueryRow(ctx, "SELECT generation FROM consumer_groups WHERE name=$1 AND topic=$2 FOR UPDATE", group, topic).Scan(&gen)
	if errors.Is(e, pgx.ErrNoRows) {
		return domain.ErrNotFound
	}
	if e != nil {
		return e
	}
	rows, e := tx.Query(ctx, "SELECT member FROM consumer_members WHERE group_name=$1 AND topic=$2 ORDER BY member", group, topic)
	if e != nil {
		return e
	}
	var old []string
	for rows.Next() {
		var m string
		if e = rows.Scan(&m); e != nil {
			break
		}
		old = append(old, m)
	}
	rows.Close()
	if e != nil {
		return e
	}
	if e = rows.Err(); e != nil {
		return e
	}
	changed := false
	live := make([]string, 0, len(old)+1)
	for _, m := range old {
		exists, e := s.Redis.Exists(ctx, leaseKey(group, topic, m)).Result()
		if e != nil {
			return fmt.Errorf("Redis lease verification: %w", e)
		}
		if exists == 0 {
			_, e = tx.Exec(ctx, "DELETE FROM consumer_members WHERE group_name=$1 AND topic=$2 AND member=$3", group, topic, m)
			if e != nil {
				return e
			}
			changed = true
		} else {
			live = append(live, m)
		}
	}
	found := false
	for _, m := range live {
		if m == member {
			found = true
		}
	}
	switch action {
	case "join":
		if len(live) >= 1024 && !found {
			return fmt.Errorf("membership limit reached")
		}
		if e = s.Redis.Set(ctx, leaseKey(group, topic, member), "1", s.TTL).Err(); e != nil {
			return e
		}
		if !found {
			_, e = tx.Exec(ctx, "INSERT INTO consumer_members(group_name,topic,member) VALUES($1,$2,$3) ON CONFLICT DO NOTHING", group, topic, member)
			if e != nil {
				return e
			}
			live = append(live, member)
			changed = true
		}
	case "heartbeat":
		if !found {
			return domain.ErrFenced
		}
		if e = s.Redis.Set(ctx, leaseKey(group, topic, member), "1", s.TTL).Err(); e != nil {
			return e
		}
	case "leave":
		if e = s.Redis.Del(ctx, leaseKey(group, topic, member)).Err(); e != nil {
			return e
		}
		if found {
			_, e = tx.Exec(ctx, "DELETE FROM consumer_members WHERE group_name=$1 AND topic=$2 AND member=$3", group, topic, member)
			if e != nil {
				return e
			}
			filtered := live[:0]
			for _, m := range live {
				if m != member {
					filtered = append(filtered, m)
				}
			}
			live = filtered
			changed = true
		}
	}
	sort.Strings(live)
	if changed {
		gen++
		_, e = tx.Exec(ctx, "UPDATE consumer_groups SET generation=$3 WHERE name=$1 AND topic=$2", group, topic, gen)
		if e != nil {
			return e
		}
	}
	if fn != nil {
		if e = fn(tx, groupState{gen, live}); e != nil {
			return e
		}
	}
	return tx.Commit(ctx)
}
func Assign(t domain.Topic, members []string, member string) []domain.Partition {
	var out []domain.Partition
	if len(members) == 0 {
		return out
	}
	for i, p := range t.Partitions {
		if members[i%len(members)] == member {
			out = append(out, p)
		}
	}
	return out
}
func (s *Store) Membership(ctx context.Context, group, topic, member, action string) (domain.Assignment, error) {
	t, e := s.Describe(ctx, topic)
	if e != nil {
		return domain.Assignment{}, e
	}
	var out domain.Assignment
	e = s.groupTx(ctx, group, topic, member, action, func(_ pgx.Tx, g groupState) error {
		out = domain.Assignment{Generation: g.Generation, Members: g.Members, Partitions: Assign(t, g.Members, member)}
		return nil
	})
	return out, e
}
func (s *Store) Check(ctx context.Context, group, topic, member string, generation int64, partition int) error {
	t, e := s.Describe(ctx, topic)
	if e != nil {
		return e
	}
	return s.groupTx(ctx, group, topic, member, "check", func(_ pgx.Tx, g groupState) error { return owns(t, g, member, generation, partition) })
}
func owns(t domain.Topic, g groupState, member string, gen int64, p int) error {
	if gen != g.Generation {
		return domain.ErrFenced
	}
	for _, a := range Assign(t, g.Members, member) {
		if a.ID == p {
			return nil
		}
	}
	return domain.ErrFenced
}
func (s *Store) Commit(ctx context.Context, group, topic, member string, generation int64, p int, next int64) error {
	if next < 0 {
		return domain.ErrOffset
	}
	t, e := s.Describe(ctx, topic)
	if e != nil {
		return e
	}
	return s.groupTx(ctx, group, topic, member, "check", func(tx pgx.Tx, g groupState) error {
		if e := owns(t, g, member, generation, p); e != nil {
			return e
		}
		// Group offsets are monotonic. An explicit rewind requires a new group.
		_, e := tx.Exec(ctx, `INSERT INTO consumer_offsets(group_name,topic,partition,next_offset) VALUES($1,$2,$3,$4) ON CONFLICT(group_name,topic,partition) DO UPDATE SET next_offset=GREATEST(consumer_offsets.next_offset,EXCLUDED.next_offset)`, group, topic, p, next)
		return e
	})
}
func (s *Store) Offset(ctx context.Context, group, topic string, p int) (domain.ConsumerOffset, error) {
	if !domain.ValidName(group) || !domain.ValidTopicName(topic) || p < 0 {
		return domain.ConsumerOffset{}, domain.ErrInvalid
	}
	var n int64
	e := s.Pool.QueryRow(ctx, "SELECT next_offset FROM consumer_offsets WHERE group_name=$1 AND topic=$2 AND partition=$3", group, topic, p).Scan(&n)
	if errors.Is(e, pgx.ErrNoRows) {
		return domain.ConsumerOffset{}, nil
	}
	return domain.ConsumerOffset{Next: n, Exists: e == nil}, e
}
func (s *Store) Schedule(ctx context.Context, j domain.RetryRecord, target string) (domain.RetryRecord, error) {
	_, e := s.Pool.Exec(ctx, `INSERT INTO retry_jobs(id,source_topic,source_partition,source_offset,attempt,target_topic,due_at,last_error) VALUES($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT(source_topic,source_partition,source_offset) DO NOTHING`, j.ID, j.Topic, j.Partition, j.Offset, j.Attempt, target, time.Unix(0, j.Due), j.Error)
	if e != nil {
		return j, e
	}
	e = s.Pool.QueryRow(ctx, "SELECT id,attempt,due_at,last_error FROM retry_jobs WHERE source_topic=$1 AND source_partition=$2 AND source_offset=$3", j.Topic, j.Partition, j.Offset).Scan(&j.ID, &j.Attempt, newDue(&j.Due), &j.Error)
	return j, e
}

// Scan timestamptz without relying on a driver conversion to int64.
type dueScanner struct{ p *int64 }

func newDue(p *int64) *dueScanner { return &dueScanner{p} }
func (d *dueScanner) Scan(v interface{}) error {
	if t, ok := v.(time.Time); ok {
		*d.p = t.UnixNano()
		return nil
	}
	return fmt.Errorf("unexpected timestamp type %T", v)
}
func (s *Store) Due(ctx context.Context, owner int, now time.Time, limit int) ([]domain.RetryRecord, error) {
	rows, e := s.Pool.Query(ctx, `SELECT j.id,j.source_topic,j.source_partition,j.source_offset,j.attempt,j.due_at,j.last_error FROM retry_jobs j JOIN partitions p ON p.topic=j.source_topic AND p.id=j.source_partition WHERE NOT j.completed AND j.due_at<=$1 AND p.owner_id=$2 ORDER BY j.due_at LIMIT $3`, now, owner, limit)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	var out []domain.RetryRecord
	for rows.Next() {
		var j domain.RetryRecord
		var due time.Time
		if e = rows.Scan(&j.ID, &j.Topic, &j.Partition, &j.Offset, &j.Attempt, &due, &j.Error); e != nil {
			return nil, e
		}
		j.Due = due.UnixNano()
		out = append(out, j)
	}
	return out, rows.Err()
}
func (s *Store) Complete(ctx context.Context, id string) error {
	_, e := s.Pool.Exec(ctx, "UPDATE retry_jobs SET completed=true WHERE id=$1", id)
	return e
}

// PendingRetryOffsets pins the earliest incomplete source per owned partition.
// Retention must not remove a source needed by a delayed durable job.
func (s *Store) PendingRetryOffsets(ctx context.Context, owner int) (map[string]int64, error) {
	rows, e := s.Pool.Query(ctx, `SELECT j.source_topic,j.source_partition,MIN(j.source_offset) FROM retry_jobs j JOIN partitions p ON p.topic=j.source_topic AND p.id=j.source_partition WHERE NOT j.completed AND p.owner_id=$1 GROUP BY j.source_topic,j.source_partition`, owner)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := make(map[string]int64)
	for rows.Next() {
		var topic string
		var partition int
		var offset int64
		if e := rows.Scan(&topic, &partition, &offset); e != nil {
			return nil, e
		}
		out[fmt.Sprintf("%s/%d", topic, partition)] = offset
	}
	return out, rows.Err()
}
func SafeError(s string) string {
	s = strings.ToValidUTF8(s, "?")
	s = strings.Map(func(r rune) rune {
		if r < ' ' || r == 127 {
			return ' '
		}
		return r
	}, s)
	if len(s) > 512 {
		s = s[:512]
		for !utf8.ValidString(s) {
			s = s[:len(s)-1]
		}
	}
	return s
}
