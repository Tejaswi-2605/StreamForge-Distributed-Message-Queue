package broker

import (
	"context"
	"log/slog"
	"math"
	"streamforge/internal/logstore"
	"time"
)

func (b *Broker) RunRetention(ctx context.Context) {
	if b.C.RetentionSegments == 0 {
		return
	}
	tick := time.NewTicker(b.C.RetentionInterval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			work, cancel := context.WithTimeout(ctx, 5*time.Second)
			if e := b.ApplyRetention(work); e != nil && ctx.Err() == nil {
				slog.Error("retention failed", "broker_id", b.C.ID, "error", e)
			}
			cancel()
		}
	}
}

func (b *Broker) ApplyRetention(ctx context.Context) error {
	if b.C.RetentionSegments == 0 {
		return nil
	}
	b.retentionMu.Lock()
	defer b.retentionMu.Unlock()
	pins, e := b.Store.PendingRetryOffsets(ctx, b.C.ID)
	if e != nil {
		// Metadata failure must never turn into unprotected deletion.
		return e
	}
	b.mu.Lock()
	logs := make(map[string]*logstore.Log, len(b.logs))
	for key, l := range b.logs {
		logs[key] = l
	}
	b.mu.Unlock()
	for key, l := range logs {
		if e := ctx.Err(); e != nil {
			return e
		}
		pin := int64(math.MaxInt64)
		if offset, exists := pins[key]; exists {
			pin = offset
		}
		if e := l.TrimBefore(b.C.RetentionSegments, pin); e != nil {
			return e
		}
	}
	return nil
}
