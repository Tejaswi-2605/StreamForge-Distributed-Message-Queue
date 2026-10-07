// Package replay streams GH Archive NDJSON, reducing each event to selected
// envelope fields. Repository names may identify users; this is minimization,
// not anonymization. Raw payloads/actors are not sent to the broker.
package replay

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"
)

type Event struct {
	ID        string    `json:"id"`
	Type      string    `json:"type"`
	CreatedAt time.Time `json:"created_at"`
	Repo      struct {
		Name string `json:"name"`
	} `json:"repo"`
}
type Stats struct {
	Parsed           int     `json:"parsed"`
	Published        int     `json:"published"`
	Rejected         int     `json:"rejected"`
	Bytes            int64   `json:"bytes"`
	ElapsedSeconds   float64 `json:"elapsed_seconds"`
	RecordsPerSecond float64 `json:"records_per_second"`
}

func Stream(ctx context.Context, r io.Reader, max int, publish func(context.Context, Event, []byte) error) (Stats, error) {
	start := time.Now()
	var s Stats
	scan := bufio.NewScanner(r)
	scan.Buffer(make([]byte, 64<<10), 2<<20)
	for (max <= 0 || s.Parsed < max) && scan.Scan() {
		if e := ctx.Err(); e != nil {
			return s, e
		}
		raw := scan.Bytes()
		s.Parsed++
		var event Event
		if e := json.Unmarshal(raw, &event); e != nil || event.ID == "" || event.Type == "" || event.CreatedAt.IsZero() {
			s.Rejected++
			continue
		}
		b, e := json.Marshal(event)
		if e != nil {
			return s, e
		}
		if e = publish(ctx, event, b); e != nil {
			return s, fmt.Errorf("publish event %d: %w", s.Parsed, e)
		}
		s.Published++
		s.Bytes += int64(len(b))
	}
	if e := scan.Err(); e != nil {
		return s, e
	}
	s.ElapsedSeconds = time.Since(start).Seconds()
	if s.ElapsedSeconds > 0 {
		s.RecordsPerSecond = float64(s.Published) / s.ElapsedSeconds
	}
	return s, nil
}
