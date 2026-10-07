package replay

import (
	"context"
	"errors"
	"strings"
	"testing"
)

const synthetic = `{"id":"synthetic-1","type":"PushEvent","created_at":"2020-01-01T00:00:00Z","repo":{"name":"synthetic/repo"},"actor":{"email":"never-publish@example.invalid"}}
invalid json
{"id":"synthetic-2","type":"WatchEvent","created_at":"2020-01-01T00:00:01Z"}
`

func TestSyntheticReplay(t *testing.T) {
	n := 0
	s, e := Stream(context.Background(), strings.NewReader(synthetic), 0, func(_ context.Context, v Event, b []byte) error {
		n++
		if strings.Contains(string(b), "actor") || strings.Contains(string(b), "email") {
			t.Fatal("personal fields retained")
		}
		return nil
	})
	if e != nil || n != 2 || s.Parsed != 3 || s.Rejected != 1 {
		t.Fatalf("%v %v", s, e)
	}
}
func TestReplayCancellationAndFailure(t *testing.T) {
	ctx, c := context.WithCancel(context.Background())
	c()
	if _, e := Stream(ctx, strings.NewReader(synthetic), 0, func(context.Context, Event, []byte) error { return nil }); !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	want := errors.New("broker unavailable")
	if _, e := Stream(context.Background(), strings.NewReader(synthetic), 0, func(context.Context, Event, []byte) error { return want }); !errors.Is(e, want) {
		t.Fatal(e)
	}
}
func TestReplayLimit(t *testing.T) {
	s, e := Stream(context.Background(), strings.NewReader(synthetic), 1, func(context.Context, Event, []byte) error { return nil })
	if e != nil || s.Parsed != 1 || s.Published != 1 {
		t.Fatal(s, e)
	}
}
