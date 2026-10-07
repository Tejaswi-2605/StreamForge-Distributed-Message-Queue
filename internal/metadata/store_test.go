package metadata

import (
	"streamforge/internal/domain"
	"strings"
	"testing"
	"unicode/utf8"
)

func TestDeterministicAssignment(t *testing.T) {
	topic := domain.Topic{}
	for i := 0; i < 10; i++ {
		topic.Partitions = append(topic.Partitions, domain.Partition{ID: i})
	}
	members := []string{"A", "B", "C"}
	seen := map[int]bool{}
	for _, m := range members {
		for _, p := range Assign(topic, members, m) {
			if seen[p.ID] {
				t.Fatal("duplicate ownership")
			}
			seen[p.ID] = true
		}
	}
	if len(seen) != 10 {
		t.Fatal("lost partitions")
	}
	if len(Assign(topic, nil, "A")) != 0 {
		t.Fatal("empty members")
	}
}
func TestSafeError(t *testing.T) {
	v := SafeError(strings.Repeat("é", 500) + "\nsecret")
	if len(v) > 512 || !utf8.ValidString(v) || strings.Contains(v, "\n") {
		t.Fatal("unsafe error")
	}
}
