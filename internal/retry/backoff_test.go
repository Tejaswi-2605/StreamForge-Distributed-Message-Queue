package retry

import (
	"testing"
	"time"
)

func TestBackoff(t *testing.T) {
	want := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 5 * time.Second}
	for i, w := range want {
		if v := Delay(i+1, time.Second, 5*time.Second); v != w {
			t.Fatalf("%d: %v", i, v)
		}
	}
}
func TestBackoffOverflow(t *testing.T) {
	max := time.Duration(1<<63 - 1)
	if Delay(100, 1<<62, max) != max {
		t.Fatal("overflow")
	}
	if Delay(2, 0, max) != 0 {
		t.Fatal("invalid")
	}
}
