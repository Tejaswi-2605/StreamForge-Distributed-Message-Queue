package main

import (
	"testing"
	"time"
)

func TestBenchmarkClockMonotonicAndResolvesShortOperations(t *testing.T) {
	start := monotonicNow()
	previous := 0.0
	nonzero := 0
	for i := 0; i < 1000; i++ {
		elapsed := monotonicSeconds(start)
		if elapsed < previous {
			t.Fatal("clock moved backward")
		}
		if elapsed > previous {
			nonzero++
		}
		previous = elapsed
	}
	if nonzero == 0 {
		t.Fatal("clock cannot resolve any of 1000 short samples")
	}
	time.Sleep(2 * time.Millisecond)
	if monotonicSeconds(start) < 0.001 {
		t.Fatal("clock unit conversion is inconsistent")
	}
}
