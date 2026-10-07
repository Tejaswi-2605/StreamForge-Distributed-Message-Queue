//go:build !windows

package main

import "time"

type clockStamp struct{ start time.Time }

func monotonicNow() clockStamp                  { return clockStamp{time.Now()} }
func monotonicSeconds(start clockStamp) float64 { return time.Since(start.start).Seconds() }
func monotonicName() string                     { return "Go time.Now monotonic clock" }
