//go:build windows

package main

import (
	"fmt"
	"syscall"
	"unsafe"
)

type clockStamp int64

var (
	kernel32             = syscall.NewLazyDLL("kernel32.dll")
	performanceCounter   = kernel32.NewProc("QueryPerformanceCounter")
	performanceFrequency = kernel32.NewProc("QueryPerformanceFrequency")
	clockFrequency       = readCounter(performanceFrequency)
)

func readCounter(proc *syscall.LazyProc) int64 {
	var value int64
	ok, _, e := proc.Call(uintptr(unsafe.Pointer(&value)))
	if ok == 0 {
		panic(fmt.Sprintf("benchmark high-resolution clock failed: %v", e))
	}
	return value
}

func monotonicNow() clockStamp { return clockStamp(readCounter(performanceCounter)) }
func monotonicSeconds(start clockStamp) float64 {
	return float64(monotonicNow()-start) / float64(clockFrequency)
}
func monotonicName() string { return "Windows QueryPerformanceCounter" }
