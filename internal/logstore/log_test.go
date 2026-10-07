package logstore

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	pb "streamforge/api/gen"
	"streamforge/internal/domain"
	"sync"
	"testing"
)

func options() Options {
	return Options{SegmentBytes: 512, IndexInterval: 3, MaxRecord: 8192, Sync: true}
}

func TestReadByteBudgetNeverHidesRecord(t *testing.T) {
	l := openTest(t, t.TempDir())
	if _, e := l.Append(&pb.Message{Payload: []byte("small")}); e != nil {
		t.Fatal(e)
	}
	if _, e := l.Append(&pb.Message{Payload: bytes.Repeat([]byte("x"), 1024)}); e != nil {
		t.Fatal(e)
	}
	first, e := l.Read(0, 2, 128)
	if e != nil || len(first) != 1 || string(first[0].Payload) != "small" {
		t.Fatal("bounded prefix should be returned", first, e)
	}
	if _, e := l.Read(1, 1, 128); !errors.Is(e, domain.ErrRecordTooLarge) {
		t.Fatalf("oversized first record silently hidden: %v", e)
	}
	last, e := l.Read(1, 1, 2048)
	if e != nil || len(last) != 1 || len(last[0].Payload) != 1024 {
		t.Fatal("record should be available with a sufficient budget", e)
	}
}

func TestRetentionProtectsPendingOffset(t *testing.T) {
	l := openTest(t, t.TempDir())
	for i := 0; i < 30; i++ {
		if _, e := l.Append(&pb.Message{Payload: bytes.Repeat([]byte("x"), 200)}); e != nil {
			t.Fatal(e)
		}
	}
	if e := l.TrimBefore(2, 0); e != nil {
		t.Fatal(e)
	}
	first, next := l.Bounds()
	if first != 0 || next != 30 {
		t.Fatal("pinned source expired", first, next)
	}
	if e := l.TrimBefore(2, 20); e != nil {
		t.Fatal(e)
	}
	first, _ = l.Bounds()
	if first <= 0 || first > 20 {
		t.Fatal("trim crossed the pin", first)
	}
	if _, e := l.Read(20, 1, 8192); e != nil {
		t.Fatal("protected record unreadable", e)
	}
}
func openTest(t *testing.T, dir string) *Log {
	t.Helper()
	l, e := Open(dir, options())
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { l.Close() })
	return l
}
func TestAppendFetchRotationRecovery(t *testing.T) {
	dir := t.TempDir()
	l := openTest(t, dir)
	for i := 0; i < 50; i++ {
		m, e := l.Append(&pb.Message{Id: "id", Payload: bytes.Repeat([]byte{byte(i)}, 100)})
		if e != nil || m.Offset != int64(i) {
			t.Fatalf("append %d: %v %v", i, m, e)
		}
	}
	if len(l.segments) < 2 {
		t.Fatal("no rotation")
	}
	l.Close()
	l = openTest(t, dir)
	ms, e := l.Read(37, 6, 8192)
	if e != nil || len(ms) != 6 {
		t.Fatalf("read %v %v", ms, e)
	}
	for i, m := range ms {
		if m.Offset != int64(37+i) || m.Payload[0] != byte(37+i) {
			t.Fatal("index lookup changed data")
		}
	}
	m, e := l.Append(&pb.Message{Payload: []byte("after restart")})
	if e != nil || m.Offset != 50 {
		t.Fatalf("continuity %v %v", m, e)
	}
}
func TestCorruption(t *testing.T) {
	for _, kind := range []string{"header", "payload", "length", "version", "checksum", "offset"} {
		t.Run(kind, func(t *testing.T) {
			dir := t.TempDir()
			l := openTest(t, dir)
			_, e := l.Append(&pb.Message{Id: "id", Payload: []byte("abc")})
			if e != nil {
				t.Fatal(e)
			}
			l.Close()
			p := filepath.Join(dir, "00000000000000000000.log")
			b, e := os.ReadFile(p)
			if e != nil {
				t.Fatal(e)
			}
			switch kind {
			case "header":
				b = b[:7]
			case "payload":
				b = b[:len(b)-1]
			case "length":
				binary.LittleEndian.PutUint32(b[8:12], 0xffffffff)
			case "version":
				binary.LittleEndian.PutUint32(b[4:8], 9)
			case "checksum":
				b[len(b)-1] ^= 255
			case "offset":
				b[16] ^= 1
			}
			if e = os.WriteFile(p, b, 0640); e != nil {
				t.Fatal(e)
			}
			bad, e := Open(dir, options())
			if e == nil {
				bad.Close()
				t.Fatal("corruption accepted")
			}
		})
	}
}
func TestOffsetBoundsAndRetention(t *testing.T) {
	dir := t.TempDir()
	l := openTest(t, dir)
	for i := 0; i < 40; i++ {
		if _, e := l.Append(&pb.Message{Payload: bytes.Repeat([]byte("x"), 100)}); e != nil {
			t.Fatal(e)
		}
	}
	if e := l.Trim(2); e != nil {
		t.Fatal(e)
	}
	first, next := l.Bounds()
	if first <= 0 || next != 40 {
		t.Fatalf("bounds %d %d", first, next)
	}
	if _, e := l.Read(0, 1, 8192); !errors.Is(e, domain.ErrOffset) {
		t.Fatal(e)
	}
	if _, e := l.Read(41, 1, 8192); !errors.Is(e, domain.ErrOffset) {
		t.Fatal(e)
	}
	ms, e := l.Read(40, 1, 8192)
	if e != nil || len(ms) != 0 {
		t.Fatal(e)
	}
	l.Close()
	l = openTest(t, dir)
	a, b := l.Bounds()
	if a != first || b != next {
		t.Fatal("retention recovery")
	}
}
func TestConcurrentAppendUniqueOffsets(t *testing.T) {
	l := openTest(t, t.TempDir())
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for n := 0; n < 8; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 125; i++ {
				if _, e := l.Append(&pb.Message{Payload: []byte("concurrent")}); e != nil {
					errs <- e
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Fatal(e)
	}
	_, next := l.Bounds()
	if next != 1000 {
		t.Fatal(next)
	}
	for off := int64(0); off < next; {
		ms, e := l.Read(off, 100, 1<<20)
		if e != nil {
			t.Fatal(e)
		}
		for _, m := range ms {
			if m.Offset != off {
				t.Fatal("non-contiguous")
			}
			off++
		}
	}
}
func TestClosedAndOversize(t *testing.T) {
	l := openTest(t, t.TempDir())
	if _, e := l.Append(&pb.Message{Payload: make([]byte, 9000)}); e == nil {
		t.Fatal("oversize accepted")
	}
	l.Close()
	if _, e := l.Append(&pb.Message{}); !errors.Is(e, os.ErrClosed) {
		t.Fatal(e)
	}
	if _, e := l.Read(0, 1, 8192); !errors.Is(e, os.ErrClosed) {
		t.Fatal(e)
	}
}
func TestImmutableAppend(t *testing.T) {
	l := openTest(t, t.TempDir())
	m := &pb.Message{Payload: []byte("hello"), Headers: map[string]string{"x": "y"}}
	out, e := l.Append(m)
	if e != nil {
		t.Fatal(e)
	}
	m.Payload[0] = 'z'
	out.Payload[0] = 'q'
	out.Headers["x"] = "changed"
	ms, e := l.Read(0, 1, 8192)
	if e != nil || string(ms[0].Payload) != "hello" || ms[0].Headers["x"] != "y" {
		t.Fatal("mutable log data")
	}
}
func BenchmarkAppend(b *testing.B) {
	for _, size := range []int{100, 1024, 10240} {
		for _, syncMode := range []bool{false, true} {
			name := stringName(size, syncMode)
			b.Run(name, func(b *testing.B) {
				l, e := Open(b.TempDir(), Options{SegmentBytes: 16 << 20, IndexInterval: 64, MaxRecord: 1 << 20, Sync: syncMode})
				if e != nil {
					b.Fatal(e)
				}
				defer l.Close()
				m := &pb.Message{Payload: make([]byte, size)}
				b.SetBytes(int64(size))
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if _, e := l.Append(m); e != nil {
						b.Fatal(e)
					}
				}
			})
		}
	}
}
func stringName(n int, s bool) string {
	mode := "write"
	if s {
		mode = "fsync"
	}
	return fmt.Sprintf("%dB/%s", n, mode)
}
