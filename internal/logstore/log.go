// Package logstore implements CRC-protected append-only logs. A partition mutex
// owns every file handle and index; independent partitions have independent locks.
package logstore

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"google.golang.org/protobuf/proto"
	pb "streamforge/api/gen"
	"streamforge/internal/domain"
)

const headerSize = 24
const magic uint32 = 0x53464731

type Options struct {
	SegmentBytes  int64
	IndexInterval int
	MaxRecord     int
	Sync          bool
}
type indexEntry struct{ offset, position int64 }
type segment struct {
	base, end, size int64
	path            string
	file            *os.File
	index           []indexEntry
	count           int
}
type Log struct {
	mu       sync.Mutex
	dir      string
	opts     Options
	segments []*segment
	next     int64
	closed   bool
	poisoned error
}

func Open(dir string, o Options) (*Log, error) {
	if o.SegmentBytes < 256 || o.IndexInterval < 1 || o.MaxRecord < 1 || o.MaxRecord > 32<<20 {
		return nil, fmt.Errorf("invalid log options")
	}
	if e := os.MkdirAll(dir, 0750); e != nil {
		return nil, e
	}
	l := &Log{dir: dir, opts: o}
	paths, e := filepath.Glob(filepath.Join(dir, "*.log"))
	if e != nil {
		return nil, e
	}
	sort.Strings(paths)
	for _, p := range paths {
		base, e := strconv.ParseInt(strings.TrimSuffix(filepath.Base(p), ".log"), 10, 64)
		if e != nil || base < 0 {
			l.Close()
			return nil, fmt.Errorf("invalid segment filename %s", p)
		}
		if len(l.segments) > 0 && base != l.next {
			l.Close()
			return nil, fmt.Errorf("segment gap at %s", p)
		}
		s, e := l.load(p, base)
		if e != nil {
			l.Close()
			return nil, fmt.Errorf("recover %s: %w", p, e)
		}
		l.segments = append(l.segments, s)
		l.next = s.end
	}
	if len(l.segments) == 0 {
		if e := l.rotate(); e != nil {
			return nil, e
		}
	}
	return l, nil
}
func (l *Log) load(path string, base int64) (*segment, error) {
	f, e := os.OpenFile(path, os.O_RDWR, 0640)
	if e != nil {
		return nil, e
	}
	s := &segment{base: base, end: base, path: path, file: f}
	info, e := f.Stat()
	if e != nil {
		f.Close()
		return nil, e
	}
	s.size = info.Size()
	for pos := int64(0); pos < s.size; {
		m, n, e := readRecord(f, pos, s.size, l.opts.MaxRecord)
		if e != nil {
			f.Close()
			return nil, e
		}
		if m.Offset != s.end {
			f.Close()
			return nil, fmt.Errorf("non-contiguous offset: got %d want %d", m.Offset, s.end)
		}
		if s.count%l.opts.IndexInterval == 0 {
			s.index = append(s.index, indexEntry{m.Offset, pos})
		}
		s.count++
		s.end++
		pos += n
	}
	return s, nil
}
func readRecord(f *os.File, pos, size int64, max int) (*pb.Message, int64, error) {
	if size-pos < headerSize {
		return nil, 0, fmt.Errorf("truncated header at byte %d", pos)
	}
	var h [headerSize]byte
	if _, e := f.ReadAt(h[:], pos); e != nil {
		return nil, 0, e
	}
	if binary.LittleEndian.Uint32(h[:4]) != magic || binary.LittleEndian.Uint32(h[4:8]) != 1 {
		return nil, 0, fmt.Errorf("invalid magic/version at byte %d", pos)
	}
	n := int64(binary.LittleEndian.Uint32(h[8:12]))
	if n < 1 || n > int64(max) {
		return nil, 0, fmt.Errorf("invalid record length %d", n)
	}
	if n > size-pos-headerSize {
		return nil, 0, fmt.Errorf("truncated payload at byte %d", pos)
	}
	b := make([]byte, int(n))
	if _, e := f.ReadAt(b, pos+headerSize); e != nil {
		return nil, 0, e
	}
	// CRC covers header fields (including offset) as well as the protobuf body.
	crc := crc32.NewIEEE()
	_, _ = crc.Write(h[:12])
	_, _ = crc.Write(h[16:24])
	_, _ = crc.Write(b)
	if crc.Sum32() != binary.LittleEndian.Uint32(h[12:16]) {
		return nil, 0, fmt.Errorf("checksum mismatch at byte %d", pos)
	}
	m := new(pb.Message)
	if e := proto.Unmarshal(b, m); e != nil {
		return nil, 0, fmt.Errorf("invalid protobuf: %w", e)
	}
	if m.Offset != int64(binary.LittleEndian.Uint64(h[16:24])) {
		return nil, 0, fmt.Errorf("offset mismatch")
	}
	return m, n + headerSize, nil
}
func (l *Log) rotate() error {
	p := filepath.Join(l.dir, fmt.Sprintf("%020d.log", l.next))
	f, e := os.OpenFile(p, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0640)
	if e != nil {
		return e
	}
	l.segments = append(l.segments, &segment{base: l.next, end: l.next, path: p, file: f})
	return nil
}
func (l *Log) Append(m *pb.Message) (*pb.Message, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil, os.ErrClosed
	}
	if l.poisoned != nil {
		return nil, l.poisoned
	}
	out := proto.Clone(m).(*pb.Message)
	out.Offset = l.next
	b, e := proto.Marshal(out)
	if e != nil {
		return nil, e
	}
	if len(b) > l.opts.MaxRecord {
		return nil, fmt.Errorf("record too large")
	}
	s := l.segments[len(l.segments)-1]
	total := int64(headerSize + len(b))
	if s.count > 0 && s.size+total > l.opts.SegmentBytes {
		if e = l.rotate(); e != nil {
			return nil, e
		}
		s = l.segments[len(l.segments)-1]
	}
	buf := make([]byte, int(total))
	binary.LittleEndian.PutUint32(buf[:4], magic)
	binary.LittleEndian.PutUint32(buf[4:8], 1)
	binary.LittleEndian.PutUint32(buf[8:12], uint32(len(b)))
	binary.LittleEndian.PutUint64(buf[16:24], uint64(out.Offset))
	copy(buf[24:], b)
	crc := crc32.NewIEEE()
	_, _ = crc.Write(buf[:12])
	_, _ = crc.Write(buf[16:])
	binary.LittleEndian.PutUint32(buf[12:16], crc.Sum32())
	n, e := s.file.WriteAt(buf, s.size)
	if e != nil || n != len(buf) {
		if e == nil {
			e = io.ErrShortWrite
		}
		l.poisoned = fmt.Errorf("append uncertain; restart required: %w", e)
		return nil, l.poisoned
	}
	if l.opts.Sync {
		if e = s.file.Sync(); e != nil {
			l.poisoned = fmt.Errorf("sync uncertain; restart required: %w", e)
			return nil, l.poisoned
		}
	}
	if s.count%l.opts.IndexInterval == 0 {
		s.index = append(s.index, indexEntry{out.Offset, s.size})
	}
	s.count++
	s.size += total
	s.end++
	l.next++
	return out, nil
}
func (l *Log) Bounds() (int64, int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.segments[0].base, l.next
}
func (l *Log) Size() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	var n int64
	for _, s := range l.segments {
		n += s.size
	}
	return n
}
func (l *Log) Read(offset int64, limit, maxBytes int) ([]*pb.Message, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil, os.ErrClosed
	}
	if offset < l.segments[0].base || offset > l.next {
		return nil, domain.ErrOffset
	}
	if limit < 1 || maxBytes < 1 {
		return nil, domain.ErrInvalid
	}
	out := make([]*pb.Message, 0, limit)
	used := 0
	si := sort.Search(len(l.segments), func(i int) bool { return l.segments[i].end > offset })
	for ; si < len(l.segments) && len(out) < limit; si++ {
		s := l.segments[si]
		pos := int64(0)
		ix := sort.Search(len(s.index), func(i int) bool { return s.index[i].offset > offset })
		if ix > 0 {
			pos = s.index[ix-1].position
		}
		for pos < s.size && len(out) < limit {
			m, n, e := readRecord(s.file, pos, s.size, l.opts.MaxRecord)
			if e != nil {
				return nil, e
			}
			pos += n
			if m.Offset < offset {
				continue
			}
			cost := proto.Size(m)
			if used+cost > maxBytes {
				if len(out) == 0 {
					return nil, domain.ErrRecordTooLarge
				}
				return out, nil
			}
			out = append(out, m)
			used += cost
			offset = m.Offset + 1
		}
	}
	return out, nil
}

// Trim removes only inactive segments. The caller must deliberately opt in;
// slow consumers may lose access and receive ErrOffset after trimming.
func (l *Log) Trim(maxSegments int) error {
	return l.TrimBefore(maxSegments, int64(^uint64(0)>>1))
}

// TrimBefore retains any segment containing a protected offset (for example a
// pending retry source). The segment-count goal is a soft limit while pinned.
func (l *Log) TrimBefore(maxSegments int, protectedOffset int64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return os.ErrClosed
	}
	if maxSegments < 1 || protectedOffset < 0 {
		return domain.ErrInvalid
	}
	for len(l.segments) > maxSegments {
		s := l.segments[0]
		if s.end > protectedOffset {
			break
		}
		if e := s.file.Close(); e != nil {
			return e
		}
		if e := os.Remove(s.path); e != nil {
			// Windows requires closing before removal. Restore the handle if
			// deletion fails so a transient filesystem error cannot break reads.
			f, reopenErr := os.OpenFile(s.path, os.O_RDWR, 0640)
			if reopenErr == nil {
				s.file = f
			} else {
				l.poisoned = fmt.Errorf("retention handle recovery failed; restart required: %w", reopenErr)
			}
			return errors.Join(e, reopenErr)
		}
		l.segments = l.segments[1:]
	}
	return nil
}
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil
	}
	l.closed = true
	var errs []error
	for _, s := range l.segments {
		errs = append(errs, s.file.Close())
	}
	return errors.Join(errs...)
}
