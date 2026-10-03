package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/sathwikbairaboina2/kafka-go/internal/record"
)

// FsyncPolicy says when the active segment is flushed to disk.
type FsyncPolicy int

// Fsync policies.
const (
	FsyncNever FsyncPolicy = iota
	FsyncInterval
	FsyncAlways
)

// ParseFsync parses "never", "interval" or "always".
func ParseFsync(s string) (FsyncPolicy, error) {
	switch s {
	case "never":
		return FsyncNever, nil
	case "interval":
		return FsyncInterval, nil
	case "always":
		return FsyncAlways, nil
	}
	return 0, fmt.Errorf("unknown fsync policy %q (want never, interval or always)", s)
}

func (f FsyncPolicy) String() string {
	switch f {
	case FsyncNever:
		return "never"
	case FsyncInterval:
		return "interval"
	default:
		return "always"
	}
}

const (
	defaultSegmentBytes = 1 << 30
	defaultIndexBytes   = 4096
	maxSegmentBytes     = 1<<31 - 1
)

// Config is the per-partition log configuration.
type Config struct {
	SegmentBytes       int64 // roll when the active segment would exceed this; default 1 GiB
	IndexIntervalBytes int64 // default 4096
	RetentionMs        int64 // -1 disables
	RetentionBytes     int64 // -1 disables
	Fsync              FsyncPolicy
}

func (c Config) normalize() Config {
	if c.SegmentBytes <= 0 {
		c.SegmentBytes = defaultSegmentBytes
	}
	if c.SegmentBytes > maxSegmentBytes {
		c.SegmentBytes = maxSegmentBytes
	}
	if c.IndexIntervalBytes <= 0 {
		c.IndexIntervalBytes = defaultIndexBytes
	}
	return c
}

// ErrOffsetOutOfRange is returned by Read when the offset is outside [LogStartOffset, HighWatermark].
var ErrOffsetOutOfRange = errors.New("storage: offset out of range")

// ErrClosed is returned for operations on a closed partition.
var ErrClosed = errors.New("storage: partition closed")

// Partition is a multi-segment log with one writer at a time (ADR 0002).
type Partition struct {
	mu       sync.RWMutex
	dir      string
	cfg      Config
	segments []*segment
	logStart int64
	wake     chan struct{}
	closed   bool
}

func parseSegmentBase(name string) (int64, bool) {
	if !strings.HasSuffix(name, ".log") {
		return 0, false
	}
	n, err := strconv.ParseInt(strings.TrimSuffix(name, ".log"), 10, 64)
	return n, err == nil && n >= 0
}

// OpenPartition creates dir if needed, opens its segments in base order and recovers the active one.
func OpenPartition(dir string, cfg Config) (*Partition, error) {
	cfg = cfg.normalize()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create partition dir: %w", err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("list partition dir: %w", err)
	}
	var bases []int64
	for _, e := range entries {
		if b, ok := parseSegmentBase(e.Name()); ok {
			bases = append(bases, b)
		}
	}
	sort.Slice(bases, func(i, j int) bool { return bases[i] < bases[j] })
	p := &Partition{dir: dir, cfg: cfg, wake: make(chan struct{})}
	for _, b := range bases {
		s, err := openSegment(dir, b, cfg.IndexIntervalBytes)
		if err != nil {
			p.closeSegments()
			return nil, err
		}
		p.segments = append(p.segments, s)
	}
	if len(p.segments) == 0 {
		s, err := createSegment(dir, 0, cfg.IndexIntervalBytes)
		if err != nil {
			return nil, err
		}
		p.segments = append(p.segments, s)
	}
	if err := p.recover(); err != nil {
		p.closeSegments()
		return nil, err
	}
	p.logStart = p.segments[0].base
	return p, nil
}

func (p *Partition) closeSegments() {
	for _, s := range p.segments {
		_ = s.close()
	}
}

func (p *Partition) active() *segment { return p.segments[len(p.segments)-1] }

// Append sets the batch's base offset, writes it and returns that offset. Callers pass batches that
// record.Parse accepted. It rolls first when the active segment is non-empty and would exceed SegmentBytes.
func (p *Partition) Append(batch []byte, h record.Header) (int64, error) {
	return p.AppendMany([][]byte{batch}, []record.Header{h})
}

// AppendMany appends several validated batches under one lock, so their offsets are contiguous, and
// returns the offset of the first. With FsyncAlways it syncs once at the end.
func (p *Partition) AppendMany(batches [][]byte, hs []record.Header) (int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return 0, ErrClosed
	}
	first := p.active().next
	for i, batch := range batches {
		if err := p.appendLocked(batch, hs[i]); err != nil {
			return 0, err
		}
	}
	if p.cfg.Fsync == FsyncAlways {
		if err := p.active().sync(); err != nil {
			return 0, fmt.Errorf("fsync: %w", err)
		}
	}
	close(p.wake)
	p.wake = make(chan struct{})
	return first, nil
}

func (p *Partition) appendLocked(batch []byte, h record.Header) error {
	act := p.active()
	if act.size > 0 && act.size+int64(len(batch)) > p.cfg.SegmentBytes {
		if p.cfg.Fsync != FsyncNever {
			if err := act.sync(); err != nil {
				return fmt.Errorf("sync segment before roll: %w", err)
			}
		}
		s, err := createSegment(p.dir, act.next, p.cfg.IndexIntervalBytes)
		if err != nil {
			return err
		}
		p.segments = append(p.segments, s)
		act = s
	}
	base := act.next
	record.SetBaseOffset(batch, base)
	h.BaseOffset = base
	return act.append(batch, h)
}

// Wait returns a channel closed by the next Append. Take it before reading to avoid lost wakeups.
func (p *Partition) Wait() <-chan struct{} {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.wake
}

// HighWatermark is the offset the next appended record will get.
func (p *Partition) HighWatermark() int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.active().next
}

// LogStartOffset is the first offset still on disk.
func (p *Partition) LogStartOffset() int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.logStart
}

// Read returns whole batches starting with the one containing offset, up to maxBytes but always at
// least one batch. offset == HighWatermark returns (nil, nil); offsets outside the log are
// ErrOffsetOutOfRange.
func (p *Partition) Read(offset int64, maxBytes int) ([]byte, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		return nil, ErrClosed
	}
	hw := p.active().next
	if offset < p.logStart || offset > hw {
		return nil, ErrOffsetOutOfRange
	}
	if offset == hw {
		return nil, nil
	}
	i := sort.Search(len(p.segments), func(i int) bool { return p.segments[i].base > offset }) - 1
	if i < 0 {
		i = 0
	}
	var out []byte
	pos := int64(-1)
	for ; i < len(p.segments); i++ {
		s := p.segments[i]
		if pos < 0 {
			var ok bool
			if pos, ok = s.find(offset); !ok {
				pos = -1
				continue
			}
		} else {
			pos = 0
		}
		var more bool
		var err error
		out, more, err = readBatches(s, pos, maxBytes, out)
		if err != nil {
			return nil, err
		}
		if !more {
			break
		}
	}
	return out, nil
}

// readBatches appends whole batches from s starting at pos to out within the maxBytes budget (at least one
// batch overall). more reports whether the caller should continue with the next segment.
func readBatches(s *segment, pos int64, maxBytes int, out []byte) ([]byte, bool, error) {
	for pos < s.size {
		budget := maxBytes - len(out)
		if budget < 12 {
			if len(out) > 0 {
				return out, false, nil
			}
			budget = 12
		}
		buf, err := s.readAt(pos, budget)
		if err != nil {
			return nil, false, err
		}
		i := 0
		for i+12 <= len(buf) {
			size := 12 + int(int32(binary.BigEndian.Uint32(buf[i+8:])))
			if size < record.HeaderSize || i+size > len(buf) {
				break
			}
			i += size
		}
		if i == 0 {
			if len(out) > 0 {
				return out, false, nil
			}
			// The first batch is larger than the budget: return it whole anyway.
			if len(buf) < 12 {
				return nil, false, fmt.Errorf("storage: truncated batch prefix at %d in %s", pos, s.logPath())
			}
			size := 12 + int64(int32(binary.BigEndian.Uint32(buf[8:])))
			if size < record.HeaderSize || pos+size > s.size {
				return nil, false, fmt.Errorf("storage: bad batch size %d at %d in %s", size, pos, s.logPath())
			}
			whole, err := s.readAt(pos, int(size))
			if err != nil {
				return nil, false, err
			}
			return append(out, whole...), false, nil
		}
		if out == nil {
			out = buf[:i:i]
		} else {
			out = append(out, buf[:i]...)
		}
		pos += int64(i)
		if i < len(buf) {
			return out, false, nil // the next batch does not fit in the budget
		}
	}
	return out, true, nil
}

// OffsetForTimestamp resolves ListOffsets timestamps: -1 is the high watermark, -2 the log start,
// otherwise the base offset of the first batch whose MaxTimestamp >= ts, else the high watermark.
func (p *Partition) OffsetForTimestamp(ts int64) (int64, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	switch ts {
	case -1:
		return p.active().next, nil
	case -2:
		return p.logStart, nil
	}
	var hdr [record.HeaderSize]byte
	for _, s := range p.segments {
		if s.maxTimestamp < ts {
			continue
		}
		for pos := int64(0); pos+record.HeaderSize <= s.size; {
			if _, err := s.log.ReadAt(hdr[:], pos); err != nil {
				return 0, fmt.Errorf("read batch header: %w", err)
			}
			h, err := record.ReadHeader(hdr[:])
			if err != nil || h.BatchLength < record.HeaderSize-12 {
				break
			}
			if h.MaxTimestamp >= ts {
				return h.BaseOffset, nil
			}
			pos += h.Size()
		}
	}
	return p.active().next, nil
}

// Sync flushes the active segment.
func (p *Partition) Sync() error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		return nil
	}
	return p.active().sync()
}

// Close syncs and closes every segment.
func (p *Partition) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil
	}
	p.closed = true
	var first error
	if err := p.active().sync(); err != nil {
		first = err
	}
	for _, s := range p.segments {
		if err := s.close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// size returns the total bytes of all segments.
func (p *Partition) size() int64 {
	var n int64
	for _, s := range p.segments {
		n += s.size
	}
	return n
}
