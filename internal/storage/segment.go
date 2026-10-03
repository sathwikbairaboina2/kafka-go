// Package storage implements the segment log, partition manager, recovery and retention.
package storage

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/sathwikbairaboina2/kafka-go/internal/record"
)

// segment is one .log file plus its sparse .index. It is not safe for concurrent use;
// the owning Partition serialises writers and readers hold its read lock.
type segment struct {
	base         int64
	dir          string
	log, idx     *os.File
	size         int64 // bytes in the .log file
	next         int64 // offset after the last batch
	maxTimestamp int64 // -1 when empty
	index        []indexEntry
	sinceIndex   int64
	interval     int64
}

func segmentName(base int64) string { return fmt.Sprintf("%020d", base) }

func (s *segment) logPath() string { return filepath.Join(s.dir, segmentName(s.base)+".log") }
func (s *segment) idxPath() string { return filepath.Join(s.dir, segmentName(s.base)+".index") }

// createSegment creates (or reuses an empty) segment starting at base.
func createSegment(dir string, base int64, interval int64) (*segment, error) {
	return openSegment(dir, base, interval)
}

// openSegment opens (creating if missing) the files for base, loads the index and scans batch headers
// to find the next offset and the maximum timestamp. The scan trusts the headers; recovery validates.
func openSegment(dir string, base int64, interval int64) (*segment, error) {
	s := &segment{base: base, dir: dir, interval: interval, maxTimestamp: -1}
	var err error
	if s.log, err = os.OpenFile(s.logPath(), os.O_RDWR|os.O_CREATE, 0o644); err != nil {
		return nil, fmt.Errorf("open segment log: %w", err)
	}
	if s.idx, err = os.OpenFile(s.idxPath(), os.O_RDWR|os.O_CREATE, 0o644); err != nil {
		s.log.Close()
		return nil, fmt.Errorf("open segment index: %w", err)
	}
	st, err := s.log.Stat()
	if err != nil {
		s.close()
		return nil, fmt.Errorf("stat segment log: %w", err)
	}
	s.size = st.Size()
	raw, err := os.ReadFile(s.idxPath())
	if err != nil {
		s.close()
		return nil, fmt.Errorf("read segment index: %w", err)
	}
	s.index = decodeIndex(raw)
	s.headerScan()
	s.resetSinceIndex()
	return s, nil
}

func (s *segment) resetSinceIndex() {
	if n := len(s.index); n > 0 {
		s.sinceIndex = s.size - int64(s.index[n-1].pos)
	} else {
		s.sinceIndex = s.size
	}
}

// headerScan walks batch prefixes without validating CRCs, stopping at the first implausible one.
func (s *segment) headerScan() int64 {
	s.next = s.base
	s.maxTimestamp = -1
	br := bufio.NewReaderSize(io.NewSectionReader(s.log, 0, s.size), 64<<10)
	var hdr [record.HeaderSize]byte
	var pos int64
	for pos+record.HeaderSize <= s.size {
		if _, err := io.ReadFull(br, hdr[:]); err != nil {
			break
		}
		h, err := record.ReadHeader(hdr[:])
		if err != nil || h.BatchLength < record.HeaderSize-12 || pos+h.Size() > s.size {
			break
		}
		s.next = h.BaseOffset + int64(h.LastOffsetDelta) + 1
		if h.MaxTimestamp > s.maxTimestamp {
			s.maxTimestamp = h.MaxTimestamp
		}
		if _, err := br.Discard(int(h.Size()) - record.HeaderSize); err != nil {
			break
		}
		pos += h.Size()
	}
	return pos
}

// append writes one validated batch whose base offset is already set to h.BaseOffset.
func (s *segment) append(batch []byte, h record.Header) error {
	if _, err := s.log.WriteAt(batch, s.size); err != nil {
		_ = s.log.Truncate(s.size)
		return fmt.Errorf("write batch: %w", err)
	}
	pos := s.size
	s.size += int64(len(batch))
	if s.sinceIndex >= s.interval {
		e := indexEntry{rel: int32(h.BaseOffset - s.base), pos: int32(pos)}
		if _, err := s.idx.WriteAt(e.encode(), int64(len(s.index))*indexEntrySize); err != nil {
			return fmt.Errorf("write index: %w", err)
		}
		s.index = append(s.index, e)
		s.sinceIndex = 0
	}
	s.sinceIndex += int64(len(batch))
	s.next = h.BaseOffset + int64(h.LastOffsetDelta) + 1
	if h.MaxTimestamp > s.maxTimestamp {
		s.maxTimestamp = h.MaxTimestamp
	}
	return nil
}

// lookup returns the position of the greatest index entry with base+rel <= offset, else 0.
func (s *segment) lookup(offset int64) int64 {
	return lookupIndex(s.index, offset-s.base)
}

// find returns the position of the batch containing offset, or of the first batch after it.
func (s *segment) find(offset int64) (int64, bool) {
	pos := s.lookup(offset)
	var pre [27]byte
	for pos+int64(len(pre)) <= s.size {
		if _, err := s.log.ReadAt(pre[:], pos); err != nil {
			return 0, false
		}
		base := int64(binary.BigEndian.Uint64(pre[0:]))
		length := int64(int32(binary.BigEndian.Uint32(pre[8:])))
		last := base + int64(int32(binary.BigEndian.Uint32(pre[23:])))
		if length < record.HeaderSize-12 {
			return 0, false
		}
		if offset <= last {
			return pos, true
		}
		pos += 12 + length
	}
	return 0, false
}

// readAt reads up to n bytes at pos, clipped to the segment size.
func (s *segment) readAt(pos int64, n int) ([]byte, error) {
	if pos >= s.size {
		return nil, nil
	}
	if int64(n) > s.size-pos {
		n = int(s.size - pos)
	}
	buf := make([]byte, n)
	if _, err := s.log.ReadAt(buf, pos); err != nil {
		return nil, fmt.Errorf("read segment: %w", err)
	}
	return buf, nil
}

func (s *segment) sync() error {
	if err := s.log.Sync(); err != nil {
		return err
	}
	return s.idx.Sync()
}

func (s *segment) close() error {
	e1 := s.log.Close()
	e2 := s.idx.Close()
	if e1 != nil {
		return e1
	}
	return e2
}

// remove closes the segment and deletes both files.
func (s *segment) remove() error {
	_ = s.close()
	e1 := os.Remove(s.logPath())
	e2 := os.Remove(s.idxPath())
	if e1 != nil {
		return e1
	}
	return e2
}
