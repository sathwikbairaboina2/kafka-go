package storage

import (
	"bufio"
	"fmt"
	"io"
	"log/slog"

	"github.com/sathwikbairaboina2/kafka-go/internal/record"
)

// scanResult is what a validating scan of a segment found.
type scanResult struct {
	end      int64 // end of the last valid batch
	next     int64 // offset after the last valid batch
	maxTs    int64
	index    []indexEntry
	sinceIdx int64
}

// scanSegment walks s from position 0, accepting batches that parse, have a valid CRC and carry the
// expected dense base offset. It stops at the first failure.
func scanSegment(s *segment) scanResult {
	res := scanResult{next: s.base, maxTs: -1}
	br := bufio.NewReaderSize(io.NewSectionReader(s.log, 0, s.size), 256<<10)
	var prefix [12]byte
	var buf []byte
	for res.end+12 <= s.size {
		if _, err := io.ReadFull(br, prefix[:]); err != nil {
			break
		}
		size, err := record.PeekSize(prefix[:])
		if err != nil || res.end+size > s.size {
			break
		}
		if int64(cap(buf)) < size {
			buf = make([]byte, size)
		}
		buf = buf[:size]
		copy(buf, prefix[:])
		if _, err := io.ReadFull(br, buf[12:]); err != nil {
			break
		}
		h, err := record.Parse(buf)
		if err != nil || h.BaseOffset != res.next {
			break
		}
		if res.sinceIdx >= s.interval {
			res.index = append(res.index, indexEntry{rel: int32(h.BaseOffset - s.base), pos: int32(res.end)})
			res.sinceIdx = 0
		}
		res.sinceIdx += size
		res.end += size
		res.next = h.BaseOffset + int64(h.LastOffsetDelta) + 1
		if h.MaxTimestamp > res.maxTs {
			res.maxTs = h.MaxTimestamp
		}
	}
	return res
}

// writeIndex replaces the .index file with entries.
func (s *segment) writeIndex(entries []indexEntry) error {
	if err := s.idx.Truncate(0); err != nil {
		return fmt.Errorf("truncate index: %w", err)
	}
	buf := make([]byte, 0, len(entries)*indexEntrySize)
	for _, e := range entries {
		buf = append(buf, e.encode()...)
	}
	if _, err := s.idx.WriteAt(buf, 0); err != nil {
		return fmt.Errorf("write index: %w", err)
	}
	if err := s.idx.Sync(); err != nil {
		return fmt.Errorf("sync index: %w", err)
	}
	s.index = entries
	return nil
}

// indexLooksSane reports whether the loaded index has whole entries that are in range and increasing.
func (s *segment) indexLooksSane() bool {
	st, err := s.idx.Stat()
	if err != nil || st.Size()%indexEntrySize != 0 {
		return false
	}
	for i, e := range s.index {
		if e.rel < 0 || int64(e.pos) >= s.size || (i > 0 && (e.rel <= s.index[i-1].rel || e.pos <= s.index[i-1].pos)) {
			return false
		}
	}
	return true
}

// recover repairs the segments after a crash: a sealed segment with a bad index gets it rebuilt, and the
// active segment gets its torn tail truncated and its index rebuilt. Sealed data is trusted (spec).
func (p *Partition) recover() error {
	for _, s := range p.segments[:len(p.segments)-1] {
		if s.indexLooksSane() {
			continue
		}
		if err := s.writeIndex(scanSegment(s).index); err != nil {
			return err
		}
		s.resetSinceIndex()
		slog.Warn("rebuilt segment index", "segment", s.idxPath())
	}
	s := p.active()
	res := scanSegment(s)
	if res.end < s.size {
		old := s.size
		if err := s.log.Truncate(res.end); err != nil {
			return fmt.Errorf("truncate torn tail: %w", err)
		}
		if err := s.log.Sync(); err != nil {
			return fmt.Errorf("sync truncated log: %w", err)
		}
		s.size = res.end
		slog.Warn("truncated torn tail", "segment", s.logPath(), "from", old, "to", res.end)
	}
	s.next = res.next
	s.maxTimestamp = res.maxTs
	if err := s.writeIndex(res.index); err != nil {
		return err
	}
	s.resetSinceIndex()
	return nil
}
