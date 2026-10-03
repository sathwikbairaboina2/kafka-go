package storage

import (
	"fmt"
	"time"
)

// Retain deletes whole sealed segments from the front of the log: by time when RetentionMs >= 0 and the
// segment's newest timestamp is older than now-RetentionMs, and by size while RetentionBytes >= 0 and the
// log would still hold at least RetentionBytes without the oldest segment. The active segment is never
// deleted, and the log start offset moves to the first remaining base.
func (p *Partition) Retain(now time.Time) (deleted int, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return 0, ErrClosed
	}
	for len(p.segments) > 1 {
		s := p.segments[0]
		byTime := p.cfg.RetentionMs >= 0 && s.maxTimestamp >= 0 && s.maxTimestamp < now.UnixMilli()-p.cfg.RetentionMs
		bySize := p.cfg.RetentionBytes >= 0 && p.size()-s.size >= p.cfg.RetentionBytes
		if !byTime && !bySize {
			break
		}
		if err := s.remove(); err != nil {
			return deleted, fmt.Errorf("delete segment %d: %w", s.base, err)
		}
		p.segments = p.segments[1:]
		p.logStart = p.segments[0].base
		deleted++
	}
	return deleted, nil
}
