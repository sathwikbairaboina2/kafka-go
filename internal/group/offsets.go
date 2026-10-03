package group

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"

	"github.com/sathwikbairaboina2/kafka-go/internal/protocol"
)

// OffsetKey identifies one committed offset.
type OffsetKey struct {
	Group, Topic string
	Partition    int32
}

// Committed is a stored offset.
type Committed struct {
	Offset       int64
	LeaderEpoch  int32
	Metadata     *string
	CommitTimeMs int64
}

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

const (
	frameHeader   = 8 // uint32 length, uint32 crc32c
	maxFrameBytes = 1 << 20
)

// OffsetStore is an append-only, CRC-framed log of committed offsets that is replayed on open.
type OffsetStore struct {
	mu      sync.Mutex
	f       *os.File
	end     int64
	sync    bool
	entries map[OffsetKey]Committed
}

// OpenOffsetStore replays <dir>/commits.log. A torn or corrupt tail is truncated.
// With syncEachCommit every Commit is fsynced.
func OpenOffsetStore(dir string, syncEachCommit bool) (*OffsetStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create offsets dir: %w", err)
	}
	path := filepath.Join(dir, "commits.log")
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open commits.log: %w", err)
	}
	s := &OffsetStore{f: f, sync: syncEachCommit, entries: map[OffsetKey]Committed{}}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	raw := make([]byte, st.Size())
	if _, err := io.ReadFull(io.NewSectionReader(f, 0, st.Size()), raw); err != nil {
		f.Close()
		return nil, fmt.Errorf("read commits.log: %w", err)
	}
	var pos int64
	for pos+frameHeader <= int64(len(raw)) {
		n := int64(binary.BigEndian.Uint32(raw[pos:]))
		sum := binary.BigEndian.Uint32(raw[pos+4:])
		if n > maxFrameBytes || pos+frameHeader+n > int64(len(raw)) {
			break
		}
		payload := raw[pos+frameHeader : pos+frameHeader+n]
		if crc32.Checksum(payload, castagnoli) != sum {
			break
		}
		k, c, err := decodeCommit(payload)
		if err != nil {
			break
		}
		s.entries[k] = c
		pos += frameHeader + n
	}
	if pos < int64(len(raw)) {
		slog.Warn("truncated torn tail", "file", path, "from", len(raw), "to", pos)
		if err := f.Truncate(pos); err != nil {
			f.Close()
			return nil, fmt.Errorf("truncate commits.log: %w", err)
		}
		if err := f.Sync(); err != nil {
			f.Close()
			return nil, err
		}
	}
	s.end = pos
	return s, nil
}

func encodeCommit(k OffsetKey, c Committed) []byte {
	w := protocol.NewWriter(64)
	w.Int16(1)
	w.String(k.Group)
	w.String(k.Topic)
	w.Int32(k.Partition)
	w.Int64(c.Offset)
	w.Int32(c.LeaderEpoch)
	w.NullableString(c.Metadata)
	w.Int64(c.CommitTimeMs)
	payload := w.Buf()
	out := make([]byte, frameHeader, frameHeader+len(payload))
	binary.BigEndian.PutUint32(out, uint32(len(payload)))
	binary.BigEndian.PutUint32(out[4:], crc32.Checksum(payload, castagnoli))
	return append(out, payload...)
}

func decodeCommit(payload []byte) (OffsetKey, Committed, error) {
	r := protocol.NewReader(payload)
	if v := r.Int16(); v != 1 {
		return OffsetKey{}, Committed{}, fmt.Errorf("unknown commit record version %d", v)
	}
	var k OffsetKey
	var c Committed
	k.Group = r.String()
	k.Topic = r.String()
	k.Partition = r.Int32()
	c.Offset = r.Int64()
	c.LeaderEpoch = r.Int32()
	c.Metadata = r.NullableString()
	c.CommitTimeMs = r.Int64()
	if err := r.Err(); err != nil {
		return OffsetKey{}, Committed{}, err
	}
	return k, c, nil
}

// Commit appends all entries with one write call (and one fsync when configured) and applies them.
func (s *OffsetStore) Commit(entries map[OffsetKey]Committed) error {
	if len(entries) == 0 {
		return nil
	}
	var buf []byte
	for k, c := range entries {
		buf = append(buf, encodeCommit(k, c)...)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.f.WriteAt(buf, s.end); err != nil {
		_ = s.f.Truncate(s.end)
		return fmt.Errorf("write commits.log: %w", err)
	}
	if s.sync {
		if err := s.f.Sync(); err != nil {
			return fmt.Errorf("sync commits.log: %w", err)
		}
	}
	s.end += int64(len(buf))
	for k, c := range entries {
		s.entries[k] = c
	}
	return nil
}

// Get returns the committed offset for k.
func (s *OffsetStore) Get(k OffsetKey) (Committed, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.entries[k]
	return c, ok
}

// ForGroup returns every committed offset of a group.
func (s *OffsetStore) ForGroup(group string) map[OffsetKey]Committed {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[OffsetKey]Committed{}
	for k, c := range s.entries {
		if k.Group == group {
			out[k] = c
		}
	}
	return out
}

// Sync flushes the file.
func (s *OffsetStore) Sync() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.f.Sync()
}

// Close syncs and closes the file.
func (s *OffsetStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.f.Sync(); err != nil {
		s.f.Close()
		return err
	}
	return s.f.Close()
}
