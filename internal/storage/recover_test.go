package storage

import (
	"bytes"
	"math/rand/v2"
	"os"
	"path/filepath"
	"testing"

	"github.com/sathwikbairaboina2/kafka-go/internal/record"
)

func logPathOf(dir string) string { return filepath.Join(dir, segmentName(0)+".log") }

func appendFile(t testing.TB, path string, data []byte) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.Write(data); err != nil {
		t.Fatal(err)
	}
}

func fillPartition(t testing.TB, dir string, batches int, cfg Config) (hw, size int64) {
	t.Helper()
	p, err := OpenPartition(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < batches; i++ {
		appendN(t, p, 2, 50, int64(i))
	}
	hw = p.HighWatermark()
	size = p.active().size
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	return hw, size
}

// scanPos returns the position of the batch containing off by walking from position 0.
func scanPos(s *segment, off int64) int64 {
	for pos := int64(0); pos < s.size; {
		pre, _ := s.readAt(pos, 27)
		first, last := batchBaseLast(pre)
		if off >= first && off <= last {
			return pos
		}
		size, _ := record.PeekSize(pre)
		pos += size
	}
	return -1
}

func TestRecoveryTruncatesTornWrite(t *testing.T) {
	dir := t.TempDir()
	cfg := testCfg(0, 128)
	hw, size := fillPartition(t, dir, 10, cfg)

	eleventh, _ := mkBatch(t, hw, 2, 50, 99)
	garbage := make([]byte, 37)
	for i := range garbage {
		garbage[i] = byte(rand.New(rand.NewPCG(7, uint64(i))).IntN(256))
	}
	appendFile(t, logPathOf(dir), append(eleventh[:len(eleventh)/2], garbage...))

	p, err := OpenPartition(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if p.HighWatermark() != hw {
		t.Fatalf("HW %d want %d", p.HighWatermark(), hw)
	}
	st, _ := os.Stat(logPathOf(dir))
	if st.Size() != size {
		t.Fatalf("file size %d want %d", st.Size(), size)
	}
	s := p.active()
	if len(s.index) == 0 {
		t.Fatal("expected index entries with a 128 byte interval")
	}
	for _, e := range s.index {
		pre, err := s.readAt(int64(e.pos), 27)
		if err != nil {
			t.Fatal(err)
		}
		first, _ := batchBaseLast(pre)
		if first != s.base+int64(e.rel) {
			t.Fatalf("index entry %v points at batch %d", e, first)
		}
	}
	if off := appendN(t, p, 1, 5, 1); off != hw {
		t.Fatalf("append after recovery at %d, want %d", off, hw)
	}
}

func TestRecoveryStopsAtCRCFlip(t *testing.T) {
	dir := t.TempDir()
	cfg := testCfg(0, 128)
	fillPartition(t, dir, 10, cfg)
	raw, _ := os.ReadFile(logPathOf(dir))
	bs := len(raw) / 10 // all batches have the same size
	raw[7*bs+record.HeaderSize+3] ^= 0xff
	if err := os.WriteFile(logPathOf(dir), raw, 0o644); err != nil {
		t.Fatal(err)
	}
	p, err := OpenPartition(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if p.HighWatermark() != 14 { // batch 7 has base offset 7*2
		t.Fatalf("HW %d want 14", p.HighWatermark())
	}
}

func TestRecoveryRebuildsBadIndex(t *testing.T) {
	dir := t.TempDir()
	cfg := testCfg(400, 64) // several segments
	fillPartition(t, dir, 20, cfg)
	matches, _ := filepath.Glob(filepath.Join(dir, "*.index"))
	if len(matches) < 3 {
		t.Fatalf("expected several segments, got %d", len(matches))
	}
	for _, m := range matches {
		if err := os.Truncate(m, 5); err != nil {
			t.Fatal(err)
		}
	}
	p, err := OpenPartition(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	for _, s := range p.segments {
		for off := s.base; off < s.next; off++ {
			got, ok := s.find(off)
			if !ok {
				t.Fatalf("find(%d) failed", off)
			}
			if want := scanPos(s, off); got != want {
				t.Fatalf("find(%d) = %d, scan = %d", off, got, want)
			}
		}
	}
}

func FuzzRecoverSegment(f *testing.F) {
	var seed []byte
	var next int64
	for i := 0; i < 5; i++ {
		b, _ := mkBatch(f, next, 3, 30, int64(i))
		seed = append(seed, b...)
		next += 3
	}
	f.Add([]byte{})
	f.Add([]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13})
	f.Add(seed[:40])
	f.Fuzz(func(t *testing.T, extra []byte) {
		dir := t.TempDir()
		data := append(bytes.Clone(seed), extra...)
		if err := os.WriteFile(logPathOf(dir), data, 0o644); err != nil {
			t.Fatal(err)
		}
		p, err := OpenPartition(dir, testCfg(0, 64))
		if err != nil {
			t.Fatal(err)
		}
		defer p.Close()
		if p.HighWatermark() < 15 {
			t.Fatalf("lost good batches: HW %d", p.HighWatermark())
		}
		raw, _ := os.ReadFile(logPathOf(dir))
		var pos int
		var want int64
		for pos < len(raw) {
			size, err := record.PeekSize(raw[pos:])
			if err != nil || pos+int(size) > len(raw) {
				t.Fatalf("log does not end on a batch boundary at %d", pos)
			}
			h, err := record.Parse(raw[pos : pos+int(size)])
			if err != nil || h.BaseOffset != want {
				t.Fatalf("batch at %d invalid: %v base %d want %d", pos, err, h.BaseOffset, want)
			}
			want = h.BaseOffset + int64(h.LastOffsetDelta) + 1
			pos += int(size)
		}
		if want != p.HighWatermark() {
			t.Fatalf("HW %d but log ends at %d", p.HighWatermark(), want)
		}
	})
}
