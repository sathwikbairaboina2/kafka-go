package storage

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"pgregory.net/rapid"
)

func TestRetentionKeepsActiveSegment(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		cfg := Config{
			SegmentBytes:       rapid.Int64Range(200, 2000).Draw(rt, "seg"),
			IndexIntervalBytes: 64,
			RetentionMs:        rapid.Int64Range(-1, 5000).Draw(rt, "retMs"),
			RetentionBytes:     rapid.Int64Range(-1, 3000).Draw(rt, "retBytes"),
		}
		p, err := OpenPartition(t.TempDir(), cfg)
		if err != nil {
			rt.Fatal(err)
		}
		defer p.Close()
		clock := time.UnixMilli(1_000_000)
		var lastStart int64
		ops := rapid.IntRange(1, 80).Draw(rt, "ops")
		for i := 0; i < ops; i++ {
			switch rapid.IntRange(0, 2).Draw(rt, "op") {
			case 0:
				appendN(t, p, rapid.IntRange(1, 5).Draw(rt, "n"), 30, clock.UnixMilli())
			case 1:
				clock = clock.Add(time.Duration(rapid.IntRange(0, 3000).Draw(rt, "adv")) * time.Millisecond)
			case 2:
				hw := p.HighWatermark()
				if _, err := p.Retain(clock); err != nil {
					rt.Fatal(err)
				}
				if p.HighWatermark() != hw {
					rt.Fatalf("Retain changed the high watermark %d -> %d", hw, p.HighWatermark())
				}
			}
			if len(p.segments) == 0 {
				rt.Fatal("active segment deleted")
			}
			if ls := p.LogStartOffset(); ls < lastStart {
				rt.Fatalf("log start decreased %d -> %d", lastStart, ls)
			} else {
				lastStart = ls
			}
			if p.LogStartOffset() < p.HighWatermark() {
				if _, err := p.Read(p.LogStartOffset(), 1<<20); err != nil {
					rt.Fatalf("read at log start: %v", err)
				}
			}
		}
	})
}

func TestRetentionBySize(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{SegmentBytes: 1100, IndexIntervalBytes: 64, RetentionMs: -1, RetentionBytes: 3000}
	p, err := OpenPartition(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	for i := 0; i < 10; i++ {
		appendN(t, p, 1, 1000, 1) // one batch of ~1.07 KiB per segment
	}
	if len(p.segments) != 10 {
		t.Fatalf("segments = %d, want 10", len(p.segments))
	}
	n, err := p.Retain(time.UnixMilli(10))
	if err != nil {
		t.Fatal(err)
	}
	if n == 0 {
		t.Fatal("nothing deleted")
	}
	if rest := p.size() - p.segments[0].size; rest >= 3000 {
		t.Fatalf("rule not satisfied: %d bytes beyond the oldest segment", rest)
	}
	if p.LogStartOffset() != int64(n) {
		t.Fatalf("log start %d after deleting %d single-record segments", p.LogStartOffset(), n)
	}
	files, _ := filepath.Glob(filepath.Join(dir, "*.log"))
	if len(files) != 10-n {
		t.Fatalf("%d files on disk, want %d", len(files), 10-n)
	}
	if _, err := p.Read(0, 100); err != ErrOffsetOutOfRange {
		t.Fatalf("Read(0) after retention err = %v", err)
	}
}

func TestRetentionByTime(t *testing.T) {
	cfg := Config{SegmentBytes: 300, IndexIntervalBytes: 64, RetentionMs: 1000, RetentionBytes: -1}
	p, err := OpenPartition(t.TempDir(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	for i := 0; i < 6; i++ {
		appendN(t, p, 1, 200, 5000+int64(i)*1000) // each batch seals its own segment
	}
	// now = 8500: segments with max ts < 7500 go (ts 5000..7000 -> three of them, the 4th has 8000)
	n, err := p.Retain(time.UnixMilli(8500))
	if err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Fatalf("deleted %d, want 3", n)
	}
	// far future: everything sealed goes, the active segment stays
	if _, err := p.Retain(time.UnixMilli(1 << 40)); err != nil {
		t.Fatal(err)
	}
	if len(p.segments) != 1 {
		t.Fatalf("segments left = %d, want just the active one", len(p.segments))
	}
}

func TestManagerEnsureIdempotent(t *testing.T) {
	m := NewManager(t.TempDir(), testCfg(0, 0), nil)
	defer m.Close()
	if err := m.Ensure("orders", 3); err != nil {
		t.Fatal(err)
	}
	p0, _ := m.Get("orders", 0)
	appendN(t, p0, 2, 10, 1)
	if err := m.Ensure("orders", 3); err != nil {
		t.Fatal(err)
	}
	if again, _ := m.Get("orders", 0); again != p0 || again.HighWatermark() != 2 {
		t.Fatal("Ensure replaced an open partition")
	}
	if _, ok := m.Get("orders", 3); ok {
		t.Fatal("partition 3 should not exist")
	}
	if _, ok := m.Get("nope", 0); ok {
		t.Fatal("unknown topic should not exist")
	}
}

func TestManagerReopen(t *testing.T) {
	dir := t.TempDir()
	m := NewManager(dir, testCfg(0, 0), nil)
	if err := m.Ensure("t", 3); err != nil {
		t.Fatal(err)
	}
	for i := int32(0); i < 3; i++ {
		p, _ := m.Get("t", i)
		appendN(t, p, int(i)+1, 10, 1)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	m2 := NewManager(dir, testCfg(0, 0), nil)
	defer m2.Close()
	if err := m2.Ensure("t", 3); err != nil {
		t.Fatal(err)
	}
	for i := int32(0); i < 3; i++ {
		p, ok := m2.Get("t", i)
		if !ok || p.HighWatermark() != int64(i)+1 {
			t.Fatalf("partition %d: ok=%v HW=%d", i, ok, p.HighWatermark())
		}
	}
}

func TestVerifyDirOK(t *testing.T) {
	dir := t.TempDir()
	m := NewManager(dir, testCfg(400, 64), nil)
	if err := m.Ensure("t", 2); err != nil {
		t.Fatal(err)
	}
	for i := int32(0); i < 2; i++ {
		p, _ := m.Get("t", i)
		for j := 0; j < 12; j++ {
			appendN(t, p, 2, 40, 1)
		}
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(dir, "meta"), 0o755); err != nil {
		t.Fatal(err)
	}
	rep, err := VerifyDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.Problems) != 0 || rep.Partitions != 2 || rep.Records != 48 || rep.Batches != 24 || rep.Segments < 4 {
		t.Fatalf("report = %+v", rep)
	}
}

func TestVerifyDirDetectsGap(t *testing.T) {
	dir := t.TempDir()
	m := NewManager(dir, testCfg(400, 64), nil)
	if err := m.Ensure("t", 1); err != nil {
		t.Fatal(err)
	}
	p, _ := m.Get("t", 0)
	for j := 0; j < 12; j++ {
		appendN(t, p, 2, 40, 1)
	}
	m.Close()
	logs, _ := filepath.Glob(filepath.Join(dir, "t-0", "*.log"))
	if len(logs) < 3 {
		t.Fatalf("need 3+ segments, have %d", len(logs))
	}
	mid := logs[1]
	os.Remove(mid)
	os.Remove(mid[:len(mid)-4] + ".index")
	rep, err := VerifyDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, pr := range rep.Problems {
		if strings.Contains(pr, "gap") {
			found = true
		}
	}
	if !found {
		t.Fatalf("no gap problem reported: %v", rep.Problems)
	}
}
