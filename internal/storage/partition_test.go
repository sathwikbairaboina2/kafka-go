package storage

import (
	"bytes"
	"testing"
	"time"

	"github.com/sathwikbairaboina2/kafka-go/internal/record"
	"pgregory.net/rapid"
)

func testCfg(segBytes, interval int64) Config {
	return Config{SegmentBytes: segBytes, IndexIntervalBytes: interval, RetentionMs: -1, RetentionBytes: -1}
}

// appendN appends one batch of n records and returns its base offset.
func appendN(t testing.TB, p *Partition, n, valueLen int, ts int64) int64 {
	t.Helper()
	b, h := mkBatch(t, 0, n, valueLen, ts)
	off, err := p.Append(b, h)
	if err != nil {
		t.Fatal(err)
	}
	return off
}

// scanAll reads the partition from its log start and returns every batch's [base,last] in order.
func scanAll(t testing.TB, p *Partition) [][2]int64 {
	t.Helper()
	var spans [][2]int64
	off := p.LogStartOffset()
	for off < p.HighWatermark() {
		data, err := p.Read(off, 1<<20)
		if err != nil {
			t.Fatalf("read at %d: %v", off, err)
		}
		parts, err := record.Split(data)
		if err != nil || len(parts) == 0 {
			t.Fatalf("split at %d: %v (%d parts)", off, err, len(parts))
		}
		for _, b := range parts {
			h, err := record.Parse(b)
			if err != nil {
				t.Fatal(err)
			}
			spans = append(spans, [2]int64{h.BaseOffset, h.BaseOffset + int64(h.LastOffsetDelta)})
			off = h.BaseOffset + int64(h.LastOffsetDelta) + 1
		}
	}
	return spans
}

func TestAppendOffsetsDense(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		dir := t.TempDir()
		seg := rapid.Int64Range(200, 5000).Draw(rt, "segBytes")
		p, err := OpenPartition(dir, testCfg(seg, 128))
		if err != nil {
			rt.Fatal(err)
		}
		defer p.Close()
		nb := rapid.IntRange(1, 50).Draw(rt, "batches")
		var total int64
		for i := 0; i < nb; i++ {
			n := rapid.IntRange(1, 20).Draw(rt, "records")
			want := p.HighWatermark()
			b, h := mkBatch(t, 0, n, 5, int64(i))
			off, err := p.Append(b, h)
			if err != nil {
				rt.Fatal(err)
			}
			if off != want {
				rt.Fatalf("append returned %d, want previous HW %d", off, want)
			}
			total += int64(n)
		}
		if p.HighWatermark() != total {
			rt.Fatalf("HW %d want %d", p.HighWatermark(), total)
		}
		spans := scanAll(t, p)
		for i := 1; i < len(spans); i++ {
			if spans[i][0] != spans[i-1][1]+1 {
				rt.Fatalf("gap between %v and %v", spans[i-1], spans[i])
			}
		}
		for off := int64(0); off < total; off++ {
			data, err := p.Read(off, 1<<20)
			if err != nil {
				rt.Fatal(err)
			}
			h, err := record.ReadHeader(data)
			if err != nil || off < h.BaseOffset || off > h.BaseOffset+int64(h.LastOffsetDelta) {
				rt.Fatalf("offset %d not in first batch %+v (%v)", off, h, err)
			}
		}
	})
}

func TestIndexLookupMatchesScan(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		dir := t.TempDir()
		p, err := OpenPartition(dir, testCfg(1<<30, rapid.Int64Range(1, 2048).Draw(rt, "interval")))
		if err != nil {
			rt.Fatal(err)
		}
		defer p.Close()
		nb := rapid.IntRange(1, 60).Draw(rt, "batches")
		for i := 0; i < nb; i++ {
			appendN(t, p, rapid.IntRange(1, 8).Draw(rt, "records"), rapid.IntRange(1, 100).Draw(rt, "len"), 1)
		}
		s := p.segments[0]
		for off := int64(0); off < p.HighWatermark(); off++ {
			got, ok := s.find(off)
			if !ok {
				rt.Fatalf("find(%d) failed", off)
			}
			// linear scan from position 0
			var want int64 = -1
			for pos := int64(0); pos < s.size; {
				pre, _ := s.readAt(pos, 27)
				first, last := batchBaseLast(pre)
				if off >= first && off <= last {
					want = pos
					break
				}
				size, _ := record.PeekSize(pre)
				pos += size
			}
			if got != want {
				rt.Fatalf("find(%d) = %d, scan = %d", off, got, want)
			}
		}
	})
}

func TestFetchReturnsWholeBatches(t *testing.T) {
	p, err := OpenPartition(t.TempDir(), testCfg(0, 0))
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	for i := 0; i < 3; i++ {
		appendN(t, p, 1, 1000, 1)
	}
	one, err := p.Read(0, 10)
	if err != nil {
		t.Fatal(err)
	}
	parts, err := record.Split(one)
	if err != nil || len(parts) != 1 {
		t.Fatalf("Read(0,10) gave %d batches (%v)", len(parts), err)
	}
	two, err := p.Read(0, 2500)
	if err != nil {
		t.Fatal(err)
	}
	parts, err = record.Split(two)
	if err != nil || len(parts) != 2 {
		t.Fatalf("Read(0,2500) gave %d batches (%v), %d bytes", len(parts), err, len(two))
	}
	all, err := p.Read(1, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if parts, _ = record.Split(all); len(parts) != 2 {
		t.Fatalf("Read(1) gave %d batches", len(parts))
	}
	if data, err := p.Read(3, 100); data != nil || err != nil {
		t.Fatalf("Read(HW) = %v, %v", data, err)
	}
	if _, err := p.Read(4, 100); err != ErrOffsetOutOfRange {
		t.Fatalf("Read(HW+1) err = %v", err)
	}
	if _, err := p.Read(-1, 100); err != ErrOffsetOutOfRange {
		t.Fatalf("Read(-1) err = %v", err)
	}
}

func TestReadAcrossSegments(t *testing.T) {
	p, err := OpenPartition(t.TempDir(), testCfg(300, 64))
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	for i := 0; i < 10; i++ {
		appendN(t, p, 1, 150, 1)
	}
	if len(p.segments) < 3 {
		t.Fatalf("expected several segments, got %d", len(p.segments))
	}
	data, err := p.Read(0, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	parts, err := record.Split(data)
	if err != nil || len(parts) != 10 {
		t.Fatalf("got %d batches across segments (%v)", len(parts), err)
	}
}

func TestWaitClosedOnAppend(t *testing.T) {
	p, err := OpenPartition(t.TempDir(), testCfg(0, 0))
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	ch := p.Wait()
	go func() {
		time.Sleep(20 * time.Millisecond)
		appendN(t, p, 1, 10, 1)
	}()
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("Wait channel not closed by Append")
	}
}

func TestReopenKeepsOffsets(t *testing.T) {
	dir := t.TempDir()
	p, err := OpenPartition(dir, testCfg(500, 64))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		appendN(t, p, 2, 30, int64(i))
	}
	hw := p.HighWatermark()
	before, _ := p.Read(0, 1<<20)
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	p2, err := OpenPartition(dir, testCfg(500, 64))
	if err != nil {
		t.Fatal(err)
	}
	defer p2.Close()
	if p2.HighWatermark() != hw {
		t.Fatalf("HW %d want %d", p2.HighWatermark(), hw)
	}
	after, _ := p2.Read(0, 1<<20)
	if !bytes.Equal(before, after) {
		t.Fatal("bytes differ after reopen")
	}
	if off := appendN(t, p2, 1, 5, 1); off != hw {
		t.Fatalf("next append at %d, want %d", off, hw)
	}
}

func TestOffsetForTimestamp(t *testing.T) {
	p, err := OpenPartition(t.TempDir(), testCfg(400, 64))
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	for i := 0; i < 10; i++ {
		appendN(t, p, 2, 20, int64(1000+i*100)) // batch i: base 2i, max ts 1000+100i+1
	}
	if off, _ := p.OffsetForTimestamp(-1); off != 20 {
		t.Fatalf("latest = %d", off)
	}
	if off, _ := p.OffsetForTimestamp(-2); off != 0 {
		t.Fatalf("earliest = %d", off)
	}
	if off, _ := p.OffsetForTimestamp(1350); off != 8 { // first batch with max ts >= 1350 is i=4 (max 1401)
		t.Fatalf("ts 1350 -> %d", off)
	}
	if off, _ := p.OffsetForTimestamp(9999); off != 20 {
		t.Fatalf("ts beyond = %d", off)
	}
}
