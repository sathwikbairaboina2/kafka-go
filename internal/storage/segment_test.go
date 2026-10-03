package storage

import (
	"encoding/binary"
	"math/rand/v2"
	"testing"

	"github.com/sathwikbairaboina2/kafka-go/internal/record"
)

// mkBatch builds a valid batch of n records of valueLen bytes with the given base offset.
func mkBatch(t testing.TB, base int64, n, valueLen int, ts int64) ([]byte, record.Header) {
	t.Helper()
	recs := make([]record.Record, n)
	for i := range recs {
		v := make([]byte, valueLen)
		for j := range v {
			v[j] = byte(i + j)
		}
		recs[i] = record.Record{Value: v, TimestampDelta: int64(i)}
	}
	b := record.Build(ts, recs)
	record.SetBaseOffset(b, base)
	h, err := record.Parse(b)
	if err != nil {
		t.Fatal(err)
	}
	return b, h
}

func batchBaseLast(b []byte) (int64, int64) {
	base := int64(binary.BigEndian.Uint64(b))
	return base, base + int64(int32(binary.BigEndian.Uint32(b[23:])))
}

func TestSegmentAppendAndFind(t *testing.T) {
	dir := t.TempDir()
	s, err := createSegment(dir, 0, 256)
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	rng := rand.New(rand.NewPCG(1, 2))
	var next int64
	type span struct{ first, last int64 }
	var spans []span
	for i := 0; i < 50; i++ {
		n := 1 + rng.IntN(5)
		b, h := mkBatch(t, next, n, 20+rng.IntN(60), 1000)
		if err := s.append(b, h); err != nil {
			t.Fatal(err)
		}
		spans = append(spans, span{next, next + int64(n) - 1})
		next += int64(n)
	}
	if s.next != next {
		t.Fatalf("next = %d want %d", s.next, next)
	}
	for _, sp := range spans {
		for off := sp.first; off <= sp.last; off++ {
			pos, ok := s.find(off)
			if !ok {
				t.Fatalf("offset %d not found", off)
			}
			pre, err := s.readAt(pos, 27)
			if err != nil {
				t.Fatal(err)
			}
			first, last := batchBaseLast(pre)
			if first != sp.first || last != sp.last {
				t.Fatalf("offset %d resolved to batch %d..%d, want %d..%d", off, first, last, sp.first, sp.last)
			}
		}
	}
	if _, ok := s.find(next); ok {
		t.Fatal("offset past the end must not be found")
	}
}

func TestSegmentIndexSparse(t *testing.T) {
	dir := t.TempDir()
	s, err := createSegment(dir, 0, 4096)
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	var next int64
	for i := 0; i < 100; i++ {
		b, h := mkBatch(t, next, 3, 80, 1)
		if err := s.append(b, h); err != nil {
			t.Fatal(err)
		}
		next += 3
	}
	want := int(s.size / 4096)
	if got := len(s.index); got < want-1 || got > want+1 {
		t.Fatalf("index entries = %d, want about %d (size %d)", got, want, s.size)
	}
	for i := 1; i < len(s.index); i++ {
		if s.index[i].rel <= s.index[i-1].rel || s.index[i].pos <= s.index[i-1].pos {
			t.Fatalf("index not strictly increasing at %d: %v", i, s.index)
		}
	}
	if s.index[0].pos == 0 {
		t.Fatal("first entry must not be position 0 (implied)")
	}
}

func TestSegmentReopen(t *testing.T) {
	dir := t.TempDir()
	s, err := createSegment(dir, 40, 128)
	if err != nil {
		t.Fatal(err)
	}
	next := int64(40)
	for i := 0; i < 30; i++ {
		b, h := mkBatch(t, next, 2, 40, 5000+int64(i))
		if err := s.append(b, h); err != nil {
			t.Fatal(err)
		}
		next += 2
	}
	wantIdx := append([]indexEntry(nil), s.index...)
	wantSize, wantMax := s.size, s.maxTimestamp
	if err := s.close(); err != nil {
		t.Fatal(err)
	}
	s2, err := openSegment(dir, 40, 128)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.close()
	if s2.next != next || s2.size != wantSize || s2.maxTimestamp != wantMax {
		t.Fatalf("reopen next=%d size=%d max=%d, want %d %d %d", s2.next, s2.size, s2.maxTimestamp, next, wantSize, wantMax)
	}
	if len(s2.index) != len(wantIdx) {
		t.Fatalf("index len %d want %d", len(s2.index), len(wantIdx))
	}
	for i := range wantIdx {
		if s2.index[i] != wantIdx[i] {
			t.Fatalf("index[%d] = %v want %v", i, s2.index[i], wantIdx[i])
		}
	}
	// appending after reopen keeps indexing going
	b, h := mkBatch(t, next, 2, 40, 1)
	if err := s2.append(b, h); err != nil {
		t.Fatal(err)
	}
}
