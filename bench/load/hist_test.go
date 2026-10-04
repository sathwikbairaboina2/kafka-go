package main

import (
	"math"
	"testing"
	"time"
)

func TestQuantilesWithinOnePercent(t *testing.T) {
	h := NewHist()
	for ms := 1; ms <= 10000; ms++ {
		h.Record(time.Duration(ms) * time.Millisecond)
	}
	for _, q := range []float64{0.5, 0.9, 0.99, 0.999} {
		want := q * 10000 // ms
		got := float64(h.Quantile(q)) / float64(time.Millisecond)
		if math.Abs(got-want)/want > 0.01 {
			t.Errorf("q=%v: got %.2f ms, want %.2f ms", q, got, want)
		}
	}
	if h.Count() != 10000 {
		t.Fatalf("count = %d", h.Count())
	}
}

func TestMergeAndEdges(t *testing.T) {
	a, b := NewHist(), NewHist()
	a.Record(time.Millisecond)
	b.Record(time.Hour) // clamps to the last bucket
	b.Record(0)         // clamps to the first
	a.Merge(b)
	if a.Count() != 3 {
		t.Fatalf("merged count = %d", a.Count())
	}
	if q := a.Quantile(1); q < 50*time.Second {
		t.Fatalf("max quantile = %v", q)
	}
	if q := NewHist().Quantile(0.5); q != 0 {
		t.Fatalf("empty quantile = %v", q)
	}
}
