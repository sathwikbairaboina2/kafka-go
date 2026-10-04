package main

import (
	"math"
	"time"
)

const (
	histMin    = 10 * time.Microsecond
	histMax    = 60 * time.Second
	histFactor = 1.01
)

var histBuckets = int(math.Ceil(math.Log(float64(histMax)/float64(histMin))/math.Log(histFactor))) + 1

// Hist is a log-bucketed latency histogram: each bucket is 1% wider than the one before, so a
// quantile is accurate to about half a percent. It is not safe for concurrent use; give each
// goroutine its own and Merge them.
type Hist struct {
	counts []uint64
	total  uint64
}

// NewHist returns an empty histogram.
func NewHist() *Hist { return &Hist{counts: make([]uint64, histBuckets)} }

func bucketOf(d time.Duration) int {
	if d <= histMin {
		return 0
	}
	i := int(math.Ceil(math.Log(float64(d)/float64(histMin)) / math.Log(histFactor)))
	if i >= histBuckets {
		return histBuckets - 1
	}
	return i
}

// Record adds one observation.
func (h *Hist) Record(d time.Duration) {
	h.counts[bucketOf(d)]++
	h.total++
}

// Count is the number of observations.
func (h *Hist) Count() uint64 { return h.total }

// Merge adds o into h.
func (h *Hist) Merge(o *Hist) {
	for i, c := range o.counts {
		h.counts[i] += c
	}
	h.total += o.total
}

// Quantile returns the q-quantile (0 < q <= 1) as the midpoint of the bucket that holds it.
func (h *Hist) Quantile(q float64) time.Duration {
	if h.total == 0 {
		return 0
	}
	rank := uint64(math.Ceil(q * float64(h.total)))
	if rank < 1 {
		rank = 1
	}
	var seen uint64
	for i, c := range h.counts {
		seen += c
		if seen >= rank {
			if i == 0 {
				return histMin
			}
			upper := float64(histMin) * math.Pow(histFactor, float64(i))
			return time.Duration(upper / math.Sqrt(histFactor))
		}
	}
	return histMax
}
