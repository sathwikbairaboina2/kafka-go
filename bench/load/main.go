// Command load is the franz-go produce load generator used for the kgod versus Apache Kafka comparison.
// Every client produces 1 KiB records with acks=all in a closed loop; latency is the time from the
// Produce call to its acknowledgement.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

type config struct {
	Brokers     string        `json:"brokers"`
	Topic       string        `json:"topic"`
	RecordBytes int           `json:"record_bytes"`
	Duration    time.Duration `json:"-"`
	Warmup      time.Duration `json:"-"`
	Runs        int           `json:"runs"`
	Clients     int           `json:"clients"`
	Label       string        `json:"label"`
	Out         string        `json:"-"`

	DurationS float64 `json:"duration_s"`
	WarmupS   float64 `json:"warmup_s"`
	Go        string  `json:"go"`
	CPUs      int     `json:"cpus"`
	GOOS      string  `json:"goos"`
	Date      string  `json:"date"`
}

// RunStats is the outcome of one measured run.
type RunStats struct {
	Records       int64   `json:"records"`
	RecordsPerSec float64 `json:"records_per_s"`
	MBPerSec      float64 `json:"mb_per_s"` // 1 MB = 1e6 bytes
	P50Ms         float64 `json:"p50_ms"`
	P99Ms         float64 `json:"p99_ms"`
	P999Ms        float64 `json:"p999_ms"`
	Errors        int64   `json:"errors"`
}

// Result is the JSON file written to -out.
type Result struct {
	Label  string     `json:"label"`
	Config config     `json:"config"`
	Runs   []RunStats `json:"runs"`
	Median RunStats   `json:"median"`
}

func main() {
	var c config
	flag.StringVar(&c.Brokers, "brokers", "127.0.0.1:9092", "comma separated seed brokers")
	flag.StringVar(&c.Topic, "topic", "bench", "topic to produce to (must exist)")
	flag.IntVar(&c.RecordBytes, "record-bytes", 1024, "value size")
	flag.DurationVar(&c.Duration, "duration", 30*time.Second, "measured time per run")
	flag.DurationVar(&c.Warmup, "warmup", 3*time.Second, "unmeasured time before each run")
	flag.IntVar(&c.Runs, "runs", 3, "number of runs")
	flag.IntVar(&c.Clients, "clients", 4, "producer clients")
	flag.StringVar(&c.Label, "label", "", "name of the target, e.g. kgod-never")
	flag.StringVar(&c.Out, "out", "", "write the JSON result here")
	flag.Parse()

	res, err := runLoad(c)
	for i, r := range res.Runs {
		fmt.Printf("%s run %d: %.1f MB/s %.0f records/s p50=%.2fms p99=%.2fms p999=%.2fms errors=%d\n",
			c.Label, i+1, r.MBPerSec, r.RecordsPerSec, r.P50Ms, r.P99Ms, r.P999Ms, r.Errors)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "load:", err)
		os.Exit(1)
	}
	if c.Out != "" {
		raw, _ := json.MarshalIndent(res, "", "  ")
		if err := os.WriteFile(c.Out, append(raw, '\n'), 0o644); err != nil {
			fmt.Fprintln(os.Stderr, "load:", err)
			os.Exit(1)
		}
	}
	for _, r := range res.Runs {
		if r.Errors > 0 {
			os.Exit(1)
		}
	}
}

func runLoad(c config) (Result, error) {
	c.DurationS, c.WarmupS = c.Duration.Seconds(), c.Warmup.Seconds()
	c.Go, c.CPUs, c.GOOS, c.Date = runtime.Version(), runtime.NumCPU(), runtime.GOOS, time.Now().UTC().Format(time.RFC3339)
	res := Result{Label: c.Label, Config: c}
	for i := 0; i < c.Runs; i++ {
		st, err := oneRun(c)
		res.Runs = append(res.Runs, st)
		if err != nil {
			return res, fmt.Errorf("run %d: %w", i+1, err)
		}
	}
	if len(res.Runs) > 0 {
		sorted := append([]RunStats(nil), res.Runs...)
		sort.Slice(sorted, func(i, j int) bool { return sorted[i].MBPerSec < sorted[j].MBPerSec })
		res.Median = sorted[len(sorted)/2]
	}
	return res, nil
}

func oneRun(c config) (RunStats, error) {
	value := make([]byte, c.RecordBytes)
	rng := rand.New(rand.NewPCG(7, 11))
	for i := range value {
		value[i] = byte(rng.IntN(256))
	}
	start := time.Now()
	measureFrom, measureTo := start.Add(c.Warmup), start.Add(c.Warmup+c.Duration)

	var errCount, firstErrSet atomic.Int64
	var firstErr atomic.Value
	hists := make([]*Hist, c.Clients)
	counts := make([]int64, c.Clients)
	ctx, cancel := context.WithDeadline(context.Background(), measureTo)
	defer cancel()

	var wg sync.WaitGroup
	for ci := 0; ci < c.Clients; ci++ {
		cl, err := kgo.NewClient(
			kgo.SeedBrokers(strings.Split(c.Brokers, ",")...), kgo.DisableIdempotentWrite(),
			kgo.RequiredAcks(kgo.AllISRAcks()), kgo.ProducerLinger(5*time.Millisecond),
			kgo.MaxBufferedRecords(10000), kgo.ProducerBatchMaxBytes(1<<20),
		)
		if err != nil {
			return RunStats{}, err
		}
		h := NewHist()
		hists[ci] = h
		wg.Add(1)
		go func(ci int, cl *kgo.Client, h *Hist) {
			defer wg.Done()
			var mu sync.Mutex
			var n int64
			for ctx.Err() == nil {
				sent := time.Now()
				cl.Produce(ctx, &kgo.Record{Topic: c.Topic, Value: value}, func(_ *kgo.Record, err error) {
					now := time.Now()
					if err != nil {
						if now.Before(measureTo) && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
							errCount.Add(1)
							if firstErrSet.CompareAndSwap(0, 1) {
								firstErr.Store(err.Error())
							}
						}
						return
					}
					if now.Before(measureFrom) || !now.Before(measureTo) {
						return
					}
					mu.Lock()
					h.Record(now.Sub(sent))
					n++
					mu.Unlock()
				})
			}
			flushCtx, flushCancel := context.WithTimeout(context.Background(), 10*time.Second)
			_ = cl.Flush(flushCtx)
			flushCancel()
			cl.Close()
			mu.Lock()
			counts[ci] = n
			mu.Unlock()
		}(ci, cl, h)
	}
	wg.Wait()

	all := NewHist()
	var total int64
	for i, h := range hists {
		all.Merge(h)
		total += counts[i]
	}
	secs := c.Duration.Seconds()
	st := RunStats{
		Records:       total,
		RecordsPerSec: float64(total) / secs,
		MBPerSec:      float64(total) * float64(c.RecordBytes) / 1e6 / secs,
		P50Ms:         float64(all.Quantile(0.5)) / float64(time.Millisecond),
		P99Ms:         float64(all.Quantile(0.99)) / float64(time.Millisecond),
		P999Ms:        float64(all.Quantile(0.999)) / float64(time.Millisecond),
		Errors:        errCount.Load(),
	}
	if st.Errors > 0 {
		return st, fmt.Errorf("%d produce errors, first: %v", st.Errors, firstErr.Load())
	}
	return st, nil
}
