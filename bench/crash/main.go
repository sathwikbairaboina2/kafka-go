// Command crash runs the kill -9 loop from ADR 0005: it produces to a kgod child process with acks=all,
// kills the process with SIGKILL at random moments, restarts it on the same data directory, and finally
// checks that every acknowledged record is still readable at the same offset with the same value.
//
// It kills the broker process only; it does not simulate power loss (ADR 0005).
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand/v2"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"syscall"
	"time"

	"github.com/sathwikbairaboina2/kafka-go/internal/storage"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type options struct {
	Iterations int
	Fsync      string // always, never or both
	Seed       int64
	Out        string
}

// ModeResult is the outcome for one fsync mode.
type ModeResult struct {
	Mode          string  `json:"mode"`
	Iterations    int     `json:"iterations"`
	Acked         int     `json:"acked"`
	Lost          int     `json:"lost"`
	VerifyOK      bool    `json:"verify_ok"`
	RecoveryMsP50 float64 `json:"recovery_ms_p50"`
	RecoveryMsMax float64 `json:"recovery_ms_max"`
	Seed          int64   `json:"seed"`
	Go            string  `json:"go"`
	CPUs          int     `json:"cpus"`
	GOOS          string  `json:"goos"`
	Date          string  `json:"date"`
}

// Result is the file written to -out.
type Result struct {
	Runs []ModeResult `json:"runs"`
}

func main() {
	var o options
	flag.IntVar(&o.Iterations, "iterations", 20, "kill -9 cycles per fsync mode")
	flag.StringVar(&o.Fsync, "fsync", "both", "always, never or both")
	flag.Int64Var(&o.Seed, "seed", 0, "random seed (default: time based)")
	flag.StringVar(&o.Out, "out", "", "write the JSON result here")
	flag.Parse()
	if o.Seed == 0 {
		o.Seed = time.Now().UnixNano()
	}
	fmt.Printf("seed=%d\n", o.Seed)
	res, err := run(o)
	for _, r := range res.Runs {
		fmt.Printf("mode=%s acked=%d lost=%d verify=%s iterations=%d recovery_ms_p50=%.0f recovery_ms_max=%.0f\n",
			r.Mode, r.Acked, r.Lost, okStr(r.VerifyOK), r.Iterations, r.RecoveryMsP50, r.RecoveryMsMax)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "crash:", err)
		os.Exit(1)
	}
	if o.Out != "" {
		raw, _ := json.MarshalIndent(res, "", "  ")
		if err := os.WriteFile(o.Out, append(raw, '\n'), 0o644); err != nil {
			fmt.Fprintln(os.Stderr, "crash:", err)
			os.Exit(1)
		}
	}
	for _, r := range res.Runs {
		if r.Lost != 0 || !r.VerifyOK {
			os.Exit(1)
		}
	}
}

func okStr(b bool) string {
	if b {
		return "ok"
	}
	return "FAILED"
}

// run executes the loop for the requested modes.
func run(o options) (Result, error) {
	var modes []string
	switch o.Fsync {
	case "both":
		modes = []string{"always", "never"}
	case "always", "never":
		modes = []string{o.Fsync}
	default:
		return Result{}, fmt.Errorf("-fsync must be always, never or both, got %q", o.Fsync)
	}
	work, err := os.MkdirTemp("", "kgod-crash-")
	if err != nil {
		return Result{}, err
	}
	defer os.RemoveAll(work)
	bin, err := buildKgod(work)
	if err != nil {
		return Result{}, err
	}
	var res Result
	for i, mode := range modes {
		r, err := runMode(bin, filepath.Join(work, "data-"+mode), mode, o.Iterations, o.Seed+int64(i))
		res.Runs = append(res.Runs, r)
		if err != nil {
			return res, fmt.Errorf("mode %s: %w", mode, err)
		}
	}
	return res, nil
}

func moduleRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("go.mod not found above the working directory")
		}
		dir = parent
	}
}

func buildKgod(work string) (string, error) {
	root, err := moduleRoot()
	if err != nil {
		return "", err
	}
	bin := filepath.Join(work, "kgod")
	cmd := exec.Command("go", "build", "-o", bin, "./cmd/kgod")
	cmd.Dir = root
	if out, err := cmd.CombinedOutput(); err != nil {
		return "", fmt.Errorf("go build kgod: %v\n%s", err, out)
	}
	return bin, nil
}

func freeAddr() (string, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	defer l.Close()
	return l.Addr().String(), nil
}

// broker is a running kgod child process.
type broker struct {
	cmd  *exec.Cmd
	addr string
	up   time.Duration // process start to first successful connect
}

func startBroker(bin, dataDir, mode string) (*broker, error) {
	addr, err := freeAddr()
	if err != nil {
		return nil, err
	}
	cmd := exec.Command(bin, "--listen", addr, "--advertise", addr, "--data-dir", dataDir, "--fsync", mode, "--topic", "crash:3")
	start := time.Now()
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			c.Close()
			return &broker{cmd: cmd, addr: addr, up: time.Since(start)}, nil
		}
		time.Sleep(5 * time.Millisecond)
	}
	_ = cmd.Process.Kill()
	_ = cmd.Wait()
	return nil, fmt.Errorf("kgod did not accept connections within 30s")
}

func (b *broker) kill() {
	_ = b.cmd.Process.Kill() // SIGKILL
	_ = b.cmd.Wait()
}

func (b *broker) stop() error {
	_ = b.cmd.Process.Signal(syscall.SIGTERM)
	done := make(chan error, 1)
	go func() { done <- b.cmd.Wait() }()
	select {
	case <-done:
		return nil
	case <-time.After(15 * time.Second):
		_ = b.cmd.Process.Kill()
		<-done
		return fmt.Errorf("kgod did not exit after SIGTERM")
	}
}

type ack struct {
	partition int32
	offset    int64
	value     string
}

func runMode(bin, dataDir, mode string, iterations int, seed int64) (ModeResult, error) {
	res := ModeResult{Mode: mode, Iterations: iterations, Seed: seed, Go: runtime.Version(), CPUs: runtime.NumCPU(), GOOS: runtime.GOOS,
		Date: time.Now().UTC().Format(time.RFC3339)}
	rng := rand.New(rand.NewPCG(uint64(seed), 0x6b61666b61))
	var acked []ack
	var recoveries []float64

	for it := 0; it < iterations; it++ {
		b, err := startBroker(bin, dataDir, mode)
		if err != nil {
			return res, err
		}
		recoveries = append(recoveries, float64(b.up.Milliseconds()))
		got, err := produceUntilKilled(b, it, time.Duration(300+rng.IntN(1200))*time.Millisecond)
		if err != nil {
			return res, err
		}
		acked = append(acked, got...)
	}
	res.Acked = len(acked)

	b, err := startBroker(bin, dataDir, mode)
	if err != nil {
		return res, err
	}
	recoveries = append(recoveries, float64(b.up.Milliseconds()))
	read, err := readAll(b.addr)
	if err != nil {
		b.kill()
		return res, err
	}
	if err := b.stop(); err != nil {
		return res, err
	}
	for _, a := range acked {
		if v, ok := read[[2]int64{int64(a.partition), a.offset}]; !ok || v != a.value {
			res.Lost++
		}
	}
	rep, err := storage.VerifyDir(dataDir)
	if err != nil {
		return res, err
	}
	res.VerifyOK = len(rep.Problems) == 0
	for _, p := range rep.Problems {
		fmt.Fprintln(os.Stderr, "verify:", p)
	}
	sort.Float64s(recoveries)
	res.RecoveryMsP50 = recoveries[len(recoveries)/2]
	res.RecoveryMsMax = recoveries[len(recoveries)-1]
	return res, nil
}

// produceUntilKilled produces with acks=all for the given time, then SIGKILLs the broker and returns the
// records whose acknowledgement arrived before that.
func produceUntilKilled(b *broker, iter int, runFor time.Duration) ([]ack, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(b.addr), kgo.RequiredAcks(kgo.AllISRAcks()), kgo.DisableIdempotentWrite(),
		kgo.RecordPartitioner(kgo.RoundRobinPartitioner()), kgo.RecordDeliveryTimeout(5*time.Second),
	)
	if err != nil {
		b.kill()
		return nil, err
	}
	var mu sync.Mutex
	var acked []ack
	ctx, stop := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for seq := 0; ctx.Err() == nil; seq++ {
			val := fmt.Sprintf("i%d-s%d", iter, seq)
			cl.Produce(ctx, &kgo.Record{Topic: "crash", Value: []byte(val)}, func(r *kgo.Record, err error) {
				if err == nil {
					mu.Lock()
					acked = append(acked, ack{r.Partition, r.Offset, string(r.Value)})
					mu.Unlock()
				}
			})
			if seq%100 == 99 {
				time.Sleep(time.Millisecond) // pace to about 100k records/s so the data (and recovery scan) stays bounded
			}
		}
	}()
	time.Sleep(runFor)
	b.kill()
	stop()
	wg.Wait()
	cl.Close()
	mu.Lock()
	defer mu.Unlock()
	return append([]ack(nil), acked...), nil
}

// readAll consumes every partition of the crash topic from offset 0 up to its high watermark.
func readAll(addr string) (map[[2]int64]string, error) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
		"crash": {0: kgo.NewOffset().AtStart(), 1: kgo.NewOffset().AtStart(), 2: kgo.NewOffset().AtStart()},
	}), kgo.FetchMaxWait(500*time.Millisecond))
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	req := kmsg.NewPtrListOffsetsRequest()
	req.ReplicaID = -1
	topic := kmsg.NewListOffsetsRequestTopic()
	topic.Topic = "crash"
	for p := int32(0); p < 3; p++ {
		part := kmsg.NewListOffsetsRequestTopicPartition()
		part.Partition = p
		part.Timestamp = -1
		topic.Partitions = append(topic.Partitions, part)
	}
	req.Topics = append(req.Topics, topic)
	resp, err := cl.Request(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("list offsets: %w", err)
	}
	hw := map[int32]int64{}
	for _, t := range resp.(*kmsg.ListOffsetsResponse).Topics {
		for _, p := range t.Partitions {
			if p.ErrorCode != 0 {
				return nil, fmt.Errorf("list offsets partition %d: error %d", p.Partition, p.ErrorCode)
			}
			hw[p.Partition] = p.Offset
		}
	}

	read := map[[2]int64]string{}
	next := map[int32]int64{}
	done := func() bool {
		for p, h := range hw {
			if next[p] < h {
				return false
			}
		}
		return true
	}
	for !done() {
		fetches := cl.PollFetches(ctx)
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("reading back: %w (next=%v hw=%v)", err, next, hw)
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			return nil, fmt.Errorf("fetch error: %v", errs[0].Err)
		}
		fetches.EachRecord(func(r *kgo.Record) {
			read[[2]int64{int64(r.Partition), r.Offset}] = string(r.Value)
			if r.Offset+1 > next[r.Partition] {
				next[r.Partition] = r.Offset + 1
			}
		})
	}
	return read, nil
}
