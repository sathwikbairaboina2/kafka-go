// Package compat runs real client libraries (franz-go) against an in-process kgod.
package compat

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/sathwikbairaboina2/kafka-go/internal/app"
	"github.com/sathwikbairaboina2/kafka-go/internal/storage"
	"github.com/twmb/franz-go/pkg/kgo"
)

func startBroker(t *testing.T) string {
	t.Helper()
	inst, err := app.Start(app.Config{
		Listen: "127.0.0.1:0", DataDir: t.TempDir(), Topics: map[string]int32{"orders": 3},
		AutoCreate: true, DefaultPartitions: 3,
		Storage:       storage.Config{RetentionMs: -1, RetentionBytes: -1, Fsync: storage.FsyncNever},
		FsyncInterval: 50 * time.Millisecond, RetentionCheck: time.Minute,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = inst.Close(ctx)
	})
	return inst.Addr()
}

func client(t *testing.T, addr string, opts ...kgo.Opt) *kgo.Client {
	t.Helper()
	base := []kgo.Opt{
		kgo.SeedBrokers(addr), kgo.DisableIdempotentWrite(), kgo.FetchMaxWait(500 * time.Millisecond),
		kgo.RecordPartitioner(kgo.RoundRobinPartitioner()),
	}
	cl, err := kgo.NewClient(append(base, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)
	return cl
}

func produceN(t *testing.T, ctx context.Context, cl *kgo.Client, prefix string, n int) {
	t.Helper()
	recs := make([]*kgo.Record, n)
	for i := range recs {
		recs[i] = &kgo.Record{Topic: "orders", Value: []byte(fmt.Sprintf("%s-%d", prefix, i))}
	}
	if err := cl.ProduceSync(ctx, recs...).FirstErr(); err != nil {
		t.Fatalf("produce: %v", err)
	}
}

// consumeUntil polls cl until want distinct values are seen or ctx ends.
func consumeUntil(t *testing.T, ctx context.Context, cl *kgo.Client, want int) map[string]*kgo.Record {
	t.Helper()
	seen := map[string]*kgo.Record{}
	for len(seen) < want {
		fetches := cl.PollFetches(ctx)
		if err := ctx.Err(); err != nil {
			t.Fatalf("consumed %d of %d before %v", len(seen), want, err)
		}
		fetches.EachError(func(topic string, p int32, err error) { t.Errorf("fetch error %s/%d: %v", topic, p, err) })
		fetches.EachRecord(func(r *kgo.Record) { seen[string(r.Value)] = r })
		if t.Failed() {
			t.FailNow()
		}
	}
	return seen
}

func TestFranzGoProduceConsume10k(t *testing.T) {
	addr := startBroker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	producer := client(t, addr)
	produceN(t, ctx, producer, "v", 10000)

	consumer := client(t, addr, kgo.ConsumerGroup("g1"), kgo.ConsumeTopics("orders"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	seen := consumeUntil(t, ctx, consumer, 10000)

	perPartition := map[int32][]int64{}
	for _, r := range seen {
		perPartition[r.Partition] = append(perPartition[r.Partition], r.Offset)
	}
	total := 0
	for p, offs := range perPartition {
		sort.Slice(offs, func(i, j int) bool { return offs[i] < offs[j] })
		for i, o := range offs {
			if o != int64(i) {
				t.Fatalf("partition %d: offset gap at position %d (got %d)", p, i, o)
			}
		}
		total += len(offs)
	}
	if len(perPartition) != 3 || total != 10000 {
		t.Fatalf("partitions %d, records %d", len(perPartition), total)
	}
	if err := consumer.CommitUncommittedOffsets(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
}

func TestFranzGoCommitResume(t *testing.T) {
	addr := startBroker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	producer := client(t, addr)
	produceN(t, ctx, producer, "first", 100)

	opts := []kgo.Opt{kgo.ConsumerGroup("resume"), kgo.ConsumeTopics("orders"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart())}
	first, err := kgo.NewClient(append([]kgo.Opt{kgo.SeedBrokers(addr), kgo.FetchMaxWait(500 * time.Millisecond)}, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	consumeUntil(t, ctx, first, 100)
	if err := first.CommitUncommittedOffsets(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
	first.Close() // leaves the group

	produceN(t, ctx, producer, "second", 10)
	second := client(t, addr, opts...)
	seen := consumeUntil(t, ctx, second, 10)
	for v := range seen {
		if len(v) < 6 || v[:6] != "second" {
			t.Fatalf("resumed consumer saw %q; committed offsets were not honoured", v)
		}
	}
	// and nothing else is pending
	short, cancelShort := context.WithTimeout(ctx, time.Second)
	defer cancelShort()
	extra := 0
	second.PollFetches(short).EachRecord(func(r *kgo.Record) { extra++ })
	if extra != 0 {
		t.Fatalf("%d extra records after the resumed ones", extra)
	}
}

type assignment struct {
	mu    sync.Mutex
	parts map[int32]bool
}

func (a *assignment) set(parts []int32, on bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, p := range parts {
		if on {
			a.parts[p] = true
		} else {
			delete(a.parts, p)
		}
	}
}

func (a *assignment) snapshot() map[int32]bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := map[int32]bool{}
	for p := range a.parts {
		out[p] = true
	}
	return out
}

func memberClient(t *testing.T, addr string, a *assignment) *kgo.Client {
	return client(t, addr, kgo.ConsumerGroup("pair"), kgo.ConsumeTopics("orders"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.OnPartitionsAssigned(func(_ context.Context, _ *kgo.Client, m map[string][]int32) { a.set(m["orders"], true) }),
		kgo.OnPartitionsRevoked(func(_ context.Context, _ *kgo.Client, m map[string][]int32) { a.set(m["orders"], false) }),
		kgo.OnPartitionsLost(func(_ context.Context, _ *kgo.Client, m map[string][]int32) { a.set(m["orders"], false) }),
	)
}

func TestFranzGoTwoMembers(t *testing.T) {
	addr := startBroker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	a1 := &assignment{parts: map[int32]bool{}}
	a2 := &assignment{parts: map[int32]bool{}}
	c1 := memberClient(t, addr, a1)
	c2 := memberClient(t, addr, a2)

	type got struct {
		mu   sync.Mutex
		recs map[string]int32 // value -> partition
		dups int
	}
	poll := func(cl *kgo.Client, g *got, stop <-chan struct{}) {
		for {
			select {
			case <-stop:
				return
			default:
			}
			pctx, pcancel := context.WithTimeout(ctx, 500*time.Millisecond)
			cl.PollFetches(pctx).EachRecord(func(r *kgo.Record) {
				g.mu.Lock()
				defer g.mu.Unlock()
				if _, dup := g.recs[string(r.Value)]; dup {
					g.dups++
				}
				g.recs[string(r.Value)] = r.Partition
			})
			pcancel()
			if ctx.Err() != nil {
				return
			}
		}
	}
	g1 := &got{recs: map[string]int32{}}
	g2 := &got{recs: map[string]int32{}}
	stop1, stop2 := make(chan struct{}), make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); poll(c1, g1, stop1) }()
	go func() { defer wg.Done(); poll(c2, g2, stop2) }()

	deadline := time.Now().Add(60 * time.Second)
	for {
		s1, s2 := a1.snapshot(), a2.snapshot()
		disjoint := true
		for p := range s1 {
			if s2[p] {
				disjoint = false
			}
		}
		if disjoint && len(s1) > 0 && len(s2) > 0 && len(s1)+len(s2) == 3 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("assignments never settled: %v / %v", s1, s2)
		}
		time.Sleep(100 * time.Millisecond)
	}

	producer := client(t, addr)
	produceN(t, ctx, producer, "m", 3000)
	for {
		g1.mu.Lock()
		g2.mu.Lock()
		n := len(g1.recs) + len(g2.recs)
		g2.mu.Unlock()
		g1.mu.Unlock()
		if n >= 3000 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("consumed %d of 3000", n)
		}
		time.Sleep(100 * time.Millisecond)
	}
	close(stop1)
	close(stop2)
	wg.Wait()

	s1, s2 := a1.snapshot(), a2.snapshot()
	for v, p := range g1.recs {
		if _, both := g2.recs[v]; both {
			t.Fatalf("%s consumed by both members", v)
		}
		if !s1[p] {
			t.Fatalf("member 1 consumed %s from partition %d it does not own (%v)", v, p, s1)
		}
	}
	for v, p := range g2.recs {
		if !s2[p] {
			t.Fatalf("member 2 consumed %s from partition %d it does not own (%v)", v, p, s2)
		}
	}
	if len(g1.recs)+len(g2.recs) != 3000 || g1.dups+g2.dups != 0 {
		t.Fatalf("records %d dups %d", len(g1.recs)+len(g2.recs), g1.dups+g2.dups)
	}

	// closing one member moves all partitions to the other
	c1.Close()
	failAt := time.Now().Add(30 * time.Second)
	for len(a2.snapshot()) != 3 {
		if time.Now().After(failAt) || ctx.Err() != nil {
			t.Fatalf("survivor owns %v, want all 3 partitions", a2.snapshot())
		}
		pctx, pcancel := context.WithTimeout(ctx, 200*time.Millisecond)
		c2.PollFetches(pctx)
		pcancel()
	}
}

func TestFranzGoAcksZero(t *testing.T) {
	addr := startBroker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	producer := client(t, addr, kgo.RequiredAcks(kgo.NoAck()))
	produceN(t, ctx, producer, "z", 100)
	consumer := client(t, addr, kgo.ConsumeTopics("orders"), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	seen := consumeUntil(t, ctx, consumer, 100)
	if len(seen) != 100 {
		t.Fatalf("saw %d records", len(seen))
	}
}
