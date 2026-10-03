package group

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/sathwikbairaboina2/kafka-go/internal/protocol"
)

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func newCoord(t *testing.T) (*Coordinator, *fakeClock, *OffsetStore) {
	t.Helper()
	store, err := OpenOffsetStore(t.TempDir(), false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	clk := &fakeClock{t: t0}
	return NewCoordinator(store, clk.now), clk, store
}

func joinID(t *testing.T, c *Coordinator, group, client string) string {
	t.Helper()
	r := c.Join(context.Background(), group, req("", client))
	if r.Err != protocol.ErrMemberIDRequired {
		t.Fatalf("first join err = %d", r.Err)
	}
	return r.MemberID
}

func TestCoordinatorJoinBlocksUntilAllJoin(t *testing.T) {
	c, _, _ := newCoord(t)
	a := joinID(t, c, "g", "A")
	// A joins alone: completes at once
	ra := c.Join(context.Background(), "g", req(a, "A"))
	if ra.Err != 0 || ra.Generation != 1 {
		t.Fatalf("A = %+v", ra)
	}
	b := joinID(t, c, "g", "B")

	resB := make(chan JoinResult, 1)
	go func() { resB <- c.Join(context.Background(), "g", req(b, "B")) }()
	select {
	case r := <-resB:
		t.Fatalf("B returned before A rejoined: %+v", r)
	case <-time.After(100 * time.Millisecond):
	}
	resA := make(chan JoinResult, 1)
	go func() { resA <- c.Join(context.Background(), "g", req(a, "A")) }()
	for _, ch := range []chan JoinResult{resA, resB} {
		select {
		case r := <-ch:
			if r.Err != 0 || r.Generation != 2 {
				t.Fatalf("result = %+v", r)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("join did not complete after both members joined")
		}
	}
}

func TestCoordinatorJoinCancelled(t *testing.T) {
	c, _, _ := newCoord(t)
	a := joinID(t, c, "g", "A")
	c.Join(context.Background(), "g", req(a, "A"))
	b := joinID(t, c, "g", "B")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan JoinResult, 1)
	go func() { done <- c.Join(ctx, "g", req(b, "B")) }()
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case r := <-done:
		if r.Err != protocol.ErrRebalanceInProgress {
			t.Fatalf("cancelled join = %+v", r)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cancelled join did not return")
	}
}

func TestCoordinatorTickExpiresMembers(t *testing.T) {
	c, clk, _ := newCoord(t)
	a := joinID(t, c, "g", "A")
	c.Join(context.Background(), "g", req(a, "A"))
	clk.advance(11 * time.Second)
	c.Tick()
	if code := c.Heartbeat("g", a, 1); code != protocol.ErrUnknownMemberID {
		t.Fatalf("heartbeat after expiry = %d", code)
	}
	if code := c.Heartbeat("nope", a, 1); code != protocol.ErrUnknownMemberID {
		t.Fatalf("heartbeat to unknown group = %d", code)
	}
}

func TestCommitStaleGenerationNotPersisted(t *testing.T) {
	c, _, store := newCoord(t)
	a := joinID(t, c, "g", "A")
	r := c.Join(context.Background(), "g", req(a, "A"))
	c.Sync(context.Background(), "g", a, r.Generation, map[string][]byte{a: {1}})
	k := OffsetKey{"g", "t", 0}

	code, err := c.Commit("g", a, r.Generation-1, map[OffsetKey]Committed{k: {Offset: 10}})
	if err != nil || code != protocol.ErrIllegalGeneration {
		t.Fatalf("stale commit = %d, %v", code, err)
	}
	if _, ok := store.Get(k); ok {
		t.Fatal("rejected commit was persisted")
	}
	code, err = c.Commit("g", "ghost", r.Generation, map[OffsetKey]Committed{k: {Offset: 10}})
	if err != nil || code != protocol.ErrUnknownMemberID {
		t.Fatalf("unknown member commit = %d, %v", code, err)
	}
	if _, ok := store.Get(k); ok {
		t.Fatal("rejected commit was persisted")
	}
	code, err = c.Commit("g", a, r.Generation, map[OffsetKey]Committed{k: {Offset: 10}})
	if err != nil || code != 0 {
		t.Fatalf("valid commit = %d, %v", code, err)
	}
	if got := c.Fetch("g", []OffsetKey{k, {"g", "t", 1}}, false); len(got) != 1 || got[k].Offset != 10 {
		t.Fatalf("fetch = %v", got)
	}
	// a simple (generation -1) commit to a group with members is rejected, to a fresh group it is allowed
	if code, _ := c.Commit("g", "", -1, map[OffsetKey]Committed{k: {Offset: 99}}); code != protocol.ErrIllegalGeneration {
		t.Fatalf("simple commit to busy group = %d", code)
	}
	if code, _ := c.Commit("fresh", "", -1, map[OffsetKey]Committed{{"fresh", "t", 0}: {Offset: 3}}); code != 0 {
		t.Fatalf("simple commit to fresh group = %d", code)
	}
	if all := c.Fetch("fresh", nil, true); len(all) != 1 {
		t.Fatalf("fetch all = %v", all)
	}
}
