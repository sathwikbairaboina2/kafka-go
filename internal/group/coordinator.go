package group

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"sync"
	"time"

	"github.com/sathwikbairaboina2/kafka-go/internal/protocol"
)

// Coordinator serialises access to the groups and owns the offset store.
type Coordinator struct {
	mu     sync.Mutex
	groups map[string]*Group
	store  *OffsetStore
	clock  func() time.Time
}

// NewCoordinator returns a Coordinator. clock defaults to time.Now.
func NewCoordinator(store *OffsetStore, clock func() time.Time) *Coordinator {
	if clock == nil {
		clock = time.Now
	}
	return &Coordinator{groups: map[string]*Group{}, store: store, clock: clock}
}

func randomID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// group returns the group, creating it. Callers hold c.mu.
func (c *Coordinator) group(id string) *Group {
	g, ok := c.groups[id]
	if !ok {
		g = NewGroup(id, randomID)
		c.groups[id] = g
	}
	return g
}

// Join blocks until the join phase completes. A cancelled ctx returns ErrRebalanceInProgress.
func (c *Coordinator) Join(ctx context.Context, group string, req JoinRequest) JoinResult {
	ch := make(chan JoinResult, 1)
	c.mu.Lock()
	c.group(group).Join(c.clock(), req, func(r JoinResult) { ch <- r })
	c.mu.Unlock()
	select {
	case r := <-ch:
		return r
	case <-ctx.Done():
		return JoinResult{Err: protocol.ErrRebalanceInProgress, Generation: -1}
	}
}

// Sync blocks until the leader's assignment is available.
func (c *Coordinator) Sync(ctx context.Context, group, member string, gen int32, a map[string][]byte) SyncResult {
	ch := make(chan SyncResult, 1)
	c.mu.Lock()
	c.group(group).Sync(c.clock(), member, gen, a, func(r SyncResult) { ch <- r })
	c.mu.Unlock()
	select {
	case r := <-ch:
		return r
	case <-ctx.Done():
		return SyncResult{Err: protocol.ErrRebalanceInProgress}
	}
}

// Heartbeat refreshes a member; an unknown group is an unknown member.
func (c *Coordinator) Heartbeat(group, member string, gen int32) int16 {
	c.mu.Lock()
	defer c.mu.Unlock()
	g, ok := c.groups[group]
	if !ok {
		return protocol.ErrUnknownMemberID
	}
	return g.Heartbeat(c.clock(), member, gen)
}

// Leave removes a member from its group.
func (c *Coordinator) Leave(group, member string) int16 {
	c.mu.Lock()
	defer c.mu.Unlock()
	g, ok := c.groups[group]
	if !ok {
		return protocol.ErrUnknownMemberID
	}
	return g.Leave(c.clock(), member)
}

// Commit validates once per request, then persists all offsets. It returns the request's error code;
// when it is non-zero nothing was persisted.
func (c *Coordinator) Commit(group, member string, gen int32, offsets map[OffsetKey]Committed) (int16, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if code := c.group(group).ValidateCommit(member, gen); code != protocol.ErrNone {
		return code, nil
	}
	if err := c.store.Commit(offsets); err != nil {
		return protocol.ErrNone, err
	}
	return protocol.ErrNone, nil
}

// Fetch returns committed offsets for keys, or every offset of the group when all is true.
func (c *Coordinator) Fetch(group string, keys []OffsetKey, all bool) map[OffsetKey]Committed {
	if all {
		return c.store.ForGroup(group)
	}
	out := map[OffsetKey]Committed{}
	for _, k := range keys {
		if v, ok := c.store.Get(k); ok {
			out[k] = v
		}
	}
	return out
}

// Run ticks every group each interval (default 200 ms) until ctx is done.
func (c *Coordinator) Run(ctx context.Context, every time.Duration) {
	if every <= 0 {
		every = 200 * time.Millisecond
	}
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			c.Tick()
		}
	}
}

// Tick advances every group's timers once.
func (c *Coordinator) Tick() {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.clock()
	for _, g := range c.groups {
		g.Tick(now)
	}
}
