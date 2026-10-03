// Package group implements the consumer group state machine, the coordinator that serialises it and
// the durable committed-offset store.
package group

import (
	"sort"
	"time"

	"github.com/sathwikbairaboina2/kafka-go/internal/protocol"
)

// State is the group lifecycle state.
type State int

// Group states.
const (
	Empty State = iota
	PreparingRebalance
	CompletingRebalance
	Stable
)

func (s State) String() string {
	return [...]string{"Empty", "PreparingRebalance", "CompletingRebalance", "Stable"}[s]
}

// Protocol is one assignment protocol a member supports.
type Protocol struct {
	Name     string
	Metadata []byte
}

// JoinRequest is a member's request to join the next generation.
type JoinRequest struct {
	MemberID, ClientID               string
	SessionTimeout, RebalanceTimeout time.Duration
	ProtocolType                     string
	Protocols                        []Protocol
}

// MemberMeta is a member's metadata for the chosen protocol.
type MemberMeta struct {
	ID       string
	Metadata []byte
}

// JoinResult is the reply to a join.
type JoinResult struct {
	Err        int16
	Generation int32
	Protocol   string
	Leader     string
	MemberID   string
	Members    []MemberMeta // only in the leader's result
}

// SyncResult is the reply to a sync.
type SyncResult struct {
	Err        int16
	Assignment []byte
}

type member struct {
	id               string
	clientID         string
	protocols        []Protocol
	sessionTimeout   time.Duration
	rebalanceTimeout time.Duration
	lastHeartbeat    time.Time
	joinSeq          int
	pendingJoin      func(JoinResult)
	pendingSync      func(SyncResult)
	assignment       []byte
}

// Group is not safe for concurrent use; Coordinator serialises calls. Replies may be invoked later
// (from Join, Sync, Leave or Tick) and each reply func is called at most once.
type Group struct {
	id           string
	newID        func() string
	state        State
	generation   int32
	protocolType string
	protocol     string
	leader       string
	members      map[string]*member
	pending      map[string]time.Time // issued member ids that have not joined yet
	deadline     time.Time            // end of the join phase
	seq          int
}

// NewGroup returns an empty group. newID supplies unique member id suffixes.
func NewGroup(id string, newID func() string) *Group {
	return &Group{id: id, newID: newID, members: map[string]*member{}, pending: map[string]time.Time{}}
}

// State returns the current state.
func (g *Group) State() State { return g.state }

// Generation returns the current generation.
func (g *Group) Generation() int32 { return g.generation }

// Members returns the member ids, sorted.
func (g *Group) Members() []string {
	out := make([]string, 0, len(g.members))
	for id := range g.members {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

func (g *Group) ordered() []*member {
	out := make([]*member, 0, len(g.members))
	for _, m := range g.members {
		out = append(out, m)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].joinSeq < out[j].joinSeq })
	return out
}

// commonProtocol returns the first protocol of the earliest-joined member that every member lists.
func commonProtocol(members []*member) (string, bool) {
	if len(members) == 0 {
		return "", false
	}
	for _, p := range members[0].protocols {
		all := true
		for _, m := range members[1:] {
			found := false
			for _, q := range m.protocols {
				if q.Name == p.Name {
					found = true
					break
				}
			}
			if !found {
				all = false
				break
			}
		}
		if all {
			return p.Name, true
		}
	}
	return "", false
}

// Join handles a JoinGroup request. reply is called now or later, once.
func (g *Group) Join(now time.Time, req JoinRequest, reply func(JoinResult)) {
	if req.MemberID == "" {
		id := req.ClientID + "-" + g.newID()
		g.pending[id] = now.Add(req.SessionTimeout)
		reply(JoinResult{Err: protocol.ErrMemberIDRequired, MemberID: id, Generation: -1})
		return
	}
	existing, isMember := g.members[req.MemberID]
	_, isPending := g.pending[req.MemberID]
	if !isMember && !isPending {
		reply(JoinResult{Err: protocol.ErrUnknownMemberID, MemberID: req.MemberID, Generation: -1})
		return
	}
	if len(req.Protocols) == 0 || (len(g.members) > 0 && g.protocolType != req.ProtocolType) {
		reply(JoinResult{Err: protocol.ErrInconsistentGroupProtocol, MemberID: req.MemberID, Generation: -1})
		return
	}
	// every member, including the joiner with its new list, must share a protocol
	candidate := &member{id: req.MemberID, protocols: req.Protocols, joinSeq: -1}
	probe := []*member{candidate}
	for _, m := range g.ordered() {
		if m.id != req.MemberID {
			probe = append(probe, m)
		}
	}
	if len(probe) > 1 {
		if _, ok := commonProtocol(probe); !ok {
			reply(JoinResult{Err: protocol.ErrInconsistentGroupProtocol, MemberID: req.MemberID, Generation: -1})
			return
		}
	}

	var m *member
	if isMember {
		m = existing
		if m.pendingJoin != nil {
			old := m.pendingJoin
			m.pendingJoin = nil
			old(JoinResult{Err: protocol.ErrRebalanceInProgress, MemberID: m.id, Generation: -1})
		}
	} else {
		delete(g.pending, req.MemberID)
		g.seq++
		m = &member{id: req.MemberID, joinSeq: g.seq}
		g.members[m.id] = m
	}
	m.clientID = req.ClientID
	m.protocols = req.Protocols
	m.sessionTimeout = req.SessionTimeout
	m.rebalanceTimeout = req.RebalanceTimeout
	m.lastHeartbeat = now
	m.pendingJoin = reply
	if len(g.members) == 1 {
		g.protocolType = req.ProtocolType
	}
	if g.state != PreparingRebalance {
		g.prepareRebalance(now)
	}
	g.maybeComplete(now)
}

// prepareRebalance enters PreparingRebalance; pending syncs are told to rejoin.
func (g *Group) prepareRebalance(now time.Time) {
	g.state = PreparingRebalance
	var max time.Duration
	for _, m := range g.members {
		if m.rebalanceTimeout > max {
			max = m.rebalanceTimeout
		}
	}
	g.deadline = now.Add(max)
	for _, m := range g.ordered() {
		if m.pendingSync != nil {
			cb := m.pendingSync
			m.pendingSync = nil
			cb(SyncResult{Err: protocol.ErrRebalanceInProgress})
		}
	}
}

func (g *Group) allJoined() bool {
	for _, m := range g.members {
		if m.pendingJoin == nil {
			return false
		}
	}
	return len(g.members) > 0
}

func (g *Group) maybeComplete(now time.Time) {
	if g.state == PreparingRebalance && g.allJoined() {
		g.completeJoin(now)
	}
}

// completeJoin ends the join phase: members that did not rejoin are dropped, the generation advances
// and every pending join gets its result.
func (g *Group) completeJoin(now time.Time) {
	for id, m := range g.members {
		if m.pendingJoin == nil {
			g.dropMember(id, m)
		}
	}
	g.generation++
	ms := g.ordered()
	if len(ms) == 0 {
		g.resetEmpty()
		return
	}
	proto, _ := commonProtocol(ms)
	g.protocol = proto
	if _, ok := g.members[g.leader]; !ok {
		g.leader = ms[0].id
	}
	g.state = CompletingRebalance
	g.deadline = time.Time{}
	var all []MemberMeta
	for _, m := range ms {
		m.assignment = nil
		m.lastHeartbeat = now
		var meta []byte
		for _, p := range m.protocols {
			if p.Name == proto {
				meta = p.Metadata
				break
			}
		}
		all = append(all, MemberMeta{ID: m.id, Metadata: meta})
	}
	for _, m := range ms {
		cb := m.pendingJoin
		m.pendingJoin = nil
		res := JoinResult{Generation: g.generation, Protocol: proto, Leader: g.leader, MemberID: m.id}
		if m.id == g.leader {
			res.Members = all
		}
		cb(res)
	}
}

// dropMember removes a member, answering anything it was waiting for.
func (g *Group) dropMember(id string, m *member) {
	delete(g.members, id)
	if m.pendingJoin != nil {
		cb := m.pendingJoin
		m.pendingJoin = nil
		cb(JoinResult{Err: protocol.ErrUnknownMemberID, MemberID: id, Generation: -1})
	}
	if m.pendingSync != nil {
		cb := m.pendingSync
		m.pendingSync = nil
		cb(SyncResult{Err: protocol.ErrUnknownMemberID})
	}
}

func (g *Group) resetEmpty() {
	g.state = Empty
	g.leader = ""
	g.protocol = ""
	g.protocolType = ""
	g.deadline = time.Time{}
}

// afterRemoval moves the group on once members have been removed.
func (g *Group) afterRemoval(now time.Time) {
	if len(g.members) == 0 {
		g.resetEmpty()
		return
	}
	if g.state != PreparingRebalance {
		g.prepareRebalance(now)
	}
	g.maybeComplete(now)
}

// Sync handles a SyncGroup request. The leader supplies the assignments.
func (g *Group) Sync(now time.Time, memberID string, gen int32, assignments map[string][]byte, reply func(SyncResult)) {
	m, ok := g.members[memberID]
	switch {
	case !ok:
		reply(SyncResult{Err: protocol.ErrUnknownMemberID})
		return
	case gen != g.generation:
		reply(SyncResult{Err: protocol.ErrIllegalGeneration})
		return
	case g.state == PreparingRebalance:
		reply(SyncResult{Err: protocol.ErrRebalanceInProgress})
		return
	}
	m.lastHeartbeat = now
	if g.state == Stable {
		reply(SyncResult{Assignment: m.assignment})
		return
	}
	// CompletingRebalance
	if memberID != g.leader {
		if m.pendingSync != nil {
			old := m.pendingSync
			m.pendingSync = nil
			old(SyncResult{Err: protocol.ErrRebalanceInProgress})
		}
		m.pendingSync = reply
		return
	}
	for id, mm := range g.members {
		a := assignments[id]
		if a == nil {
			a = []byte{}
		}
		mm.assignment = a
	}
	g.state = Stable
	for _, mm := range g.ordered() {
		if mm.pendingSync != nil {
			cb := mm.pendingSync
			mm.pendingSync = nil
			cb(SyncResult{Assignment: mm.assignment})
		}
	}
	reply(SyncResult{Assignment: m.assignment})
}

// Heartbeat refreshes a member's session and tells it whether it must rejoin.
func (g *Group) Heartbeat(now time.Time, memberID string, gen int32) int16 {
	m, ok := g.members[memberID]
	if !ok {
		return protocol.ErrUnknownMemberID
	}
	m.lastHeartbeat = now
	switch {
	case gen != g.generation:
		return protocol.ErrIllegalGeneration
	case g.state == PreparingRebalance:
		return protocol.ErrRebalanceInProgress
	}
	return protocol.ErrNone
}

// Leave removes a member and triggers a rebalance for the rest.
func (g *Group) Leave(now time.Time, memberID string) int16 {
	m, ok := g.members[memberID]
	if !ok {
		return protocol.ErrUnknownMemberID
	}
	g.dropMember(memberID, m)
	g.afterRemoval(now)
	return protocol.ErrNone
}

// Tick expires silent members and unused member ids and applies the join-phase deadline.
func (g *Group) Tick(now time.Time) {
	for id, dl := range g.pending {
		if !now.Before(dl) {
			delete(g.pending, id)
		}
	}
	removed := false
	for id, m := range g.members {
		if m.pendingJoin == nil && now.Sub(m.lastHeartbeat) > m.sessionTimeout {
			g.dropMember(id, m)
			removed = true
		}
	}
	if removed {
		g.afterRemoval(now)
	}
	if g.state == PreparingRebalance && !now.Before(g.deadline) {
		g.completeJoin(now)
	}
}

// ValidateCommit checks the member and generation of an offset commit.
func (g *Group) ValidateCommit(memberID string, gen int32) int16 {
	if gen < 0 && memberID == "" {
		if g.state == Empty {
			return protocol.ErrNone
		}
		return protocol.ErrIllegalGeneration
	}
	if _, ok := g.members[memberID]; !ok {
		return protocol.ErrUnknownMemberID
	}
	if gen != g.generation {
		return protocol.ErrIllegalGeneration
	}
	if g.state == CompletingRebalance {
		return protocol.ErrRebalanceInProgress
	}
	return protocol.ErrNone
}
