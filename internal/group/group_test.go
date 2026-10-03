package group

import (
	"fmt"
	"testing"
	"time"

	"github.com/sathwikbairaboina2/kafka-go/internal/protocol"
)

var t0 = time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)

func newTestGroup() *Group {
	n := 0
	return NewGroup("g", func() string { n++; return fmt.Sprintf("%04d", n) })
}

type joinBox struct {
	n   int
	res JoinResult
}

func (b *joinBox) reply(r JoinResult) { b.n++; b.res = r }

type syncBox struct {
	n   int
	res SyncResult
}

func (b *syncBox) reply(r SyncResult) { b.n++; b.res = r }

func req(member, client string) JoinRequest {
	return JoinRequest{
		MemberID: member, ClientID: client, SessionTimeout: 10 * time.Second, RebalanceTimeout: 30 * time.Second,
		ProtocolType: "consumer", Protocols: []Protocol{{Name: "range", Metadata: []byte(client)}},
	}
}

// joinNew runs the MEMBER_ID_REQUIRED round trip and returns the member id and the box for the real join.
func joinNew(t *testing.T, g *Group, now time.Time, client string) (string, *joinBox) {
	t.Helper()
	first := &joinBox{}
	g.Join(now, req("", client), first.reply)
	if first.n != 1 || first.res.Err != protocol.ErrMemberIDRequired || first.res.MemberID == "" {
		t.Fatalf("first join = %+v (%d replies)", first.res, first.n)
	}
	box := &joinBox{}
	g.Join(now, req(first.res.MemberID, client), box.reply)
	return first.res.MemberID, box
}

func TestTwoMembersJoinSyncStable(t *testing.T) {
	g := newTestGroup()
	a, boxA := joinNew(t, g, t0, "A")
	// with one member the join completes at once: generation 1, A leads
	if boxA.n != 1 || boxA.res.Err != 0 || boxA.res.Generation != 1 || boxA.res.Leader != a || len(boxA.res.Members) != 1 {
		t.Fatalf("A join = %+v", boxA)
	}
	b, boxB := joinNew(t, g, t0, "B")
	if boxB.n != 0 {
		t.Fatal("B must wait for A to rejoin")
	}
	if g.State() != PreparingRebalance {
		t.Fatalf("state = %v", g.State())
	}
	boxA2 := &joinBox{}
	g.Join(t0, req(a, "A"), boxA2.reply)
	if boxA2.n != 1 || boxB.n != 1 || boxA2.res.Generation != 2 || boxB.res.Generation != 2 {
		t.Fatalf("after A rejoin: A=%+v B=%+v", boxA2, boxB)
	}
	if boxA2.res.Leader != a || boxB.res.Leader != a {
		t.Fatalf("leader = %q / %q, want %q", boxA2.res.Leader, boxB.res.Leader, a)
	}
	if len(boxA2.res.Members) != 2 || len(boxB.res.Members) != 0 {
		t.Fatalf("members in results: leader %d follower %d", len(boxA2.res.Members), len(boxB.res.Members))
	}
	if boxA2.res.Members[0].ID != a || string(boxA2.res.Members[0].Metadata) != "A" || boxA2.res.Members[1].ID != b {
		t.Fatalf("member metadata = %+v", boxA2.res.Members)
	}
	if g.State() != CompletingRebalance {
		t.Fatalf("state = %v", g.State())
	}

	// follower waits, leader's sync releases both
	syncB, syncA := &syncBox{}, &syncBox{}
	g.Sync(t0, b, 2, nil, syncB.reply)
	if syncB.n != 0 {
		t.Fatal("follower sync must wait for the leader")
	}
	g.Sync(t0, a, 2, map[string][]byte{a: []byte("pa"), b: []byte("pb")}, syncA.reply)
	if syncA.n != 1 || string(syncA.res.Assignment) != "pa" || syncB.n != 1 || string(syncB.res.Assignment) != "pb" {
		t.Fatalf("sync results A=%+v B=%+v", syncA, syncB)
	}
	if g.State() != Stable {
		t.Fatalf("state = %v", g.State())
	}
	// a later sync in Stable answers at once with the stored assignment
	again := &syncBox{}
	g.Sync(t0, b, 2, nil, again.reply)
	if again.n != 1 || string(again.res.Assignment) != "pb" {
		t.Fatalf("stable sync = %+v", again)
	}
	if g.Heartbeat(t0, a, 2) != 0 || g.Heartbeat(t0, b, 2) != 0 {
		t.Fatal("heartbeats in Stable must succeed")
	}
}

func stable2(t *testing.T) (*Group, string, string) {
	t.Helper()
	g := newTestGroup()
	a, _ := joinNew(t, g, t0, "A")
	b, boxB := joinNew(t, g, t0, "B")
	g.Join(t0, req(a, "A"), (&joinBox{}).reply)
	if boxB.n != 1 {
		t.Fatal("setup: B not joined")
	}
	g.Sync(t0, a, 2, map[string][]byte{a: {1}, b: {2}}, (&syncBox{}).reply)
	g.Sync(t0, b, 2, nil, (&syncBox{}).reply)
	if g.State() != Stable {
		t.Fatalf("setup: state %v", g.State())
	}
	return g, a, b
}

func TestHeartbeatDuringRebalance(t *testing.T) {
	g, a, _ := stable2(t)
	c, boxC := joinNew(t, g, t0, "C")
	_ = c
	if boxC.n != 0 {
		t.Fatal("C should wait")
	}
	if code := g.Heartbeat(t0, a, 2); code != protocol.ErrRebalanceInProgress {
		t.Fatalf("heartbeat = %d, want 27", code)
	}
}

func TestSessionExpiry(t *testing.T) {
	g, a, b := stable2(t)
	// B keeps heartbeating, A goes silent
	later := t0.Add(8 * time.Second)
	g.Heartbeat(later, b, 2)
	g.Tick(later)
	if len(g.Members()) != 2 {
		t.Fatal("no one should expire yet")
	}
	later = t0.Add(11 * time.Second)
	g.Heartbeat(later, b, 2)
	g.Tick(later)
	if m := g.Members(); len(m) != 1 || m[0] != b {
		t.Fatalf("members after expiry = %v (a=%s)", m, a)
	}
	if g.State() != PreparingRebalance {
		t.Fatalf("state = %v, want PreparingRebalance", g.State())
	}
	box := &joinBox{}
	g.Join(later, req(b, "B"), box.reply)
	if box.n != 1 || box.res.Generation != 3 || box.res.Leader != b {
		t.Fatalf("B rejoin = %+v", box)
	}
}

func TestJoinDeadlineDropsSilentMember(t *testing.T) {
	g, a, b := stable2(t)
	boxA := &joinBox{}
	g.Join(t0, req(a, "A"), boxA.reply) // B never rejoins
	if boxA.n != 0 {
		t.Fatal("A must wait for B")
	}
	for _, sec := range []int{9, 18, 27} { // B keeps its session alive but never rejoins
		if code := g.Heartbeat(t0.Add(time.Duration(sec)*time.Second), b, 2); code != protocol.ErrRebalanceInProgress {
			t.Fatalf("heartbeat = %d", code)
		}
		g.Tick(t0.Add(time.Duration(sec) * time.Second))
	}
	g.Tick(t0.Add(29 * time.Second))
	if boxA.n != 0 {
		t.Fatal("deadline not reached")
	}
	g.Tick(t0.Add(31 * time.Second))
	if boxA.n != 1 || boxA.res.Generation != 3 || len(g.Members()) != 1 || g.Members()[0] != a {
		t.Fatalf("after deadline: %+v members %v (b=%s)", boxA, g.Members(), b)
	}
}

func TestLeaveLastMemberEmpties(t *testing.T) {
	g := newTestGroup()
	a, _ := joinNew(t, g, t0, "A")
	if code := g.Leave(t0, a); code != 0 {
		t.Fatalf("leave = %d", code)
	}
	if g.State() != Empty || len(g.Members()) != 0 {
		t.Fatalf("state %v members %v", g.State(), g.Members())
	}
	if code := g.Leave(t0, a); code != protocol.ErrUnknownMemberID {
		t.Fatalf("second leave = %d", code)
	}
}

func TestLeaveTriggersRebalance(t *testing.T) {
	g, a, b := stable2(t)
	if g.Leave(t0, b) != 0 {
		t.Fatal("leave failed")
	}
	if g.State() != PreparingRebalance {
		t.Fatalf("state = %v", g.State())
	}
	box := &joinBox{}
	g.Join(t0, req(a, "A"), box.reply)
	if box.n != 1 || box.res.Generation != 3 {
		t.Fatalf("A rejoin = %+v", box)
	}
}

func TestCommitStaleGenerationRejected(t *testing.T) {
	g, a, _ := stable2(t)
	if code := g.ValidateCommit(a, 2); code != 0 {
		t.Fatalf("current generation = %d", code)
	}
	if code := g.ValidateCommit(a, 1); code != protocol.ErrIllegalGeneration {
		t.Fatalf("stale generation = %d, want 22", code)
	}
	if code := g.ValidateCommit("ghost", 2); code != protocol.ErrUnknownMemberID {
		t.Fatalf("unknown member = %d, want 25", code)
	}
	if code := g.ValidateCommit("", -1); code != protocol.ErrIllegalGeneration {
		t.Fatalf("simple commit to a non-empty group = %d, want 22", code)
	}
	empty := newTestGroup()
	if code := empty.ValidateCommit("", -1); code != 0 {
		t.Fatalf("simple commit to an empty group = %d", code)
	}
}

func TestJoinErrors(t *testing.T) {
	g := newTestGroup()
	box := &joinBox{}
	g.Join(t0, req("never-issued", "X"), box.reply)
	if box.res.Err != protocol.ErrUnknownMemberID {
		t.Fatalf("unknown member id = %d", box.res.Err)
	}
	a, _ := joinNew(t, g, t0, "A")
	_ = a
	other := req("", "B")
	other.ProtocolType = "connect"
	first := &joinBox{}
	g.Join(t0, other, first.reply)
	other.MemberID = first.res.MemberID
	bad := &joinBox{}
	g.Join(t0, other, bad.reply)
	if bad.res.Err != protocol.ErrInconsistentGroupProtocol {
		t.Fatalf("protocol type mismatch = %d", bad.res.Err)
	}
	third := &joinBox{}
	g.Join(t0, req("", "C"), third.reply)
	noShare := req(third.res.MemberID, "C")
	noShare.Protocols = []Protocol{{Name: "sticky"}}
	r := &joinBox{}
	g.Join(t0, noShare, r.reply)
	if r.res.Err != protocol.ErrInconsistentGroupProtocol {
		t.Fatalf("no shared protocol = %d", r.res.Err)
	}
}

func TestSyncErrors(t *testing.T) {
	g, a, _ := stable2(t)
	for _, c := range []struct {
		member string
		gen    int32
		want   int16
	}{{"ghost", 2, protocol.ErrUnknownMemberID}, {a, 1, protocol.ErrIllegalGeneration}} {
		box := &syncBox{}
		g.Sync(t0, c.member, c.gen, nil, box.reply)
		if box.n != 1 || box.res.Err != c.want {
			t.Fatalf("sync(%s,%d) = %+v, want %d", c.member, c.gen, box, c.want)
		}
	}
}
