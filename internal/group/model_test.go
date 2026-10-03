package group

import (
	"fmt"
	"testing"
	"time"

	"github.com/sathwikbairaboina2/kafka-go/internal/protocol"
	"pgregory.net/rapid"
)

func TestGroupModel(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		n := 0
		g := NewGroup("g", func() string { n++; return fmt.Sprintf("%d", n) })
		now := t0
		var ids []string
		lastGen := map[string]int32{}
		var counters []*int
		prevGen := int32(0)

		joinCB := func(id string) (func(JoinResult), *int) {
			c := new(int)
			counters = append(counters, c)
			return func(r JoinResult) {
				*c++
				if r.Err == 0 && id != "" {
					lastGen[id] = r.Generation
				}
			}, c
		}
		syncCB := func() func(SyncResult) {
			c := new(int)
			counters = append(counters, c)
			return func(SyncResult) { *c++ }
		}
		pick := func(label string) (string, bool) {
			if len(ids) == 0 {
				return "", false
			}
			return ids[rapid.IntRange(0, len(ids)-1).Draw(rt, label)], true
		}
		genFor := func(id string) int32 {
			switch rapid.IntRange(0, 3).Draw(rt, "genpick") {
			case 0:
				return g.Generation() - 1
			case 1:
				return g.Generation() + 1
			default:
				if v, ok := lastGen[id]; ok {
					return v
				}
				return g.Generation()
			}
		}

		rt.Repeat(map[string]func(*rapid.T){
			"newMember": func(rt *rapid.T) {
				var got string
				first := new(int)
				counters = append(counters, first)
				g.Join(now, req("", "c"), func(r JoinResult) { *first++; got = r.MemberID })
				if got == "" {
					rt.Fatal("MEMBER_ID_REQUIRED reply carried no id")
				}
				ids = append(ids, got)
				cb, _ := joinCB(got)
				g.Join(now, req(got, "c"), cb)
			},
			"rejoin": func(rt *rapid.T) {
				if id, ok := pick("rejoinMember"); ok {
					cb, _ := joinCB(id)
					g.Join(now, req(id, "c"), cb)
				}
			},
			"syncLeader": func(rt *rapid.T) {
				if id, ok := pick("syncLeaderMember"); ok {
					a := map[string][]byte{}
					for _, m := range g.Members() {
						a[m] = []byte(m)
					}
					g.Sync(now, id, genFor(id), a, syncCB())
				}
			},
			"syncFollower": func(rt *rapid.T) {
				if id, ok := pick("syncFollowerMember"); ok {
					g.Sync(now, id, genFor(id), nil, syncCB())
				}
			},
			"heartbeat": func(rt *rapid.T) {
				if id, ok := pick("hbMember"); ok {
					g.Heartbeat(now, id, genFor(id))
				}
			},
			"leave": func(rt *rapid.T) {
				if id, ok := pick("leaveMember"); ok {
					g.Leave(now, id)
				}
			},
			"tick": func(rt *rapid.T) {
				now = now.Add(time.Duration(rapid.IntRange(0, 60).Draw(rt, "advance")) * time.Second)
				g.Tick(now)
			},
			"commit": func(rt *rapid.T) {
				id, ok := pick("commitMember")
				if !ok {
					id = ""
				}
				gen := rapid.Int32Range(-1, g.Generation()+1).Draw(rt, "commitGen")
				code := g.ValidateCommit(id, gen)
				if code == 0 {
					simple := id == "" && gen < 0 && g.State() == Empty
					isMember := false
					for _, m := range g.Members() {
						isMember = isMember || m == id
					}
					if !simple && !(isMember && gen == g.Generation()) {
						rt.Fatalf("ValidateCommit(%q,%d) = 0 but member=%v generation=%d", id, gen, isMember, g.Generation())
					}
				}
			},
			"": func(rt *rapid.T) {
				if g.Generation() < prevGen {
					rt.Fatalf("generation decreased %d -> %d", prevGen, g.Generation())
				}
				prevGen = g.Generation()
				if (g.State() == Empty) != (len(g.Members()) == 0) {
					rt.Fatalf("state %v with %d members", g.State(), len(g.Members()))
				}
				if g.State() == Stable || g.State() == CompletingRebalance {
					if _, ok := g.members[g.leader]; !ok {
						rt.Fatalf("leader %q is not a member in %v", g.leader, g.State())
					}
				}
				if g.State() == Stable {
					for id, m := range g.members {
						if m.assignment == nil {
							rt.Fatalf("member %s has no assignment in Stable", id)
						}
					}
				}
				if g.State() == PreparingRebalance && g.deadline.IsZero() {
					rt.Fatal("Preparing without a deadline")
				}
				for i, c := range counters {
					if *c > 1 {
						rt.Fatalf("reply %d called %d times", i, *c)
					}
				}
			},
		})
	})
}

func TestModelCoversRebalanceCodes(t *testing.T) {
	// sanity: the error constants the model relies on are distinct
	codes := map[int16]bool{}
	for _, c := range []int16{protocol.ErrIllegalGeneration, protocol.ErrUnknownMemberID, protocol.ErrRebalanceInProgress, protocol.ErrMemberIDRequired} {
		if codes[c] {
			t.Fatalf("duplicate code %d", c)
		}
		codes[c] = true
	}
}
