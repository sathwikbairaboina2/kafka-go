package broker

import (
	"sync"
	"testing"

	"github.com/sathwikbairaboina2/kafka-go/internal/protocol"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestFindCoordinator(t *testing.T) {
	e := newEnv(t)
	req := kmsg.NewPtrFindCoordinatorRequest()
	req.CoordinatorKey = "g"
	body, _ := e.call(t, req)
	var out kmsg.FindCoordinatorResponse
	decode(t, &out, 2, body)
	if out.ErrorCode != 0 || out.NodeID != 1 || out.Host != "localhost" || out.Port != 9092 {
		t.Fatalf("resp = %+v", out)
	}

	req = kmsg.NewPtrFindCoordinatorRequest()
	body, _ = e.call(t, req) // empty key
	out = kmsg.FindCoordinatorResponse{}
	decode(t, &out, 2, body)
	if out.ErrorCode != protocol.ErrInvalidGroupID {
		t.Fatalf("empty key = %d", out.ErrorCode)
	}

	req = kmsg.NewPtrFindCoordinatorRequest()
	req.CoordinatorKey = "txn"
	req.CoordinatorType = 1
	body, _ = e.call(t, req)
	out = kmsg.FindCoordinatorResponse{}
	decode(t, &out, 2, body)
	if out.ErrorCode != protocol.ErrCoordinatorNotAvailable {
		t.Fatalf("transaction key type = %d", out.ErrorCode)
	}
}

func joinReq(group, member string, session int32) *kmsg.JoinGroupRequest {
	req := kmsg.NewPtrJoinGroupRequest()
	req.Group = group
	req.MemberID = member
	req.SessionTimeoutMillis = session
	req.RebalanceTimeoutMillis = 30000
	req.ProtocolType = "consumer"
	req.Protocols = []kmsg.JoinGroupRequestProtocol{{Name: "range", Metadata: []byte("meta-" + member)}}
	return req
}

func doJoin(t testing.TB, e *env, group, member string) kmsg.JoinGroupResponse {
	t.Helper()
	body, _ := e.call(t, joinReq(group, member, 10000))
	var out kmsg.JoinGroupResponse
	decode(t, &out, 5, body)
	return out
}

func TestJoinGroupValidation(t *testing.T) {
	e := newEnv(t)
	if out := doJoin(t, e, "", ""); out.ErrorCode != protocol.ErrInvalidGroupID {
		t.Fatalf("empty group id = %d", out.ErrorCode)
	}
	body, _ := e.call(t, joinReq("g", "", 100))
	var out kmsg.JoinGroupResponse
	decode(t, &out, 5, body)
	if out.ErrorCode != protocol.ErrInvalidSessionTimeout {
		t.Fatalf("session 100ms = %d", out.ErrorCode)
	}
	if out := doJoin(t, e, "g", ""); out.ErrorCode != protocol.ErrMemberIDRequired || out.MemberID == "" {
		t.Fatalf("first join = %+v", out)
	}
}

func TestTwoMemberJoinSyncHeartbeatLeave(t *testing.T) {
	e := newEnv(t)
	a := doJoin(t, e, "g", "").MemberID
	b := doJoin(t, e, "g", "").MemberID
	if a == b {
		t.Fatal("member ids must differ")
	}
	// A joins alone (gen 1) and then both rejoin together (gen 2)
	if r := doJoin(t, e, "g", a); r.ErrorCode != 0 || r.Generation != 1 || r.LeaderID != a {
		t.Fatalf("A solo join = %+v", r)
	}
	var wg sync.WaitGroup
	results := make([]kmsg.JoinGroupResponse, 2)
	for i, m := range []string{a, b} {
		wg.Add(1)
		go func() { defer wg.Done(); results[i] = doJoin(t, e, "g", m) }()
	}
	wg.Wait()
	for i, r := range results {
		if r.ErrorCode != 0 || r.Generation != 2 || r.LeaderID != a || *r.Protocol != "range" {
			t.Fatalf("join %d = %+v", i, r)
		}
	}
	if len(results[0].Members) != 2 || len(results[1].Members) != 0 {
		t.Fatalf("members: leader %d follower %d", len(results[0].Members), len(results[1].Members))
	}

	syncs := make([]kmsg.SyncGroupResponse, 2)
	for i, m := range []string{a, b} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req := kmsg.NewPtrSyncGroupRequest()
			req.Group, req.Generation, req.MemberID = "g", 2, m
			if m == a {
				req.GroupAssignment = []kmsg.SyncGroupRequestGroupAssignment{
					{MemberID: a, MemberAssignment: []byte("pa")}, {MemberID: b, MemberAssignment: []byte("pb")},
				}
			}
			body, _ := e.call(t, req)
			decode(t, &syncs[i], 3, body)
		}()
	}
	wg.Wait()
	if string(syncs[0].MemberAssignment) != "pa" || string(syncs[1].MemberAssignment) != "pb" || syncs[0].ErrorCode != 0 {
		t.Fatalf("syncs = %+v", syncs)
	}

	hb := func(member string, gen int32) int16 {
		req := kmsg.NewPtrHeartbeatRequest()
		req.Group, req.Generation, req.MemberID = "g", gen, member
		body, _ := e.call(t, req)
		var out kmsg.HeartbeatResponse
		decode(t, &out, 3, body)
		return out.ErrorCode
	}
	if hb(a, 2) != 0 || hb(b, 2) != 0 || hb(a, 1) != protocol.ErrIllegalGeneration || hb("ghost", 2) != protocol.ErrUnknownMemberID {
		t.Fatal("unexpected heartbeat codes")
	}

	leave := kmsg.NewPtrLeaveGroupRequest()
	leave.Group, leave.MemberID = "g", b
	body, _ := e.call(t, leave)
	var lo kmsg.LeaveGroupResponse
	decode(t, &lo, 1, body)
	if lo.ErrorCode != 0 {
		t.Fatalf("leave = %d", lo.ErrorCode)
	}
	if code := hb(a, 2); code != protocol.ErrRebalanceInProgress {
		t.Fatalf("heartbeat after leave = %d, want 27", code)
	}
}

func TestOffsetCommitThenFetch(t *testing.T) {
	e := newEnv(t)
	produce(t, e, -1, "orders", 0, mkBatch(1, "x")) // creates the topic with 3 partitions

	commit := func(member string, gen int32, partition int32, offset int64) kmsg.OffsetCommitResponse {
		req := kmsg.NewPtrOffsetCommitRequest()
		req.Group, req.MemberID, req.Generation = "g", member, gen
		meta := "md"
		req.Topics = []kmsg.OffsetCommitRequestTopic{{Topic: "orders", Partitions: []kmsg.OffsetCommitRequestTopicPartition{
			{Partition: partition, Offset: offset, LeaderEpoch: -1, Metadata: &meta},
		}}}
		body, _ := e.call(t, req)
		var out kmsg.OffsetCommitResponse
		decode(t, &out, 7, body)
		return out
	}
	// a simple consumer (generation -1, no member) may commit to an empty group
	if out := commit("", -1, 1, 77); out.Topics[0].Partitions[0].ErrorCode != 0 {
		t.Fatalf("simple commit = %d", out.Topics[0].Partitions[0].ErrorCode)
	}
	// unknown partition
	if out := commit("", -1, 9, 1); out.Topics[0].Partitions[0].ErrorCode != protocol.ErrUnknownTopicOrPartition {
		t.Fatalf("unknown partition = %d", out.Topics[0].Partitions[0].ErrorCode)
	}

	fetch := func(topics []kmsg.OffsetFetchRequestTopic) kmsg.OffsetFetchResponse {
		req := kmsg.NewPtrOffsetFetchRequest()
		req.Group = "g"
		req.Topics = topics
		body, _ := e.call(t, req)
		var out kmsg.OffsetFetchResponse
		decode(t, &out, 7, body)
		return out
	}
	out := fetch([]kmsg.OffsetFetchRequestTopic{{Topic: "orders", Partitions: []int32{0, 1}}})
	ps := out.Topics[0].Partitions
	if ps[0].Offset != -1 || ps[0].LeaderEpoch != -1 || ps[0].Metadata == nil || *ps[0].Metadata != "" || ps[0].ErrorCode != 0 {
		t.Fatalf("missing partition = %+v", ps[0])
	}
	if ps[1].Offset != 77 || *ps[1].Metadata != "md" {
		t.Fatalf("committed partition = %+v", ps[1])
	}
	all := fetch(nil) // null topics
	if len(all.Topics) != 1 || len(all.Topics[0].Partitions) != 1 || all.Topics[0].Partitions[0].Offset != 77 {
		t.Fatalf("all = %+v", all.Topics)
	}
}

func TestOffsetCommitStaleGenerationRejected(t *testing.T) {
	e := newEnv(t)
	produce(t, e, -1, "orders", 0, mkBatch(1, "x"))
	a := doJoin(t, e, "g", "").MemberID
	if r := doJoin(t, e, "g", a); r.ErrorCode != 0 {
		t.Fatalf("join = %+v", r)
	}
	sync := kmsg.NewPtrSyncGroupRequest()
	sync.Group, sync.Generation, sync.MemberID = "g", 1, a
	sync.GroupAssignment = []kmsg.SyncGroupRequestGroupAssignment{{MemberID: a, MemberAssignment: []byte("x")}}
	e.call(t, sync)

	req := kmsg.NewPtrOffsetCommitRequest()
	req.Group, req.MemberID, req.Generation = "g", a, 0 // stale: the group is at generation 1
	req.Topics = []kmsg.OffsetCommitRequestTopic{{Topic: "orders", Partitions: []kmsg.OffsetCommitRequestTopicPartition{{Partition: 0, Offset: 5, LeaderEpoch: -1}}}}
	body, _ := e.call(t, req)
	var out kmsg.OffsetCommitResponse
	decode(t, &out, 7, body)
	if code := out.Topics[0].Partitions[0].ErrorCode; code != protocol.ErrIllegalGeneration {
		t.Fatalf("stale commit = %d", code)
	}
	fetchReq := kmsg.NewPtrOffsetFetchRequest()
	fetchReq.Group = "g"
	fetchReq.Topics = []kmsg.OffsetFetchRequestTopic{{Topic: "orders", Partitions: []int32{0}}}
	body, _ = e.call(t, fetchReq)
	var fo kmsg.OffsetFetchResponse
	decode(t, &fo, 7, body)
	if fo.Topics[0].Partitions[0].Offset != -1 {
		t.Fatalf("stale commit was persisted: offset %d", fo.Topics[0].Partitions[0].Offset)
	}
}
