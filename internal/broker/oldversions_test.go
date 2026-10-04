package broker

import (
	"testing"

	"github.com/sathwikbairaboina2/kafka-go/internal/protocol"
	"github.com/sathwikbairaboina2/kafka-go/internal/record"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// librdkafka only enables message format v2 and consumer groups when the broker advertises the old
// minimum versions, so the oldest supported version of each API must work end to end.

func TestProduceV3AndFetchV4(t *testing.T) {
	e := newEnv(t)
	body, _ := e.callV(t, produceReq(-1, "orders", 0, mkBatch(2, "old")), 3)
	var pr kmsg.ProduceResponse
	decode(t, &pr, 3, body)
	if p := pr.Topics[0].Partitions[0]; p.ErrorCode != 0 || p.BaseOffset != 0 {
		t.Fatalf("produce v3 = %+v", p)
	}
	body, _ = e.callV(t, fetchReq("orders", 0, 0, 0, 1, 1<<20, 1<<20), 4)
	var fr kmsg.FetchResponse
	decode(t, &fr, 4, body)
	p := fr.Topics[0].Partitions[0]
	h, err := record.ReadHeader(p.RecordBatches)
	if err != nil || p.ErrorCode != 0 || h.RecordCount != 2 {
		t.Fatalf("fetch v4 = %+v (%v)", p, err)
	}
	// an empty fetch must carry a zero-length record set, never a null one
	body, _ = e.callV(t, fetchReq("orders", 0, 2, 0, 1, 1<<20, 1<<20), 4)
	fr = kmsg.FetchResponse{}
	decode(t, &fr, 4, body)
	if len(fr.Topics[0].Partitions[0].RecordBatches) != 0 {
		t.Fatalf("expected empty records")
	}
}

func TestGroupOldVersionsEndToEnd(t *testing.T) {
	e := newEnv(t)
	produce(t, e, -1, "orders", 0, mkBatch(1, "x"))

	// FindCoordinator v0
	fc := kmsg.NewPtrFindCoordinatorRequest()
	fc.CoordinatorKey = "g"
	body, _ := e.callV(t, fc, 0)
	var fo kmsg.FindCoordinatorResponse
	decode(t, &fo, 0, body)
	if fo.ErrorCode != 0 || fo.NodeID != 1 {
		t.Fatalf("FindCoordinator v0 = %+v", fo)
	}

	// JoinGroup v1: no MEMBER_ID_REQUIRED round trip, the broker assigns the id and joins at once
	join := joinReq("g", "", 10000)
	body, _ = e.callV(t, join, 1)
	var jo kmsg.JoinGroupResponse
	decode(t, &jo, 1, body)
	if jo.ErrorCode != 0 || jo.MemberID == "" || jo.Generation != 1 || jo.LeaderID != jo.MemberID {
		t.Fatalf("JoinGroup v1 = %+v", jo)
	}
	member := jo.MemberID

	// SyncGroup v0
	sync := kmsg.NewPtrSyncGroupRequest()
	sync.Group, sync.Generation, sync.MemberID = "g", 1, member
	sync.GroupAssignment = []kmsg.SyncGroupRequestGroupAssignment{{MemberID: member, MemberAssignment: []byte("a")}}
	body, _ = e.callV(t, sync, 0)
	var so kmsg.SyncGroupResponse
	decode(t, &so, 0, body)
	if so.ErrorCode != 0 || string(so.MemberAssignment) != "a" {
		t.Fatalf("SyncGroup v0 = %+v", so)
	}

	// Heartbeat v0
	hb := kmsg.NewPtrHeartbeatRequest()
	hb.Group, hb.Generation, hb.MemberID = "g", 1, member
	body, _ = e.callV(t, hb, 0)
	var ho kmsg.HeartbeatResponse
	decode(t, &ho, 0, body)
	if ho.ErrorCode != 0 {
		t.Fatalf("Heartbeat v0 = %d", ho.ErrorCode)
	}

	// OffsetCommit v2 then OffsetFetch v1
	oc := kmsg.NewPtrOffsetCommitRequest()
	oc.Group, oc.MemberID, oc.Generation = "g", member, 1
	oc.Topics = []kmsg.OffsetCommitRequestTopic{{Topic: "orders", Partitions: []kmsg.OffsetCommitRequestTopicPartition{{Partition: 0, Offset: 1}}}}
	body, _ = e.callV(t, oc, 2)
	var oco kmsg.OffsetCommitResponse
	decode(t, &oco, 2, body)
	if oco.Topics[0].Partitions[0].ErrorCode != 0 {
		t.Fatalf("OffsetCommit v2 = %+v", oco)
	}
	of := kmsg.NewPtrOffsetFetchRequest()
	of.Group = "g"
	of.Topics = []kmsg.OffsetFetchRequestTopic{{Topic: "orders", Partitions: []int32{0}}}
	body, _ = e.callV(t, of, 1)
	var ofo kmsg.OffsetFetchResponse
	decode(t, &ofo, 1, body)
	if ofo.Topics[0].Partitions[0].Offset != 1 {
		t.Fatalf("OffsetFetch v1 = %+v", ofo)
	}

	// LeaveGroup v0
	lv := kmsg.NewPtrLeaveGroupRequest()
	lv.Group, lv.MemberID = "g", member
	body, _ = e.callV(t, lv, 0)
	var lo kmsg.LeaveGroupResponse
	decode(t, &lo, 0, body)
	if lo.ErrorCode != protocol.ErrNone {
		t.Fatalf("LeaveGroup v0 = %d", lo.ErrorCode)
	}
}
