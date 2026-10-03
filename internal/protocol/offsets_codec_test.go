package protocol

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestHeartbeatOracle(t *testing.T) {
	req := kmsg.NewPtrHeartbeatRequest()
	req.Group = "g"
	req.Generation = 7
	req.MemberID = "m"
	var q HeartbeatRequest
	r := NewReader(kmsgBody(req, 3))
	if err := q.Decode(r, 3); err != nil {
		t.Fatal(err)
	}
	mustConsume(t, r)
	if q.GroupID != "g" || q.GenerationID != 7 || q.MemberID != "m" || q.GroupInstanceID != nil {
		t.Fatalf("req = %+v", q)
	}
	w := NewWriter(0)
	(&HeartbeatResponse{ErrorCode: ErrRebalanceInProgress}).Encode(w, 3)
	var out kmsg.HeartbeatResponse
	kmsgDecode(t, &out, 3, w.Buf())
	if out.ErrorCode != ErrRebalanceInProgress {
		t.Fatalf("resp = %+v", out)
	}
}

func TestLeaveGroupOracle(t *testing.T) {
	req := kmsg.NewPtrLeaveGroupRequest()
	req.Group = "g"
	req.MemberID = "m"
	var q LeaveGroupRequest
	r := NewReader(kmsgBody(req, 1))
	if err := q.Decode(r, 1); err != nil {
		t.Fatal(err)
	}
	mustConsume(t, r)
	if q.GroupID != "g" || q.MemberID != "m" {
		t.Fatalf("req = %+v", q)
	}
	w := NewWriter(0)
	(&LeaveGroupResponse{ErrorCode: ErrUnknownMemberID}).Encode(w, 1)
	var out kmsg.LeaveGroupResponse
	kmsgDecode(t, &out, 1, w.Buf())
	if out.ErrorCode != ErrUnknownMemberID {
		t.Fatalf("resp = %+v", out)
	}
}

func TestOffsetCommitOracle(t *testing.T) {
	req := kmsg.NewPtrOffsetCommitRequest()
	req.Group = "g"
	req.Generation = 2
	req.MemberID = "m"
	req.Topics = []kmsg.OffsetCommitRequestTopic{
		{Topic: "a", Partitions: []kmsg.OffsetCommitRequestTopicPartition{
			{Partition: 0, Offset: 100, LeaderEpoch: -1, Metadata: sp("meta")},
			{Partition: 1, Offset: 5, LeaderEpoch: -1},
		}},
		{Topic: "b", Partitions: []kmsg.OffsetCommitRequestTopicPartition{{Partition: 3, Offset: 9, LeaderEpoch: 4}}},
	}
	var q OffsetCommitRequest
	r := NewReader(kmsgBody(req, 7))
	if err := q.Decode(r, 7); err != nil {
		t.Fatal(err)
	}
	mustConsume(t, r)
	if q.GroupID != "g" || q.GenerationID != 2 || q.MemberID != "m" || len(q.Topics) != 2 || len(q.Topics[0].Partitions) != 2 {
		t.Fatalf("req = %+v", q)
	}
	p := q.Topics[0].Partitions[0]
	if p.Index != 0 || p.Offset != 100 || p.LeaderEpoch != -1 || p.Metadata == nil || *p.Metadata != "meta" {
		t.Fatalf("p = %+v", p)
	}
	if q.Topics[0].Partitions[1].Metadata != nil || q.Topics[1].Partitions[0].LeaderEpoch != 4 {
		t.Fatalf("others = %+v", q.Topics)
	}

	resp := OffsetCommitResponse{Topics: []CommitTopicResponse{
		{Name: "a", Partitions: []CommitPartitionResponse{{Index: 0}, {Index: 1, ErrorCode: ErrIllegalGeneration}}},
	}}
	w := NewWriter(0)
	resp.Encode(w, 7)
	var out kmsg.OffsetCommitResponse
	kmsgDecode(t, &out, 7, w.Buf())
	if len(out.Topics) != 1 || out.Topics[0].Topic != "a" || out.Topics[0].Partitions[1].ErrorCode != ErrIllegalGeneration {
		t.Fatalf("resp = %+v", out)
	}
}

func TestOffsetFetchOracle(t *testing.T) {
	req := kmsg.NewPtrOffsetFetchRequest()
	req.Group = "g"
	req.Topics = []kmsg.OffsetFetchRequestTopic{{Topic: "a", Partitions: []int32{0, 2}}}
	var q OffsetFetchRequest
	r := NewReader(kmsgBody(req, 7))
	if err := q.Decode(r, 7); err != nil {
		t.Fatal(err)
	}
	mustConsume(t, r)
	if q.GroupID != "g" || q.AllTopics || len(q.Topics) != 1 || q.Topics[0].Name != "a" || len(q.Topics[0].Partitions) != 2 || q.Topics[0].Partitions[1] != 2 {
		t.Fatalf("req = %+v", q)
	}

	all := kmsg.NewPtrOffsetFetchRequest()
	all.Group = "g"
	all.Topics = nil // null array
	q = OffsetFetchRequest{}
	r = NewReader(kmsgBody(all, 7))
	if err := q.Decode(r, 7); err != nil {
		t.Fatal(err)
	}
	mustConsume(t, r)
	if !q.AllTopics || len(q.Topics) != 0 {
		t.Fatalf("all = %+v", q)
	}

	resp := OffsetFetchResponse{Topics: []OffsetFetchTopicResponse{
		{Name: "a", Partitions: []OffsetFetchPartitionResponse{
			{Index: 0, Offset: 55, LeaderEpoch: -1, Metadata: sp("m")},
			{Index: 2, Offset: -1, LeaderEpoch: -1, Metadata: sp("")},
		}},
	}}
	w := NewWriter(0)
	resp.Encode(w, 7)
	var out kmsg.OffsetFetchResponse
	kmsgDecode(t, &out, 7, w.Buf())
	if out.ErrorCode != 0 || len(out.Topics) != 1 || len(out.Topics[0].Partitions) != 2 {
		t.Fatalf("resp = %+v", out)
	}
	p := out.Topics[0].Partitions[0]
	if p.Partition != 0 || p.Offset != 55 || p.LeaderEpoch != -1 || p.Metadata == nil || *p.Metadata != "m" {
		t.Fatalf("p = %+v", p)
	}
	if out.Topics[0].Partitions[1].Offset != -1 {
		t.Fatalf("p1 = %+v", out.Topics[0].Partitions[1])
	}
}
