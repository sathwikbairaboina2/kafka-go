package protocol

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestHeartbeatOracle(t *testing.T) {
	for v := int16(0); v <= 3; v++ {
		req := kmsg.NewPtrHeartbeatRequest()
		req.Group = "g"
		req.Generation = 7
		req.MemberID = "m"
		var q HeartbeatRequest
		r := NewReader(kmsgBody(req, v))
		if err := q.Decode(r, v); err != nil {
			t.Fatalf("v%d: %v", v, err)
		}
		mustConsume(t, r)
		if q.GroupID != "g" || q.GenerationID != 7 || q.MemberID != "m" || q.GroupInstanceID != nil {
			t.Fatalf("v%d req = %+v", v, q)
		}
		w := NewWriter(0)
		(&HeartbeatResponse{ErrorCode: ErrRebalanceInProgress}).Encode(w, v)
		var out kmsg.HeartbeatResponse
		kmsgDecode(t, &out, v, w.Buf())
		if out.ErrorCode != ErrRebalanceInProgress {
			t.Fatalf("v%d resp = %+v", v, out)
		}
	}
}

func TestLeaveGroupOracle(t *testing.T) {
	for v := int16(0); v <= 1; v++ {
		req := kmsg.NewPtrLeaveGroupRequest()
		req.Group = "g"
		req.MemberID = "m"
		var q LeaveGroupRequest
		r := NewReader(kmsgBody(req, v))
		if err := q.Decode(r, v); err != nil {
			t.Fatalf("v%d: %v", v, err)
		}
		mustConsume(t, r)
		if q.GroupID != "g" || q.MemberID != "m" {
			t.Fatalf("v%d req = %+v", v, q)
		}
		w := NewWriter(0)
		(&LeaveGroupResponse{ErrorCode: ErrUnknownMemberID}).Encode(w, v)
		var out kmsg.LeaveGroupResponse
		kmsgDecode(t, &out, v, w.Buf())
		if out.ErrorCode != ErrUnknownMemberID {
			t.Fatalf("v%d resp = %+v", v, out)
		}
	}
}

func TestOffsetCommitOracle(t *testing.T) {
	for v := int16(2); v <= 7; v++ {
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
		r := NewReader(kmsgBody(req, v))
		if err := q.Decode(r, v); err != nil {
			t.Fatalf("v%d: %v", v, err)
		}
		mustConsume(t, r)
		if q.GroupID != "g" || q.GenerationID != 2 || q.MemberID != "m" || len(q.Topics) != 2 || len(q.Topics[0].Partitions) != 2 {
			t.Fatalf("v%d req = %+v", v, q)
		}
		p := q.Topics[0].Partitions[0]
		if p.Index != 0 || p.Offset != 100 || p.LeaderEpoch != -1 || p.Metadata == nil || *p.Metadata != "meta" {
			t.Fatalf("v%d p = %+v", v, p)
		}
		wantEpoch := int32(4)
		if v < 6 {
			wantEpoch = -1
		}
		if q.Topics[0].Partitions[1].Metadata != nil || q.Topics[1].Partitions[0].LeaderEpoch != wantEpoch {
			t.Fatalf("v%d others = %+v", v, q.Topics)
		}

		resp := OffsetCommitResponse{Topics: []CommitTopicResponse{
			{Name: "a", Partitions: []CommitPartitionResponse{{Index: 0}, {Index: 1, ErrorCode: ErrIllegalGeneration}}},
		}}
		w := NewWriter(0)
		resp.Encode(w, v)
		var out kmsg.OffsetCommitResponse
		kmsgDecode(t, &out, v, w.Buf())
		if len(out.Topics) != 1 || out.Topics[0].Topic != "a" || out.Topics[0].Partitions[1].ErrorCode != ErrIllegalGeneration {
			t.Fatalf("v%d resp = %+v", v, out)
		}
	}
}

func TestOffsetFetchOracle(t *testing.T) {
	for v := int16(1); v <= 7; v++ {
		req := kmsg.NewPtrOffsetFetchRequest()
		req.Group = "g"
		req.Topics = []kmsg.OffsetFetchRequestTopic{{Topic: "a", Partitions: []int32{0, 2}}}
		var q OffsetFetchRequest
		r := NewReader(kmsgBody(req, v))
		if err := q.Decode(r, v); err != nil {
			t.Fatalf("v%d: %v", v, err)
		}
		mustConsume(t, r)
		if q.GroupID != "g" || q.AllTopics || len(q.Topics) != 1 || q.Topics[0].Name != "a" || len(q.Topics[0].Partitions) != 2 || q.Topics[0].Partitions[1] != 2 {
			t.Fatalf("v%d req = %+v", v, q)
		}

		if v >= 2 { // the null topics array exists from v2
			all := kmsg.NewPtrOffsetFetchRequest()
			all.Group = "g"
			all.Topics = nil
			q = OffsetFetchRequest{}
			r = NewReader(kmsgBody(all, v))
			if err := q.Decode(r, v); err != nil {
				t.Fatalf("v%d all: %v", v, err)
			}
			mustConsume(t, r)
			if !q.AllTopics || len(q.Topics) != 0 {
				t.Fatalf("v%d all = %+v", v, q)
			}
		}

		resp := OffsetFetchResponse{Topics: []OffsetFetchTopicResponse{
			{Name: "a", Partitions: []OffsetFetchPartitionResponse{
				{Index: 0, Offset: 55, LeaderEpoch: -1, Metadata: sp("m")},
				{Index: 2, Offset: -1, LeaderEpoch: -1, Metadata: sp("")},
			}},
		}}
		w := NewWriter(0)
		resp.Encode(w, v)
		var out kmsg.OffsetFetchResponse
		kmsgDecode(t, &out, v, w.Buf())
		if out.ErrorCode != 0 || len(out.Topics) != 1 || len(out.Topics[0].Partitions) != 2 {
			t.Fatalf("v%d resp = %+v", v, out)
		}
		p := out.Topics[0].Partitions[0]
		if p.Partition != 0 || p.Offset != 55 || (v >= 5 && p.LeaderEpoch != -1) || p.Metadata == nil || *p.Metadata != "m" {
			t.Fatalf("v%d p = %+v", v, p)
		}
		if out.Topics[0].Partitions[1].Offset != -1 {
			t.Fatalf("v%d p1 = %+v", v, out.Topics[0].Partitions[1])
		}
	}
}
