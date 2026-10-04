package protocol

import (
	"bytes"
	"testing"

	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestJoinGroupRequestOracle(t *testing.T) {
	for v := int16(0); v <= 5; v++ {
		req := kmsg.NewPtrJoinGroupRequest()
		req.Group = "g"
		req.SessionTimeoutMillis = 45000
		req.RebalanceTimeoutMillis = 60000
		req.MemberID = "m-1"
		req.InstanceID = sp("inst")
		req.ProtocolType = "consumer"
		req.Protocols = []kmsg.JoinGroupRequestProtocol{
			{Name: "range", Metadata: []byte{1, 2, 3}},
			{Name: "roundrobin", Metadata: []byte{}},
		}
		var q JoinGroupRequest
		r := NewReader(kmsgBody(req, v))
		if err := q.Decode(r, v); err != nil {
			t.Fatalf("v%d: %v", v, err)
		}
		mustConsume(t, r)
		wantRebalance := int32(60000)
		if v == 0 {
			wantRebalance = 45000 // v0 has no rebalance timeout; it reuses the session timeout
		}
		if q.GroupID != "g" || q.SessionTimeoutMs != 45000 || q.RebalanceTimeoutMs != wantRebalance || q.MemberID != "m-1" ||
			q.ProtocolType != "consumer" || len(q.Protocols) != 2 ||
			q.Protocols[0].Name != "range" || !bytes.Equal(q.Protocols[0].Metadata, []byte{1, 2, 3}) || q.Protocols[1].Name != "roundrobin" {
			t.Fatalf("v%d req = %+v", v, q)
		}
		if (v >= 5) != (q.GroupInstanceID != nil && *q.GroupInstanceID == "inst") {
			t.Fatalf("v%d instance id = %v", v, q.GroupInstanceID)
		}
	}
}

func TestJoinGroupResponseOracle(t *testing.T) {
	for v := int16(0); v <= 5; v++ {
		resp := JoinGroupResponse{
			ErrorCode: 0, GenerationID: 3, ProtocolName: "range", Leader: "m-1", MemberID: "m-1",
			Members: []JoinMember{{MemberID: "m-1", Metadata: []byte{1}}, {MemberID: "m-2", Metadata: []byte{2, 2}}},
		}
		w := NewWriter(0)
		resp.Encode(w, v)
		var out kmsg.JoinGroupResponse
		kmsgDecode(t, &out, v, w.Buf())
		if out.Generation != 3 || out.Protocol == nil || *out.Protocol != "range" || out.LeaderID != "m-1" || out.MemberID != "m-1" ||
			len(out.Members) != 2 || out.Members[1].MemberID != "m-2" || !bytes.Equal(out.Members[1].ProtocolMetadata, []byte{2, 2}) {
			t.Fatalf("v%d resp = %+v", v, out)
		}

		resp = JoinGroupResponse{ErrorCode: ErrMemberIDRequired, GenerationID: -1, MemberID: "c-abc"}
		w = NewWriter(0)
		resp.Encode(w, v)
		out = kmsg.JoinGroupResponse{}
		kmsgDecode(t, &out, v, w.Buf())
		if out.ErrorCode != ErrMemberIDRequired || out.MemberID != "c-abc" || out.Generation != -1 || len(out.Members) != 0 {
			t.Fatalf("v%d err resp = %+v", v, out)
		}
	}
}

func TestSyncGroupOracle(t *testing.T) {
	for v := int16(0); v <= 3; v++ {
		req := kmsg.NewPtrSyncGroupRequest()
		req.Group = "g"
		req.Generation = 4
		req.MemberID = "m-1"
		req.GroupAssignment = []kmsg.SyncGroupRequestGroupAssignment{
			{MemberID: "m-1", MemberAssignment: []byte{9, 9}},
			{MemberID: "m-2", MemberAssignment: []byte{8}},
		}
		var q SyncGroupRequest
		r := NewReader(kmsgBody(req, v))
		if err := q.Decode(r, v); err != nil {
			t.Fatalf("v%d: %v", v, err)
		}
		mustConsume(t, r)
		if q.GroupID != "g" || q.GenerationID != 4 || q.MemberID != "m-1" || q.GroupInstanceID != nil ||
			len(q.Assignments) != 2 || q.Assignments[1].MemberID != "m-2" || !bytes.Equal(q.Assignments[0].Assignment, []byte{9, 9}) {
			t.Fatalf("v%d req = %+v", v, q)
		}

		resp := SyncGroupResponse{Assignment: []byte{1, 2, 3}}
		w := NewWriter(0)
		resp.Encode(w, v)
		var out kmsg.SyncGroupResponse
		kmsgDecode(t, &out, v, w.Buf())
		if out.ErrorCode != 0 || !bytes.Equal(out.MemberAssignment, []byte{1, 2, 3}) {
			t.Fatalf("v%d resp = %+v", v, out)
		}
	}
}

func TestDecodeCopiesGroupBytes(t *testing.T) {
	req := kmsg.NewPtrSyncGroupRequest()
	req.Group = "g"
	req.MemberID = "m"
	req.GroupAssignment = []kmsg.SyncGroupRequestGroupAssignment{{MemberID: "m", MemberAssignment: []byte{5, 5, 5}}}
	body := kmsgBody(req, 3)
	var q SyncGroupRequest
	if err := q.Decode(NewReader(body), 3); err != nil {
		t.Fatal(err)
	}
	for i := range body {
		body[i] = 0
	}
	if !bytes.Equal(q.Assignments[0].Assignment, []byte{5, 5, 5}) {
		t.Fatalf("assignment aliases frame buffer: %v", q.Assignments[0].Assignment)
	}
}
