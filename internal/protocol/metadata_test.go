package protocol

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kmsg"
)

func sp(s string) *string { return &s }

func TestMetadataRequestOracle(t *testing.T) {
	named := kmsg.NewPtrMetadataRequest()
	named.Topics = []kmsg.MetadataRequestTopic{{Topic: sp("a")}, {Topic: sp("b")}}
	named.AllowAutoTopicCreation = true

	all := kmsg.NewPtrMetadataRequest() // nil topics = null array

	empty := kmsg.NewPtrMetadataRequest()
	empty.Topics = []kmsg.MetadataRequestTopic{}

	var q MetadataRequest
	r := NewReader(kmsgBody(named, 4))
	if err := q.Decode(r, 4); err != nil {
		t.Fatal(err)
	}
	mustConsume(t, r)
	if q.AllTopics || len(q.Topics) != 2 || q.Topics[0] != "a" || q.Topics[1] != "b" || !q.AllowAutoCreate {
		t.Fatalf("named = %+v", q)
	}

	q = MetadataRequest{}
	r = NewReader(kmsgBody(all, 4))
	if err := q.Decode(r, 4); err != nil {
		t.Fatal(err)
	}
	mustConsume(t, r)
	if !q.AllTopics || len(q.Topics) != 0 {
		t.Fatalf("all = %+v", q)
	}

	q = MetadataRequest{}
	r = NewReader(kmsgBody(empty, 4))
	if err := q.Decode(r, 4); err != nil {
		t.Fatal(err)
	}
	mustConsume(t, r)
	if q.AllTopics || len(q.Topics) != 0 {
		t.Fatalf("empty = %+v", q)
	}
}

func TestMetadataResponseOracle(t *testing.T) {
	cluster := "kafka-go"
	resp := MetadataResponse{
		Brokers:      []MetadataBroker{{NodeID: 1, Host: "h", Port: 9092}},
		ClusterID:    &cluster,
		ControllerID: 1,
		Topics: []MetadataTopic{
			{Name: "orders", Partitions: []MetadataPartition{
				{Index: 0, Leader: 1, Replicas: []int32{1}, ISR: []int32{1}},
				{Index: 1, Leader: 1, Replicas: []int32{1}, ISR: []int32{1}},
			}},
			{ErrorCode: ErrUnknownTopicOrPartition, Name: "missing"},
		},
	}
	w := NewWriter(0)
	resp.Encode(w, 4)
	var out kmsg.MetadataResponse
	kmsgDecode(t, &out, 4, w.Buf())
	if len(out.Brokers) != 1 || out.Brokers[0].NodeID != 1 || out.Brokers[0].Host != "h" || out.Brokers[0].Port != 9092 {
		t.Fatalf("brokers = %+v", out.Brokers)
	}
	if out.ClusterID == nil || *out.ClusterID != cluster || out.ControllerID != 1 {
		t.Fatalf("cluster/controller = %v %d", out.ClusterID, out.ControllerID)
	}
	if len(out.Topics) != 2 || *out.Topics[0].Topic != "orders" || len(out.Topics[0].Partitions) != 2 {
		t.Fatalf("topics = %+v", out.Topics)
	}
	p := out.Topics[0].Partitions[1]
	if p.Partition != 1 || p.Leader != 1 || len(p.Replicas) != 1 || len(p.ISR) != 1 {
		t.Fatalf("partition = %+v", p)
	}
	if out.Topics[1].ErrorCode != ErrUnknownTopicOrPartition || *out.Topics[1].Topic != "missing" {
		t.Fatalf("missing topic = %+v", out.Topics[1])
	}
}

func TestFindCoordinatorOracle(t *testing.T) {
	for v := int16(0); v <= 2; v++ {
		req := kmsg.NewPtrFindCoordinatorRequest()
		req.CoordinatorKey = "grp"
		req.CoordinatorType = 0
		var q FindCoordinatorRequest
		r := NewReader(kmsgBody(req, v))
		if err := q.Decode(r, v); err != nil {
			t.Fatalf("v%d: %v", v, err)
		}
		mustConsume(t, r)
		if q.Key != "grp" || q.KeyType != 0 {
			t.Fatalf("v%d req = %+v", v, q)
		}

		resp := FindCoordinatorResponse{NodeID: 1, Host: "h", Port: 9092}
		w := NewWriter(0)
		resp.Encode(w, v)
		var out kmsg.FindCoordinatorResponse
		kmsgDecode(t, &out, v, w.Buf())
		if out.ErrorCode != 0 || out.NodeID != 1 || out.Host != "h" || out.Port != 9092 {
			t.Fatalf("v%d resp = %+v", v, out)
		}

		msg := "nope"
		resp = FindCoordinatorResponse{ErrorCode: ErrInvalidGroupID, ErrorMessage: &msg}
		w = NewWriter(0)
		resp.Encode(w, v)
		out = kmsg.FindCoordinatorResponse{}
		kmsgDecode(t, &out, v, w.Buf())
		if out.ErrorCode != ErrInvalidGroupID || (v >= 1 && (out.ErrorMessage == nil || *out.ErrorMessage != "nope")) {
			t.Fatalf("v%d err resp = %+v", v, out)
		}
	}
}
