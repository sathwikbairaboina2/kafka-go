package protocol

import (
	"bytes"
	"testing"

	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestProduceRequestOracle(t *testing.T) {
	rec := bytes.Repeat([]byte{0xab, 0xcd}, 35) // 70 arbitrary bytes
	req := kmsg.NewPtrProduceRequest()
	req.Acks = -1
	req.TimeoutMillis = 1500
	req.Topics = []kmsg.ProduceRequestTopic{
		{Topic: "a", Partitions: []kmsg.ProduceRequestTopicPartition{{Partition: 0, Records: rec}, {Partition: 1}}},
		{Topic: "b", Partitions: []kmsg.ProduceRequestTopicPartition{{Partition: 2, Records: rec}, {Partition: 3}}},
	}
	for v := int16(3); v <= 7; v++ {
		checkProduceRequest(t, req, v, rec)
	}
}

func checkProduceRequest(t *testing.T, req *kmsg.ProduceRequest, v int16, rec []byte) {
	t.Helper()
	var q ProduceRequest
	r := NewReader(kmsgBody(req, v))
	if err := q.Decode(r, v); err != nil {
		t.Fatalf("v%d: %v", v, err)
	}
	mustConsume(t, r)
	if q.TransactionalID != nil || q.Acks != -1 || q.TimeoutMs != 1500 || len(q.Topics) != 2 {
		t.Fatalf("req = %+v", q)
	}
	if q.Topics[0].Name != "a" || q.Topics[1].Name != "b" || len(q.Topics[1].Partitions) != 2 {
		t.Fatalf("topics = %+v", q.Topics)
	}
	if !bytes.Equal(q.Topics[0].Partitions[0].Records, rec) || q.Topics[1].Partitions[0].Index != 2 {
		t.Fatalf("partition 0 = %+v", q.Topics[0].Partitions[0])
	}
	if len(q.Topics[0].Partitions[1].Records) != 0 {
		t.Fatalf("nil records decoded as %d bytes", len(q.Topics[0].Partitions[1].Records))
	}
}

func TestProduceResponseOracle(t *testing.T) {
	resp := ProduceResponse{Topics: []ProduceTopicResponse{
		{Name: "a", Partitions: []ProducePartitionResponse{
			{Index: 0, BaseOffset: 42, LogAppendTimeMs: -1, LogStartOffset: 7},
			{Index: 1, ErrorCode: ErrCorruptMessage, BaseOffset: -1, LogAppendTimeMs: -1, LogStartOffset: -1},
		}},
		{Name: "b", Partitions: []ProducePartitionResponse{{Index: 5, BaseOffset: 1}}},
	}}
	for v := int16(3); v <= 7; v++ {
		w := NewWriter(0)
		resp.Encode(w, v)
		var out kmsg.ProduceResponse
		kmsgDecode(t, &out, v, w.Buf())
		if len(out.Topics) != 2 || out.Topics[0].Topic != "a" || len(out.Topics[0].Partitions) != 2 {
			t.Fatalf("v%d topics = %+v", v, out.Topics)
		}
		p := out.Topics[0].Partitions[0]
		wantStart := int64(7)
		if v < 5 {
			wantStart = -1 // field absent before v5, kmsg defaults it to -1
		}
		if p.Partition != 0 || p.BaseOffset != 42 || p.LogAppendTime != -1 || p.LogStartOffset != wantStart || p.ErrorCode != 0 {
			t.Fatalf("v%d p0 = %+v", v, p)
		}
		if out.Topics[0].Partitions[1].ErrorCode != ErrCorruptMessage || out.Topics[1].Partitions[0].Partition != 5 {
			t.Fatalf("v%d others = %+v", v, out.Topics)
		}
	}
}

func TestListOffsetsOracle(t *testing.T) {
	req := kmsg.NewPtrListOffsetsRequest()
	req.ReplicaID = -1
	req.IsolationLevel = 1
	req.Topics = []kmsg.ListOffsetsRequestTopic{
		{Topic: "a", Partitions: []kmsg.ListOffsetsRequestTopicPartition{{Partition: 0, Timestamp: -1}, {Partition: 1, Timestamp: -2}}},
		{Topic: "b", Partitions: []kmsg.ListOffsetsRequestTopicPartition{{Partition: 2, Timestamp: 1700000000000}}},
	}
	var q ListOffsetsRequest
	r := NewReader(kmsgBody(req, 2))
	if err := q.Decode(r, 2); err != nil {
		t.Fatal(err)
	}
	mustConsume(t, r)
	if q.ReplicaID != -1 || q.IsolationLevel != 1 || len(q.Topics) != 2 ||
		q.Topics[0].Partitions[1].Timestamp != -2 || q.Topics[1].Partitions[0].Timestamp != 1700000000000 ||
		q.Topics[1].Name != "b" || q.Topics[1].Partitions[0].Index != 2 {
		t.Fatalf("req = %+v", q)
	}

	resp := ListOffsetsResponse{Topics: []ListOffsetsTopicResponse{
		{Name: "a", Partitions: []ListOffsetsPartitionResponse{
			{Index: 0, Timestamp: -1, Offset: 99},
			{Index: 1, ErrorCode: ErrUnknownTopicOrPartition, Timestamp: -1, Offset: -1},
		}},
	}}
	w := NewWriter(0)
	resp.Encode(w, 2)
	var out kmsg.ListOffsetsResponse
	kmsgDecode(t, &out, 2, w.Buf())
	if len(out.Topics) != 1 || out.Topics[0].Topic != "a" || len(out.Topics[0].Partitions) != 2 ||
		out.Topics[0].Partitions[0].Offset != 99 || out.Topics[0].Partitions[0].Timestamp != -1 ||
		out.Topics[0].Partitions[1].ErrorCode != ErrUnknownTopicOrPartition {
		t.Fatalf("resp = %+v", out)
	}
}
