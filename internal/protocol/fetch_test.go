package protocol

import (
	"bytes"
	"testing"

	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestFetchRequestOracle(t *testing.T) {
	req := kmsg.NewPtrFetchRequest()
	req.ReplicaID = -1
	req.MaxWaitMillis = 500
	req.MinBytes = 1
	req.MaxBytes = 1 << 20
	req.IsolationLevel = 1
	req.SessionID = 0
	req.SessionEpoch = -1
	req.Rack = "rack-1"
	req.Topics = []kmsg.FetchRequestTopic{
		{Topic: "a", Partitions: []kmsg.FetchRequestTopicPartition{
			{Partition: 0, CurrentLeaderEpoch: -1, FetchOffset: 17, LogStartOffset: -1, PartitionMaxBytes: 4096},
			{Partition: 2, CurrentLeaderEpoch: -1, FetchOffset: 0, LogStartOffset: -1, PartitionMaxBytes: 8192},
		}},
		{Topic: "b", Partitions: []kmsg.FetchRequestTopicPartition{
			{Partition: 1, CurrentLeaderEpoch: -1, FetchOffset: 5, LogStartOffset: -1, PartitionMaxBytes: 100},
		}},
	}
	req.ForgottenTopics = []kmsg.FetchRequestForgottenTopic{{Topic: "old", Partitions: []int32{0, 1}}}

	var q FetchRequest
	r := NewReader(kmsgBody(req, 11))
	if err := q.Decode(r, 11); err != nil {
		t.Fatal(err)
	}
	mustConsume(t, r)
	if q.MaxWaitMs != 500 || q.MinBytes != 1 || q.MaxBytes != 1<<20 || q.IsolationLevel != 1 || q.SessionEpoch != -1 {
		t.Fatalf("req = %+v", q)
	}
	if len(q.Topics) != 2 || q.Topics[0].Name != "a" || len(q.Topics[0].Partitions) != 2 {
		t.Fatalf("topics = %+v", q.Topics)
	}
	p := q.Topics[0].Partitions[0]
	if p.Index != 0 || p.FetchOffset != 17 || p.PartitionMaxBytes != 4096 {
		t.Fatalf("p = %+v", p)
	}
	if q.Topics[1].Partitions[0].Index != 1 || q.Topics[1].Partitions[0].FetchOffset != 5 {
		t.Fatalf("p = %+v", q.Topics[1].Partitions[0])
	}
}

func TestFetchResponseOracle(t *testing.T) {
	rec := bytes.Repeat([]byte{7}, 90)
	resp := FetchResponse{Topics: []FetchTopicResponse{
		{Name: "a", Partitions: []FetchPartitionResponse{
			{Index: 0, HighWatermark: 10, LastStableOffset: 10, LogStart: 2, Records: rec},
			{Index: 1, HighWatermark: 3, LastStableOffset: 3, LogStart: 0},
			{Index: 2, ErrorCode: ErrOffsetOutOfRange, HighWatermark: -1, LastStableOffset: -1, LogStart: -1},
		}},
	}}
	w := NewWriter(0)
	resp.Encode(w, 11)
	var out kmsg.FetchResponse
	kmsgDecode(t, &out, 11, w.Buf())
	if out.ErrorCode != 0 || out.SessionID != 0 || len(out.Topics) != 1 || len(out.Topics[0].Partitions) != 3 {
		t.Fatalf("resp = %+v", out)
	}
	p0 := out.Topics[0].Partitions[0]
	if p0.HighWatermark != 10 || p0.LastStableOffset != 10 || p0.LogStartOffset != 2 || !bytes.Equal(p0.RecordBatches, rec) ||
		p0.PreferredReadReplica != -1 || p0.AbortedTransactions != nil {
		t.Fatalf("p0 = %+v", p0)
	}
	if len(out.Topics[0].Partitions[1].RecordBatches) != 0 {
		t.Fatalf("p1 records = %d bytes", len(out.Topics[0].Partitions[1].RecordBatches))
	}
	if out.Topics[0].Partitions[2].ErrorCode != ErrOffsetOutOfRange {
		t.Fatalf("p2 = %+v", out.Topics[0].Partitions[2])
	}
}
