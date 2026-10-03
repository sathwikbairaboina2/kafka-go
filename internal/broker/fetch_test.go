package broker

import (
	"testing"
	"time"

	"github.com/sathwikbairaboina2/kafka-go/internal/protocol"
	"github.com/sathwikbairaboina2/kafka-go/internal/record"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func fetchReq(topic string, partition int32, offset int64, maxWait, minBytes, maxBytes, partMax int32) *kmsg.FetchRequest {
	req := kmsg.NewPtrFetchRequest()
	req.MaxWaitMillis = maxWait
	req.MinBytes = minBytes
	req.MaxBytes = maxBytes
	req.Topics = []kmsg.FetchRequestTopic{{Topic: topic, Partitions: []kmsg.FetchRequestTopicPartition{
		{Partition: partition, FetchOffset: offset, PartitionMaxBytes: partMax, CurrentLeaderEpoch: -1, LogStartOffset: -1},
	}}}
	return req
}

func doFetch(t testing.TB, e *env, req *kmsg.FetchRequest) fetchView {
	t.Helper()
	body, _ := e.call(t, req)
	var out kmsg.FetchResponse
	decode(t, &out, 11, body)
	p := out.Topics[0].Partitions[0]
	return fetchView{ErrorCode: p.ErrorCode, HighWatermark: p.HighWatermark, LogStart: p.LogStartOffset, Records: p.RecordBatches}
}

func TestFetchFromOffset(t *testing.T) {
	e := newEnv(t)
	for i := 0; i < 5; i++ {
		produce(t, e, -1, "orders", 0, mkBatch(2, "v")) // batch i covers offsets 2i, 2i+1
	}
	p := doFetch(t, e, fetchReq("orders", 0, 5, 0, 1, 1<<20, 1<<20)) // offset 5 is in batch [4,5]
	if p.ErrorCode != 0 || p.HighWatermark != 10 || p.LogStart != 0 {
		t.Fatalf("partition = %+v", p)
	}
	h, err := record.ReadHeader(p.Records)
	if err != nil || h.BaseOffset != 4 {
		t.Fatalf("first batch base %d (%v), want 4", h.BaseOffset, err)
	}
	parts, err := record.Split(p.Records)
	if err != nil || len(parts) != 3 {
		t.Fatalf("got %d batches (%v), want 3 (offsets 4..9)", len(parts), err)
	}
}

func TestFetchRespectsMaxBytes(t *testing.T) {
	e := newEnv(t)
	for i := 0; i < 5; i++ {
		produce(t, e, -1, "orders", 0, mkBatch(1, "0123456789"))
	}
	p := doFetch(t, e, fetchReq("orders", 0, 0, 0, 1, 1<<20, 1)) // 1-byte budget still yields one whole batch
	parts, err := record.Split(p.Records)
	if err != nil || len(parts) != 1 {
		t.Fatalf("got %d batches (%v), want exactly 1", len(parts), err)
	}
}

func TestFetchOutOfRange(t *testing.T) {
	e := newEnv(t)
	produce(t, e, -1, "orders", 0, mkBatch(2, "v"))
	if p := doFetch(t, e, fetchReq("orders", 0, 99, 0, 1, 1<<20, 1<<20)); p.ErrorCode != protocol.ErrOffsetOutOfRange {
		t.Fatalf("offset 99 error = %d", p.ErrorCode)
	}
	if p := doFetch(t, e, fetchReq("orders", 0, 2, 0, 1, 1<<20, 1<<20)); p.ErrorCode != 0 || len(p.Records) != 0 || p.HighWatermark != 2 {
		t.Fatalf("offset == HW: %+v", p)
	}
	if p := doFetch(t, e, fetchReq("nope", 0, 0, 0, 1, 1<<20, 1<<20)); p.ErrorCode != protocol.ErrUnknownTopicOrPartition {
		t.Fatalf("unknown topic error = %d", p.ErrorCode)
	}
}

func TestFetchLongPollWakesOnAppend(t *testing.T) {
	e := newEnv(t)
	produce(t, e, -1, "orders", 0, mkBatch(1, "seed"))
	part, _ := e.logs.Get("orders", 0)
	type result struct {
		p   fetchView
		dur time.Duration
	}
	done := make(chan result, 1)
	start := time.Now()
	go func() { done <- result{doFetch(t, e, fetchReq("orders", 0, 1, 5000, 1, 1<<20, 1<<20)), 0} }()
	time.Sleep(100 * time.Millisecond)
	batch := mkBatch(1, "wake")
	h, _ := record.Parse(batch)
	if _, err := part.Append(batch, h); err != nil {
		t.Fatal(err)
	}
	select {
	case r := <-done:
		if time.Since(start) > 2*time.Second || len(r.p.Records) == 0 {
			t.Fatalf("long poll took %v and returned %d bytes", time.Since(start), len(r.p.Records))
		}
	case <-time.After(3 * time.Second):
		t.Fatal("long poll was not woken by the append")
	}
}

func TestFetchLongPollTimesOut(t *testing.T) {
	e := newEnv(t)
	produce(t, e, -1, "orders", 0, mkBatch(1, "seed"))
	start := time.Now()
	p := doFetch(t, e, fetchReq("orders", 0, 1, 200, 1, 1<<20, 1<<20))
	if d := time.Since(start); d < 180*time.Millisecond || d > 2*time.Second {
		t.Fatalf("took %v, want about 200ms", d)
	}
	if p.ErrorCode != 0 || len(p.Records) != 0 {
		t.Fatalf("partition = %+v", p)
	}
}

func TestListOffsetsEarliestLatest(t *testing.T) {
	e := newEnv(t)
	for i := 0; i < 3; i++ {
		produce(t, e, -1, "orders", 0, mkBatch(2, "v"))
	}
	req := kmsg.NewPtrListOffsetsRequest()
	req.Topics = []kmsg.ListOffsetsRequestTopic{{Topic: "orders", Partitions: []kmsg.ListOffsetsRequestTopicPartition{
		{Partition: 0, Timestamp: -2}, {Partition: 0, Timestamp: -1}, {Partition: 9, Timestamp: -1},
	}}}
	body, _ := e.call(t, req)
	var out kmsg.ListOffsetsResponse
	decode(t, &out, 2, body)
	ps := out.Topics[0].Partitions
	if ps[0].Offset != 0 || ps[1].Offset != 6 || ps[2].ErrorCode != protocol.ErrUnknownTopicOrPartition {
		t.Fatalf("partitions = %+v", ps)
	}
}

type fetchView struct {
	ErrorCode     int16
	HighWatermark int64
	LogStart      int64
	Records       []byte
}
