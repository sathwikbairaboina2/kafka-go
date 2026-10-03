package broker

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"math/rand/v2"
	"testing"

	"github.com/sathwikbairaboina2/kafka-go/internal/protocol"
	"github.com/sathwikbairaboina2/kafka-go/internal/record"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func mkBatch(n int, value string) []byte {
	recs := make([]record.Record, n)
	for i := range recs {
		recs[i] = record.Record{Key: []byte("k"), Value: []byte(value)}
	}
	return record.Build(1_700_000_000_000, recs)
}

func produceReq(acks int16, topic string, partition int32, records []byte) *kmsg.ProduceRequest {
	req := kmsg.NewPtrProduceRequest()
	req.Acks = acks
	req.TimeoutMillis = 1000
	req.Topics = []kmsg.ProduceRequestTopic{{Topic: topic, Partitions: []kmsg.ProduceRequestTopicPartition{{Partition: partition, Records: records}}}}
	return req
}

func produce(t testing.TB, e *env, acks int16, topic string, partition int32, records []byte) kmsg.ProduceResponse {
	t.Helper()
	body, respond := e.call(t, produceReq(acks, topic, partition, records))
	if !respond {
		t.Fatal("expected a response")
	}
	var out kmsg.ProduceResponse
	decode(t, &out, 7, body)
	return out
}

func TestProduceAssignsOffsets(t *testing.T) {
	e := newEnv(t)
	out := produce(t, e, -1, "orders", 0, mkBatch(3, "a"))
	p := out.Topics[0].Partitions[0]
	if p.ErrorCode != 0 || p.BaseOffset != 0 || p.LogAppendTime != -1 || p.LogStartOffset != 0 {
		t.Fatalf("first = %+v", p)
	}
	out = produce(t, e, -1, "orders", 0, mkBatch(3, "b"))
	if p = out.Topics[0].Partitions[0]; p.BaseOffset != 3 {
		t.Fatalf("second base offset = %d, want 3", p.BaseOffset)
	}
	part, _ := e.logs.Get("orders", 0)
	if part.HighWatermark() != 6 {
		t.Fatalf("HW = %d", part.HighWatermark())
	}
	// several batches in one request land contiguously
	both := append(mkBatch(2, "c"), mkBatch(2, "d")...)
	out = produce(t, e, 1, "orders", 0, both)
	if p = out.Topics[0].Partitions[0]; p.BaseOffset != 6 || part.HighWatermark() != 10 {
		t.Fatalf("multi-batch base %d HW %d", p.BaseOffset, part.HighWatermark())
	}
}

func TestProduceUnknownTopicPartition(t *testing.T) {
	e := newEnv(t)
	out := produce(t, e, -1, "orders", 9, mkBatch(1, "a"))
	if code := out.Topics[0].Partitions[0].ErrorCode; code != protocol.ErrUnknownTopicOrPartition {
		t.Fatalf("partition 9 error = %d", code)
	}
	e2 := newEnv(t, func(c *Config) { c.AutoCreate = false })
	out = produce(t, e2, -1, "orders", 0, mkBatch(1, "a"))
	if code := out.Topics[0].Partitions[0].ErrorCode; code != protocol.ErrUnknownTopicOrPartition {
		t.Fatalf("no auto-create error = %d", code)
	}
}

func TestProduceRejectsCorruptBatch(t *testing.T) {
	e := newEnv(t)
	produce(t, e, -1, "orders", 0, mkBatch(2, "seed"))
	part, _ := e.logs.Get("orders", 0)
	hw := part.HighWatermark()
	good := mkBatch(3, "payload-payload-payload")
	rng := rand.New(rand.NewPCG(42, 99))
	for i := 0; i < 200; i++ {
		bad := bytes.Clone(good)
		bad[17+rng.IntN(len(bad)-17)] ^= byte(1 + rng.IntN(255))
		out := produce(t, e, -1, "orders", 0, bad)
		if code := out.Topics[0].Partitions[0].ErrorCode; code != protocol.ErrCorruptMessage {
			t.Fatalf("iteration %d: error code %d, want CORRUPT_MESSAGE", i, code)
		}
		if part.HighWatermark() != hw {
			t.Fatalf("iteration %d: HW moved %d -> %d", i, hw, part.HighWatermark())
		}
	}
	// a good batch followed by a bad one appends neither
	mixed := append(bytes.Clone(good), good[:len(good)-1]...)
	out := produce(t, e, -1, "orders", 0, mixed)
	if out.Topics[0].Partitions[0].ErrorCode != protocol.ErrCorruptMessage || part.HighWatermark() != hw {
		t.Fatalf("mixed request changed the log: code %d HW %d", out.Topics[0].Partitions[0].ErrorCode, part.HighWatermark())
	}
	// empty records and a wrong magic number are rejected too
	if code := produce(t, e, -1, "orders", 0, nil).Topics[0].Partitions[0].ErrorCode; code != protocol.ErrCorruptMessage {
		t.Fatalf("nil records code %d", code)
	}
	wrongMagic := bytes.Clone(good)
	wrongMagic[16] = 1
	if code := produce(t, e, -1, "orders", 0, wrongMagic).Topics[0].Partitions[0].ErrorCode; code != protocol.ErrCorruptMessage {
		t.Fatalf("magic 1 code %d", code)
	}
}

func TestProduceRejectsTransactionalAndControl(t *testing.T) {
	e := newEnv(t)
	for _, attr := range []uint16{1 << 4, 1 << 5} {
		b := mkBatch(1, "x")
		b[21], b[22] = byte(attr>>8), byte(attr)
		// recompute the CRC after changing attributes
		fixCRC(b)
		out := produce(t, e, -1, "orders", 0, b)
		if code := out.Topics[0].Partitions[0].ErrorCode; code != protocol.ErrInvalidRecord {
			t.Fatalf("attributes %x: code %d", attr, code)
		}
	}
}

func TestProduceAcksZeroNoResponse(t *testing.T) {
	e := newEnv(t)
	body, respond := e.call(t, produceReq(0, "orders", 0, mkBatch(2, "z")))
	if respond || body != nil {
		t.Fatalf("acks=0 responded: %v %x", respond, body)
	}
	part, _ := e.logs.Get("orders", 0)
	if part.HighWatermark() != 2 {
		t.Fatalf("acks=0 batch was not appended: HW %d", part.HighWatermark())
	}
}

func TestProduceInvalidAcks(t *testing.T) {
	e := newEnv(t)
	out := produce(t, e, 5, "orders", 0, mkBatch(1, "x"))
	if code := out.Topics[0].Partitions[0].ErrorCode; code != 21 {
		t.Fatalf("acks=5 code %d, want 21", code)
	}
}

func fixCRC(b []byte) {
	binary.BigEndian.PutUint32(b[17:], crc32.Checksum(b[21:], crc32.MakeTable(crc32.Castagnoli)))
}
