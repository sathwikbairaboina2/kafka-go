package record

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"testing"

	"github.com/twmb/franz-go/pkg/kmsg"
)

func sample() []byte {
	return Build(1_700_000_000_000, []Record{
		{TimestampDelta: 0, Key: nil, Value: []byte("v0")},
		{TimestampDelta: 5, Key: []byte("k1"), Value: []byte("value-1"), Headers: []RecordHeader{{Key: "h", Value: []byte("hv")}}},
		{TimestampDelta: 9, Key: []byte("k2"), Value: nil},
	})
}

func TestBuildParseRoundTrip(t *testing.T) {
	b := sample()
	h, err := Parse(b)
	if err != nil {
		t.Fatal(err)
	}
	if h.RecordCount != 3 || h.LastOffsetDelta != 2 || h.Magic != 2 || h.MaxTimestamp != 1_700_000_000_009 {
		t.Fatalf("header = %+v", h)
	}
	recs, err := Records(b)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 3 {
		t.Fatalf("records = %d", len(recs))
	}
	if recs[0].Key != nil || string(recs[0].Value) != "v0" {
		t.Fatalf("rec0 = %+v", recs[0])
	}
	if string(recs[1].Key) != "k1" || len(recs[1].Headers) != 1 || recs[1].Headers[0].Key != "h" || string(recs[1].Headers[0].Value) != "hv" || recs[1].TimestampDelta != 5 || recs[1].OffsetDelta != 1 {
		t.Fatalf("rec1 = %+v", recs[1])
	}
	if recs[2].Value != nil || string(recs[2].Key) != "k2" {
		t.Fatalf("rec2 = %+v", recs[2])
	}
}

func TestBuildMatchesKmsg(t *testing.T) {
	b := sample()
	var kb kmsg.RecordBatch
	if err := kb.ReadFrom(b); err != nil {
		t.Fatal(err)
	}
	h, _ := Parse(b)
	if kb.NumRecords != 3 || uint32(kb.CRC) != h.CRC || kb.LastOffsetDelta != 2 || kb.Magic != 2 {
		t.Fatalf("kmsg view = %+v", kb)
	}

	// reverse: a batch assembled by kmsg from our record bytes parses with ours
	built := kmsg.RecordBatch{
		FirstOffset: 0, Length: int32(49 + len(b) - HeaderSize), Magic: 2, LastOffsetDelta: 2,
		FirstTimestamp: 1, MaxTimestamp: 2, ProducerID: -1, ProducerEpoch: -1, FirstSequence: -1,
		NumRecords: 3, Records: b[HeaderSize:],
	}
	out := built.AppendTo(nil)
	built.CRC = int32(crc32.Checksum(out[21:], crc32.MakeTable(crc32.Castagnoli)))
	out = built.AppendTo(nil)
	h2, err := Parse(out)
	if err != nil {
		t.Fatalf("kmsg-built batch: %v", err)
	}
	if h2.RecordCount != 3 {
		t.Fatalf("header = %+v", h2)
	}
	recs, err := Records(out)
	if err != nil || len(recs) != 3 {
		t.Fatalf("records %d err %v", len(recs), err)
	}
}

func TestParseRejects(t *testing.T) {
	good := sample()
	t.Run("magic1", func(t *testing.T) {
		b := bytes.Clone(good)
		b[16] = 1
		if _, err := Parse(b); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("everyByteFlip", func(t *testing.T) {
		for i := 17; i < len(good); i++ {
			b := bytes.Clone(good)
			b[i] ^= 0x01
			if _, err := Parse(b); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("flip at %d accepted (err %v)", i, err)
			}
		}
	})
	t.Run("truncated", func(t *testing.T) {
		if _, err := Parse(good[:len(good)-1]); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("lengthTooLarge", func(t *testing.T) {
		b := bytes.Clone(good)
		binary.BigEndian.PutUint32(b[8:], binary.BigEndian.Uint32(b[8:])+1)
		if _, err := Parse(b); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("short", func(t *testing.T) {
		if _, err := Parse(good[:20]); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("err = %v", err)
		}
	})
}

func TestSetBaseOffsetKeepsCRC(t *testing.T) {
	b := sample()
	SetBaseOffset(b, 52311)
	h, err := Parse(b)
	if err != nil {
		t.Fatal(err)
	}
	if h.BaseOffset != 52311 {
		t.Fatalf("base = %d", h.BaseOffset)
	}
}

func TestSplit(t *testing.T) {
	a, b := sample(), Build(5, []Record{{Value: []byte("x")}})
	parts, err := Split(append(bytes.Clone(a), b...))
	if err != nil || len(parts) != 2 || !bytes.Equal(parts[0], a) || !bytes.Equal(parts[1], b) {
		t.Fatalf("parts %d err %v", len(parts), err)
	}
	if _, err := Split(append(bytes.Clone(a), 1, 2, 3, 4, 5)); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("trailing garbage err = %v", err)
	}
	if _, err := Split(a[:len(a)-3]); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("partial err = %v", err)
	}
}

func TestPeekSize(t *testing.T) {
	b := sample()
	n, err := PeekSize(b[:12])
	if err != nil || n != int64(len(b)) {
		t.Fatalf("n %d err %v", n, err)
	}
	short := make([]byte, 12)
	binary.BigEndian.PutUint32(short[8:], 10)
	if _, err := PeekSize(short); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("err = %v", err)
	}
}

func TestRecordsCompressed(t *testing.T) {
	b := sample()
	binary.BigEndian.PutUint16(b[21:], 2) // snappy bit; CRC not rechecked by Records
	if _, err := Records(b); !errors.Is(err, ErrCompressed) {
		t.Fatalf("err = %v", err)
	}
}

func FuzzParseBatch(f *testing.F) {
	f.Add(sample())
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, b []byte) {
		h, err := Parse(b)
		if err == nil && h.Size() != int64(len(b)) {
			t.Fatalf("Parse ok but size %d != len %d", h.Size(), len(b))
		}
		_, _ = Records(b)
		_, _ = Split(b)
	})
}
