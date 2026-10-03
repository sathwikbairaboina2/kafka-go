package record

import (
	"encoding/binary"
	"hash/crc32"
)

// Record is one uncompressed record.
type Record struct {
	OffsetDelta    int32
	TimestampDelta int64
	Key, Value     []byte // nil encodes as length -1
	Headers        []RecordHeader
}

// RecordHeader is one record header.
type RecordHeader struct {
	Key   string
	Value []byte
}

func appendBytes(dst, b []byte) []byte {
	if b == nil {
		return binary.AppendVarint(dst, -1)
	}
	dst = binary.AppendVarint(dst, int64(len(b)))
	return append(dst, b...)
}

func appendRecord(dst []byte, r Record) []byte {
	var body []byte
	body = append(body, 0) // attributes
	body = binary.AppendVarint(body, r.TimestampDelta)
	body = binary.AppendVarint(body, int64(r.OffsetDelta))
	body = appendBytes(body, r.Key)
	body = appendBytes(body, r.Value)
	body = binary.AppendVarint(body, int64(len(r.Headers)))
	for _, h := range r.Headers {
		body = binary.AppendVarint(body, int64(len(h.Key)))
		body = append(body, h.Key...)
		body = appendBytes(body, h.Value)
	}
	dst = binary.AppendVarint(dst, int64(len(body)))
	return append(dst, body...)
}

// Build encodes an uncompressed batch with base offset 0 and a valid CRC. Record offset deltas are
// set to the record index.
func Build(baseTimestamp int64, records []Record) []byte {
	var body []byte
	maxTs := baseTimestamp
	for i, r := range records {
		r.OffsetDelta = int32(i)
		if baseTimestamp+r.TimestampDelta > maxTs {
			maxTs = baseTimestamp + r.TimestampDelta
		}
		body = appendRecord(body, r)
	}
	b := make([]byte, HeaderSize, HeaderSize+len(body))
	binary.BigEndian.PutUint32(b[8:], uint32(HeaderSize-12+len(body)))
	binary.BigEndian.PutUint32(b[12:], 0)
	b[16] = 2
	binary.BigEndian.PutUint16(b[21:], 0)
	last := int32(0)
	if len(records) > 0 {
		last = int32(len(records) - 1)
	}
	binary.BigEndian.PutUint32(b[23:], uint32(last))
	binary.BigEndian.PutUint64(b[27:], uint64(baseTimestamp))
	binary.BigEndian.PutUint64(b[35:], uint64(maxTs))
	binary.BigEndian.PutUint64(b[43:], ^uint64(0)) // producer id -1
	binary.BigEndian.PutUint16(b[51:], 0xffff)     // producer epoch -1
	binary.BigEndian.PutUint32(b[53:], 0xffffffff) // base sequence -1
	binary.BigEndian.PutUint32(b[57:], uint32(len(records)))
	b = append(b, body...)
	binary.BigEndian.PutUint32(b[17:], crc32.Checksum(b[21:], castagnoli))
	return b
}
