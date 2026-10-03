// Package record parses and validates Kafka record batches (magic 2) and builds them for tests and tools.
package record

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

// HeaderSize is the number of bytes before the first record.
const HeaderSize = 61

// ErrCorrupt is wrapped by every validation failure.
var ErrCorrupt = errors.New("record: corrupt batch")

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// Header is the fixed batch header.
type Header struct {
	BaseOffset           int64
	BatchLength          int32
	PartitionLeaderEpoch int32
	Magic                int8
	CRC                  uint32
	Attributes           int16
	LastOffsetDelta      int32
	BaseTimestamp        int64
	MaxTimestamp         int64
	ProducerID           int64
	ProducerEpoch        int16
	BaseSequence         int32
	RecordCount          int32
}

// Compression returns the codec bits of the attributes (0 none, 1 gzip, 2 snappy, 3 lz4, 4 zstd).
func (h Header) Compression() int { return int(h.Attributes & 7) }

// Transactional reports whether the batch is part of a transaction.
func (h Header) Transactional() bool { return h.Attributes&(1<<4) != 0 }

// Control reports whether the batch is a control batch.
func (h Header) Control() bool { return h.Attributes&(1<<5) != 0 }

// Size is the total bytes of the batch on disk: 12 + BatchLength.
func (h Header) Size() int64 { return 12 + int64(h.BatchLength) }

func decodeHeader(b []byte) Header {
	return Header{
		BaseOffset:           int64(binary.BigEndian.Uint64(b[0:])),
		BatchLength:          int32(binary.BigEndian.Uint32(b[8:])),
		PartitionLeaderEpoch: int32(binary.BigEndian.Uint32(b[12:])),
		Magic:                int8(b[16]),
		CRC:                  binary.BigEndian.Uint32(b[17:]),
		Attributes:           int16(binary.BigEndian.Uint16(b[21:])),
		LastOffsetDelta:      int32(binary.BigEndian.Uint32(b[23:])),
		BaseTimestamp:        int64(binary.BigEndian.Uint64(b[27:])),
		MaxTimestamp:         int64(binary.BigEndian.Uint64(b[35:])),
		ProducerID:           int64(binary.BigEndian.Uint64(b[43:])),
		ProducerEpoch:        int16(binary.BigEndian.Uint16(b[51:])),
		BaseSequence:         int32(binary.BigEndian.Uint32(b[53:])),
		RecordCount:          int32(binary.BigEndian.Uint32(b[57:])),
	}
}

// Parse validates one batch occupying all of b: length, magic 2, CRC-32C of b[21:], and sane counts.
// Any failure wraps ErrCorrupt.
func Parse(b []byte) (Header, error) {
	if len(b) < HeaderSize {
		return Header{}, fmt.Errorf("%w: %d bytes, need at least %d", ErrCorrupt, len(b), HeaderSize)
	}
	h := decodeHeader(b)
	if h.Size() != int64(len(b)) {
		return Header{}, fmt.Errorf("%w: batch length says %d bytes, have %d", ErrCorrupt, h.Size(), len(b))
	}
	if h.Magic != 2 {
		return Header{}, fmt.Errorf("%w: magic %d", ErrCorrupt, h.Magic)
	}
	if crc32.Checksum(b[21:], castagnoli) != h.CRC {
		return Header{}, fmt.Errorf("%w: crc mismatch", ErrCorrupt)
	}
	if h.LastOffsetDelta < 0 || h.RecordCount < 0 {
		return Header{}, fmt.Errorf("%w: negative count", ErrCorrupt)
	}
	return h, nil
}

// PeekSize reads only the 12-byte prefix and returns the total batch size, or ErrCorrupt when
// the batch length is shorter than a header.
func PeekSize(prefix []byte) (int64, error) {
	if len(prefix) < 12 {
		return 0, fmt.Errorf("%w: short prefix", ErrCorrupt)
	}
	n := int64(int32(binary.BigEndian.Uint32(prefix[8:])))
	if n < HeaderSize-12 {
		return 0, fmt.Errorf("%w: batch length %d", ErrCorrupt, n)
	}
	return 12 + n, nil
}

// Split cuts a Produce records field into batches. A trailing partial batch is ErrCorrupt.
func Split(records []byte) ([][]byte, error) {
	var out [][]byte
	for len(records) > 0 {
		size, err := PeekSize(records)
		if err != nil {
			return nil, err
		}
		if size > int64(len(records)) {
			return nil, fmt.Errorf("%w: partial trailing batch", ErrCorrupt)
		}
		out = append(out, records[:size:size])
		records = records[size:]
	}
	return out, nil
}

// SetBaseOffset overwrites bytes 0..7; the base offset is outside the CRC.
func SetBaseOffset(b []byte, offset int64) { binary.BigEndian.PutUint64(b, uint64(offset)) }

// ReadHeader decodes the header of b without validating the CRC; it needs len(b) >= HeaderSize.
func ReadHeader(b []byte) (Header, error) {
	if len(b) < HeaderSize {
		return Header{}, fmt.Errorf("%w: short header", ErrCorrupt)
	}
	return decodeHeader(b), nil
}
