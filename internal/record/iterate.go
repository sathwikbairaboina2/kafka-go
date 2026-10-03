package record

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// ErrCompressed is returned by Records for compressed batches.
var ErrCompressed = errors.New("record: compressed batch")

type rdr struct {
	b   []byte
	off int
	err error
}

func (r *rdr) varint() int64 {
	if r.err != nil {
		return 0
	}
	if r.off > len(r.b) {
		r.err = fmt.Errorf("%w: read past end", ErrCorrupt)
		return 0
	}
	v, n := binary.Varint(r.b[r.off:])
	if n <= 0 {
		r.err = fmt.Errorf("%w: bad varint", ErrCorrupt)
		return 0
	}
	r.off += n
	return v
}

// bytes reads a length-prefixed field where -1 means nil.
func (r *rdr) bytes() []byte {
	n := r.varint()
	if r.err != nil || n == -1 {
		return nil
	}
	if n < 0 || n > int64(len(r.b)-r.off) {
		r.err = fmt.Errorf("%w: bad field length %d", ErrCorrupt, n)
		return nil
	}
	p := r.b[r.off : r.off+int(n) : r.off+int(n)]
	r.off += int(n)
	return p
}

// Records decodes the records of an uncompressed batch; compressed batches return ErrCompressed.
// Key, value and header slices alias b.
func Records(b []byte) ([]Record, error) {
	h, err := ReadHeader(b)
	if err != nil {
		return nil, err
	}
	if h.Compression() != 0 {
		return nil, ErrCompressed
	}
	if h.RecordCount < 0 || int64(h.RecordCount) > int64(len(b)-HeaderSize) {
		return nil, fmt.Errorf("%w: record count %d", ErrCorrupt, h.RecordCount)
	}
	r := &rdr{b: b, off: HeaderSize}
	out := make([]Record, 0, h.RecordCount)
	for i := int32(0); i < h.RecordCount; i++ {
		size := r.varint()
		if r.err != nil {
			return nil, r.err
		}
		if size < 1 || size > int64(len(b)-r.off) {
			return nil, fmt.Errorf("%w: record length %d", ErrCorrupt, size)
		}
		end := r.off + int(size)
		sub := &rdr{b: b[:end], off: r.off + 1} // skip attributes
		var rec Record
		rec.TimestampDelta = sub.varint()
		rec.OffsetDelta = int32(sub.varint())
		rec.Key = sub.bytes()
		rec.Value = sub.bytes()
		nh := sub.varint()
		if sub.err == nil && (nh < 0 || nh > int64(end-sub.off)) {
			sub.err = fmt.Errorf("%w: header count %d", ErrCorrupt, nh)
		}
		for j := int64(0); j < nh && sub.err == nil; j++ {
			k := sub.bytes()
			v := sub.bytes()
			rec.Headers = append(rec.Headers, RecordHeader{Key: string(k), Value: v})
		}
		if sub.err != nil {
			return nil, sub.err
		}
		out = append(out, rec)
		r.off = end
	}
	return out, nil
}
