package protocol

import (
	"encoding/binary"
	"errors"
)

// ErrShort is returned when a read needs more bytes than remain.
var ErrShort = errors.New("protocol: short buffer")

// ErrMalformed is returned for invalid lengths and encodings.
var ErrMalformed = errors.New("protocol: malformed field")

// Reader decodes big-endian Kafka primitives. The first error sticks; later reads return zero values.
type Reader struct {
	b   []byte
	off int
	err error
}

// NewReader returns a Reader over b.
func NewReader(b []byte) *Reader { return &Reader{b: b} }

// Err returns the first error encountered, if any.
func (r *Reader) Err() error { return r.err }

// Remaining returns the number of unread bytes.
func (r *Reader) Remaining() int { return len(r.b) - r.off }

func (r *Reader) fail(err error) {
	if r.err == nil {
		r.err = err
	}
}

func (r *Reader) take(n int) []byte {
	if r.err != nil {
		return nil
	}
	if n < 0 {
		r.fail(ErrMalformed)
		return nil
	}
	if n > len(r.b)-r.off {
		r.fail(ErrShort)
		return nil
	}
	p := r.b[r.off : r.off+n : r.off+n]
	r.off += n
	return p
}

// Int8 reads a signed byte.
func (r *Reader) Int8() int8 {
	p := r.take(1)
	if p == nil {
		return 0
	}
	return int8(p[0])
}

// Int16 reads a big-endian int16.
func (r *Reader) Int16() int16 {
	p := r.take(2)
	if p == nil {
		return 0
	}
	return int16(binary.BigEndian.Uint16(p))
}

// Int32 reads a big-endian int32.
func (r *Reader) Int32() int32 {
	p := r.take(4)
	if p == nil {
		return 0
	}
	return int32(binary.BigEndian.Uint32(p))
}

// Int64 reads a big-endian int64.
func (r *Reader) Int64() int64 {
	p := r.take(8)
	if p == nil {
		return 0
	}
	return int64(binary.BigEndian.Uint64(p))
}

// Uint32 reads a big-endian uint32.
func (r *Reader) Uint32() uint32 {
	p := r.take(4)
	if p == nil {
		return 0
	}
	return binary.BigEndian.Uint32(p)
}

// Bool reads one byte as a boolean.
func (r *Reader) Bool() bool {
	p := r.take(1)
	return p != nil && p[0] != 0
}

// Uvarint reads an unsigned LEB128 value of at most 10 bytes.
func (r *Reader) Uvarint() uint64 {
	if r.err != nil {
		return 0
	}
	var v uint64
	for i := 0; i < 10; i++ {
		if r.off >= len(r.b) {
			r.fail(ErrShort)
			return 0
		}
		c := r.b[r.off]
		r.off++
		if i == 9 && c > 1 {
			r.fail(ErrMalformed)
			return 0
		}
		v |= uint64(c&0x7f) << (7 * uint(i))
		if c < 0x80 {
			return v
		}
	}
	r.fail(ErrMalformed)
	return 0
}

// Varint reads a zigzag-encoded signed varint.
func (r *Reader) Varint() int64 {
	u := r.Uvarint()
	return int64(u>>1) ^ -int64(u&1)
}

// String reads an int16-length string; a null (-1) length is malformed.
func (r *Reader) String() string {
	n := int(r.Int16())
	if r.err != nil {
		return ""
	}
	if n < 0 {
		r.fail(ErrMalformed)
		return ""
	}
	return string(r.take(n))
}

// NullableString reads an int16-length string where -1 means nil.
func (r *Reader) NullableString() *string {
	n := int(r.Int16())
	if r.err != nil {
		return nil
	}
	if n == -1 {
		return nil
	}
	if n < 0 {
		r.fail(ErrMalformed)
		return nil
	}
	s := string(r.take(n))
	if r.err != nil {
		return nil
	}
	return &s
}

// CompactString reads a string prefixed by uvarint length+1; null is malformed.
func (r *Reader) CompactString() string {
	n := r.Uvarint()
	if r.err != nil {
		return ""
	}
	if n == 0 || n-1 > uint64(r.Remaining()) {
		r.fail(ErrMalformed)
		return ""
	}
	return string(r.take(int(n - 1)))
}

// CompactNullableString reads a compact string where 0 means nil.
func (r *Reader) CompactNullableString() *string {
	n := r.Uvarint()
	if r.err != nil || n == 0 {
		return nil
	}
	if n-1 > uint64(r.Remaining()) {
		r.fail(ErrMalformed)
		return nil
	}
	s := string(r.take(int(n - 1)))
	return &s
}

// Bytes reads int32-length bytes and returns a sub-slice of the buffer, not a copy.
func (r *Reader) Bytes() []byte {
	n := int(r.Int32())
	if r.err != nil {
		return nil
	}
	if n < 0 {
		r.fail(ErrMalformed)
		return nil
	}
	p := r.take(n)
	if p == nil && r.err == nil {
		p = []byte{}
	}
	return p
}

// NullableBytes reads int32-length bytes where -1 means nil.
func (r *Reader) NullableBytes() []byte {
	n := int(r.Int32())
	if r.err != nil || n == -1 {
		return nil
	}
	if n < 0 {
		r.fail(ErrMalformed)
		return nil
	}
	p := r.take(n)
	if p == nil && r.err == nil {
		p = []byte{}
	}
	return p
}

// ArrayLen reads an int32 array length; -1 (null) is returned as -1. A length larger than the
// remaining bytes is ErrMalformed, since every element takes at least one byte.
func (r *Reader) ArrayLen() int {
	n := int(r.Int32())
	if r.err != nil {
		return 0
	}
	if n == -1 {
		return -1
	}
	if n < 0 || n > r.Remaining() {
		r.fail(ErrMalformed)
		return 0
	}
	return n
}

// CompactArrayLen reads a uvarint length+1; 0 (null) is returned as -1.
func (r *Reader) CompactArrayLen() int {
	n := r.Uvarint()
	if r.err != nil {
		return 0
	}
	if n == 0 {
		return -1
	}
	if n-1 > uint64(r.Remaining()) {
		r.fail(ErrMalformed)
		return 0
	}
	return int(n - 1)
}

// SkipTaggedFields skips a KIP-482 tagged field section.
func (r *Reader) SkipTaggedFields() {
	n := r.Uvarint()
	if r.err != nil {
		return
	}
	if n > uint64(r.Remaining()) {
		r.fail(ErrMalformed)
		return
	}
	for i := uint64(0); i < n; i++ {
		r.Uvarint() // tag
		size := r.Uvarint()
		if r.err != nil {
			return
		}
		if size > uint64(r.Remaining()) {
			r.fail(ErrShort)
			return
		}
		r.take(int(size))
	}
}

// Raw reads n bytes and returns a sub-slice of the buffer.
func (r *Reader) Raw(n int) []byte { return r.take(n) }
