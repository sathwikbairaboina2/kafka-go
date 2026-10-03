package protocol

import "encoding/binary"

// Writer appends big-endian Kafka primitives.
type Writer struct{ b []byte }

// NewWriter returns a Writer with the given initial capacity.
func NewWriter(capacity int) *Writer { return &Writer{b: make([]byte, 0, capacity)} }

// Buf returns the bytes written so far.
func (w *Writer) Buf() []byte { return w.b }

// Int8 appends a signed byte.
func (w *Writer) Int8(v int8) { w.b = append(w.b, byte(v)) }

// Int16 appends a big-endian int16.
func (w *Writer) Int16(v int16) { w.b = binary.BigEndian.AppendUint16(w.b, uint16(v)) }

// Int32 appends a big-endian int32.
func (w *Writer) Int32(v int32) { w.b = binary.BigEndian.AppendUint32(w.b, uint32(v)) }

// Int64 appends a big-endian int64.
func (w *Writer) Int64(v int64) { w.b = binary.BigEndian.AppendUint64(w.b, uint64(v)) }

// Uint32 appends a big-endian uint32.
func (w *Writer) Uint32(v uint32) { w.b = binary.BigEndian.AppendUint32(w.b, v) }

// Bool appends one byte, 1 for true.
func (w *Writer) Bool(v bool) {
	if v {
		w.b = append(w.b, 1)
	} else {
		w.b = append(w.b, 0)
	}
}

// Uvarint appends an unsigned LEB128 value.
func (w *Writer) Uvarint(v uint64) { w.b = binary.AppendUvarint(w.b, v) }

// Varint appends a zigzag-encoded signed varint.
func (w *Writer) Varint(v int64) { w.b = binary.AppendUvarint(w.b, uint64(v<<1)^uint64(v>>63)) }

// String appends an int16-length string.
func (w *Writer) String(s string) {
	w.Int16(int16(len(s)))
	w.b = append(w.b, s...)
}

// NullableString appends an int16-length string, -1 for nil.
func (w *Writer) NullableString(s *string) {
	if s == nil {
		w.Int16(-1)
		return
	}
	w.String(*s)
}

// CompactString appends a string prefixed by uvarint length+1.
func (w *Writer) CompactString(s string) {
	w.Uvarint(uint64(len(s)) + 1)
	w.b = append(w.b, s...)
}

// CompactNullableString appends a compact string, 0 for nil.
func (w *Writer) CompactNullableString(s *string) {
	if s == nil {
		w.Uvarint(0)
		return
	}
	w.CompactString(*s)
}

// Bytes appends int32-length bytes.
func (w *Writer) Bytes(b []byte) {
	w.Int32(int32(len(b)))
	w.b = append(w.b, b...)
}

// NullableBytes appends int32-length bytes, -1 for nil.
func (w *Writer) NullableBytes(b []byte) {
	if b == nil {
		w.Int32(-1)
		return
	}
	w.Bytes(b)
}

// ArrayLen appends an int32 array length; n < 0 writes -1.
func (w *Writer) ArrayLen(n int) {
	if n < 0 {
		w.Int32(-1)
		return
	}
	w.Int32(int32(n))
}

// CompactArrayLen appends uvarint n+1; n < 0 writes 0.
func (w *Writer) CompactArrayLen(n int) {
	if n < 0 {
		w.Uvarint(0)
		return
	}
	w.Uvarint(uint64(n) + 1)
}

// EmptyTaggedFields appends an empty tag buffer.
func (w *Writer) EmptyTaggedFields() { w.b = append(w.b, 0) }

// Raw appends b verbatim.
func (w *Writer) Raw(b []byte) { w.b = append(w.b, b...) }
