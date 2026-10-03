package protocol

import (
	"bytes"
	"errors"
	"math"
	"strings"
	"testing"

	"pgregory.net/rapid"
)

func ptr(s string) *string { return &s }

func TestRoundTripPrimitives(t *testing.T) {
	long := strings.Repeat("x", 300)
	w := NewWriter(0)
	w.Int8(-5)
	w.Int16(math.MinInt16)
	w.Int32(math.MaxInt32)
	w.Int64(math.MinInt64)
	w.Int64(math.MaxInt64)
	w.Int64(-1)
	w.Int64(0)
	w.Uint32(0xdeadbeef)
	w.Bool(true)
	w.Bool(false)
	w.Uvarint(0)
	w.Uvarint(300)
	w.Varint(-1)
	w.Varint(math.MinInt64)
	w.String("")
	w.String("abc")
	w.NullableString(nil)
	w.NullableString(ptr("n"))
	w.CompactString(long)
	w.CompactString("")
	w.CompactNullableString(nil)
	w.CompactNullableString(ptr("cn"))
	w.Bytes([]byte{1, 2, 3})
	w.NullableBytes(nil)
	w.NullableBytes([]byte{})
	w.ArrayLen(2)
	w.ArrayLen(-1)
	w.CompactArrayLen(1)
	w.CompactArrayLen(-1)
	w.Raw([]byte{9, 9})
	// two tagged fields: {tag 0, 3 bytes}, {tag 5, 0 bytes}
	w.Raw([]byte{2, 0, 3, 'a', 'b', 'c', 5, 0})
	w.EmptyTaggedFields()

	r := NewReader(append(w.Buf(), 0xff, 0xff)) // padding so array lens pass the remaining check
	chk := func(ok bool, what string) {
		t.Helper()
		if !ok {
			t.Fatalf("mismatch: %s", what)
		}
	}
	chk(r.Int8() == -5, "int8")
	chk(r.Int16() == math.MinInt16, "int16")
	chk(r.Int32() == math.MaxInt32, "int32")
	chk(r.Int64() == math.MinInt64, "int64 min")
	chk(r.Int64() == math.MaxInt64, "int64 max")
	chk(r.Int64() == -1, "int64 -1")
	chk(r.Int64() == 0, "int64 0")
	chk(r.Uint32() == 0xdeadbeef, "uint32")
	chk(r.Bool(), "bool true")
	chk(!r.Bool(), "bool false")
	chk(r.Uvarint() == 0, "uvarint 0")
	chk(r.Uvarint() == 300, "uvarint 300")
	chk(r.Varint() == -1, "varint -1")
	chk(r.Varint() == math.MinInt64, "varint min")
	chk(r.String() == "", "empty string")
	chk(r.String() == "abc", "string")
	chk(r.NullableString() == nil, "nullable nil")
	chk(*r.NullableString() == "n", "nullable n")
	chk(r.CompactString() == long, "compact long")
	chk(r.CompactString() == "", "compact empty")
	chk(r.CompactNullableString() == nil, "compact nullable nil")
	chk(*r.CompactNullableString() == "cn", "compact nullable")
	chk(bytes.Equal(r.Bytes(), []byte{1, 2, 3}), "bytes")
	chk(r.NullableBytes() == nil, "nullable bytes nil")
	chk(r.NullableBytes() != nil, "nullable bytes empty")
	chk(r.ArrayLen() == 2, "array 2")
	chk(r.ArrayLen() == -1, "array null")
	chk(r.CompactArrayLen() == 1, "compact array 1")
	chk(r.CompactArrayLen() == -1, "compact array null")
	chk(bytes.Equal(r.Raw(2), []byte{9, 9}), "raw")
	r.SkipTaggedFields()
	r.SkipTaggedFields()
	if r.Err() != nil {
		t.Fatalf("err: %v", r.Err())
	}
	if r.Remaining() != 2 {
		t.Fatalf("remaining = %d, want 2 (padding)", r.Remaining())
	}
	r.Raw(2)
	mustConsume(t, r)
}

func TestZigzagVectors(t *testing.T) {
	cases := []struct {
		v    int64
		want []byte
	}{
		{0, []byte{0x00}}, {-1, []byte{0x01}}, {1, []byte{0x02}}, {-2, []byte{0x03}},
		{63, []byte{0x7e}}, {-64, []byte{0x7f}}, {64, []byte{0x80, 0x01}},
	}
	for _, c := range cases {
		w := NewWriter(0)
		w.Varint(c.v)
		if !bytes.Equal(w.Buf(), c.want) {
			t.Errorf("Varint(%d) = %x, want %x", c.v, w.Buf(), c.want)
		}
		r := NewReader(c.want)
		if got := r.Varint(); got != c.v {
			t.Errorf("read %x = %d, want %d", c.want, got, c.v)
		}
	}
}

func TestReaderShortAndSticky(t *testing.T) {
	r := NewReader([]byte{0, 1})
	if v := r.Int32(); v != 0 || !errors.Is(r.Err(), ErrShort) {
		t.Fatalf("Int32 = %d err %v", v, r.Err())
	}
	if v := r.Int8(); v != 0 || !errors.Is(r.Err(), ErrShort) {
		t.Fatalf("Int8 after error = %d err %v", v, r.Err())
	}
}

func TestHostileArrayLen(t *testing.T) {
	r := NewReader([]byte{0x7f, 0xff, 0xff, 0xff, 1, 2, 3})
	if n := r.ArrayLen(); n != 0 || !errors.Is(r.Err(), ErrMalformed) {
		t.Fatalf("ArrayLen = %d err %v", n, r.Err())
	}
	r = NewReader([]byte{0xff, 0xff, 0xff, 0x7f, 1})
	if n := r.CompactArrayLen(); n != 0 || !errors.Is(r.Err(), ErrMalformed) {
		t.Fatalf("CompactArrayLen = %d err %v", n, r.Err())
	}
}

func TestStringNegativeLengthMalformed(t *testing.T) {
	r := NewReader([]byte{0xff, 0xff})
	r.String()
	if !errors.Is(r.Err(), ErrMalformed) {
		t.Fatalf("err = %v", r.Err())
	}
}

func TestVarintRoundTripRapid(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		u := rapid.Uint64().Draw(t, "u")
		s := rapid.Int64().Draw(t, "s")
		w := NewWriter(0)
		w.Uvarint(u)
		w.Varint(s)
		r := NewReader(w.Buf())
		if got := r.Uvarint(); got != u {
			t.Fatalf("uvarint %d != %d", got, u)
		}
		if got := r.Varint(); got != s {
			t.Fatalf("varint %d != %d", got, s)
		}
		mustConsume(t, r)
	})
}

func FuzzReader(f *testing.F) {
	w := NewWriter(0)
	w.Int16(3)
	w.String("abc")
	w.CompactString("hello")
	w.ArrayLen(1)
	w.Int32(7)
	w.Varint(-300)
	w.Bytes([]byte{1, 2})
	w.EmptyTaggedFields()
	f.Add(w.Buf())
	f.Add([]byte{})
	f.Add([]byte{0x7f, 0xff, 0xff, 0xff})
	f.Fuzz(func(t *testing.T, b []byte) {
		r := NewReader(b)
		r.Int8()
		r.Int16()
		r.Int32()
		r.Int64()
		r.Uint32()
		r.Bool()
		r.Uvarint()
		r.Varint()
		r.String()
		r.NullableString()
		r.CompactString()
		r.CompactNullableString()
		r.Bytes()
		r.NullableBytes()
		r.ArrayLen()
		r.CompactArrayLen()
		r.SkipTaggedFields()
		r.Raw(3)
		_ = r.Remaining()
		_ = r.Err()
	})
}
