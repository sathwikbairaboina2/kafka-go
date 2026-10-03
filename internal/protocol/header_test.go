package protocol

import (
	"encoding/binary"
	"testing"

	"github.com/twmb/franz-go/pkg/kmsg"
)

func supportedRequests() map[int16]kmsg.Request {
	return map[int16]kmsg.Request{
		KeyProduce:         kmsg.NewPtrProduceRequest(),
		KeyFetch:           kmsg.NewPtrFetchRequest(),
		KeyListOffsets:     kmsg.NewPtrListOffsetsRequest(),
		KeyMetadata:        kmsg.NewPtrMetadataRequest(),
		KeyOffsetCommit:    kmsg.NewPtrOffsetCommitRequest(),
		KeyOffsetFetch:     kmsg.NewPtrOffsetFetchRequest(),
		KeyFindCoordinator: kmsg.NewPtrFindCoordinatorRequest(),
		KeyJoinGroup:       kmsg.NewPtrJoinGroupRequest(),
		KeyHeartbeat:       kmsg.NewPtrHeartbeatRequest(),
		KeyLeaveGroup:      kmsg.NewPtrLeaveGroupRequest(),
		KeySyncGroup:       kmsg.NewPtrSyncGroupRequest(),
		KeyApiVersions:     kmsg.NewPtrApiVersionsRequest(),
	}
}

func TestParseHeaderFromKmsg(t *testing.T) {
	for key, req := range supportedRequests() {
		rng := Supported[key]
		for v := rng.Min; v <= rng.Max; v++ {
			req.SetVersion(v)
			b := kmsg.NewRequestFormatter(kmsg.FormatterClientID("kgo-test")).AppendRequest(nil, req, 42)
			if got := int(binary.BigEndian.Uint32(b[:4])); got != len(b)-4 {
				t.Fatalf("key %d v%d: size prefix %d, want %d", key, v, got, len(b)-4)
			}
			r := NewReader(b[4:])
			h, err := ParseRequestHeader(r)
			if err != nil {
				t.Fatalf("key %d v%d: %v", key, v, err)
			}
			if h.APIKey != key || h.APIVersion != v || h.CorrelationID != 42 || h.ClientID == nil || *h.ClientID != "kgo-test" {
				t.Fatalf("key %d v%d: header %+v", key, v, h)
			}
		}
	}
}

func TestSupportedTable(t *testing.T) {
	for key, rng := range Supported {
		for v := rng.Min; v <= rng.Max; v++ {
			if !IsSupported(key, v) {
				t.Errorf("key %d v%d should be supported", key, v)
			}
		}
		if IsSupported(key, rng.Min-1) || IsSupported(key, rng.Max+1) {
			t.Errorf("key %d: neighbours of %v must be unsupported", key, rng)
		}
	}
	if IsSupported(75, 0) {
		t.Error("key 75 must be unsupported")
	}
}

func TestResponseHeaderVersion(t *testing.T) {
	cases := []struct{ key, v, want int16 }{
		{KeyApiVersions, 3, 0}, {KeyApiVersions, 0, 0}, {KeyProduce, 7, 0}, {KeyProduce, 9, 1},
	}
	for _, c := range cases {
		if got := ResponseHeaderVersion(c.key, c.v); got != c.want {
			t.Errorf("ResponseHeaderVersion(%d,%d) = %d, want %d", c.key, c.v, got, c.want)
		}
	}
}

func TestAppendResponseHeader(t *testing.T) {
	w := NewWriter(0)
	AppendResponseHeader(w, RequestHeader{APIKey: KeyProduce, APIVersion: 7, CorrelationID: 5})
	if len(w.Buf()) != 4 {
		t.Fatalf("v0 header = %x", w.Buf())
	}
	w = NewWriter(0)
	AppendResponseHeader(w, RequestHeader{APIKey: KeyProduce, APIVersion: 9, CorrelationID: 5})
	if len(w.Buf()) != 5 {
		t.Fatalf("v1 header = %x", w.Buf())
	}
}
