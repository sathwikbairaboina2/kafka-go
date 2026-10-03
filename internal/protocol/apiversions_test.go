package protocol

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestApiVersionsRequestOracle(t *testing.T) {
	for v := int16(0); v <= 3; v++ {
		req := kmsg.NewPtrApiVersionsRequest()
		req.ClientSoftwareName = "kgo"
		req.ClientSoftwareVersion = "1.0"
		var got ApiVersionsRequest
		r := NewReader(kmsgBody(req, v))
		if err := got.Decode(r, v); err != nil {
			t.Fatalf("v%d: %v", v, err)
		}
		mustConsume(t, r)
		if v == 3 && (got.ClientSoftwareName != "kgo" || got.ClientSoftwareVersion != "1.0") {
			t.Fatalf("v3 names = %+v", got)
		}
		if v < 3 && (got.ClientSoftwareName != "" || got.ClientSoftwareVersion != "") {
			t.Fatalf("v%d names should be empty: %+v", v, got)
		}
	}
}

func TestApiVersionsResponseOracle(t *testing.T) {
	for v := int16(0); v <= 3; v++ {
		resp := ApiVersionsResponse{Keys: SupportedKeys()}
		w := NewWriter(0)
		resp.Encode(w, v)
		var out kmsg.ApiVersionsResponse
		kmsgDecode(t, &out, v, w.Buf())
		if out.ErrorCode != 0 || len(out.ApiKeys) != len(Supported) {
			t.Fatalf("v%d: err %d keys %d", v, out.ErrorCode, len(out.ApiKeys))
		}
		for i, k := range resp.Keys {
			g := out.ApiKeys[i]
			if g.ApiKey != k.Key || g.MinVersion != k.Min || g.MaxVersion != k.Max {
				t.Fatalf("v%d key %d: got %+v want %+v", v, i, g, k)
			}
		}
	}
}

func TestSupportedKeysSorted(t *testing.T) {
	keys := SupportedKeys()
	for i := 1; i < len(keys); i++ {
		if keys[i-1].Key >= keys[i].Key {
			t.Fatalf("not sorted: %v", keys)
		}
	}
}

func TestApiVersionsErrorV0(t *testing.T) {
	resp := ApiVersionsResponse{ErrorCode: ErrUnsupportedVersion, Keys: SupportedKeys()}
	w := NewWriter(0)
	resp.Encode(w, 0)
	var out kmsg.ApiVersionsResponse
	kmsgDecode(t, &out, 0, w.Buf())
	if out.ErrorCode != ErrUnsupportedVersion || len(out.ApiKeys) != len(Supported) {
		t.Fatalf("got err %d keys %d", out.ErrorCode, len(out.ApiKeys))
	}
}
