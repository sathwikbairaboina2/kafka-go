package server

import (
	"context"
	"encoding/binary"
	"io"
	"log/slog"
	"net"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sathwikbairaboina2/kafka-go/internal/protocol"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// echoHandler writes key and correlation id into the body; Produce with acks=0 gets no response.
type echoHandler struct{ calls atomic.Int64 }

func (e *echoHandler) Handle(_ context.Context, h protocol.RequestHeader, body *protocol.Reader) ([]byte, bool, error) {
	e.calls.Add(1)
	if h.APIKey == protocol.KeyProduce {
		var q protocol.ProduceRequest
		if err := q.Decode(body, h.APIVersion); err != nil {
			return nil, false, err
		}
		if q.Acks == 0 {
			return nil, false, nil
		}
	}
	w := protocol.NewWriter(8)
	w.Int16(h.APIKey)
	w.Int32(h.CorrelationID)
	return w.Buf(), true, nil
}

func start(t *testing.T, h Handler, opts Options) (*Server, string) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	opts.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	s := New(h, opts)
	done := make(chan error, 1)
	go func() { done <- s.Serve(l) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = s.Shutdown(ctx)
		if err := <-done; err != net.ErrClosed {
			t.Errorf("Serve returned %v, want net.ErrClosed", err)
		}
	})
	return s, l.Addr().String()
}

func dial(t *testing.T, addr string) net.Conn {
	t.Helper()
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	_ = c.SetDeadline(time.Now().Add(20 * time.Second))
	return c
}

func readResp(t *testing.T, c net.Conn) []byte {
	t.Helper()
	var sz [4]byte
	if _, err := io.ReadFull(c, sz[:]); err != nil {
		t.Fatalf("read size: %v", err)
	}
	b := make([]byte, binary.BigEndian.Uint32(sz[:]))
	if _, err := io.ReadFull(c, b); err != nil {
		t.Fatalf("read body: %v", err)
	}
	return b
}

func frame(req kmsg.Request, corr int32) []byte {
	req.SetVersion(protocol.Supported[req.Key()].Max)
	return kmsg.NewRequestFormatter(kmsg.FormatterClientID("t")).AppendRequest(nil, req, corr)
}

// rawFrame builds a header-only frame for any key and version.
func rawFrame(key, version int16, corr int32) []byte {
	w := protocol.NewWriter(32)
	w.Int32(0)
	w.Int16(key)
	w.Int16(version)
	w.Int32(corr)
	s := "t"
	w.NullableString(&s)
	if protocol.IsFlexible(key, version) {
		w.EmptyTaggedFields()
	}
	b := w.Buf()
	binary.BigEndian.PutUint32(b, uint32(len(b)-4))
	return b
}

func TestPipelinedRequestsInOrder(t *testing.T) {
	h := &echoHandler{}
	_, addr := start(t, h, Options{})
	c := dial(t, addr)

	var wantKeys []int16
	var wantCorr []int32
	var all []byte
	for i := int32(0); i < 1000; i++ {
		switch i % 3 {
		case 0:
			all = append(all, frame(kmsg.NewPtrMetadataRequest(), i)...)
			wantKeys, wantCorr = append(wantKeys, protocol.KeyMetadata), append(wantCorr, i)
		case 1:
			req := kmsg.NewPtrFetchRequest()
			all = append(all, frame(req, i)...)
			wantKeys, wantCorr = append(wantKeys, protocol.KeyFetch), append(wantCorr, i)
		case 2:
			req := kmsg.NewPtrProduceRequest()
			req.Acks = 0
			req.Topics = []kmsg.ProduceRequestTopic{{Topic: "t", Partitions: []kmsg.ProduceRequestTopicPartition{{Partition: 0, Records: []byte{1, 2, 3}}}}}
			all = append(all, frame(req, i)...) // no response expected
		}
	}
	go func() { _, _ = c.Write(all) }()
	for n := range wantCorr {
		b := readResp(t, c)
		corr := int32(binary.BigEndian.Uint32(b[0:]))
		key := int16(binary.BigEndian.Uint16(b[4:]))
		if corr != wantCorr[n] || key != wantKeys[n] {
			t.Fatalf("response %d: corr %d key %d, want corr %d key %d", n, corr, key, wantCorr[n], wantKeys[n])
		}
		if int32(binary.BigEndian.Uint32(b[6:])) != corr {
			t.Fatalf("body correlation id mismatch")
		}
	}
	// a final request proves nothing extra (acks=0 responses) was queued before it
	_, _ = c.Write(frame(kmsg.NewPtrMetadataRequest(), 5000))
	if corr := int32(binary.BigEndian.Uint32(readResp(t, c))); corr != 5000 {
		t.Fatalf("stray response before final request: corr %d", corr)
	}
}

func TestVersionGate(t *testing.T) {
	h := &echoHandler{}
	_, addr := start(t, h, Options{})
	for key, rng := range protocol.Supported {
		var bad []int16
		if rng.Min > 0 {
			bad = append(bad, rng.Min-1)
		}
		bad = append(bad, rng.Max+1)
		for _, v := range bad {
			c := dial(t, addr)
			before := h.calls.Load()
			if _, err := c.Write(rawFrame(key, v, 77)); err != nil {
				t.Fatal(err)
			}
			if key == protocol.KeyApiVersions {
				b := readResp(t, c)
				if int32(binary.BigEndian.Uint32(b)) != 77 {
					t.Fatalf("correlation id %x", b[:4])
				}
				var resp kmsg.ApiVersionsResponse
				resp.SetVersion(0)
				if err := resp.ReadFrom(b[4:]); err != nil {
					t.Fatal(err)
				}
				if resp.ErrorCode != protocol.ErrUnsupportedVersion || len(resp.ApiKeys) != len(protocol.Supported) {
					t.Fatalf("ApiVersions v%d: err %d keys %d", v, resp.ErrorCode, len(resp.ApiKeys))
				}
			} else {
				var one [1]byte
				if _, err := c.Read(one[:]); err != io.EOF {
					t.Fatalf("key %d v%d: expected close, read err %v", key, v, err)
				}
			}
			if h.calls.Load() != before {
				t.Fatalf("handler called for unsupported key %d v%d", key, v)
			}
		}
	}
	c := dial(t, addr)
	_, _ = c.Write(rawFrame(75, 0, 1))
	var one [1]byte
	if _, err := c.Read(one[:]); err != io.EOF {
		t.Fatalf("unknown key: expected close, got %v", err)
	}
	if h.calls.Load() != 0 {
		t.Fatalf("handler was called %d times", h.calls.Load())
	}
}

func TestOversizedFrameRejected(t *testing.T) {
	_, addr := start(t, &echoHandler{}, Options{})
	c := dial(t, addr)
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	if _, err := c.Write([]byte{0x7f, 0xff, 0xff, 0xff}); err != nil {
		t.Fatal(err)
	}
	var one [1]byte
	if _, err := c.Read(one[:]); err != io.EOF {
		t.Fatalf("expected close, got %v", err)
	}
	runtime.ReadMemStats(&after)
	if grown := after.TotalAlloc - before.TotalAlloc; grown > 8<<20 {
		t.Fatalf("allocated %d bytes for a rejected frame", grown)
	}
	// negative and undersized sizes close too
	for _, sz := range [][]byte{{0xff, 0xff, 0xff, 0xff}, {0, 0, 0, 3}} {
		c := dial(t, addr)
		_, _ = c.Write(sz)
		if _, err := c.Read(one[:]); err != io.EOF {
			t.Fatalf("size %x: expected close, got %v", sz, err)
		}
	}
}

func TestShutdownClosesConnections(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := New(&echoHandler{}, Options{})
	done := make(chan error, 1)
	go func() { done <- s.Serve(l) }()
	c := dial(t, l.Addr().String())
	_, _ = c.Write(frame(kmsg.NewPtrMetadataRequest(), 1))
	readResp(t, c) // connection is established and being served
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != net.ErrClosed {
		t.Fatalf("Serve = %v", err)
	}
	var one [1]byte
	if _, err := c.Read(one[:]); err == nil {
		t.Fatal("connection still open after Shutdown")
	}
	if _, err := net.DialTimeout("tcp", l.Addr().String(), 500*time.Millisecond); err == nil {
		t.Fatal("listener still accepting after Shutdown")
	}
}
