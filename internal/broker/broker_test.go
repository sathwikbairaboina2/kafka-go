package broker

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/sathwikbairaboina2/kafka-go/internal/group"
	"github.com/sathwikbairaboina2/kafka-go/internal/meta"
	"github.com/sathwikbairaboina2/kafka-go/internal/protocol"
	"github.com/sathwikbairaboina2/kafka-go/internal/storage"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type env struct {
	b     *Broker
	meta  *meta.Store
	logs  *storage.Manager
	corr  atomic.Int32
	coord *group.Coordinator
}

func newEnv(t testing.TB, mutate ...func(*Config)) *env {
	t.Helper()
	dir := t.TempDir()
	ms, err := meta.OpenStore(dir + "/meta")
	if err != nil {
		t.Fatal(err)
	}
	lm := storage.NewManager(dir, storage.Config{RetentionMs: -1, RetentionBytes: -1}, nil)
	t.Cleanup(func() { lm.Close() })
	cfg := Config{NodeID: 1, Host: "localhost", Port: 9092, ClusterID: "kafka-go", AutoCreate: true, DefaultPartitions: 3}
	for _, m := range mutate {
		m(&cfg)
	}
	store, err := group.OpenOffsetStore(dir+"/__offsets", false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	coord := group.NewCoordinator(store, nil)
	b := New(cfg, ms, lm)
	b.SetCoordinator(coord)
	return &env{b: b, meta: ms, logs: lm, coord: coord}
}

// call encodes req with kmsg at the broker's supported version, runs the handler and returns the raw body.
func (e *env) call(t testing.TB, req kmsg.Request) ([]byte, bool) {
	t.Helper()
	req.SetVersion(protocol.Supported[req.Key()].Max)
	frame := kmsg.NewRequestFormatter(kmsg.FormatterClientID("test")).AppendRequest(nil, req, e.corr.Add(1))
	r := protocol.NewReader(frame[4:])
	h, err := protocol.ParseRequestHeader(r)
	if err != nil {
		t.Fatal(err)
	}
	body, respond, err := e.b.Handle(context.Background(), h, r)
	if err != nil {
		t.Fatalf("handle key %d: %v", req.Key(), err)
	}
	return body, respond
}

func decode(t testing.TB, resp kmsg.Response, v int16, b []byte) {
	t.Helper()
	resp.SetVersion(v)
	if err := resp.ReadFrom(b); err != nil {
		t.Fatalf("decode %T: %v", resp, err)
	}
}

func sptr(s string) *string { return &s }

func TestApiVersionsAllVersions(t *testing.T) {
	e := newEnv(t)
	for v := int16(0); v <= 3; v++ {
		var hdr = protocol.RequestHeader{APIKey: protocol.KeyApiVersions, APIVersion: v}
		req := kmsg.NewPtrApiVersionsRequest()
		req.ClientSoftwareName, req.ClientSoftwareVersion = "x", "1"
		req.SetVersion(v)
		body := req.AppendTo(nil)
		resp, ok, err := e.b.Handle(context.Background(), hdr, protocol.NewReader(body))
		if err != nil || !ok {
			t.Fatalf("v%d: ok=%v err=%v", v, ok, err)
		}
		var out kmsg.ApiVersionsResponse
		decode(t, &out, v, resp)
		if out.ErrorCode != 0 || len(out.ApiKeys) != len(protocol.Supported) {
			t.Fatalf("v%d: %+v", v, out)
		}
	}
}

func TestMetadataAutoCreate(t *testing.T) {
	e := newEnv(t)
	req := kmsg.NewPtrMetadataRequest()
	req.Topics = []kmsg.MetadataRequestTopic{{Topic: sptr("orders")}}
	req.AllowAutoTopicCreation = true
	body, _ := e.call(t, req)
	var out kmsg.MetadataResponse
	decode(t, &out, 4, body)
	if len(out.Brokers) != 1 || out.Brokers[0].Host != "localhost" || out.Brokers[0].Port != 9092 || out.ControllerID != 1 {
		t.Fatalf("brokers/controller = %+v", out)
	}
	if len(out.Topics) != 1 || out.Topics[0].ErrorCode != 0 || len(out.Topics[0].Partitions) != 3 {
		t.Fatalf("topics = %+v", out.Topics)
	}
	for _, p := range out.Topics[0].Partitions {
		if p.Leader != 1 || len(p.Replicas) != 1 || len(p.ISR) != 1 {
			t.Fatalf("partition = %+v", p)
		}
	}
	if _, ok := e.logs.Get("orders", 2); !ok {
		t.Fatal("storage partitions were not created")
	}

	// without AllowAutoTopicCreation an unknown topic is an error
	req = kmsg.NewPtrMetadataRequest()
	req.Topics = []kmsg.MetadataRequestTopic{{Topic: sptr("nope")}, {Topic: sptr("bad name")}}
	req.AllowAutoTopicCreation = false
	body, _ = e.call(t, req)
	out = kmsg.MetadataResponse{}
	decode(t, &out, 4, body)
	if out.Topics[0].ErrorCode != protocol.ErrUnknownTopicOrPartition || out.Topics[1].ErrorCode != protocol.ErrInvalidTopic {
		t.Fatalf("errors = %d %d", out.Topics[0].ErrorCode, out.Topics[1].ErrorCode)
	}

	// null topics = all existing topics
	body, _ = e.call(t, kmsg.NewPtrMetadataRequest())
	out = kmsg.MetadataResponse{}
	decode(t, &out, 4, body)
	if len(out.Topics) != 1 || *out.Topics[0].Topic != "orders" {
		t.Fatalf("all topics = %+v", out.Topics)
	}
}

func TestMetadataAutoCreateDisabled(t *testing.T) {
	e := newEnv(t, func(c *Config) { c.AutoCreate = false })
	req := kmsg.NewPtrMetadataRequest()
	req.Topics = []kmsg.MetadataRequestTopic{{Topic: sptr("orders")}}
	req.AllowAutoTopicCreation = true
	body, _ := e.call(t, req)
	var out kmsg.MetadataResponse
	decode(t, &out, 4, body)
	if out.Topics[0].ErrorCode != protocol.ErrUnknownTopicOrPartition {
		t.Fatalf("error = %d", out.Topics[0].ErrorCode)
	}
}
