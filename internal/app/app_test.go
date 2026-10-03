package app

import (
	"context"
	"encoding/binary"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/sathwikbairaboina2/kafka-go/internal/protocol"
	"github.com/sathwikbairaboina2/kafka-go/internal/storage"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func testConfig(dir string) Config {
	return Config{
		Listen: "127.0.0.1:0", DataDir: dir, Topics: map[string]int32{"orders": 3},
		AutoCreate: true, DefaultPartitions: 3,
		Storage:       storage.Config{RetentionMs: -1, RetentionBytes: -1, Fsync: storage.FsyncInterval},
		FsyncInterval: 20 * time.Millisecond, RetentionCheck: time.Minute,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

func TestStartAndClose(t *testing.T) {
	dir := t.TempDir()
	inst, err := Start(testConfig(dir))
	if err != nil {
		t.Fatal(err)
	}
	c, err := net.Dial("tcp", inst.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(5 * time.Second))
	req := kmsg.NewPtrApiVersionsRequest()
	req.SetVersion(0)
	frame := kmsg.NewRequestFormatter().AppendRequest(nil, req, 9)
	if _, err := c.Write(frame); err != nil {
		t.Fatal(err)
	}
	var sz [4]byte
	if _, err := io.ReadFull(c, sz[:]); err != nil {
		t.Fatal(err)
	}
	body := make([]byte, binary.BigEndian.Uint32(sz[:]))
	if _, err := io.ReadFull(c, body); err != nil {
		t.Fatal(err)
	}
	if binary.BigEndian.Uint32(body) != 9 {
		t.Fatalf("correlation id = %d", binary.BigEndian.Uint32(body))
	}
	var resp kmsg.ApiVersionsResponse
	resp.SetVersion(0)
	if err := resp.ReadFrom(body[4:]); err != nil {
		t.Fatal(err)
	}
	if resp.ErrorCode != 0 || len(resp.ApiKeys) != len(protocol.Supported) {
		t.Fatalf("resp = %+v", resp)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := inst.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := net.DialTimeout("tcp", inst.Addr(), 300*time.Millisecond); err == nil {
		t.Fatal("still listening after Close")
	}
}

func TestRestartKeepsTopics(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(dir)
	inst, err := Start(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := inst.Close(ctx); err != nil {
		t.Fatal(err)
	}
	cfg.Topics = nil // topics come from the meta store on the second start
	inst, err = Start(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := inst.logs.Get("orders", 2); !ok {
		t.Fatal("topic orders partition 2 was not reopened")
	}
	if err := inst.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestAdvertiseOverride(t *testing.T) {
	cfg := testConfig(t.TempDir())
	cfg.Advertise = "broker.example:9999"
	inst, err := Start(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	defer inst.Close(ctx)
	c, err := net.Dial("tcp", inst.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(5 * time.Second))
	req := kmsg.NewPtrMetadataRequest()
	req.SetVersion(4)
	req.Topics = []kmsg.MetadataRequestTopic{}
	if _, err := c.Write(kmsg.NewRequestFormatter().AppendRequest(nil, req, 1)); err != nil {
		t.Fatal(err)
	}
	var sz [4]byte
	io.ReadFull(c, sz[:])
	body := make([]byte, binary.BigEndian.Uint32(sz[:]))
	io.ReadFull(c, body)
	var resp kmsg.MetadataResponse
	resp.SetVersion(4)
	if err := resp.ReadFrom(body[4:]); err != nil {
		t.Fatal(err)
	}
	if resp.Brokers[0].Host != "broker.example" || resp.Brokers[0].Port != 9999 {
		t.Fatalf("broker = %+v", resp.Brokers[0])
	}
}
