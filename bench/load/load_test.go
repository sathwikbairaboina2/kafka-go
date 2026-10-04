package main

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/sathwikbairaboina2/kafka-go/internal/app"
	"github.com/sathwikbairaboina2/kafka-go/internal/storage"
)

func TestLoadSmoke(t *testing.T) {
	if testing.Short() {
		t.Skip("runs a 2 s load")
	}
	inst, err := app.Start(app.Config{
		Listen: "127.0.0.1:0", DataDir: t.TempDir(), Topics: map[string]int32{"bench": 8},
		AutoCreate: true, DefaultPartitions: 8,
		Storage:       storage.Config{RetentionMs: -1, RetentionBytes: -1, Fsync: storage.FsyncNever},
		FsyncInterval: 50 * time.Millisecond, RetentionCheck: time.Minute,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = inst.Close(ctx)
	}()
	res, err := runLoad(config{
		Brokers: inst.Addr(), Topic: "bench", RecordBytes: 1024, Duration: 2 * time.Second, Warmup: 0, Runs: 1, Clients: 2, Label: "smoke",
	})
	if err != nil {
		t.Fatal(err)
	}
	r := res.Runs[0]
	if r.Records == 0 || r.Errors != 0 || r.MBPerSec <= 0 || r.P50Ms <= 0 {
		t.Fatalf("run = %+v", r)
	}
}
