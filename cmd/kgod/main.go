// Command kgod is a single-node Kafka-compatible broker.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/sathwikbairaboina2/kafka-go/internal/app"
	"github.com/sathwikbairaboina2/kafka-go/internal/storage"
)

// topicFlags collects repeatable --topic name:partitions values.
type topicFlags map[string]int32

func (t topicFlags) String() string { return fmt.Sprint(map[string]int32(t)) }

func (t topicFlags) Set(v string) error {
	name, n, ok := strings.Cut(v, ":")
	if !ok {
		return fmt.Errorf("want name:partitions, got %q", v)
	}
	p, err := strconv.Atoi(n)
	if err != nil || p < 1 || p > 1000 {
		return fmt.Errorf("bad partition count in %q", v)
	}
	t[name] = int32(p)
	return nil
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	slog.SetDefault(logger)

	topics := topicFlags{}
	listen := flag.String("listen", ":9092", "address to listen on")
	advertise := flag.String("advertise", "", "host:port clients should connect to (default: the bound address)")
	dataDir := flag.String("data-dir", "./data", "directory for logs, topic metadata and committed offsets")
	flag.Var(topics, "topic", "topic to create at startup as name:partitions (repeatable)")
	autoCreate := flag.Bool("auto-create", true, "create topics on Metadata or Produce")
	defaultParts := flag.Int("default-partitions", 3, "partitions for auto-created topics")
	segmentBytes := flag.Int64("segment-bytes", 1<<30, "roll the active segment past this size")
	retentionMs := flag.Int64("retention-ms", 604800000, "delete sealed segments older than this; -1 disables")
	retentionBytes := flag.Int64("retention-bytes", -1, "keep about this many bytes per partition; -1 disables")
	retentionCheck := flag.Duration("retention-check", 30*time.Second, "how often retention runs")
	fsync := flag.String("fsync", "interval", "never, interval or always")
	fsyncInterval := flag.Duration("fsync-interval", 50*time.Millisecond, "flush period for --fsync interval")
	maxFrame := flag.Int("max-frame-bytes", 104857600, "largest request frame accepted")
	flag.Parse()

	policy, err := storage.ParseFsync(*fsync)
	if err != nil {
		fmt.Fprintln(os.Stderr, "kgod:", err)
		os.Exit(2)
	}
	inst, err := app.Start(app.Config{
		Listen: *listen, Advertise: *advertise, DataDir: *dataDir, Topics: topics,
		AutoCreate: *autoCreate, DefaultPartitions: int32(*defaultParts),
		Storage: storage.Config{
			SegmentBytes: *segmentBytes, RetentionMs: *retentionMs, RetentionBytes: *retentionBytes, Fsync: policy,
		},
		FsyncInterval: *fsyncInterval, RetentionCheck: *retentionCheck,
		MaxFrameBytes: int32(*maxFrame), Logger: logger,
	})
	if err != nil {
		logger.Error("start failed", "err", err)
		os.Exit(1)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	s := <-sig
	logger.Info("shutting down", "signal", s.String())
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := inst.Close(ctx); err != nil {
		logger.Error("shutdown error", "err", err)
		os.Exit(1)
	}
}
