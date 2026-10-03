// Package app wires the broker, storage, groups and server together for kgod, the compat tests and
// the crash harness.
package app

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/sathwikbairaboina2/kafka-go/internal/broker"
	"github.com/sathwikbairaboina2/kafka-go/internal/group"
	"github.com/sathwikbairaboina2/kafka-go/internal/meta"
	"github.com/sathwikbairaboina2/kafka-go/internal/server"
	"github.com/sathwikbairaboina2/kafka-go/internal/storage"
)

// Config is everything Start needs.
type Config struct {
	Listen, Advertise, DataDir    string
	Topics                        map[string]int32
	AutoCreate                    bool
	DefaultPartitions             int32
	Storage                       storage.Config
	FsyncInterval, RetentionCheck time.Duration
	MaxFrameBytes                 int32
	Logger                        *slog.Logger
}

// Instance is a running broker.
type Instance struct {
	addr    string
	srv     *server.Server
	logs    *storage.Manager
	offsets *group.OffsetStore
	cancel  context.CancelFunc
	loops   sync.WaitGroup
	serveCh chan error
}

// Start opens the stores, binds the listener and starts serving. It returns once the listener is bound.
// An empty Advertise means the bound listener address.
func Start(cfg Config) (*Instance, error) {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	ms, err := meta.OpenStore(filepath.Join(cfg.DataDir, "meta"))
	if err != nil {
		return nil, err
	}
	for name, n := range cfg.Topics {
		if _, _, err := ms.Create(name, n); err != nil {
			return nil, fmt.Errorf("create topic %q: %w", name, err)
		}
	}
	logs := storage.NewManager(cfg.DataDir, cfg.Storage, nil)
	for _, t := range ms.List() {
		if err := logs.Ensure(t.Name, t.Partitions); err != nil {
			_ = logs.Close()
			return nil, err
		}
	}
	offsets, err := group.OpenOffsetStore(filepath.Join(cfg.DataDir, "__offsets"), cfg.Storage.Fsync == storage.FsyncAlways)
	if err != nil {
		_ = logs.Close()
		return nil, err
	}
	ln, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		_ = logs.Close()
		_ = offsets.Close()
		return nil, fmt.Errorf("listen on %q: %w", cfg.Listen, err)
	}
	advertise := cfg.Advertise
	if advertise == "" {
		advertise = ln.Addr().String()
	}
	host, portStr, err := net.SplitHostPort(advertise)
	if err != nil {
		_ = ln.Close()
		_ = logs.Close()
		_ = offsets.Close()
		return nil, fmt.Errorf("advertise address %q: %w", advertise, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		_ = ln.Close()
		_ = logs.Close()
		_ = offsets.Close()
		return nil, fmt.Errorf("advertise port %q: %w", portStr, err)
	}

	coord := group.NewCoordinator(offsets, nil)
	b := broker.New(broker.Config{
		NodeID: 1, Host: host, Port: int32(port), ClusterID: "kafka-go",
		AutoCreate: cfg.AutoCreate, DefaultPartitions: cfg.DefaultPartitions,
	}, ms, logs)
	b.SetCoordinator(coord)
	srv := server.New(b, server.Options{MaxFrameBytes: cfg.MaxFrameBytes, Logger: cfg.Logger})

	ctx, cancel := context.WithCancel(context.Background())
	inst := &Instance{addr: ln.Addr().String(), srv: srv, logs: logs, offsets: offsets, cancel: cancel, serveCh: make(chan error, 1)}
	inst.loops.Add(2)
	go func() { defer inst.loops.Done(); logs.Run(ctx, cfg.FsyncInterval, cfg.RetentionCheck) }()
	go func() { defer inst.loops.Done(); coord.Run(ctx, 200*time.Millisecond) }()
	go func() { inst.serveCh <- srv.Serve(ln) }()
	cfg.Logger.Info("kgod listening", "addr", inst.addr, "advertise", advertise, "fsync", cfg.Storage.Fsync.String())
	return inst, nil
}

// Addr returns the bound listener address.
func (i *Instance) Addr() string { return i.addr }

// Close shuts the server down, stops the background loops, then syncs and closes the stores.
func (i *Instance) Close(ctx context.Context) error {
	err := i.srv.Shutdown(ctx)
	i.cancel()
	i.loops.Wait()
	if e := i.offsets.Close(); e != nil && err == nil {
		err = e
	}
	if e := i.logs.Close(); e != nil && err == nil {
		err = e
	}
	return err
}
