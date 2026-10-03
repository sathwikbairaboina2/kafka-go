package storage

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"sync"
	"time"
)

// TopicPartition identifies one partition log.
type TopicPartition struct {
	Topic     string
	Partition int32
}

// Manager is the registry of partition logs under one data directory.
type Manager struct {
	mu    sync.RWMutex
	dir   string
	cfg   Config
	parts map[TopicPartition]*Partition
	clock func() time.Time
}

// NewManager returns a Manager rooted at dataDir. clock is used for retention.
func NewManager(dataDir string, cfg Config, clock func() time.Time) *Manager {
	if clock == nil {
		clock = time.Now
	}
	return &Manager{dir: dataDir, cfg: cfg, parts: map[TopicPartition]*Partition{}, clock: clock}
}

// Ensure opens or creates <dataDir>/<topic>-<p> for every partition of the topic. It is idempotent.
func (m *Manager) Ensure(topic string, partitions int32) error {
	m.mu.RLock()
	all := true
	for i := int32(0); i < partitions && all; i++ {
		_, all = m.parts[TopicPartition{topic, i}]
	}
	m.mu.RUnlock()
	if all {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := int32(0); i < partitions; i++ {
		tp := TopicPartition{topic, i}
		if _, ok := m.parts[tp]; ok {
			continue
		}
		p, err := OpenPartition(filepath.Join(m.dir, fmt.Sprintf("%s-%d", topic, i)), m.cfg)
		if err != nil {
			return fmt.Errorf("open %s-%d: %w", topic, i, err)
		}
		m.parts[tp] = p
	}
	return nil
}

// Get returns the partition log if it exists.
func (m *Manager) Get(topic string, partition int32) (*Partition, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	p, ok := m.parts[TopicPartition{topic, partition}]
	return p, ok
}

func (m *Manager) all() []*Partition {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]*Partition, 0, len(m.parts))
	for _, p := range m.parts {
		out = append(out, p)
	}
	return out
}

// Run syncs every partition each fsyncInterval when Fsync is FsyncInterval and applies retention every
// retentionCheck. It returns when ctx is done.
func (m *Manager) Run(ctx context.Context, fsyncInterval, retentionCheck time.Duration) {
	var syncC, retC <-chan time.Time
	if m.cfg.Fsync == FsyncInterval && fsyncInterval > 0 {
		t := time.NewTicker(fsyncInterval)
		defer t.Stop()
		syncC = t.C
	}
	if retentionCheck > 0 {
		t := time.NewTicker(retentionCheck)
		defer t.Stop()
		retC = t.C
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-syncC:
			for _, p := range m.all() {
				if err := p.Sync(); err != nil {
					slog.Error("fsync failed", "dir", p.dir, "err", err)
				}
			}
		case <-retC:
			now := m.clock()
			for _, p := range m.all() {
				if n, err := p.Retain(now); err != nil {
					slog.Error("retention failed", "dir", p.dir, "err", err)
				} else if n > 0 {
					slog.Info("retention deleted segments", "dir", p.dir, "segments", n)
				}
			}
		}
	}
}

// Close syncs and closes every partition.
func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	var first error
	for tp, p := range m.parts {
		if err := p.Close(); err != nil && first == nil {
			first = err
		}
		delete(m.parts, tp)
	}
	return first
}
