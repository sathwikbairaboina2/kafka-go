// Package meta stores topic metadata in a JSON file that is replaced atomically.
package meta

import (
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"sync"
)

// Topic is one topic's metadata.
type Topic struct {
	Name       string `json:"name"`
	ID         string `json:"id"` // UUID v4, canonical 8-4-4-4-12 form
	Partitions int32  `json:"partitions"`
}

// ErrInvalidName is returned for topic names Kafka would reject, and for bad partition counts.
var ErrInvalidName = errors.New("meta: invalid topic name")

const maxPartitions = 1000

var nameRE = regexp.MustCompile(`^[a-zA-Z0-9._-]{1,249}$`)

// Store persists topics in <dir>/topics.json. It is safe for concurrent use.
type Store struct {
	mu     sync.Mutex
	dir    string
	topics map[string]Topic
}

type fileFormat struct {
	Topics []Topic `json:"topics"`
}

// OpenStore opens the store in dir, creating dir if needed. A missing file means no topics.
func OpenStore(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create meta dir: %w", err)
	}
	s := &Store{dir: dir, topics: map[string]Topic{}}
	raw, err := os.ReadFile(filepath.Join(dir, "topics.json"))
	if errors.Is(err, os.ErrNotExist) {
		return s, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read topics.json: %w", err)
	}
	var f fileFormat
	if err := json.Unmarshal(raw, &f); err != nil {
		return nil, fmt.Errorf("parse topics.json: %w", err)
	}
	for _, t := range f.Topics {
		s.topics[t.Name] = t
	}
	return s, nil
}

// List returns all topics sorted by name.
func (s *Store) List() []Topic {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.listLocked()
}

func (s *Store) listLocked() []Topic {
	out := make([]Topic, 0, len(s.topics))
	for _, t := range s.topics {
		out = append(out, t)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// Get returns the topic if it exists.
func (s *Store) Get(name string) (Topic, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.topics[name]
	return t, ok
}

// ValidName reports whether name is an acceptable topic name.
func ValidName(name string) bool {
	return nameRE.MatchString(name) && name != "." && name != ".."
}

// Create is idempotent: an existing topic is returned with created == false and its partition count unchanged.
func (s *Store) Create(name string, partitions int32) (t Topic, created bool, err error) {
	if !ValidName(name) || partitions < 1 || partitions > maxPartitions {
		return Topic{}, false, ErrInvalidName
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if t, ok := s.topics[name]; ok {
		return t, false, nil
	}
	id, err := newUUID()
	if err != nil {
		return Topic{}, false, err
	}
	t = Topic{Name: name, ID: id, Partitions: partitions}
	s.topics[name] = t
	if err := s.persistLocked(); err != nil {
		delete(s.topics, name)
		return Topic{}, false, err
	}
	return t, true, nil
}

func (s *Store) persistLocked() error {
	raw, err := json.MarshalIndent(fileFormat{Topics: s.listLocked()}, "", "  ")
	if err != nil {
		return fmt.Errorf("encode topics: %w", err)
	}
	tmp := filepath.Join(s.dir, "topics.json.tmp")
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}
	if _, err := f.Write(raw); err != nil {
		f.Close()
		return fmt.Errorf("write temp file: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("sync temp file: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close temp file: %w", err)
	}
	if err := os.Rename(tmp, filepath.Join(s.dir, "topics.json")); err != nil {
		return fmt.Errorf("rename topics.json: %w", err)
	}
	if runtime.GOOS == "windows" {
		return nil
	}
	d, err := os.Open(s.dir)
	if err != nil {
		return fmt.Errorf("open meta dir: %w", err)
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("sync meta dir: %w", err)
	}
	return nil
}

func newUUID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("generate topic id: %w", err)
	}
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16]), nil
}
