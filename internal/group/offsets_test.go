package group

import (
	"os"
	"path/filepath"
	"testing"
)

func ms(s string) *string { return &s }

func TestOffsetStoreReplay(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenOffsetStore(dir, false)
	if err != nil {
		t.Fatal(err)
	}
	k1 := OffsetKey{"g", "t", 0}
	k2 := OffsetKey{"g", "t", 1}
	k3 := OffsetKey{"other", "t", 0}
	if err := s.Commit(map[OffsetKey]Committed{k1: {Offset: 5, LeaderEpoch: -1, CommitTimeMs: 1}, k2: {Offset: 9, Metadata: ms("m")}}); err != nil {
		t.Fatal(err)
	}
	if err := s.Commit(map[OffsetKey]Committed{k1: {Offset: 7, LeaderEpoch: 3, CommitTimeMs: 2}, k3: {Offset: 1}}); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := OpenOffsetStore(dir, true)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if c, ok := s2.Get(k1); !ok || c.Offset != 7 || c.LeaderEpoch != 3 || c.CommitTimeMs != 2 {
		t.Fatalf("k1 = %+v %v (latest must win)", c, ok)
	}
	if c, ok := s2.Get(k2); !ok || c.Offset != 9 || c.Metadata == nil || *c.Metadata != "m" {
		t.Fatalf("k2 = %+v %v", c, ok)
	}
	if got := s2.ForGroup("g"); len(got) != 2 {
		t.Fatalf("ForGroup(g) = %v", got)
	}
	if _, ok := s2.Get(OffsetKey{"g", "t", 99}); ok {
		t.Fatal("unexpected entry")
	}
}

func TestOffsetStoreTornTail(t *testing.T) {
	dir := t.TempDir()
	s, _ := OpenOffsetStore(dir, false)
	k := OffsetKey{"g", "t", 0}
	if err := s.Commit(map[OffsetKey]Committed{k: {Offset: 42}}); err != nil {
		t.Fatal(err)
	}
	s.Close()
	path := filepath.Join(dir, "commits.log")
	st, _ := os.Stat(path)
	f, _ := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	f.Write([]byte{1, 2, 3, 4, 5, 6, 7})
	f.Close()

	s2, err := OpenOffsetStore(dir, false)
	if err != nil {
		t.Fatal(err)
	}
	if c, ok := s2.Get(k); !ok || c.Offset != 42 {
		t.Fatalf("entry lost: %+v %v", c, ok)
	}
	st2, _ := os.Stat(path)
	if st2.Size() != st.Size() {
		t.Fatalf("tail not truncated: %d vs %d", st2.Size(), st.Size())
	}
	// new commits land after the truncated tail and survive another reopen
	if err := s2.Commit(map[OffsetKey]Committed{{"g", "t", 1}: {Offset: 8}}); err != nil {
		t.Fatal(err)
	}
	s2.Close()
	s3, _ := OpenOffsetStore(dir, false)
	defer s3.Close()
	if len(s3.ForGroup("g")) != 2 {
		t.Fatalf("entries after reopen = %v", s3.ForGroup("g"))
	}
}

func TestOffsetStoreCorruptFrame(t *testing.T) {
	dir := t.TempDir()
	s, _ := OpenOffsetStore(dir, false)
	s.Commit(map[OffsetKey]Committed{{"g", "t", 0}: {Offset: 1}})
	s.Commit(map[OffsetKey]Committed{{"g", "t", 1}: {Offset: 2}})
	s.Close()
	path := filepath.Join(dir, "commits.log")
	raw, _ := os.ReadFile(path)
	raw[len(raw)-3] ^= 0xff // corrupt the second frame
	os.WriteFile(path, raw, 0o644)
	s2, err := OpenOffsetStore(dir, false)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if len(s2.ForGroup("g")) != 1 {
		t.Fatalf("entries = %v, want only the first", s2.ForGroup("g"))
	}
}
