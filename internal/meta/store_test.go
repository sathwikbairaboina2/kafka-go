package meta

import (
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
)

func TestCreateGetList(t *testing.T) {
	s, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	b, created, err := s.Create("b", 3)
	if err != nil || !created || b.Partitions != 3 {
		t.Fatalf("create b: %+v %v %v", b, created, err)
	}
	if _, _, err := s.Create("a", 1); err != nil {
		t.Fatal(err)
	}
	if !regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`).MatchString(b.ID) {
		t.Fatalf("id %q is not a v4 uuid", b.ID)
	}
	again, created, err := s.Create("b", 9)
	if err != nil || created || again != b {
		t.Fatalf("second create: %+v %v %v", again, created, err)
	}
	got, ok := s.Get("b")
	if !ok || got != b {
		t.Fatalf("get: %+v %v", got, ok)
	}
	if _, ok := s.Get("zzz"); ok {
		t.Fatal("unknown topic found")
	}
	l := s.List()
	if len(l) != 2 || l[0].Name != "a" || l[1].Name != "b" {
		t.Fatalf("list = %+v", l)
	}
}

func TestReopenPersists(t *testing.T) {
	dir := t.TempDir()
	s, _ := OpenStore(dir)
	topic, _, _ := s.Create("orders", 4)
	s2, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := s2.Get("orders")
	if !ok || got != topic {
		t.Fatalf("after reopen: %+v %v", got, ok)
	}
}

func TestInvalidNames(t *testing.T) {
	s, _ := OpenStore(t.TempDir())
	for _, name := range []string{"", ".", "..", "a/b", "sp ace", strings.Repeat("x", 250)} {
		if _, _, err := s.Create(name, 1); !errors.Is(err, ErrInvalidName) {
			t.Errorf("Create(%q) err = %v", name, err)
		}
	}
	for _, p := range []int32{0, -1, 1001} {
		if _, _, err := s.Create("ok", p); !errors.Is(err, ErrInvalidName) {
			t.Errorf("Create with %d partitions err = %v", p, err)
		}
	}
	if _, _, err := s.Create(strings.Repeat("x", 249), 1000); err != nil {
		t.Errorf("max name/partitions rejected: %v", err)
	}
}

func TestNoTmpLeftBehind(t *testing.T) {
	dir := t.TempDir()
	s, _ := OpenStore(dir)
	for i := 0; i < 5; i++ {
		if _, _, err := s.Create(strings.Repeat("t", i+1), 1); err != nil {
			t.Fatal(err)
		}
	}
	files, _ := os.ReadDir(dir)
	for _, f := range files {
		if f.Name() != "topics.json" {
			t.Fatalf("unexpected file %s", f.Name())
		}
	}
	if _, err := os.Stat(filepath.Join(dir, "topics.json.tmp")); !os.IsNotExist(err) {
		t.Fatal("tmp file left behind")
	}
}

func TestConcurrentCreateOnce(t *testing.T) {
	s, _ := OpenStore(t.TempDir())
	var wg sync.WaitGroup
	var mu sync.Mutex
	created := 0
	ids := map[string]bool{}
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			topic, c, err := s.Create("same", 2)
			if err != nil {
				t.Error(err)
				return
			}
			mu.Lock()
			defer mu.Unlock()
			ids[topic.ID] = true
			if c {
				created++
			}
		}()
	}
	wg.Wait()
	if created != 1 || len(ids) != 1 {
		t.Fatalf("created %d times with %d ids", created, len(ids))
	}
}
