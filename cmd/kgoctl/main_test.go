package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sathwikbairaboina2/kafka-go/internal/record"
	"github.com/sathwikbairaboina2/kafka-go/internal/storage"
)

// makeDir writes two partitions of a topic through the storage layer.
func makeDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	m := storage.NewManager(dir, storage.Config{SegmentBytes: 500, RetentionMs: -1, RetentionBytes: -1}, nil)
	if err := m.Ensure("t", 2); err != nil {
		t.Fatal(err)
	}
	for p := int32(0); p < 2; p++ {
		part, _ := m.Get("t", p)
		for i := 0; i < 6; i++ {
			b := record.Build(1000, []record.Record{
				{Key: []byte("k"), Value: []byte("hello")},
				{Key: nil, Value: []byte(strings.Repeat("x", 100))},
			})
			h, _ := record.Parse(b)
			if _, err := part.Append(b, h); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	return dir
}

func TestKgoctlVerifyAndDump(t *testing.T) {
	dir := makeDir(t)
	var out, errb bytes.Buffer
	if code := run([]string{"verify-log", dir}, &out, &errb); code != 0 || !strings.HasPrefix(out.String(), "OK partitions=2 ") || !strings.Contains(out.String(), "records=24") {
		t.Fatalf("verify = %d %q %q", code, out.String(), errb.String())
	}

	logs, _ := filepath.Glob(filepath.Join(dir, "t-0", "*.log"))
	out.Reset()
	if code := run([]string{"dump-log", logs[0]}, &out, &errb); code != 0 {
		t.Fatalf("dump = %d %q", code, errb.String())
	}
	s := out.String()
	for _, want := range []string{"crc=ok", "codec=none", "records=2", `key="k" value="hello"`, "key=nil"} {
		if !strings.Contains(s, want) {
			t.Fatalf("dump output lacks %q:\n%s", want, s)
		}
	}
}

func TestKgoctlVerifyFailsOnCorruption(t *testing.T) {
	dir := makeDir(t)
	logs, _ := filepath.Glob(filepath.Join(dir, "t-1", "*.log"))
	raw, _ := os.ReadFile(logs[0])
	raw[record.HeaderSize+2] ^= 0xff
	if err := os.WriteFile(logs[0], raw, 0o644); err != nil {
		t.Fatal(err)
	}
	var out, errb bytes.Buffer
	if code := run([]string{"verify-log", dir}, &out, &errb); code != 1 || !strings.Contains(out.String(), "FAIL problems=") {
		t.Fatalf("verify = %d %q", code, out.String())
	}
	out.Reset()
	run([]string{"dump-log", logs[0]}, &out, &errb)
	if !strings.Contains(out.String(), "crc=BAD") {
		t.Fatalf("dump did not flag the bad crc:\n%s", out.String())
	}
}

func TestKgoctlUsage(t *testing.T) {
	var out, errb bytes.Buffer
	if code := run(nil, &out, &errb); code != 2 || !strings.Contains(errb.String(), "usage") {
		t.Fatalf("no args = %d %q", code, errb.String())
	}
	if code := run([]string{"nope", "x"}, &out, &errb); code != 2 {
		t.Fatalf("unknown command = %d", code)
	}
}
