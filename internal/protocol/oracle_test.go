package protocol

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kmsg"
)

// kmsgBody returns req's body bytes at version v as franz-go encodes them.
func kmsgBody(req kmsg.Request, v int16) []byte {
	req.SetVersion(v)
	return req.AppendTo(nil)
}

// kmsgDecode decodes b into resp at version v and fails the test on error.
func kmsgDecode(t *testing.T, resp kmsg.Response, v int16, b []byte) {
	t.Helper()
	resp.SetVersion(v)
	if err := resp.ReadFrom(b); err != nil {
		t.Fatalf("kmsg %T v%d: %v", resp, v, err)
	}
}

// mustConsume fails if r has an error or unread bytes.
func mustConsume(t interface {
	Helper()
	Fatalf(string, ...any)
}, r *Reader) {
	t.Helper()
	if err := r.Err(); err != nil {
		t.Fatalf("reader error: %v", err)
	}
	if n := r.Remaining(); n != 0 {
		t.Fatalf("%d unread bytes", n)
	}
}
