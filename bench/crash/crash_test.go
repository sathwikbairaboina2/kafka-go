package main

import "testing"

func TestCrashLoopSmoke(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns kgod processes")
	}
	res, err := run(options{Iterations: 2, Fsync: "always", Seed: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Runs) != 1 {
		t.Fatalf("runs = %d", len(res.Runs))
	}
	r := res.Runs[0]
	if r.Acked == 0 {
		t.Fatal("no records were acknowledged; the test proves nothing")
	}
	if r.Lost != 0 || !r.VerifyOK {
		t.Fatalf("lost=%d verify_ok=%v acked=%d", r.Lost, r.VerifyOK, r.Acked)
	}
}

func TestRunRejectsBadMode(t *testing.T) {
	if _, err := run(options{Iterations: 1, Fsync: "sometimes"}); err == nil {
		t.Fatal("expected an error for an unknown fsync mode")
	}
}
