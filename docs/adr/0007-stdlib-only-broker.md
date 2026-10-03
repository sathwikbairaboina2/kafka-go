# ADR 0007: The broker uses only the Go standard library; franz-go is a test oracle

Date: 2026-10-04 · Status: accepted

## Decision

Everything under `cmd/` and `internal/` imports only the standard library in non-test files. franz-go's `kmsg`
encodes requests and decodes responses in `*_oracle_test.go` files to check our codec, franz-go's `kgo` client
drives `compat/` tests and `bench/`, and `pgregory.net/rapid` drives property and model tests.
A gate (`go list -deps`) fails if a non-test broker package pulls a non-standard import.

## What I gave up

- **Speed of delivery.** kmsg already implements every message; writing our own codec is most of the work.
- **Independence of the oracle.** If kmsg and our codec agree on a wrong layout, the oracle tests pass; the
  kcat compat run (a second, unrelated implementation) is the backstop.
