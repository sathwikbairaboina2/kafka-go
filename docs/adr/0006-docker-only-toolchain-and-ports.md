# ADR 0006: The Go toolchain runs in Docker; host ports 5380-5389

Date: 2026-10-04 · Status: accepted

## Decision

`scripts/go.ps1`, `scripts/go.sh` and the Makefile run `go` inside `golang:1.26` with cache volumes
`kafka-go-gomod` and `kafka-go-gobuild`. CI uses `actions/setup-go` with Go 1.26. The machine is shared with other
projects, so this repo publishes only host ports 5380 (kgod) and 5382 (Apache Kafka), every container name starts
with `kafka-go-`, and the compose project is `kafka-go`.

## What I gave up

- **Edit-test latency.** Every `go` command pays container start-up.
- **Host-side clients by default.** kgod advertises `kafka-go-kgod:9092`, which resolves only inside the compose
  network. Host clients must set `KGOD_ADVERTISE=localhost:5380`.
- **Benchmark realism.** Docker Desktop on WSL 2 is the benchmark machine; numbers are labelled with it.
