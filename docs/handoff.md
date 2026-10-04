# Handoff

## 2026-10-04 · Claude (sonnet-builder) · main

Changed: kafka-go v0.1 built: protocol codecs, record batches, segment storage with recovery, groups, broker, kgod/kgoctl, compat tests, kill -9 crash loop, load generator, measured Kafka comparison, CI, README, DEVDOCS.
Left: Opus review, final board update, DEVDOCS publish to the board. v0.2 items are listed in the README.
Verify: `scripts/go.ps1 test -race -count=1 ./...`, `scripts/dock.ps1 sh scripts/check.sh`, `bash scripts/compat-kcat.sh`, `bash scripts/bench.sh` (gates in docs/superpowers/plans/2026-10-04-kafka-go.md).

## 2026-10-04 · Claude (Opus lead review) · main

Changed: Reviewed the v0.1 build (storage append/read/recovery, produce, fetch long poll, group state machine). No broker correctness bugs found. Accepted the builder's ruling on `TestTwoMemberJoinSyncHeartbeatLeave`: the original ordering raced, and the broker behaves like Kafka (a rejoin waits for known members until the rebalance deadline). Fixed the README quickstart. kgod advertises `kafka-go-kgod:9092` by default, so host kcat on `localhost:5380` could not reach partitions. The quickstart now uses `KGOD_ADVERTISE=127.0.0.1:5380`, verified with host-network kcat; `localhost` fails on IPv6 `::1`. Added a noise caveat under the README headline. Rewrote `docs/DEVDOCS.md` as a clear developer guide.
Gates (re-run by Opus, real output): `go test -race -count=1 ./...` all ok (storage 24.3 s); check.sh gofmt/vet/stdlib-only ok; FuzzParseBatch 15 s PASS (8.8M execs); FuzzRecoverSegment 15 s PASS (only 236 execs, file I/O bound); crash smoke 3 iterations: always acked=99200 lost=0, never acked=136700 lost=0, verify ok both (one never-mode recovery took 10982 ms); docker build ok; compat-kcat.sh PASS; no leftover kafka-go-kgod/kafka containers. `bench/report` over the committed results reprints the README table and headline exactly. bench.sh was not re-run in this review (about 10 minutes per run); numbers come from the builder's run in `bench/results/`.
Left: v0.2 items (metrics, .timeindex, admin APIs, group commit for fsync always, faster recovery). CI has never run (no remote). The benchmark ratio against Kafka is noisy (Kafka varied about 2x between runs).
Verify: the gate block in docs/superpowers/plans/2026-10-04-kafka-go.md, then `powershell -NoProfile -File scripts/go.ps1 run ./bench/report`.
