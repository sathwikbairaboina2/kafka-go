# ADR 0005: The crash test kills the process, it does not cut the power

Date: 2026-10-04 · Status: accepted

## Context

`kill -9` stops the broker process, but the kernel keeps the page cache. Bytes written with `write(2)` survive a
process kill even without fsync. Only an OS crash or power loss tests fsync.

## Decision

`bench/crash` runs the broker as a child process, produces with `acks=-1`, SIGKILLs the child at a random moment,
restarts it on the same data directory and checks every acked offset is present with the same bytes. It runs once
with `--fsync always` and once with `--fsync never` and reports both. The README states the claim as "acked writes
survive a broker process crash", not "survive power loss". Torn writes are tested separately by corrupting segment
tails in unit tests and by fuzzing recovery.

## What I gave up

- **A power-loss claim.** That needs a VM with a faulty block device (for example dm-flakey) and is not attempted.
- **Real torn writes from the crash loop.** A killed process does not tear a `write(2)`, so the loop mostly
  exercises restart and recovery, not truncation.
