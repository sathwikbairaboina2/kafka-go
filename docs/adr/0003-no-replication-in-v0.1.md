# ADR 0003: Single broker, no replication, no KRaft in v0.1

Date: 2026-10-04 · Status: accepted

## Decision

kgod is one broker with node id 1. Metadata always names it as leader, sole replica and sole ISR member.
FindCoordinator always returns it. `acks=1` and `acks=-1` behave the same: the write is acknowledged after it is
in the log (and fsynced with `--fsync always`).

## What I gave up

- **Availability and durability beyond one disk.** Losing the host loses the data.
- **The hardest half of Kafka**: leader election, ISR, high-watermark propagation, KRaft. These are the stretch goal.
- **A like-for-like comparison** with a replicated Kafka cluster. The benchmark compares against a single-node
  Apache Kafka with replication factor 1.
