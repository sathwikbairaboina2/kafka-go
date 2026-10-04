# ADR 0004: Support one version per API, chosen by what real clients send

Date: 2026-10-04 · Status: accepted

## Context

Each Kafka API has up to 17 versions. Clients pick the highest version both sides support, so a broker can
advertise a narrow range and clients negotiate down.

## Decision

Support exactly the version kcat 1.7.1 (librdkafka 1.8.2) sends, which franz-go v1.22.1 also accepts: Produce 7,
Fetch 11, ListOffsets 2, Metadata 4, OffsetCommit 7, OffsetFetch 7, FindCoordinator 2, JoinGroup 5, Heartbeat 3,
LeaveGroup 1, SyncGroup 3, and ApiVersions 0-3. The versions were captured with `kcat -X debug=protocol` against
`apache/kafka:4.3.1`, and franz-go was run with `kgo.MaxVersions` capped at this table against the same broker
(produce, group consume and commit all succeeded). ApiVersions v3 is the one flexible (KIP-482) version, so the
codec still implements compact types and tagged fields.

## What I gave up

- **Newer clients that require newer minimums.** Apache Kafka 4.x Java clients and librdkafka 2.x may refuse
  this broker or lose features. Compatibility is claimed only for the two pinned clients.
- **Topic IDs, fetch sessions, KIP-848 groups** and every feature that arrives in later versions.
- **Breadth of codec coverage.** Most flexible-version code paths are exercised only by ApiVersions v3.

## Amendment (build, 2026-10-04): ranges, because of how librdkafka detects features

The first build served exactly one version per API and kcat could not produce, consume or join a group.
librdkafka 1.8.2 does not just pick the highest common version; it first enables features by checking that
the broker's advertised range **overlaps** a fixed range per API (`src/rdkafka_feature.c`). Message format v2
needs Produce 3 and Fetch 4. Consumer groups need FindCoordinator 0, JoinGroup 0, SyncGroup 0, Heartbeat 0,
LeaveGroup 0, OffsetCommit 1-2 and OffsetFetch 1. A broker that advertises only Produce 7 therefore receives
legacy message sets at Produce v7 ("Broker: Invalid message").

The supported table is now: Produce 3-7, Fetch 4-11, ListOffsets 2, Metadata 4, OffsetCommit 2-7,
OffsetFetch 1-7, FindCoordinator 0-2, JoinGroup 0-5, Heartbeat 0-3, LeaveGroup 0-1, SyncGroup 0-3 and
ApiVersions 0-3. Every version in a range is implemented and checked against franz-go's `kmsg` codec in both
directions, and clients still negotiate the maximum. The cost is more codec branches and tests instead of one
layout per API; the benefit is that kcat works, with the same behaviour as Apache Kafka 4.3.1 for the same
commands (checked with `kcat -G` on both brokers). OffsetFetch v6 and v7 are flexible (KIP-482), which the
first table got wrong.
