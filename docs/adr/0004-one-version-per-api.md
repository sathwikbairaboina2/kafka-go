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
