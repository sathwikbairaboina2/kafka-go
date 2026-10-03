# ADR 0008: Topics come from flags or auto-creation, not CreateTopics

Date: 2026-10-04 · Status: accepted

## Decision

`kgod --topic orders:3` creates topics at start-up. With `--auto-create=true` (default), a Metadata request with
`allow_auto_topic_creation=true` or a Produce to an unknown topic creates it with `--default-partitions` (3).
Topic metadata lives in `data/meta/topics.json`, replaced atomically. CreateTopics (19) and DeleteTopics (20) are v0.2.

## What I gave up

- **Admin tooling.** `kafka-topics.sh --create` and franz-go's `kadm` cannot create topics on kgod.
- **Typos become topics** when auto-create is on, as in Kafka's default configuration.
- **Changing partition counts** after creation.
