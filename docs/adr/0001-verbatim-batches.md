# ADR 0001: Store record batches verbatim

Date: 2026-10-04 · Status: accepted

## Context

A Produce request carries record batches (format v2). Kafka's CRC-32C covers bytes 21 to the end of a batch,
and `baseOffset` sits at bytes 0-7, outside the CRC.

## Decision

The broker validates the batch header (magic 2, `batchLength` matches the bytes received, CRC-32C), overwrites
`baseOffset` with the next offset, and writes the bytes to the segment unchanged. Fetch returns the same bytes.
Compressed batches are validated and stored without decompressing.

## What I gave up

- **Per-record validation.** A batch whose CRC is valid but whose records are malformed is stored as-is;
  the client that wrote it is trusted for the record section.
- **Broker-side recompression and LogAppendTime.** `log_append_time_ms` is always -1 and timestamps are the producer's.
- **Record-level tooling for compressed batches** in v0.1: `kgoctl dump-log` prints only the batch header for them.
