# ADR 0002: One writer per partition, guarded by a RWMutex

Date: 2026-10-04 · Status: accepted

## Decision

Each `storage.Partition` has one `sync.RWMutex`. Append takes the write lock, assigns offsets, writes, updates the
index and (with `--fsync always`) calls `File.Sync` before unlocking. Reads take the read lock and only see bytes
up to the committed end of the active segment. Offsets are assigned only inside the write lock.

## What I gave up

- **Group commit.** With `--fsync always`, each Produce pays its own fsync while holding the lock, so concurrent
  producers to one partition serialise on the disk. Kafka-style group commit is a v0.2 optimisation.
- **Reads concurrent with an append to the same partition.** Readers wait for the append in progress.
- **Lock-free designs.** Simpler reasoning was worth more than the throughput they might add.
