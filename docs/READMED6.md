## Backfill & Replay (Day 6)

This project supports **cold replays**: rebuilding Silver and Gold from immutable Bronze data.

### Why Replay Works

This pipeline supports safe cold replays because each layer is designed for deterministic, reproducible behavior.

#### Bronze – Immutable Source of Truth

The Bronze layer is append-only and stores raw, unmodified events.  
It is never updated or overwritten.

Because Bronze data is immutable:
- Reprocessing always reads the exact same input.
- Derived layers (Silver, Gold) can be safely rebuilt.
- Replay does not depend on runtime state.

Bronze guarantees input stability — same input always produces the same downstream results.

---

#### Silver – Idempotent Transformation Layer

The Silver layer performs:

- Schema enforcement
- Data validation
- Derived column creation
- Deduplication using a composite key
- Watermarking for event-time correctness

Silver transformations are deterministic and idempotent.

Idempotent means:
> Running the same transformation multiple times produces the same final dataset.

If Bronze is replayed:
- The same events are processed.
- Deduplication prevents duplicate amplification.
- The resulting Silver output is identical.

This ensures replay does not introduce double counting.

---

#### Gold – Deterministic Aggregation Layer

The Gold layer computes window-based aggregations such as:

- Events per minute
- Delay buckets per minute
- Latency percentiles per minute

Gold logic is purely derived from Silver and uses event-time windows with watermarks.

Because:
- Window boundaries are deterministic
- Aggregations depend only on input data
- No external state is used

Rebuilding Gold from the same Silver dataset produces identical results.

---

#### Checkpoints – Progress Tracking

Each streaming job uses a checkpoint directory to store:

- Source offsets
- Commit logs
- Watermark state
- Aggregation state

Checkpoints enable:
- Crash recovery
- Safe restarts
- Continuation without reprocessing everything

However, correctness comes from deterministic logic — checkpoints only optimize progress.

---

#### `_spark_metadata` – Safe File Sink Commits

Each streaming output folder contains:

This directory tracks:
- Batch commit logs
- File-level metadata
- Compaction history

It ensures:
- A batch is either fully committed or not committed at all
- Partial writes do not corrupt results
- Duplicate writes are avoided

---

#### Important: Output Path and Checkpoint Must Be Paired

For every streaming job:

- Output directory
- Checkpoint directory

must be treated as a pair.

If one is deleted without the other, Spark may fail with errors such as:

- `BATCH_METADATA_NOT_FOUND`
- Missing `.compact` files

For a clean replay:
1. Stop the streaming job.
2. Delete both the output directory and its corresponding checkpoint.
3. Restart the job.

---

### Replay Guarantee

By deleting Silver and Gold outputs (and their checkpoints) and rebuilding from Bronze, we verified:

- No duplicate amplification
- No data loss
- Identical aggregate results before and after replay

This demonstrates effectively-once semantics at the result level.

### Cold Replay Procedure (Rebuild from Bronze)

The following steps simulate a full downstream layer loss and validate deterministic replay from Bronze.

#### 1) Stop Streaming Jobs

Stop all running Silver and Gold streaming processes:

- Press `Ctrl + C` in each terminal running a Spark job
- Confirm no Spark jobs are running:

```bash
ps aux | grep spark | grep -v grep