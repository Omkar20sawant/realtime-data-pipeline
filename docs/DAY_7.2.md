# Day 7 -- Performance & Production Hardening (Part 2)

## Objective

Validate Spark query optimization behavior and confirm that partitioning
improves query performance for analytics workloads.

This step focuses on understanding how Spark executes queries internally
using the `explain(True)` command and verifying that partition pruning
works correctly on the Silver dataset.

------------------------------------------------------------------------

# Dataset Used

Silver table:

    silver/events_v2

This dataset is written by the Silver streaming pipeline and partitioned
by:

    event_date

Example directory structure:

    silver/events_v2/
        event_date=2026-03-07/
        event_date=2026-03-08/
        event_date=2026-03-09/

Partitioning allows Spark to skip irrelevant directories when executing
queries.

------------------------------------------------------------------------

# Step 1 -- Start Spark Session

Navigate to the project root:

    cd realtime-data-pipeline

Start PySpark:

    pyspark

------------------------------------------------------------------------

# Step 2 -- Load Silver Dataset

    df = spark.read.parquet("silver/events_v2")
    df.printSchema()

Observed schema:

-   event_id
-   event_ts
-   store_id
-   sku
-   qty
-   price
-   channel
-   \_ingest_ts
-   \_source_file
-   \_run_id
-   \_pipeline_version
-   ingest_date
-   event_hour
-   processing_delay_sec
-   delay_bucket
-   dedup_comp_key
-   event_date

The key partition column is:

    event_date

------------------------------------------------------------------------

# Step 3 -- Inspect Query Execution Plan

Test query filtering by partition column:

    df.filter("event_date = DATE '2026-03-08'").explain(True)

Spark produced a physical plan containing:

    PartitionFilters: [isnotnull(event_date), event_date = 2026-03-08]

------------------------------------------------------------------------

# Key Observation -- Partition Pruning

The presence of:

    PartitionFilters

confirms that Spark performs **partition pruning**.

Instead of scanning all files:

    silver/events_v2/*

Spark reads only the required partition:

    silver/events_v2/event_date=2026-03-08/*

This significantly reduces:

-   disk reads
-   file scans
-   execution time

------------------------------------------------------------------------

# Good Query Pattern

Efficient analytics queries filter directly on the partition column.

Example:

    df.filter("event_date = DATE '2026-03-08'")

or

    WHERE event_date BETWEEN DATE '2026-03-01' AND DATE '2026-03-08'

These queries enable partition pruning.

------------------------------------------------------------------------

# Inefficient Query Pattern

Example:

    df.filter("year(event_ts) = 2026")

This forces Spark to compute a function on each row, which may prevent
effective partition pruning.

Result:

Spark must scan more partitions and process more data.

------------------------------------------------------------------------

# Why Partitioning by Date Helps Analytics

Most analytical queries operate on time windows such as:

-   daily reports
-   weekly trends
-   monthly dashboards
-   historical backfills

Partitioning by `event_date` allows Spark to efficiently limit the
dataset scanned during these queries.

Benefits include:

-   Faster queries
-   Lower compute cost
-   Reduced I/O
-   Scalable analytics as data grows

------------------------------------------------------------------------

# Architecture Context

The pipeline follows a Medallion Architecture pattern:

    Bronze → Silver → Gold

Layers:

    Bronze
    Raw event ingestion

    Silver
    Cleaned, deduplicated, partitioned dataset

    Gold
    Aggregated analytics tables

The Silver layer is optimized for downstream analytics by partitioning
on `event_date`.

------------------------------------------------------------------------

# Day 7 Outcome

Verified that:

-   Silver dataset is correctly partitioned
-   Spark execution plan confirms partition pruning
-   Filtering by partition column significantly improves query
    efficiency

This completes **Day 7 -- Performance Hardening**.
