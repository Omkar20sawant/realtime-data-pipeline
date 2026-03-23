# Day 2 – Event Generation & Streaming Simulation

Day 2 focused on building a streaming data simulation system to mimic real-time event ingestion.

## Objective

Since Kafka is not used initially, a file-based streaming simulation was implemented to generate continuous event data.

Goals:
- Simulate real-time event production
- Create realistic event schema
- Continuously write JSON files
- Prepare input for Bronze ingestion

## Event Schema Design

Each event represents a transactional record:

- event_id
- event_ts
- store_id
- sku
- qty
- price
- channel

Example:

{
  "event_id": "uuid",
  "event_ts": "2026-03-07 10:01:23",
  "store_id": 101,
  "sku": "SKU_ABC",
  "qty": 2,
  "price": 19.99,
  "channel": "web"
}

## Event Generator Implementation

A Python script (generate_events.py) was created to:

- Generate structured events
- Write JSON files continuously
- Simulate micro-batches
- Use current timestamps

Files are written to:

data/raw_events/

This directory acts as a simulated streaming source.

## Why File-Based Streaming?

Advantages:
- No Kafka dependency
- Easy local testing
- Easy to inspect files
- Supports replay scenarios

Spark Structured Streaming detects new files automatically.

## Streaming Simulation Behavior

The generator:
- Writes small JSON batches
- Simulates ingestion delays
- Produces continuous event flow

This mimics:

Producer → Topic → Consumer

using local directories.

## Outcome of Day 2

- Streaming simulation implemented
- Event schema defined
- Continuous data generation validated
- Input source for Bronze layer ready
