# Day 1 – Project Setup

Day 1 focused on setting up the development environment, defining project goals, and establishing the initial folder structure for the real-time data pipeline.

## Project Objective

The goal of this project is to build a real-time data pipeline using:

- Python
- Apache Spark (Structured Streaming)
- Parquet-based data lake architecture
- Bronze → Silver → Gold layered design

The system simulates streaming events, processes them incrementally, and produces analytics-ready datasets.

This project mimics production-grade streaming architecture and demonstrates:

- Layered data modeling
- Deterministic transformations
- Replayability
- Exactly-once semantics at the result level

## Development Environment Setup

### Python Environment

A dedicated virtual environment was created to isolate dependencies.

Example setup:

conda create -n realtime-pipeline python=3.11
conda activate realtime-pipeline

Or using venv:

python -m venv realtime-pipeline
source realtime-pipeline/bin/activate

Dependencies installed:

pip install pyspark duckdb

Why isolate the environment?

- Prevent dependency conflicts
- Ensure reproducibility
- Maintain consistent Spark + Python versions

## Apache Spark Setup

Apache Spark was installed via PySpark.

Verification command:

python -c "import pyspark; print(pyspark.__version__)"

Spark Structured Streaming is used to:

- Read streaming files
- Apply transformations
- Maintain state (watermarks, aggregations)
- Write incremental outputs

## Initial Folder Structure

realtime-data-pipeline/
│
├── data/
│   └── raw_events/
├── bronze/
│   └── events/
├── silver/
│   └── events/
├── gold/
├── spark_jobs/
├── checkpoints/
└── README.md

This structure mirrors production data lake architecture and separates storage layers from compute logic.

## Architectural Design Decision

The pipeline follows a Medallion Architecture pattern:

Bronze → Silver → Gold

Bronze:
- Raw ingested data
- Append-only
- No transformations

Silver:
- Validated, cleaned, deduplicated data
- Schema enforcement
- Event-time processing

Gold:
- Aggregated metrics
- Business-ready datasets
- Optimized for querying

## Outcome of Day 1

- Development environment configured
- Spark verified and working
- Project directory structure finalized
- Architecture defined
- Foundation ready for streaming simulation
