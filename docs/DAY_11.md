# Day 11 — Kafka Integration (Real-Time Streaming)

## Summary
- Integrated Apache Kafka as real-time ingestion layer
- Replaced file-based simulated streaming
- Built Python Kafka producer
- Updated Spark Bronze to read from Kafka
- Reset Bronze, Silver, Gold for clean pipeline
- Verified end-to-end flow

## Architecture
Producer → Kafka → Bronze → Silver → Gold → DuckDB → Streamlit

## Key Learnings
- Kafka topics, partitions, offsets
- Spark Structured Streaming with Kafka
- Checkpointing and state management
- Real-time vs batch ingestion differences

## Commands Used

### Start Kafka
docker compose up -d

### Create Topic
docker exec -it kafka kafka-topics --create --topic events_raw --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

### Run Producer
python producer/kafka_event_producer.py

### Run Bronze
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1 spark_jobs/bronze_ingest.py

## Outcome
- Fully functional real-time streaming pipeline
- Production-style architecture
- Strong foundation for advanced DE concepts
