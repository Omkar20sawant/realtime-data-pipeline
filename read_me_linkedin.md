Over the past 11 days I built a production-grade real-time data pipeline from scratch — and I learned more than I expected.

The stack:
→ Apache Kafka (event ingestion)
→ Apache Spark Structured Streaming
→ Medallion Architecture (Bronze → Silver → Gold)
→ Parquet data lake with partition pruning
→ DuckDB for lightweight OLAP analytics
→ Streamlit for live monitoring dashboard

What I built, day by day:
✅ Simulated streaming events with a Python Kafka producer
✅ Bronze layer: immutable, append-only raw ingestion
✅ Silver layer: deduplication (SHA2 composite key), watermarks, schema validation, date partitioning
✅ Gold layer: events/min, delay buckets, latency percentiles (p50–p99)
✅ Crash recovery & deterministic replay from Bronze
✅ DuckDB querying Gold Parquet files directly with SQL
✅ Streamlit dashboard with KPI cards, trend charts, auto-refresh & anomaly alerts
✅ Production polish: YAML config, env vars, structured logging, modular architecture

The thing I'm most proud of? The replay engineering. I simulated a production failure by deleting Silver + Gold, then proved the system could rebuild deterministically from Bronze — exactly once, no duplicate records.

I also simulated the entire project first with a Python file-based generator before adding Kafka — deliberately, to understand the end-to-end flow before introducing a real message broker. That progression taught me more than jumping straight to Kafka would have.

And the lesson that hit hardest: checkpointing. I thought it was just a config option you set and forget. Then I deleted the wrong folder and my "live" pipeline metrics started showing 7-day-old delays as real data. Turns out checkpointing and backfill strategy don't just affect reliability — they determine whether your output is actually correct. That's a different thing entirely.

This pattern — Kafka → Spark → Parquet lake → DuckDB → Dashboard — is what real data teams run at scale. Building it end-to-end gave me a much deeper appreciation for why every design decision exists.

Day 12 is already in progress — schema evolution, data validation, backfill strategy, and Dockerizing the whole pipeline.

And after that? A brand new project: ML-driven predictions on a live real-world dataset. Same production-grade pipeline thinking, but with a model at the end instead of a dashboard metric.

Follow along if that sounds interesting.

Code on GitHub → https://github.com/Omkar20sawant/realtime-data-pipeline 🚀

#DataEngineering #ApacheSpark #Kafka #Python #DataLake #Streaming #Parquet #DuckDB #Streamlit #MedallionArchitecture #Portfolio