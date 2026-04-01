# Architecture Brief: Disruption Recovery

## Philosophy: "Integrity in Chaos"
During a service disruption, the primary enemy is not just the volume of data, but the divergence of truth. When airline signals flood the system, they often contradict the internal "Master" state (Booking Master). My philosophy for this engine was to prioritize observability and idempotence over raw speed, ensuring that every micro-batch yields the same "Gold" insight without double-counting revenue or missing a high-priority customer.

## Phase A: The Production Audit
The primary challenge of the audit was the 30MB RAM constraint. A traditional "load everything into a DataFrame" approach would fail immediately on a 5M row file.
- **Strategy**: I implemented a single-pass streaming parser in pure Python. Use of the standard `json` library with line-by-line reads ensures the memory footprint remains constant (bytes, not megabytes), regardless of the input file size.
- **Discovery**: The audit revealed significant "Schema Drift" in the price field—some events used numeric strings while others used nested objects. The program normalizes these on-the-fly to ensure the revenue report is accurate.

## Phase B: The Automated Pipeline (Medallion)
The pipeline follows a simplified Medallion Architecture (Bronze -> Silver -> Gold), orchestrated via Apache Airflow.

- **Bronze**: When it detects a new batch file from Producer, it grabs it.
- **Silver**: It runs PySpark transformations (cleans it, deduplicates it, joins it with Master Bookings).
- **Gold**: It saves the final calculated metrics into the PostgreSQL database.

### 1. Robust Streaming Mimic (Decoupled Execution)
To handle the 5M row constraint locally, execution is completely decoupled:
- **The Simulator**: Run `python scripts/simulate_stream.py` on your local terminal. It trickles data into an input folder.
- **The Consumer**: *Do not* run `spark_streamer.py` yourself. It runs permanently as a background daemon inside its own Docker container (`egt_spark_streamer`). Because traditional directory watchers in Spark Structured Streaming can be flaky over Windows mounts, it uses a **Continuous Polling Loop**. When it sees a file in the input directory, it processes it, updates Postgres, and moves the file to an archive.

### 2. Data Challenges & Mitigations
- **The "Price" Problem**: As discovered in the audit, the stream is inconsistent. Our processing logic uses a `coalesce` approach in Spark to handle both flat and nested price formats.
- **The "Duplication" Noise**: Automated systems often retry signals during outages. We use an idempotent deduplication strategy by ignoring duplicate `event_id`s within each micro-batch.
- **The Signal-Booking Disconnect**: The most critical problem is when an airline cancels a seat but master record still shows it as active. We bridge this gap through a real-time reconciliation join. This surfaces the "Cancellation Signal" vs "Master Intent" conflict immediately.

## Business Intuition & Anomaly Detection
We don't just process data; we prioritize **Recovery Mission Success**. 
- **Priority Scoring**: We calculate a dynamic `priority_score` based on:
    - **Customer Tier**: Platinum/Gold customers get a fixed boost (TIER_WEIGHTS).
    - **Revenue Exposure**: High-value transactions are prioritized.
    - **Conflict Severity**: An active cancellation signal on a confirmed booking triggers a high-severity flag.
- **Actionable Gold Layer**: The final output is not a raw log, but a prioritized list of High-Value Recovery Targets that the Customer Support team can use to triage the disruption.

## Engineering Standards
- **Professional Orchestration**: The Airflow DAG acts as a monitoring plane with a pre-configured `postgres_default` connection.
- **Pure DB Sink**: To eliminate the fragility of local JVM-JDBC drivers, I transitioned to a direct Psycopg2 sink. This ensures that every processed record reaches the Gold layer without silent failures. 
- **Standalone Cli**: Phase A remains a standalone CLI tool, providing a restricted environment-friendly entry point for high-level distribution auditing.
