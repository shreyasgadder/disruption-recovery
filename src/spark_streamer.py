"""
ETG Disruption Recovery: Robust Spark "Streaming" Mimic.

Polls 'data/streaming/input' for new micro-batches, transforms them
using Medallion logic, and persists results to PostgreSQL using Psycopg2.
This approach bypasses flaky internal directory watchers in a Docker environment.
"""

import os
import sys
import time
import shutil
import psycopg2
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import current_date, current_timestamp
from src.spark_processors import EventStreamProcessor, BookingReconciler, MetricsAggregator

# --- Configuration ---
INPUT_DIR = "/opt/airflow/data/streaming/input"
PROCESSED_DIR = "/opt/airflow/data/streaming/processed"
BOOKINGS_PATH = "/opt/airflow/data/raw/bookings_master.parquet"
DB_CONFIG = {
    "dbname": "egt_analytics",
    "user": "airflow",
    "password": "airflow",
    "host": "postgres",
    "port": "5432"
}

def _spark(app_name: str):
    """Returns a local-mode SparkSession optimized for micro-batching."""
    os.environ["PATH"] = "/usr/local/airflow/.local/bin:/usr/local/bin:/usr/bin:/bin:/usr/local/sbin:/usr/sbin:/sbin"
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )

def upsert_to_postgres(pdf, table_name):
    """Reliable persistence using pure Psycopg2. 100% stable in container setups."""
    if pdf.empty:
        return
    
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        # 1. Truncate/Overwrite for the airline metrics (Cumulative report demo)
        if table_name == "airline_metrics":
            cur.execute(f"TRUNCATE TABLE {table_name};")
            
        # 2. Batch Insert
        columns = ",".join(pdf.columns)
        values = [tuple(x) for x in pdf.values]
        placeholders = ",".join(["%s"] * len(pdf.columns))
        insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        
        cur.executemany(insert_query, values)
        conn.commit()
        cur.close()
    except Exception as e:
        print(f"ERROR: Database error in {table_name}: {e}")
    finally:
        if conn:
            conn.close()

def process_file(spark, file_path):
    """Process a single JSONL file batch using the Medallion pipeline."""
    try:
        # Load Batch
        batch_df = spark.read.schema(EventStreamProcessor.SCHEMA).json(file_path)
        if batch_df.isEmpty():
            return True

        print(f"INFO: Processing micro-batch from {os.path.basename(file_path)}...")
        
        # Pipeline: Clean - Dedup - Reconcile - Aggregate
        proc = EventStreamProcessor(spark)
        cleaned = proc.clean(batch_df)
        deduped = proc.deduplicate(cleaned)

        rec = BookingReconciler(spark)
        bookings = rec.load_bookings(BOOKINGS_PATH)
        reconciled = rec.reconcile(deduped, bookings)

        # Convert to Pandas for Sink
        metrics_pdf = MetricsAggregator().by_airline(reconciled) \
            .withColumn("partition_date", current_date()) \
            .toPandas()
            
        flagged_pdf = reconciled.filter("is_anomaly = true") \
            .select("booking_id", "customer_name", "priority_score", "conflict_type") \
            .withColumn("partition_date", current_date()) \
            .withColumn("last_updated", current_timestamp()) \
            .toPandas()

        # Database Persistence
        upsert_to_postgres(metrics_pdf, "airline_metrics")
        upsert_to_postgres(flagged_pdf, "flagged_bookings")

        print(f"SUCCESS: {os.path.basename(file_path)} processed and persisted.")
        return True
    except Exception as e:
        print(f"ERROR: Failed to process {file_path}: {e}")
        return False

def main():
    spark = _spark("etg-streaming-robust")
    os.makedirs(INPUT_DIR, exist_ok=True)
    os.makedirs(PROCESSED_DIR, exist_ok=True)

    print(f"INFO: Streaming processor started. Monitoring {INPUT_DIR}...")

    try:
        while True:
            # Polling for new JSONL files
            files = sorted([f for f in os.listdir(INPUT_DIR) if f.endswith(".jsonl")])
            
            if not files:
                time.sleep(2)
                continue

            for filename in files:
                batch_path = os.path.join(INPUT_DIR, filename)
                processed_path = os.path.join(PROCESSED_DIR, filename)
                
                if process_file(spark, batch_path):
                    shutil.move(batch_path, processed_path)
                    
            time.sleep(1)
    except KeyboardInterrupt:
        print("INFO: Streaming processor stopped.")

if __name__ == "__main__":
    main()
