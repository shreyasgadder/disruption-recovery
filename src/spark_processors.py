"""
PySpark processing logic for Phase B pipeline.

Three processors:
  EventStreamProcessor  — ingest, validate, deduplicate stream events
  BookingReconciler     — join events ↔ bookings, detect conflicts
  MetricsAggregator     — produce airline-level and flagged-booking tables
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, when, to_timestamp, coalesce, lit,
    sum as spark_sum, count, max as spark_max,
    collect_set, row_number, get_json_object,
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
)
from pyspark.sql.window import Window


# ── Event Stream Processor ──────────────────────────────────────────

class EventStreamProcessor:
    """Load, clean, and deduplicate the JSONL event stream."""

    # Explicit schema avoids Spark's full-file inference scan.
    SCHEMA = StructType([
        StructField("booking_id", StringType(), True),
        StructField("event_id", StringType(), True),
        StructField("airline", StringType(), True),
        StructField("action", StringType(), True),
        StructField("status", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("price", StringType(), True),   # may be numeric or JSON
    ])

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def load(self, path: str) -> DataFrame:
        """Read JSONL with explicit schema (PERMISSIVE to avoid data loss)."""
        return (
            self.spark.read
            .option("mode", "PERMISSIVE")
            .schema(self.SCHEMA)
            .json(path)
        )

    def clean(self, df: DataFrame) -> DataFrame:
        """
        Validate rows and normalise the price column.

        Price handling:
          - Plain numeric string "811.63"   → cast to double
          - Nested JSON '{"amount":724.8}'  → extract via get_json_object
          - Null / unparseable             → marked invalid
        """
        # Normalise price: try direct cast first, fall back to JSON extract
        df = df.withColumn(
            "price_clean",
            coalesce(
                col("price").cast(DoubleType()),
                get_json_object(col("price"), "$.amount").cast(DoubleType()),
            ),
        )

        # Parse timestamp with multiple precision levels
        df = df.withColumn(
            "event_ts",
            coalesce(
                to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"),
                to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS"),
                to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss"),
            ),
        )

        # Mark valid rows
        df = df.withColumn(
            "is_valid",
            (col("event_id").isNotNull())
            & (col("airline").isNotNull())
            & (col("action").isNotNull())
            & (col("event_ts").isNotNull())
            & (col("price_clean").isNotNull())
            & (col("price_clean") >= 0),
        )

        return df

    def deduplicate(self, df: DataFrame) -> DataFrame:
        """Keep earliest occurrence of each event_id."""
        return df.dropDuplicates(["event_id"])


# ── Booking Reconciler ──────────────────────────────────────────────

# Tier weight used for priority scoring.  PLATINUM customers need
# attention first during a disruption.
TIER_WEIGHTS = {"PLATINUM": 40, "GOLD": 30, "SILVER": 20, "BRONZE": 10}


class BookingReconciler:
    """
    Join processed events against booking master and flag conflicts.

    Booking master schema: booking_id, customer_name, tier, region
    (no amount or status column — we derive everything from the stream).
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def load_bookings(self, path: str) -> DataFrame:
        return self.spark.read.parquet(path)

    def reconcile(
        self, events: DataFrame, bookings: DataFrame
    ) -> DataFrame:
        """
        Aggregate events per booking, join with master, detect conflicts.

        Conflict types surfaced:
          CANCELLATION_SIGNAL   — latest event status is CANCELLED
          REFUND_AFTER_CONFIRM  — booking was confirmed but also refunded
          EXCESSIVE_ACTIVITY    — unusually high event count (>5)
        """
        # Summarise events per booking
        event_agg = (
            events.filter("is_valid = true")
            .groupBy("booking_id")
            .agg(
                spark_max("airline").alias("airline"),
                spark_sum("price_clean").alias("total_revenue"),
                count("event_id").alias("event_count"),
                spark_max("event_ts").alias("latest_event_ts"),
                spark_max("status").alias("latest_status"),
                collect_set("action").alias("actions"),
            )
        )

        # Join — left join keeps bookings with zero events visible
        joined = bookings.join(event_agg, on="booking_id", how="left")

        # ── Conflict detection ──────────────────────────────────────
        joined = joined.withColumn(
            "conflict_type",
            when(
                col("latest_status") == "CANCELLED",
                lit("CANCELLATION_SIGNAL"),
            )
            .when(
                col("actions").isNotNull()
                & col("actions").cast("string").contains("REFUND")
                & col("actions").cast("string").contains("BOOKING_CONFIRMED"),
                lit("REFUND_AFTER_CONFIRM"),
            )
            .when(
                col("event_count") > 5,
                lit("EXCESSIVE_ACTIVITY"),
            )
            .otherwise(lit(None).cast(StringType())),
        )

        joined = joined.withColumn(
            "is_anomaly", col("conflict_type").isNotNull()
        )

        # ── Priority score (higher = investigate first) ─────────────
        tier_score = when(col("tier") == "PLATINUM", 40).when(
            col("tier") == "GOLD", 30
        ).when(col("tier") == "SILVER", 20).otherwise(10)

        conflict_score = when(col("is_anomaly"), 50).otherwise(0)

        revenue_score = when(
            col("total_revenue") > 1000, 30
        ).when(col("total_revenue") > 500, 15).otherwise(0)

        joined = joined.withColumn(
            "priority_score",
            (tier_score + conflict_score + revenue_score).cast("integer"),
        )

        return joined


# ── Metrics Aggregator ──────────────────────────────────────────────

class MetricsAggregator:
    """Produce airline-level summary metrics."""

    @staticmethod
    def by_airline(df: DataFrame) -> DataFrame:
        return (
            df.filter(col("airline").isNotNull())
            .groupBy("airline")
            .agg(
                spark_sum(
                    when(col("event_count").isNotNull(), col("event_count")).otherwise(0)
                ).alias("total_events"),
                spark_sum(
                    when(col("total_revenue").isNotNull(), col("total_revenue")).otherwise(0.0)
                ).alias("total_revenue"),
                count(when(col("is_anomaly"), 1)).alias("anomaly_count"),
            )
            .orderBy(col("total_revenue").desc())
        )
