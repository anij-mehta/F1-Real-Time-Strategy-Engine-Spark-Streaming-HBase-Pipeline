"""Run Bronze archival, Silver aggregation, metrics, and Gold HBase prediction."""

from __future__ import annotations

import logging
import time
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, lit, max as spark_max, sum as spark_sum, when

from config import (
    APP_NAME,
    BRONZE_CHECKPOINT_PATH,
    BRONZE_PARQUET_PATH,
    SILVER_PARQUET_PATH,
    SPARK_MASTER,
    SHUFFLE_PARTITIONS,
    STREAM_HOST,
    STREAM_PORT,
)
from gold import process_gold_layer
from hbase_sink import write_pipeline_health
from silver import build_silver, classify_rows, parse_bronze


LOGGER = logging.getLogger(__name__)


def log_batch_metrics(batch_df, batch_id: int, started_at: float | None = None) -> None:
    """Emit data-quality counts and processing latency for every input micro-batch."""
    started_at = time.monotonic() if started_at is None else started_at
    metrics = (
        batch_df.agg(
            count(lit(1)).alias("rows"),
            spark_sum(when(~col("is_silver_eligible"), lit(1)).otherwise(lit(0))).alias("filtered_rows"),
            spark_sum(when(col("is_invalid"), lit(1)).otherwise(lit(0))).alias("invalid_rows"),
            spark_sum(when(col("is_pit_lap"), lit(1)).otherwise(lit(0))).alias("pit_lap_rows"),
            spark_sum(when(col("is_sc_or_vsc"), lit(1)).otherwise(lit(0))).alias("sc_vsc_rows"),
            spark_sum(when(col("is_lap_time_outlier"), lit(1)).otherwise(lit(0))).alias("outlier_rows"),
            spark_max("ReplaySentAt").alias("latest_event_time"),
        )
        .first()
    )
    event_lag_seconds = None
    if metrics["latest_event_time"] is not None:
        event_lag_seconds = (datetime.now() - metrics["latest_event_time"]).total_seconds()
    LOGGER.info(
        "Quality batch=%s rows=%s filtered_rows=%s invalid_rows=%s pit_lap_rows=%s "
        "sc_vsc_rows=%s outlier_rows=%s batch_latency_ms=%.1f event_lag_seconds=%s",
        batch_id,
        metrics["rows"],
        metrics["filtered_rows"],
        metrics["invalid_rows"],
        metrics["pit_lap_rows"],
        metrics["sc_vsc_rows"],
        metrics["outlier_rows"],
        (time.monotonic() - started_at) * 1000,
        None if event_lag_seconds is None else round(event_lag_seconds, 3),
    )
    try:
        write_pipeline_health(
            (time.monotonic() - started_at) * 1000,
            int(metrics["rows"] or 0),
            event_lag_seconds,
        )
    except Exception:
        LOGGER.exception("Could not persist health metrics for Bronze batch=%s", batch_id)


def archive_bronze_and_log(batch_df, batch_id: int) -> None:
    """Persist raw typed telemetry and emit quality metrics in one source query."""
    started_at = time.monotonic()
    batch_df.write.mode("append").partitionBy("Driver").parquet(BRONZE_PARQUET_PATH)
    log_batch_metrics(batch_df, batch_id, started_at)


def archive_silver_and_process_gold(batch_df, batch_id: int) -> None:
    """Persist Silver aggregates before producing the Gold HBase strategy signal."""
    batch_df.write.mode("append").partitionBy("Driver").parquet(SILVER_PARQUET_PATH)
    process_gold_layer(batch_df, batch_id)


def process_replay_batch(batch_df, batch_id: int) -> None:
    """Process each socket micro-batch through Bronze, Silver, and Gold."""
    bronze_batch = classify_rows(parse_bronze(batch_df))

    # Persist Bronze and record quality metrics.
    archive_bronze_and_log(bronze_batch, batch_id)

    # Derive Silver from the same batch, then persist and process Gold.
    silver_batch = build_silver(bronze_batch)
    archive_silver_and_process_gold(silver_batch, batch_id)


def start_pipeline() -> None:
    spark = (
        SparkSession.builder.appName(APP_NAME)
        .master(SPARK_MASTER)
        .config("spark.sql.shuffle.partitions", SHUFFLE_PARTITIONS)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # The socket source cannot recover offsets, so a checkpoint left by an earlier
    # run makes the restarted query fail ("IndexOutOfBounds ... deleting N").
    checkpoint = spark._jvm.org.apache.hadoop.fs.Path(BRONZE_CHECKPOINT_PATH + "_single_query")
    checkpoint.getFileSystem(spark._jsc.hadoopConfiguration()).delete(checkpoint, True)

    raw_stream = (
        spark.readStream.format("socket")
        .option("host", STREAM_HOST)
        .option("port", STREAM_PORT)
        .load()
    )

    query = (
        raw_stream.writeStream
        .outputMode("append")
        .foreachBatch(process_replay_batch)
        # Use a fresh checkpoint because the query structure has changed.
        .option("checkpointLocation", BRONZE_CHECKPOINT_PATH + "_single_query")
        .start()
    )

    LOGGER.info("Started single-query Bronze -> Silver -> Gold pipeline: %s", query.id)
    query.awaitTermination()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    start_pipeline()
