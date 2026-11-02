"""Bronze parsing, data-quality classification, and Silver aggregation."""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    avg,
    col,
    coalesce,
    concat_ws,
    from_json,
    first,
    lit,
    when,
    window,
)

from config import (
    MAX_LAP_TIME_SECONDS,
    MIN_LAP_TIME_SECONDS,
    SC_VSC_TRACK_STATUS_PATTERN,
    WATERMARK_DELAY,
    WINDOW_DURATION,
    WINDOW_SLIDE,
)
from schema import TELEMETRY_SCHEMA


def parse_bronze(raw_stream: DataFrame) -> DataFrame:
    """Keep the original JSON alongside the explicitly typed telemetry fields."""
    return (
        raw_stream.select(col("value").alias("raw_json"), from_json(col("value"), TELEMETRY_SCHEMA).alias("data"))
        .select("raw_json", "data.*")
        .withColumn("event_time", col("LapCompletionTime"))
    )


def classify_rows(bronze_df: DataFrame) -> DataFrame:
    """Annotate each record so metrics can report why it did not reach Silver."""
    required = (
        col("Driver").isNotNull()
        & col("LapNumber").isNotNull()
        & col("GapToAhead").isNotNull()
        & col("TyreLife").isNotNull()
        & col("FuelCorrectedLapTime").isNotNull()
        & col("event_time").isNotNull()
    )
    pit_lap = coalesce(col("IsInLap"), lit(False)) | coalesce(col("IsOutLap"), lit(False))
    sc_or_vsc = coalesce(col("TrackStatus").rlike(SC_VSC_TRACK_STATUS_PATTERN), lit(False))
    lap_time_outlier = (
        col("LapTime").isNull()
        | (col("LapTime") < lit(MIN_LAP_TIME_SECONDS))
        | (col("LapTime") > lit(MAX_LAP_TIME_SECONDS))
    )
    return (
        bronze_df.withColumn("is_invalid", ~required)
        .withColumn("is_pit_lap", pit_lap)
        .withColumn("is_sc_or_vsc", sc_or_vsc)
        .withColumn("is_lap_time_outlier", lap_time_outlier)
        .withColumn(
            "filter_reason",
            concat_ws(
                ",",
                when(~required, lit("invalid")),
                when(pit_lap, lit("in_or_out_lap")),
                when(sc_or_vsc, lit("sc_or_vsc")),
                when(lap_time_outlier, lit("lap_time_outlier")),
            ),
        )
        .withColumn("is_silver_eligible", required & ~pit_lap & ~sc_or_vsc & ~lap_time_outlier)
    )


def build_silver(bronze_df: DataFrame) -> DataFrame:
    """Aggregate valid racing laps by driver in the configured event-time window."""
    return (
        bronze_df.filter(col("is_silver_eligible"))
        .withWatermark("event_time", WATERMARK_DELAY)
        .groupBy(
            window(col("event_time"), WINDOW_DURATION, WINDOW_SLIDE),
            col("Driver"),
            col("Stint"),
            col("LapNumber"),
        )
        .agg(
            avg("GapToAhead").alias("AvgGap"),
            avg("TyreLife").alias("AvgTyreLife"),
            avg("FuelCorrectedLapTime").alias("AvgFuelCorrectedLapTime"),
            avg("LapTimeDelta").alias("AvgLapTimeDelta"),
            avg("Position").alias("AvgPosition"),
            first("Compound", ignorenulls=True).alias("Compound"),
            first("TrackStatus", ignorenulls=True).alias("TrackStatus"),
            first("ReplaySentAt", ignorenulls=True).alias("ReplaySentAt"),
        )
    )
