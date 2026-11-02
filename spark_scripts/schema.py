"""Explicit schema for the JSONL telemetry payload emitted by f1_streamer.py."""

from pyspark.sql.types import BooleanType, DoubleType, IntegerType, StringType, StructField, StructType, TimestampType


TELEMETRY_SCHEMA = StructType(
    [
        StructField("Driver", StringType(), True),
        StructField("LapNumber", IntegerType(), True),
        StructField("LapTime", DoubleType(), True),
        StructField("LapTimeDelta", DoubleType(), True),
        StructField("GapToAhead", DoubleType(), True),
        StructField("Compound", StringType(), True),
        StructField("TyreLife", DoubleType(), True),
        StructField("FreshTyre", StringType(), True),
        StructField("Position", IntegerType(), True),
        StructField("Stint", IntegerType(), True),
        StructField("TrackStatus", StringType(), True),
        StructField("FuelCorrectedLapTime", DoubleType(), True),
        StructField("LapCompletionTime", TimestampType(), True),
        StructField("ReplaySentAt", TimestampType(), True),
        StructField("IsPitStop", BooleanType(), True),
        StructField("IsInLap", BooleanType(), True),
        StructField("IsOutLap", BooleanType(), True),
    ]
)
