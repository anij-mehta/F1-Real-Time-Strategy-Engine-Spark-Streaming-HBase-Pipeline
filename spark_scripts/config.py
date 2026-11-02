"""Central streaming configuration; documentation should reference these constants."""

import os

APP_NAME = "F1-Strategy-Gold"
SPARK_MASTER = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
SHUFFLE_PARTITIONS = int(os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", "4"))
STREAM_HOST = os.getenv("F1_STREAM_HOST", "streamer")
STREAM_PORT = int(os.getenv("F1_STREAM_PORT", "9999"))

# Silver event-time aggregation: a five-second window that advances every second.
WINDOW_DURATION = "5 seconds"
WINDOW_SLIDE = "1 second"
WATERMARK_DELAY = "10 seconds"

HDFS_ROOT = os.getenv("F1_HDFS_ROOT", "hdfs://namenode:8020/f1")
BRONZE_PARQUET_PATH = f"{HDFS_ROOT}/bronze"
SILVER_PARQUET_PATH = f"{HDFS_ROOT}/silver"
CHECKPOINT_ROOT = f"{HDFS_ROOT}/checkpoints"
BRONZE_CHECKPOINT_PATH = f"{CHECKPOINT_ROOT}/bronze"
SILVER_CHECKPOINT_PATH = f"{CHECKPOINT_ROOT}/silver"

MIN_LAP_TIME_SECONDS = 40.0
MAX_LAP_TIME_SECONDS = 180.0
SC_VSC_TRACK_STATUS_CODES = ("4", "6", "7")
SC_VSC_TRACK_STATUS_PATTERN = f"[{''.join(SC_VSC_TRACK_STATUS_CODES)}]"
PIT_DECAY_THRESHOLD_SECONDS = 0.2
PIT_PROBABILITY_THRESHOLD = 0.5
PIT_MODEL_PATH = f"{HDFS_ROOT}/models/pit_classifier"

# The model consumes these Silver-compatible columns in both training and Gold inference.
PIT_NUMERIC_FEATURE_COLUMNS = (
    "AvgTyreLife",
    "LapNumber",
    "Stint",
    "AvgLapTimeDelta",
    "AvgPosition",
    "AvgGap",
)
PIT_CATEGORICAL_FEATURE_COLUMNS = ("Compound", "TrackStatus")

# Gold safeguards and explainable tyre-cliff detection.
MIN_REGRESSION_POINTS = 3
CLIFF_ROLLING_LAP_COUNT = 3
CLIFF_BASELINE_DELTA_COUNT = 3
CLIFF_BASELINE_MULTIPLIER = 2.0
CLIFF_MIN_BASELINE_SECONDS = 0.01
