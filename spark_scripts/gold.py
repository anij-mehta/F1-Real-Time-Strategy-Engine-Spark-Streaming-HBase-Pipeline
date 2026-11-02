"""Per-driver tyre-decay regression, stint degradation, and cliff detection."""

from __future__ import annotations

import logging
import time
from collections import defaultdict
from collections.abc import Iterable
from typing import Any

from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array
from pyspark.sql.functions import col, lit

from config import (
    CLIFF_BASELINE_DELTA_COUNT,
    CLIFF_BASELINE_MULTIPLIER,
    CLIFF_MIN_BASELINE_SECONDS,
    CLIFF_ROLLING_LAP_COUNT,
    MIN_REGRESSION_POINTS,
    PIT_MODEL_PATH,
    PIT_PROBABILITY_THRESHOLD,
    SILVER_PARQUET_PATH,
)
from hbase_sink import write_predictions
from strategy_rules import pit_decision


LOGGER = logging.getLogger(__name__)
PIT_MODEL: PipelineModel | None = None


def attach_pit_probabilities(batch_df):
    """Attach classifier probability when a trained PipelineModel is available on HDFS."""
    global PIT_MODEL
    if PIT_MODEL is None:
        try:
            PIT_MODEL = PipelineModel.load(PIT_MODEL_PATH)
            LOGGER.info("Loaded pit classifier from %s", PIT_MODEL_PATH)
        except Exception as exc:
            LOGGER.info("Pit classifier is not available at %s yet: %s", PIT_MODEL_PATH, exc)
    if PIT_MODEL is None:
        return batch_df.withColumn("pit_probability", lit(None).cast("double"))
    try:
        return PIT_MODEL.transform(batch_df).withColumn("pit_probability", vector_to_array(col("probability"))[1])
    except Exception as exc:
        LOGGER.exception("Pit classifier inference failed: %s", exc)
        return batch_df.withColumn("pit_probability", lit(None).cast("double"))


def load_silver_history(batch_df):
    """Return archived Silver laps so short live micro-batches still have context.

    The Silver archive is written before Gold runs for a batch.  Reading it here
    provides the previous clean laps needed for a per-driver regression and
    cliff calculation without writing duplicate decisions for old windows.
    Re-runs append the same windows again, so duplicates are dropped.
    """
    try:
        return batch_df.sparkSession.read.parquet(SILVER_PARQUET_PATH).dropDuplicates(
            ["Driver", "Stint", "LapNumber", "window"]
        )
    except Exception as exc:
        LOGGER.warning("Silver history is unavailable at %s: %s", SILVER_PARQUET_PATH, exc)
        return batch_df


def fit_slope(rows: Iterable[Any], label_column: str) -> float | None:
    """Least-squares slope of label ~ AvgTyreLife, or None for weak samples.

    Equivalent to an unregularised MLlib LinearRegression on one feature, but
    solved on the driver so a batch does not launch a Spark job per fit.
    """
    points = [
        (float(row["AvgTyreLife"]), float(row[label_column]))
        for row in rows
        if row["AvgTyreLife"] is not None and row[label_column] is not None
    ]
    if len(points) < MIN_REGRESSION_POINTS:
        return None
    mean_x = sum(x for x, _ in points) / len(points)
    mean_y = sum(y for _, y in points) / len(points)
    variance = sum((x - mean_x) ** 2 for x, _ in points)
    if variance == 0:
        return None
    return sum((x - mean_x) * (y - mean_y) for x, y in points) / variance


def distinct_laps(rows: Iterable[Any]) -> list[Any]:
    """Keep the latest overlapping-window observation for each lap in a stint."""
    by_lap: dict[int, Any] = {}
    for row in rows:
        lap_number = row["LapNumber"]
        if lap_number is None or row["AvgFuelCorrectedLapTime"] is None:
            continue
        existing = by_lap.get(lap_number)
        if existing is None or row["window"].end > existing["window"].end:
            by_lap[lap_number] = row
    return [by_lap[lap] for lap in sorted(by_lap)]


def detect_cliff(rows: Iterable[Any]) -> tuple[bool, float | None, float | None]:
    """Compare the latest three lap-to-lap losses with early-stint baseline loss."""
    laps = distinct_laps(rows)
    deltas = [
        laps[index]["AvgFuelCorrectedLapTime"] - laps[index - 1]["AvgFuelCorrectedLapTime"]
        for index in range(1, len(laps))
    ]
    if len(deltas) < CLIFF_BASELINE_DELTA_COUNT + CLIFF_ROLLING_LAP_COUNT:
        return False, None, None
    baseline = sum(abs(delta) for delta in deltas[:CLIFF_BASELINE_DELTA_COUNT]) / CLIFF_BASELINE_DELTA_COUNT
    latest_rolling_delta = sum(deltas[-CLIFF_ROLLING_LAP_COUNT:]) / CLIFF_ROLLING_LAP_COUNT
    threshold = max(CLIFF_MIN_BASELINE_SECONDS, baseline * CLIFF_BASELINE_MULTIPLIER)
    return latest_rolling_delta > threshold, latest_rolling_delta, baseline


def format_slope(value: float | None) -> str:
    return "unavailable" if value is None else f"{value:.2f}s/lap"


def process_gold_layer(batch_df, batch_id: int) -> None:
    """Fit per-driver/per-stint regressions and persist explainable PIT/STAY decisions."""
    started_at = time.monotonic()
    if batch_df.count() == 0:
        LOGGER.info("Gold batch=%s rows=0 latency_ms=%.1f", batch_id, (time.monotonic() - started_at) * 1000)
        return

    predictions_by_window: dict[tuple[str, Any], dict] = {}
    try:
        current_rows = attach_pit_probabilities(batch_df).collect()
        history_rows = load_silver_history(batch_df).collect()
        history_by_driver: dict[str, list[Any]] = defaultdict(list)
        for row in history_rows:
            history_by_driver[row["Driver"]].append(row)
        current_by_driver_stint: dict[tuple[str, Any], list[Any]] = defaultdict(list)
        for row in current_rows:
            current_by_driver_stint[(row["Driver"], row["Stint"])].append(row)

        for (driver, stint), current_stint_rows in current_by_driver_stint.items():
            driver_rows = history_by_driver.get(driver, [])
            rows = [row for row in driver_rows if row["Stint"] == stint]
            if not rows:
                continue
            # Required regression: AvgGap versus AvgTyreLife, per driver and stint
            # (tyre life resets at each stop, so stints must not be mixed).
            gap_decay = fit_slope(rows, "AvgGap")
            # Stronger degradation signal: per-driver, per-stint fuel-corrected lap-time slope.
            fuel_stint_decay = fit_slope(rows, "AvgFuelCorrectedLapTime")
            cliff_detected, rolling_delta, baseline_delta = detect_cliff(rows)
            available_decays = [value for value in (gap_decay, fuel_stint_decay) if value is not None]
            decision_decay = max([0.0, *available_decays])
            # Fit from all archived laps, but upsert only this batch's latest
            # window so historical HBase rows are never rewritten as new data.
            latest_row = max(
                current_stint_rows,
                key=lambda row: (row["window"].end, row["LapNumber"] or 0),
            )
            probability_value = latest_row["pit_probability"]
            pit_probability = None if probability_value is None else float(probability_value)
            model_signal = pit_probability is not None and pit_probability >= PIT_PROBABILITY_THRESHOLD
            decision = pit_decision(decision_decay, cliff_detected, pit_probability)
            reason = (
                f"decay={format_slope(decision_decay)}, "
                f"cliff={str(cliff_detected).lower()}, "
                f"pit_probability={'unavailable' if pit_probability is None else f'{pit_probability:.3f}'}, "
                f"pit_model_signal={str(model_signal).lower()}, "
                f"gap_decay={format_slope(gap_decay)}, "
                f"stint_decay={format_slope(fuel_stint_decay)}, "
                f"rolling_delta={format_slope(rolling_delta)}, "
                f"baseline={format_slope(baseline_delta)}"
            )
            prediction = {
                "row": latest_row,
                "decay": decision_decay,
                "decision": decision,
                "reason": reason,
                "pit_probability": pit_probability,
            }
            window_key = (driver, latest_row["window"].start)
            previous = predictions_by_window.get(window_key)
            rank = (
                decision == "PIT",
                decision_decay,
                -1.0 if pit_probability is None else pit_probability,
            )
            if previous is None or rank > previous["rank"]:
                prediction["rank"] = rank
                predictions_by_window[window_key] = prediction
            LOGGER.info("Gold batch=%s driver=%s stint=%s %s", batch_id, driver, stint, reason)

        predictions = list(predictions_by_window.values())
        if predictions:
            write_predictions(predictions, batch_df.sparkSession.sparkContext)
        LOGGER.info(
            "Gold batch=%s rows=%s predictions=%s latency_ms=%.1f",
            batch_id,
            len(current_rows),
            len(predictions),
            (time.monotonic() - started_at) * 1000,
        )
    except Exception:
        LOGGER.exception("Gold batch=%s failed after %.1fms", batch_id, (time.monotonic() - started_at) * 1000)
        raise
