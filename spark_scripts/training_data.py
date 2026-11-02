"""Build labelled pit-window training laps from FastF1 and export them as JSON lines.

Runs in the Python 3.11 image (FastF1 3.x needs it); Spark trains from the file.
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import os
from pathlib import Path
from typing import Any

import fastf1
import pandas as pd


LOGGER = logging.getLogger(__name__)
DEFAULT_YEARS = (2023, 2024)
DEFAULT_GPS = ("Bahrain", "Saudi", "Australia")
LABEL_HORIZON_LAPS = 2


def is_missing(value: Any) -> bool:
    try:
        return value is None or bool(pd.isna(value))
    except (TypeError, ValueError):
        return False


def as_float(value: Any, default: float = 0.0) -> float:
    if is_missing(value):
        return default
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    return parsed if math.isfinite(parsed) else default


def timedelta_seconds(value: Any) -> float | None:
    if is_missing(value):
        return None
    try:
        return as_float(value.total_seconds(), default=float("nan"))
    except AttributeError:
        return None


def calculate_gaps(laps: pd.DataFrame) -> dict[Any, float]:
    """Calculate timing-line gaps to Position-1 for each lap number."""
    gaps: dict[Any, float] = {}
    for _, group in laps.groupby("LapNumber", dropna=True, sort=False):
        ordered = group.sort_values("_completion_seconds", kind="stable")
        completion_by_position: dict[int, float] = {}
        for _, row in ordered.iterrows():
            position = int(as_float(row.get("Position"), 0.0))
            if position > 0:
                completion_by_position.setdefault(position, row["_completion_seconds"])
        for index, row in ordered.iterrows():
            position = int(as_float(row.get("Position"), 0.0))
            if position == 1:
                gaps[index] = 0.0
            elif position > 1 and position - 1 in completion_by_position:
                gaps[index] = max(0.0, row["_completion_seconds"] - completion_by_position[position - 1])
            else:
                gaps[index] = 0.0
    return gaps


def load_race_records(year: int, gp: str) -> list[dict[str, Any]]:
    """Load one race and label each lap by whether a pit event follows within two laps."""
    race_id = f"{year}_{gp}"
    LOGGER.info("Loading %s", race_id)
    session = fastf1.get_session(year, gp, "R")
    session.load(telemetry=False, weather=False)
    laps = session.laps.copy()
    laps["_completion_seconds"] = laps["Time"].map(timedelta_seconds)
    laps["_lap_time_seconds"] = laps["LapTime"].map(timedelta_seconds)
    laps = laps.dropna(subset=["Driver", "LapNumber", "_completion_seconds"]).copy()
    laps.sort_values(["Driver", "LapNumber", "_completion_seconds"], kind="stable", inplace=True)
    gaps = calculate_gaps(laps)
    records: list[dict[str, Any]] = []

    for driver, driver_laps in laps.groupby("Driver", sort=False):
        driver_laps = driver_laps.reset_index().rename(columns={"index": "_source_index"})
        pitted = [
            not is_missing(row.get("PitInTime")) or not is_missing(row.get("PitOutTime"))
            for _, row in driver_laps.iterrows()
        ]
        lap_numbers = [int(as_float(row["LapNumber"])) for _, row in driver_laps.iterrows()]
        pitted_lap_numbers = {
            lap_numbers[offset] for offset, did_pit in enumerate(pitted) if did_pit
        }
        latest_lap_number = max(lap_numbers)
        for offset, (_, row) in enumerate(driver_laps.iterrows()):
            lap_number = lap_numbers[offset]
            if lap_number + LABEL_HORIZON_LAPS > latest_lap_number:
                continue
            previous_lap_time = as_float(driver_laps.iloc[offset - 1]["_lap_time_seconds"], 0.0) if offset else 0.0
            current_lap_time = as_float(row["_lap_time_seconds"], 0.0)
            records.append(
                {
                    "Race": race_id,
                    "Driver": str(driver),
                    "Compound": "UNKNOWN" if is_missing(row.get("Compound")) else str(row.get("Compound")),
                    "TrackStatus": "UNKNOWN" if is_missing(row.get("TrackStatus")) else str(row.get("TrackStatus")),
                    "AvgTyreLife": as_float(row.get("TyreLife")),
                    "LapNumber": lap_number,
                    "Stint": int(as_float(row.get("Stint"))),
                    "AvgLapTimeDelta": current_lap_time - previous_lap_time,
                    "AvgPosition": as_float(row.get("Position")),
                    "AvgGap": gaps.get(row["_source_index"], 0.0),
                    "label": float(
                        any(
                            lap_number + horizon in pitted_lap_numbers
                            for horizon in range(1, LABEL_HORIZON_LAPS + 1)
                        )
                    ),
                }
            )
    LOGGER.info("Loaded %s labelled lap records from %s", len(records), race_id)
    return records


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--years", type=int, nargs="+", default=DEFAULT_YEARS)
    parser.add_argument("--gps", nargs="+", default=DEFAULT_GPS)
    parser.add_argument("--output", type=Path, default=Path("/opt/f1/docs/training_data.jsonl"))
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    cache_directory = Path(os.getenv("FASTF1_CACHE_DIR", "/tmp/.fastf1-cache"))
    cache_directory.mkdir(parents=True, exist_ok=True)
    fastf1.Cache.enable_cache(str(cache_directory))
    records: list[dict[str, Any]] = []
    for year in args.years:
        for gp in args.gps:
            try:
                records.extend(load_race_records(year, gp))
            except Exception:
                LOGGER.exception("Skipping unavailable race %s_%s", year, gp)
    if not records:
        raise RuntimeError("No labelled training records were loaded")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8", newline="\n") as output:
        for record in records:
            output.write(json.dumps(record) + "\n")
    LOGGER.info("Wrote %s records to %s", len(records), args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
