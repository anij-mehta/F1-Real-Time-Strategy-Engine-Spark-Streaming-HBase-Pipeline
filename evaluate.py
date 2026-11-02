"""Replay a FastF1 race, score HBase pit triggers, and benchmark the stream."""

import argparse
from datetime import datetime, timezone
import json
import logging
import math
import os
from pathlib import Path
import re
import statistics
import subprocess
import sys
import time
from typing import Any

import fastf1

from f1_streamer import prepare_laps


LOGGER = logging.getLogger(__name__)
ROOT = Path(__file__).resolve().parent
DOCS = ROOT / "docs"
RESULTS_PATH = DOCS / "evaluation_runs.json"
THRESHOLD_VALUES = tuple(round(0.1 + 0.05 * index, 2) for index in range(7))
CLASSIFIER_THRESHOLD = 0.5
HISTORY_LIMIT = 5000
CLIFF_PATTERN = re.compile(r"cliff=true", re.IGNORECASE)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--year", type=int, default=2024)
    parser.add_argument("--gp", default="Bahrain")
    parser.add_argument("--speed", type=int, choices=(1, 5, 20), default=1)
    parser.add_argument("--threshold", type=float, default=0.2, help="default regression margin in seconds")
    parser.add_argument("--max-lead-laps", type=int, default=5, help="maximum lead for a trigger to match a stop")
    parser.add_argument("--settle-seconds", type=float, default=10.0)
    parser.add_argument("--timeout-seconds", type=float, default=180.0)
    parser.add_argument("--report", type=Path, default=DOCS / "evaluation.md")
    parser.add_argument("--skip-replay", action="store_true", help="score records already present in HBase")
    parser.add_argument(
        "--replay-command",
        nargs="+",
        default=["docker", "compose", "--profile", "tools", "run", "--rm", "--build", "--no-deps", "streamer"],
        help="command used to launch the one-shot FastF1 stream replay",
    )
    return parser.parse_args()


def parse_datetime(value: Any) -> datetime | None:
    if not value or value == "unavailable":
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed.astimezone(timezone.utc)


def as_float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def load_race(
    year: int, gp: str
) -> tuple[list[tuple[float, dict[str, Any]]], dict[str, set[int]]]:
    cache = ROOT / ".fastf1-cache"
    cache.mkdir(exist_ok=True)
    fastf1.Cache.enable_cache(str(cache))
    session = fastf1.get_session(year, gp, "R")
    session.load(telemetry=False, weather=False)
    replay_laps = prepare_laps(session)
    if not replay_laps:
        raise RuntimeError(f"FastF1 returned no replayable laps for {year} {gp}")
    actual_stops: dict[str, set[int]] = {}
    for _, row in session.laps.iterrows():
        if row.get("PitInTime") is None or str(row.get("PitInTime")) == "NaT":
            continue
        driver = row.get("Driver")
        lap = as_float(row.get("LapNumber"))
        if driver is not None and lap is not None:
            actual_stops.setdefault(str(driver), set()).add(int(lap))
    if not actual_stops:
        raise RuntimeError(f"No actual pit-in laps found in {year} {gp}; refusing to report misleading metrics")
    return replay_laps, actual_stops


def replay_race(args: argparse.Namespace, record_count: int) -> float:
    command = [
        *args.replay_command,
        "--year",
        str(args.year),
        "--gp",
        args.gp,
        "--speed",
        str(args.speed),
        "--once",
        "--client-timeout",
        str(args.timeout_seconds),
    ]
    LOGGER.info("Starting replay with speed=%sx", args.speed)
    print(
        "Starting the one-shot replay server. Start `make submit` in another terminal now "
        "so Spark can connect.",
        flush=True,
    )
    started = time.monotonic()
    completed = subprocess.run(
        command,
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    elapsed = time.monotonic() - started
    if completed.stdout:
        print(completed.stdout, end="")
    if completed.stderr:
        print(completed.stderr, end="", file=sys.stderr)
    if completed.returncode:
        raise RuntimeError(
            f"Replay command failed with exit code {completed.returncode}. "
            "Start the Spark streaming consumer first; see docs/evaluation.md."
        )
    summary = re.search(
        r"Replay throughput records=(\d+) elapsed_seconds=([0-9.]+) records_per_second=[0-9.]+",
        completed.stdout + completed.stderr,
    )
    if summary is None:
        LOGGER.warning("Streamer throughput summary was absent; using full replay command duration")
        return elapsed
    if int(summary.group(1)) != record_count:
        raise RuntimeError(
            f"Streamer replayed {summary.group(1)} records; FastF1 prepared {record_count}"
        )
    return float(summary.group(2))


def read_race_records(driver_names: set[str], sent_after: datetime, sent_before: datetime, sink: Any) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for driver in sorted(driver_names):
        for record in sink.get_history(driver, HISTORY_LIMIT):
            sent_at = parse_datetime(record.get("replay_sent_at"))
            if sent_at is not None and sent_after <= sent_at <= sent_before:
                record["_sent_at"] = sent_at
                record["_processed_at"] = parse_datetime(record.get("processed_at"))
                records.append(record)
    return records


def collect_records(
    drivers: set[str],
    sent_after: datetime,
    args: argparse.Namespace,
    sink: Any,
) -> list[dict[str, Any]]:
    deadline = time.monotonic() + args.timeout_seconds
    quiet_since: float | None = None
    previous_signature: tuple[int, str | None] | None = None
    latest: list[dict[str, Any]] = []
    while time.monotonic() < deadline:
        latest = read_race_records(drivers, sent_after, datetime.now(timezone.utc), sink)
        latest_processed = max(
            (record["_processed_at"] for record in latest if record.get("_processed_at") is not None),
            default=None,
        )
        signature = (len(latest), None if latest_processed is None else latest_processed.isoformat())
        if latest and signature == previous_signature:
            if quiet_since is None:
                quiet_since = time.monotonic()
            elif time.monotonic() - quiet_since >= args.settle_seconds:
                return latest
        else:
            quiet_since = None
            previous_signature = signature
        time.sleep(1.0)
    if latest:
        LOGGER.warning("HBase collection timed out after receiving %s race records", len(latest))
        return latest
    raise TimeoutError(
        "No replay-tagged Gold rows reached HBase. Verify that Spark is connected to the one-shot streamer "
        "and that HBase is reachable."
    )


def one_record_per_lap(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    latest: dict[tuple[str, int], dict[str, Any]] = {}
    for record in records:
        lap = as_float(record.get("lap"))
        if lap is None:
            continue
        key = (record["driver"], int(lap))
        previous = latest.get(key)
        if previous is None or (
            record.get("_processed_at") or datetime.min.replace(tzinfo=timezone.utc)
        ) > (
            previous.get("_processed_at") or datetime.min.replace(tzinfo=timezone.utc)
        ):
            latest[key] = record
    return sorted(latest.values(), key=lambda record: (record["driver"], int(float(record["lap"]))))


def rule_flags(record: dict[str, Any], threshold: float) -> dict[str, bool]:
    decay = as_float(record.get("decay"))
    probability = as_float(record.get("pit_prob", record.get("pit_probability")))
    is_cliff = bool(CLIFF_PATTERN.search(record.get("reason", "")))
    regression = (decay is not None and decay > threshold) or is_cliff
    classifier = probability is not None and probability >= CLASSIFIER_THRESHOLD
    return {"Regression": regression, "Classifier": classifier, "Combined": regression or classifier}


def pit_episodes(records: list[dict[str, Any]], rule: str, threshold: float) -> dict[str, list[int]]:
    by_driver: dict[str, list[int]] = {}
    for driver in sorted({record["driver"] for record in records}):
        driver_records = [record for record in records if record["driver"] == driver]
        flagged_laps = sorted(
            {
                int(float(record["lap"]))
                for record in driver_records
                if rule_flags(record, threshold)[rule]
            }
        )
        episodes: list[int] = []
        for lap in flagged_laps:
            if not episodes or lap > episodes[-1] + 1:
                episodes.append(lap)
        by_driver[driver] = episodes
    return by_driver


def score_rule(
    records: list[dict[str, Any]],
    actual_stops: dict[str, set[int]],
    rule: str,
    threshold: float,
    max_lead_laps: int,
) -> dict[str, float | int | None]:
    triggers = pit_episodes(records, rule, threshold)
    trigger_count = sum(len(laps) for laps in triggers.values())
    stop_count = sum(len(laps) for laps in actual_stops.values())
    matched = 0
    lead_times: list[int] = []
    for driver, driver_triggers in triggers.items():
        available_stops = set(actual_stops.get(driver, set()))
        for trigger_lap in driver_triggers:
            candidates = [
                pit_lap
                for pit_lap in available_stops
                if 0 <= pit_lap - trigger_lap <= max_lead_laps
            ]
            if not candidates:
                continue
            pit_lap = min(candidates)
            available_stops.remove(pit_lap)
            matched += 1
            lead_times.append(pit_lap - trigger_lap)
    return {
        "precision": matched / trigger_count if trigger_count else 0.0,
        "recall": matched / stop_count if stop_count else 0.0,
        "mean_lead_laps": statistics.fmean(lead_times) if lead_times else None,
        "matched": matched,
        "triggers": trigger_count,
        "actual_stops": stop_count,
    }


def percentile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    position = (len(ordered) - 1) * fraction
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    return ordered[lower] * (upper - position) + ordered[upper] * (position - lower)


def measure_latency(records: list[dict[str, Any]]) -> dict[str, float | int | None]:
    latencies = [
        (record["_processed_at"] - record["_sent_at"]).total_seconds() * 1000
        for record in records
        if record.get("_processed_at") is not None and record["_processed_at"] >= record["_sent_at"]
    ]
    return {
        "samples": len(latencies),
        "p50_ms": percentile(latencies, 0.50),
        "p95_ms": percentile(latencies, 0.95),
    }


def svg_plot(
    path: Path,
    title: str,
    x_values: list[float],
    series: list[tuple[str, list[float | None], str]],
    x_label: str,
    y_label: str,
    y_max: float,
    y_suffix: str = "",
) -> None:
    width, height = 900, 470
    left, right, top, bottom = 85, 35, 70, 85
    chart_width, chart_height = width - left - right, height - top - bottom
    x_min = min(x_values, default=0.0)
    x_max = max(x_values, default=1.0)
    if x_min == x_max:
        x_max = x_min + 1
    y_max = max(y_max, 1e-6)
    elements = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="#0b0f14"/>',
        f'<text x="{width / 2}" y="34" text-anchor="middle" fill="#e6edf3" font-size="21" font-family="Arial">{title}</text>',
    ]
    for tick in range(6):
        value = y_max * tick / 5
        y = top + chart_height * (1 - tick / 5)
        elements.append(f'<line x1="{left}" y1="{y:.1f}" x2="{width-right}" y2="{y:.1f}" stroke="#263241"/>')
        elements.append(f'<text x="{left-12}" y="{y+5:.1f}" text-anchor="end" fill="#9eacba" font-size="12" font-family="Arial">{value:.2f}{y_suffix}</text>')
    elements.extend(
        [
            f'<line x1="{left}" y1="{top}" x2="{left}" y2="{top+chart_height}" stroke="#718096"/>',
            f'<line x1="{left}" y1="{top+chart_height}" x2="{width-right}" y2="{top+chart_height}" stroke="#718096"/>',
            f'<text x="{left+chart_width/2}" y="{height-22}" text-anchor="middle" fill="#cbd5e1" font-size="14" font-family="Arial">{x_label}</text>',
            f'<text transform="translate(20 {top+chart_height/2}) rotate(-90)" text-anchor="middle" fill="#cbd5e1" font-size="14" font-family="Arial">{y_label}</text>',
        ]
    )
    for index, (name, values, color) in enumerate(series):
        points = []
        for x_value, y_value in zip(x_values, values):
            if y_value is None:
                continue
            x = left + (x_value - x_min) / (x_max - x_min) * chart_width
            y = top + (1 - max(0.0, min(y_max, y_value)) / y_max) * chart_height
            points.append((x, y))
        if points:
            point_string = " ".join(f"{x:.1f},{y:.1f}" for x, y in points)
            elements.append(f'<polyline points="{point_string}" fill="none" stroke="{color}" stroke-width="3"/>')
            elements.extend(
                f'<circle cx="{x:.1f}" cy="{y:.1f}" r="4" fill="{color}"/>' for x, y in points
            )
        legend_x = left + (index % 3) * 255
        legend_y = 53 + (index // 3) * 18
        elements.append(f'<line x1="{legend_x}" y1="{legend_y}" x2="{legend_x+24}" y2="{legend_y}" stroke="{color}" stroke-width="3"/>')
        elements.append(f'<text x="{legend_x+31}" y="{legend_y+5}" fill="#cbd5e1" font-size="12" font-family="Arial">{name}</text>')
    elements.append("</svg>")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(elements), encoding="utf-8")


def save_threshold_plot(thresholds: list[dict[str, Any]]) -> None:
    colors = {"Regression": "#ff5263", "Classifier": "#43b9ff", "Combined": "#3ac98a"}
    series = [
        (f"{rule} precision", [entry[rule]["precision"] for entry in thresholds], colors[rule])
        for rule in colors
    ]
    series.extend(
        (f"{rule} recall", [entry[rule]["recall"] for entry in thresholds], color)
        for rule, color in colors.items()
    )
    svg_plot(
        DOCS / "evaluation_threshold_sweep.svg",
        "Pit trigger threshold sweep · precision and recall",
        [entry["threshold"] for entry in thresholds],
        series,
        "Regression margin (seconds)",
        "Score",
        1.0,
    )


def save_throughput_plot(runs: list[dict[str, Any]]) -> None:
    ordered = sorted(runs, key=lambda run: run["speed"])
    speeds = [float(run["speed"]) for run in ordered]
    series = [("Input records/sec", [run["records_per_second"] for run in ordered], "#43b9ff")]
    maximum = max([run["records_per_second"] for run in ordered] or [1.0])
    svg_plot(
        DOCS / "evaluation_throughput.svg",
        "Telemetry replay throughput",
        speeds,
        series,
        "Replay speed multiplier",
        "Input records/sec",
        maximum * 1.1,
    )
    latency_series = [
        ("p50 latency (ms)", [run["p50_ms"] for run in ordered], "#3ac98a"),
        ("p95 latency (ms)", [run["p95_ms"] for run in ordered], "#ff5263"),
    ]
    latency_maximum = max(
        [value for _, values, _ in latency_series for value in values if value is not None] or [1.0]
    )
    svg_plot(
        DOCS / "evaluation_latency.svg",
        "HBase end-to-end latency",
        speeds,
        latency_series,
        "Replay speed multiplier",
        "Latency (ms)",
        latency_maximum * 1.1,
    )


def format_metric(value: Any, digits: int = 3) -> str:
    return "n/a" if value is None else f"{float(value):.{digits}f}"


def write_report(
    path: Path,
    race_id: str,
    current_run: dict[str, Any],
    runs: list[dict[str, Any]],
    max_lead_laps: int,
) -> None:
    metric_rows = "\n".join(
        f"| {rule} | {result['precision']:.3f} | {result['recall']:.3f} | "
        f"{format_metric(result['mean_lead_laps'], 2)} | {result['matched']} / {result['triggers']} |"
        for rule, result in current_run["metrics"].items()
    )
    throughput_rows = "\n".join(
        f"| {run['speed']}x | {run['input_records']} | {run['records_per_second']:.2f} | "
        f"{format_metric(run['p50_ms'], 1)} | {format_metric(run['p95_ms'], 1)} | {run['latency_samples']} |"
        for run in sorted(runs, key=lambda item: item["speed"])
    )
    thresholds = current_run["threshold_sweep"]
    threshold_rows = "\n".join(
        f"| {entry['threshold']:.2f} | "
        + " | ".join(
            f"{entry[rule]['precision']:.3f} / {entry[rule]['recall']:.3f} / "
            f"{format_metric(entry[rule]['mean_lead_laps'], 2)}"
            for rule in ("Regression", "Classifier", "Combined")
        )
        + " |"
        for entry in thresholds
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "# Race Pit Strategy Evaluation\n\n"
        f"- Race: `{race_id}`\n"
        f"- Current replay speed: `{current_run['speed']}x`\n"
        f"- Trigger matching: one-to-one actual stop match within 0–{max_lead_laps} laps after trigger; "
        "adjacent PIT laps are collapsed into one trigger episode.\n"
        f"- Classifier cutoff: `{CLASSIFIER_THRESHOLD:.2f}`; regression rule is decay above the margin or "
        "the Gold cliff flag; combined rule is either signal.\n"
        "- Mean lead time is averaged over matched triggers only. No matched trigger is reported as `n/a`.\n\n"
        "## Strategy metrics\n\n"
        "| Rule | Precision | Recall | Mean lead (laps) | Matched / triggers |\n"
        "| --- | ---: | ---: | ---: | ---: |\n"
        f"{metric_rows}\n\n"
        "## Throughput benchmark\n\n"
        "Records/sec measures all replayed FastF1 telemetry input records. End-to-end latency is measured "
        "from the source telemetry `ReplaySentAt` carried by a scored Gold record to its HBase write timestamp.\n\n"
        "| Replay speed | Input records | Records/sec | p50 latency (ms) | p95 latency (ms) | Samples |\n"
        "| ---: | ---: | ---: | ---: | ---: | ---: |\n"
        f"{throughput_rows or '| n/a | n/a | n/a | n/a | n/a | 0 |'}\n\n"
        "## Regression threshold sweep\n\n"
        "| Margin (s) | Regression P / R / lead | Classifier P / R / lead | Combined P / R / lead |\n"
        "| ---: | ---: | ---: | ---: |\n"
        f"{threshold_rows}\n\n"
        "![Threshold sweep](evaluation_threshold_sweep.svg)\n\n"
        "![Throughput benchmark](evaluation_throughput.svg)\n\n"
        "![End-to-end latency](evaluation_latency.svg)\n\n"
        "Run `python evaluate.py --speed 1`, `--speed 5`, and `--speed 20` to build the throughput table. "
        "Each speed requires a fresh Spark streaming query and a completed HBase initialization. "
        "The report is updated with each run; results are stored in `evaluation_runs.json`.\n",
        encoding="utf-8",
        newline="\n",
    )


def main() -> int:
    args = parse_args()
    if args.threshold <= 0 or args.max_lead_laps < 0 or args.settle_seconds < 0 or args.timeout_seconds <= 0:
        raise ValueError(
            "threshold and timeout must be positive; lead laps and settle seconds must be non-negative"
        )
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    os.environ.setdefault("HBASE_HOST", "localhost")

    sys_path = str(ROOT / "spark_scripts")
    if sys_path not in sys.path:
        sys.path.insert(0, sys_path)
    import hbase_sink

    replay_laps, actual_stops = load_race(args.year, args.gp)
    race_id = f"{args.year}_{args.gp}"
    drivers = {str(payload["Driver"]) for _, payload in replay_laps}
    current_started = datetime.now(timezone.utc)
    if args.skip_replay:
        replay_elapsed = 0.0
        sent_after = datetime.min.replace(tzinfo=timezone.utc)
    else:
        replay_elapsed = replay_race(args, len(replay_laps))
        sent_after = current_started
    records = collect_records(drivers, sent_after, args, hbase_sink)
    records = one_record_per_lap(records)
    if not records:
        raise RuntimeError("No usable HBase Gold records were found for the selected race replay")

    metrics = {
        rule: score_rule(records, actual_stops, rule, args.threshold, args.max_lead_laps)
        for rule in ("Regression", "Classifier", "Combined")
    }
    sweep = []
    for threshold in THRESHOLD_VALUES:
        sweep.append(
            {
                "threshold": threshold,
                **{
                    rule: score_rule(records, actual_stops, rule, threshold, args.max_lead_laps)
                    for rule in ("Regression", "Classifier", "Combined")
                },
            }
        )
    latencies = measure_latency(records)
    input_records = len(replay_laps)
    current_run = {
        "race": race_id,
        "speed": args.speed,
        "input_records": input_records,
        "replay_elapsed_seconds": replay_elapsed,
        "records_per_second": input_records / replay_elapsed if replay_elapsed else 0.0,
        "latency_samples": latencies["samples"],
        "p50_ms": latencies["p50_ms"],
        "p95_ms": latencies["p95_ms"],
        "metrics": metrics,
        "threshold_sweep": sweep,
    }

    previous_runs: dict[str, Any] = {}
    if RESULTS_PATH.exists():
        previous_runs = json.loads(RESULTS_PATH.read_text(encoding="utf-8"))
    race_runs = previous_runs.get(race_id, {})
    race_runs[str(args.speed)] = current_run
    previous_runs[race_id] = race_runs
    DOCS.mkdir(parents=True, exist_ok=True)
    RESULTS_PATH.write_text(json.dumps(previous_runs, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    runs = list(race_runs.values())
    save_threshold_plot(sweep)
    save_throughput_plot(runs)
    write_report(args.report, race_id, current_run, runs, args.max_lead_laps)
    LOGGER.info("Wrote evaluation results for %s speed=%sx to %s", race_id, args.speed, args.report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
