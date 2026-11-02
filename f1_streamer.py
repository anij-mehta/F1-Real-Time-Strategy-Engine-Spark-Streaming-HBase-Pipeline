"""Replay FastF1 lap data as newline-delimited JSON over a TCP socket."""

import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import json
import logging
import math
import os
import socket
import time
from pathlib import Path
from typing import Any, Iterable

import fastf1
import pandas as pd


LOGGER = logging.getLogger(__name__)
FUEL_BURN_SECONDS_PER_LAP = 0.03
DEFAULT_DRY_RUN_PATH = Path("f1_stream_dry_run.jsonl")


def positive_float(value: str) -> float:
    parsed = float(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be greater than zero")
    return parsed


def valid_port(value: str) -> int:
    parsed = int(value)
    if not 1 <= parsed <= 65535:
        raise argparse.ArgumentTypeError("must be between 1 and 65535")
    return parsed


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--year", type=int, default=2024, help="race season (default: 2024)")
    parser.add_argument("--gp", default="Bahrain", help="Grand Prix name or event identifier")
    parser.add_argument("--session", default="R", help="FastF1 session identifier (default: R)")
    parser.add_argument(
        "--speed",
        type=positive_float,
        default=float(os.getenv("F1_REPLAY_SPEED", "1")),
        help="replay multiplier; 2 is twice as fast (default: $F1_REPLAY_SPEED or 1)",
    )
    parser.add_argument("--once", action="store_true", help="serve one client and exit after its replay")
    parser.add_argument(
        "--client-timeout",
        type=positive_float,
        default=None,
        help="optional timeout in seconds waiting for the replay client",
    )
    parser.add_argument(
        "--host", default=os.getenv("F1_STREAM_BIND_HOST", "0.0.0.0"), help="TCP bind host"
    )
    parser.add_argument(
        "--port", type=valid_port, default=int(os.getenv("F1_STREAM_PORT", "9999")), help="TCP bind port"
    )
    parser.add_argument(
        "--dry-run",
        nargs="?",
        const=DEFAULT_DRY_RUN_PATH,
        type=Path,
        default=None,
        metavar="PATH",
        help=f"write JSONL to PATH instead of serving (default: {DEFAULT_DRY_RUN_PATH})",
    )
    return parser.parse_args()


def is_missing(value: Any) -> bool:
    """Return true for scalar pandas/NumPy missing values without raising."""
    if value is None:
        return True
    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):
        return False


def safe_float(value: Any, default: float | None = None) -> float | None:
    if is_missing(value):
        return default
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    return parsed if math.isfinite(parsed) else default


def safe_int(value: Any, default: int | None = None) -> int | None:
    parsed = safe_float(value)
    return default if parsed is None else int(parsed)


def safe_string(value: Any, default: str | None = None) -> str | None:
    return default if is_missing(value) else str(value)


def timedelta_seconds(value: Any) -> float | None:
    """Convert a FastF1/Pandas timedelta to a finite number of seconds."""
    if is_missing(value):
        return None
    try:
        seconds = value.total_seconds()
    except AttributeError:
        try:
            seconds = pd.to_timedelta(value).total_seconds()
        except (TypeError, ValueError):
            return None
    return safe_float(seconds)


def session_start_timestamp(session: fastf1.core.Session, last_completion: float) -> pd.Timestamp:
    """Return the session start in UTC for an absolute lap-completion timestamp."""
    session_date = getattr(session, "date", None)
    if is_missing(session_date):
        LOGGER.warning("FastF1 session has no date; deriving a temporary replay epoch")
        return pd.Timestamp.now(tz="UTC") - pd.to_timedelta(last_completion, unit="s")
    timestamp = pd.Timestamp(session_date)
    if timestamp.tzinfo is None:
        return timestamp.tz_localize("UTC")
    return timestamp.tz_convert("UTC")


def prepare_laps(session: fastf1.core.Session) -> list[tuple[float, dict[str, Any]]]:
    """Build race-completion-ordered payloads with per-lap gaps to position ahead."""
    laps = session.laps.copy()
    laps["_completion_seconds"] = laps["Time"].map(timedelta_seconds)
    laps = laps.dropna(subset=["_completion_seconds"]).copy()
    laps.sort_values("_completion_seconds", kind="stable", inplace=True)
    if laps.empty:
        return []
    laps["_lap_time_seconds"] = laps["LapTime"].map(timedelta_seconds)
    laps["_lap_time_delta"] = laps.groupby("Driver", dropna=False)["_lap_time_seconds"].diff()
    session_start = session_start_timestamp(session, float(laps["_completion_seconds"].max()))

    gap_by_row: dict[Any, float] = {}
    for _, lap_group in laps.groupby("LapNumber", dropna=True, sort=False):
        ordered_group = lap_group.sort_values("_completion_seconds", kind="stable")
        completion_by_position: dict[int, float] = {}
        for _, row in ordered_group.iterrows():
            position = safe_int(row.get("Position"))
            completion = safe_float(row["_completion_seconds"])
            if position is not None and position > 0 and completion is not None:
                completion_by_position.setdefault(position, completion)

        for row_index, row in ordered_group.iterrows():
            position = safe_int(row.get("Position"))
            completion = safe_float(row["_completion_seconds"])
            if position == 1:
                gap_by_row[row_index] = 0.0
            elif position is not None and position > 1 and completion is not None:
                ahead_completion = completion_by_position.get(position - 1)
                gap_by_row[row_index] = max(0.0, completion - ahead_completion) if ahead_completion is not None else 0.0
            else:
                gap_by_row[row_index] = 0.0

    replay_laps: list[tuple[float, dict[str, Any]]] = []
    for row_index, row in laps.iterrows():
        completion = safe_float(row["_completion_seconds"])
        if completion is None:
            continue
        lap_number = safe_int(row.get("LapNumber"))
        lap_time = timedelta_seconds(row.get("LapTime"))
        is_in_lap = not is_missing(row.get("PitInTime"))
        is_out_lap = not is_missing(row.get("PitOutTime"))
        replay_laps.append(
            (
                completion,
                {
                    "Driver": safe_string(row.get("Driver"), "UNK"),
                    "LapNumber": lap_number,
                    "LapTime": lap_time,
                    "LapTimeDelta": safe_float(row.get("_lap_time_delta"), 0.0),
                    "GapToAhead": round(gap_by_row.get(row_index, 0.0), 6),
                    "Compound": safe_string(row.get("Compound"), "UNKNOWN"),
                    "TyreLife": safe_float(row.get("TyreLife")),
                    "FreshTyre": safe_string(row.get("FreshTyre")),
                    "Position": safe_int(row.get("Position")),
                    "Stint": safe_int(row.get("Stint")),
                    "TrackStatus": safe_string(row.get("TrackStatus"), "UNKNOWN"),
                    "FuelCorrectedLapTime": (
                        None
                        if lap_time is None or lap_number is None
                        else round(lap_time - FUEL_BURN_SECONDS_PER_LAP * lap_number, 6)
                    ),
                    "LapCompletionTime": (session_start + pd.to_timedelta(completion, unit="s")).isoformat(),
                    "IsPitStop": is_in_lap or is_out_lap,
                    "IsInLap": is_in_lap,
                    "IsOutLap": is_out_lap,
                },
            )
        )
    return replay_laps


def json_lines(replay_laps: Iterable[tuple[float, dict[str, Any]]]) -> Iterable[str]:
    for _, payload in replay_laps:
        yield json.dumps(payload, allow_nan=False, separators=(",", ":")) + "\n"


def replay_to_client(connection: socket.socket, replay_laps: list[tuple[float, dict[str, Any]]], speed: float) -> None:
    previous_completion: float | None = None
    replay_started = time.monotonic()
    for completion, payload in replay_laps:
        if previous_completion is not None:
            time.sleep(max(0.0, completion - previous_completion) / speed)
        replay_payload = dict(payload)
        replay_payload["ReplaySentAt"] = datetime.now(timezone.utc).isoformat()
        connection.sendall(
            (json.dumps(replay_payload, allow_nan=False, separators=(",", ":")) + "\n").encode("utf-8")
        )
        payload["ReplaySentAt"] = replay_payload["ReplaySentAt"]
        previous_completion = completion
    elapsed = time.monotonic() - replay_started
    LOGGER.info(
        "Replay throughput records=%s elapsed_seconds=%.3f records_per_second=%.3f",
        len(replay_laps),
        elapsed,
        len(replay_laps) / elapsed if elapsed else 0.0,
    )


def serve_client(
    connection: socket.socket,
    address: tuple[str, int],
    replay_laps: list[tuple[float, dict[str, Any]]],
    speed: float,
) -> None:
    with connection:
        try:
            replay_to_client(connection, replay_laps, speed)
            LOGGER.info("Replay completed for %s:%s", *address)
        except (BrokenPipeError, ConnectionAbortedError, ConnectionResetError, OSError) as exc:
            LOGGER.info("Client disconnected (%s:%s): %s", *address, exc)


def serve(
    replay_laps: list[tuple[float, dict[str, Any]]],
    host: str,
    port: int,
    speed: float,
    once: bool = False,
    client_timeout: float | None = None,
) -> None:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind((host, port))
        server_socket.listen(1)
        if client_timeout is not None:
            server_socket.settimeout(client_timeout)
        LOGGER.info("Telemetry replay server listening on %s:%s", host, port)
        if once:
            try:
                connection, address = server_socket.accept()
            except socket.timeout as exc:
                raise TimeoutError(f"No Spark client connected within {client_timeout}s") from exc
            connection.settimeout(None)
            serve_client(connection, address, replay_laps, speed)
            return
        with ThreadPoolExecutor(thread_name_prefix="f1-replay-client") as clients:
            while True:
                connection, address = server_socket.accept()
                LOGGER.info("Client connected: %s:%s", *address)
                clients.submit(serve_client, connection, address, replay_laps, speed)


def enable_cache() -> None:
    cache_directory = Path(os.getenv("FASTF1_CACHE_DIR", ".fastf1-cache"))
    cache_directory.mkdir(parents=True, exist_ok=True)
    fastf1.Cache.enable_cache(str(cache_directory))
    LOGGER.info("FastF1 cache enabled at %s", cache_directory.resolve())


def main() -> int:
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    try:
        enable_cache()
        LOGGER.info("Loading FastF1 session: year=%s gp=%s session=%s", args.year, args.gp, args.session)
        session = fastf1.get_session(args.year, args.gp, args.session)
        session.load(telemetry=False, weather=False, laps=True, messages=False)
        replay_laps = prepare_laps(session)
        if not replay_laps:
            raise RuntimeError("FastF1 returned no laps with a valid completion time")
        LOGGER.info("Prepared %s replay records", len(replay_laps))
        if args.dry_run is not None:
            with args.dry_run.open("w", encoding="utf-8", newline="\n") as output:
                output.writelines(json_lines(replay_laps))
            LOGGER.info("Wrote dry-run JSONL to %s", args.dry_run)
            return 0
        serve(
            replay_laps,
            args.host,
            args.port,
            args.speed,
            once=args.once,
            client_timeout=args.client_timeout,
        )
    except KeyboardInterrupt:
        LOGGER.info("Telemetry replay stopped")
        return 0
    except Exception:
        LOGGER.exception("Telemetry replay failed")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
