"""HBase persistence and dashboard queries for Gold strategy records."""

from __future__ import annotations

import logging
import os
import time
from collections.abc import Iterable, Iterator
from datetime import datetime, timezone
from typing import Any

import happybase
from thrift.Thrift import TException


LOGGER = logging.getLogger(__name__)
TABLE_NAME = "f1_leaderboard"
PIPELINE_HEALTH_ROW = b"!PIPELINE_HEALTH"
BATCH_SIZE = 100
MAX_WRITE_ATTEMPTS = 3
MAX_TIMESTAMP_MICROSECONDS = (1 << 63) - 1


def _connection_pool() -> happybase.ConnectionPool:
    return happybase.ConnectionPool(
        size=1,
        host=os.getenv("HBASE_HOST", "hbase"),
        port=int(os.getenv("HBASE_PORT", "9090")),
        timeout=5000,
    )


def _row_key(driver: str, timestamp: datetime) -> bytes:
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)
    else:
        timestamp = timestamp.astimezone(timezone.utc)
    elapsed = timestamp - datetime(1970, 1, 1, tzinfo=timezone.utc)
    timestamp_micros = (
        elapsed.days * 86_400_000_000 + elapsed.seconds * 1_000_000 + elapsed.microseconds
    )
    reversed_timestamp = MAX_TIMESTAMP_MICROSECONDS - timestamp_micros
    return f"{driver}_{reversed_timestamp:019d}".encode("utf-8")


def _encode_value(value: Any) -> bytes:
    return b"unavailable" if value is None else str(value).encode("utf-8")


def _serialize_prediction(prediction: dict[str, Any]) -> tuple[bytes, dict[bytes, bytes]]:
    row = prediction["row"]
    timestamp = row["window"].start
    return (
        _row_key(row["Driver"], timestamp),
        {
            b"cf_race:avg_gap": _encode_value(row["AvgGap"]),
            b"cf_race:tyre_life": _encode_value(row["AvgTyreLife"]),
            b"cf_race:lap": _encode_value(row["LapNumber"]),
            b"cf_race:position": _encode_value(row["AvgPosition"]),
            b"cf_race:compound": _encode_value(row["Compound"]),
            b"cf_race:lap_time_delta": _encode_value(row["AvgLapTimeDelta"]),
            b"cf_strat:replay_sent_at": _encode_value(row["ReplaySentAt"]),
            b"cf_strat:decay": _encode_value(prediction["decay"]),
            b"cf_strat:decision": _encode_value(prediction["decision"]),
            b"cf_strat:pit_prob": _encode_value(prediction["pit_probability"]),
            b"cf_strat:reason": _encode_value(prediction["reason"]),
        },
    )


def _write_partition(records: Iterator[tuple[bytes, dict[bytes, bytes]]]) -> None:
    """Reuse one pooled connection per Spark partition and retry failed batches."""
    pool = _connection_pool()
    batch_records: list[tuple[bytes, dict[bytes, bytes]]] = []
    for record in records:
        batch_records.append(record)
        if len(batch_records) == BATCH_SIZE:
            _send_batch(pool, batch_records)
            batch_records.clear()
    if batch_records:
        _send_batch(pool, batch_records)


def _send_batch(pool: Any, records: list[tuple[bytes, dict[bytes, bytes]]]) -> None:
    for attempt in range(1, MAX_WRITE_ATTEMPTS + 1):
        try:
            with pool.connection() as connection:
                try:
                    batch = connection.table(TABLE_NAME).batch(batch_size=len(records))
                    for row_key, columns in records:
                        batch.put(
                            row_key,
                            {
                                **columns,
                                b"cf_strat:processed_at": datetime.now(timezone.utc).isoformat().encode("utf-8"),
                            },
                        )
                    batch.send()
                except TException:
                    connection.close()
                    raise
            return
        except TException:
            if attempt == MAX_WRITE_ATTEMPTS:
                LOGGER.exception("HBase batch write failed after %s attempts", attempt)
                raise
            time.sleep(0.25 * (2 ** (attempt - 1)))


def write_predictions(predictions: Iterable[dict[str, Any]], spark_context: Any = None) -> None:
    """Upsert Gold records in batches, using one pooled connection per partition."""
    records = [_serialize_prediction(prediction) for prediction in predictions]
    if not records:
        return
    if spark_context is None:
        _write_partition(iter(records))
        return

    partitions = min(len(records), max(1, spark_context.defaultParallelism))
    spark_context.parallelize(records, partitions).foreachPartition(_write_partition)


def write_pipeline_health(
    batch_latency_ms: float,
    rows_processed: int,
    stream_lag_seconds: float | None,
) -> None:
    """Keep the latest streaming health metrics in a dedicated HBase row."""
    pool = _connection_pool()
    with pool.connection() as connection:
        connection.table(TABLE_NAME).put(
            PIPELINE_HEALTH_ROW,
            {
                b"cf_strat:batch_latency_ms": _encode_value(round(batch_latency_ms, 1)),
                b"cf_strat:rows_processed": _encode_value(rows_processed),
                b"cf_strat:stream_lag_seconds": _encode_value(
                    None if stream_lag_seconds is None else round(stream_lag_seconds, 3)
                ),
                b"cf_strat:updated_at": datetime.now(timezone.utc).isoformat().encode("utf-8"),
            },
        )


def get_pipeline_health() -> dict[str, str] | None:
    """Read the latest persisted streaming health metrics for the dashboard."""
    pool = _connection_pool()
    with pool.connection() as connection:
        columns = connection.table(TABLE_NAME).row(PIPELINE_HEALTH_ROW)
    if not columns:
        return None
    return {
        key.decode("utf-8").split(":", 1)[1]: value.decode("utf-8")
        for key, value in columns.items()
    }


def _decode_row(row_key: bytes, columns: dict[bytes, bytes]) -> dict[str, Any]:
    if row_key == PIPELINE_HEALTH_ROW:
        raise ValueError("Pipeline health row does not contain a driver timestamp")
    driver_bytes, timestamp_bytes = row_key.rsplit(b"_", 1)
    try:
        reversed_timestamp = int(timestamp_bytes)
    except ValueError:
        timestamp = datetime.strptime(timestamp_bytes.decode("ascii"), "%Y%m%dT%H%M%S").replace(
            tzinfo=timezone.utc
        )
    else:
        timestamp_micros = MAX_TIMESTAMP_MICROSECONDS - reversed_timestamp
        timestamp_seconds, microsecond = divmod(timestamp_micros, 1_000_000)
        timestamp = datetime.fromtimestamp(timestamp_seconds, tz=timezone.utc).replace(microsecond=microsecond)
    record: dict[str, Any] = {
        "driver": driver_bytes.decode("utf-8"),
        "timestamp": timestamp.isoformat(),
        "row_key": row_key.decode("utf-8"),
        "_timestamp": timestamp,
    }
    for column, value in columns.items():
        family_qualifier = column.decode("utf-8")
        qualifier = family_qualifier.split(":", 1)[1]
        record[qualifier] = value.decode("utf-8")
        if qualifier == "pit_probability":
            record["pit_prob"] = record[qualifier]
    return record


def get_latest_per_driver() -> dict[str, dict[str, Any]]:
    """Return the newest scanned Gold row for each driver, keyed by driver."""
    latest: dict[str, dict[str, Any]] = {}
    pool = _connection_pool()
    with pool.connection() as connection:
        table = connection.table(TABLE_NAME)
        for row_key, columns in table.scan():
            if row_key == PIPELINE_HEALTH_ROW:
                continue
            record = _decode_row(row_key, columns)
            driver = record["driver"]
            previous = latest.get(driver)
            if previous is None or record["_timestamp"] > previous["_timestamp"]:
                latest[driver] = record
    for record in latest.values():
        del record["_timestamp"]
    return latest


def get_history(driver: str, n: int) -> list[dict[str, Any]]:
    """Return up to n newest Gold rows for one driver."""
    if n < 0:
        raise ValueError("n must be non-negative")
    if n == 0:
        return []
    prefix = f"{driver}_".encode("utf-8")
    history: list[dict[str, Any]] = []
    pool = _connection_pool()
    with pool.connection() as connection:
        table = connection.table(TABLE_NAME)
        for row_key, columns in table.scan(row_prefix=prefix):
            history.append(_decode_row(row_key, columns))
    history.sort(key=lambda record: record["_timestamp"], reverse=True)
    for record in history:
        del record["_timestamp"]
    return history[:n]
