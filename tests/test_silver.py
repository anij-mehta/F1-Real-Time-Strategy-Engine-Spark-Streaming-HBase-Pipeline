from datetime import datetime, timezone

import pytest


@pytest.fixture(scope="session")
def spark():
    spark_sql = pytest.importorskip("pyspark.sql")
    SparkSession = spark_sql.SparkSession
    session = (
        SparkSession.builder.master("local[1]")
        .appName("silver-filter-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield session
    session.stop()


def telemetry_row(driver: str, **overrides):
    row = {
        "Driver": driver,
        "LapNumber": 1,
        "GapToAhead": 0.0,
        "TyreLife": 1.0,
        "FuelCorrectedLapTime": 90.0,
        "LapTime": 90.0,
        "LapCompletionTime": datetime(2024, 3, 2, tzinfo=timezone.utc),
        "event_time": datetime(2024, 3, 2, tzinfo=timezone.utc),
        "TrackStatus": "1",
        "IsInLap": False,
        "IsOutLap": False,
    }
    row.update(overrides)
    return row


def test_filters_pit_laps_safety_car_and_lap_time_outliers(spark):
    from silver import classify_rows

    input_rows = [
        telemetry_row("VER"),
        telemetry_row("IN", IsInLap=True),
        telemetry_row("OUT", IsOutLap=True),
        telemetry_row("SC", TrackStatus="4"),
        telemetry_row("VSC", TrackStatus="6"),
        telemetry_row("SLOW", LapTime=39.9),
        telemetry_row("FAST", LapTime=180.1),
        telemetry_row("INVALID", GapToAhead=None),
        telemetry_row("NO_STATUS", TrackStatus=None),
    ]
    classified = classify_rows(spark.createDataFrame(input_rows))
    results = {row["Driver"]: row for row in classified.collect()}

    assert results["VER"]["is_silver_eligible"] is True
    assert results["NO_STATUS"]["is_silver_eligible"] is True
    assert results["IN"]["filter_reason"] == "in_or_out_lap"
    assert results["OUT"]["filter_reason"] == "in_or_out_lap"
    assert results["SC"]["filter_reason"] == "sc_or_vsc"
    assert results["VSC"]["filter_reason"] == "sc_or_vsc"
    assert results["SLOW"]["filter_reason"] == "lap_time_outlier"
    assert results["FAST"]["filter_reason"] == "lap_time_outlier"
    assert results["INVALID"]["filter_reason"] == "invalid"
    assert all(
        not results[driver]["is_silver_eligible"]
        for driver in ("IN", "OUT", "SC", "VSC", "SLOW", "FAST", "INVALID")
    )
