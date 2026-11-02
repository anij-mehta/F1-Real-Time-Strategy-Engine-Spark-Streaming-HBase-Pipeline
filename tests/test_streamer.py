import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pandas as pd

import f1_streamer


def test_dry_run_outputs_gaps_and_pit_flags(monkeypatch, tmp_path: Path):
    laps = pd.DataFrame(
        [
            {
                "Driver": "VER",
                "LapNumber": 1,
                "Time": pd.Timedelta(seconds=90),
                "LapTime": pd.Timedelta(seconds=90),
                "PitInTime": pd.NaT,
                "PitOutTime": pd.NaT,
                "Position": 1,
                "TyreLife": 1.0,
                "Stint": 1,
                "Compound": "SOFT",
                "TrackStatus": "1",
            },
            {
                "Driver": "HAM",
                "LapNumber": 1,
                "Time": pd.Timedelta(seconds=91),
                "LapTime": pd.Timedelta(seconds=91),
                "PitInTime": pd.NaT,
                "PitOutTime": pd.NaT,
                "Position": 2,
                "TyreLife": 1.0,
                "Stint": 1,
                "Compound": "MEDIUM",
                "TrackStatus": "1",
            },
            {
                "Driver": "VER",
                "LapNumber": 2,
                "Time": pd.Timedelta(seconds=180),
                "LapTime": pd.Timedelta(seconds=90),
                "PitInTime": pd.NaT,
                "PitOutTime": pd.NaT,
                "Position": 1,
                "TyreLife": 2.0,
                "Stint": 1,
                "Compound": "SOFT",
                "TrackStatus": "1",
            },
            {
                "Driver": "HAM",
                "LapNumber": 2,
                "Time": pd.Timedelta(seconds=182),
                "LapTime": pd.Timedelta(seconds=91),
                "PitInTime": pd.Timedelta(seconds=181),
                "PitOutTime": pd.NaT,
                "Position": 2,
                "TyreLife": 2.0,
                "Stint": 1,
                "Compound": "MEDIUM",
                "TrackStatus": "1",
            },
            {
                "Driver": "VER",
                "LapNumber": 3,
                "Time": pd.Timedelta(seconds=270),
                "LapTime": pd.Timedelta(seconds=90),
                "PitInTime": pd.NaT,
                "PitOutTime": pd.Timedelta(seconds=271),
                "Position": 1,
                "TyreLife": 1.0,
                "Stint": 2,
                "Compound": "MEDIUM",
                "TrackStatus": "1",
            },
        ]
    )
    session = SimpleNamespace(
        laps=laps,
        date=pd.Timestamp("2024-03-02T15:00:00Z"),
        load=lambda **kwargs: None,
    )
    monkeypatch.setattr(f1_streamer.fastf1.Cache, "enable_cache", lambda path: None)
    monkeypatch.setattr(f1_streamer.fastf1, "get_session", lambda *args: session)

    output = tmp_path / "race.jsonl"
    monkeypatch.setattr(sys, "argv", ["f1_streamer.py", "--dry-run", str(output)])

    assert f1_streamer.main() == 0
    records = [json.loads(line) for line in output.read_text(encoding="utf-8").splitlines()]
    ham_lap_one = next(record for record in records if record["Driver"] == "HAM" and record["LapNumber"] == 1)
    ham_lap_two = next(record for record in records if record["Driver"] == "HAM" and record["LapNumber"] == 2)
    ver_lap_two = next(record for record in records if record["Driver"] == "VER" and record["LapNumber"] == 2)
    ver_lap_three = next(record for record in records if record["Driver"] == "VER" and record["LapNumber"] == 3)

    assert ham_lap_one["GapToAhead"] == 1.0
    assert ham_lap_two["GapToAhead"] == 2.0
    assert ver_lap_two["GapToAhead"] == 0.0
    assert ham_lap_one["IsPitStop"] is False
    assert ham_lap_two["IsPitStop"] is True
    assert ham_lap_two["IsInLap"] is True
    assert ham_lap_two["IsOutLap"] is False
    assert ver_lap_three["IsPitStop"] is True
    assert ver_lap_three["IsInLap"] is False
    assert ver_lap_three["IsOutLap"] is True
