"""Pure decision rules shared by Gold inference and unit tests."""

from __future__ import annotations

from config import PIT_DECAY_THRESHOLD_SECONDS, PIT_PROBABILITY_THRESHOLD


def pit_decision(
    decay: float,
    cliff_detected: bool,
    pit_probability: float | None,
) -> str:
    """Combine degradation and classifier signals into the Gold decision."""
    model_signal = pit_probability is not None and pit_probability >= PIT_PROBABILITY_THRESHOLD
    return (
        "PIT"
        if decay > PIT_DECAY_THRESHOLD_SECONDS or cliff_detected or model_signal
        else "STAY"
    )
