import pytest

from spark_scripts.strategy_rules import pit_decision


@pytest.mark.parametrize(
    ("decay", "cliff", "probability", "expected"),
    [
        (0.21, False, None, "PIT"),
        (0.2, True, None, "PIT"),
        (0.0, False, 0.5, "PIT"),
        (0.0, False, 0.49, "STAY"),
        (0.2, False, None, "STAY"),
    ],
)
def test_pit_decision_combines_regression_cliff_and_classifier(decay, cliff, probability, expected):
    assert pit_decision(decay, cliff, probability) == expected
