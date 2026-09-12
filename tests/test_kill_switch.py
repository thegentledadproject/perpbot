from decimal import Decimal

from polyperps.risk.kill_switch import THRESHOLDS, KillThresholds, divergence, evaluate


def test_thresholds_pinned_as_none_in_2a():
    assert THRESHOLDS == KillThresholds(pause=None, shutdown=None)


def test_none_thresholds_pause_live_run_paper():
    assert evaluate(live_sharpe=1.0, backtest_sharpe=1.0, mode="live") == "pause"
    assert evaluate(live_sharpe=1.0, backtest_sharpe=1.0, mode="paper") == "run"


def test_with_numbers():
    t = KillThresholds(pause=Decimal("0.5"), shutdown=Decimal("1.0"))
    assert evaluate(live_sharpe=1.0, backtest_sharpe=1.2, mode="live", thresholds=t) == "run"
    assert evaluate(live_sharpe=0.5, backtest_sharpe=1.2, mode="live", thresholds=t) == "pause"
    assert evaluate(live_sharpe=-0.1, backtest_sharpe=1.2, mode="live", thresholds=t) == "shutdown"
    assert evaluate(live_sharpe=None, backtest_sharpe=1.2, mode="live", thresholds=t) == "pause"
    assert evaluate(live_sharpe=1.0, backtest_sharpe=0.0, mode="live", thresholds=t) == "pause"


def test_divergence():
    assert divergence(0.6, 1.2) == Decimal("0.5")
    assert divergence(1.0, 0.0) is None and divergence(None, 1.0) is None
