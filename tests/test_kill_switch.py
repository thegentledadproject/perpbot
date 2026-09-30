from decimal import Decimal

from polyperps.risk.kill_switch import THRESHOLDS, KillThresholds, divergence, evaluate, loss_limit


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


def test_numeric_thresholds_but_undefined_divergence_falls_back_to_mode_default():
    t = KillThresholds(pause=Decimal("0.5"), shutdown=Decimal("1.0"))
    assert evaluate(live_sharpe=1.0, backtest_sharpe=0.0, mode="paper", thresholds=t) == "run"
    assert evaluate(live_sharpe=1.0, backtest_sharpe=0.0, mode="live", thresholds=t) == "pause"


def test_loss_limit_pauses_at_minus_5_and_shuts_down_at_minus_10():
    s = Decimal(1000)
    assert loss_limit(Decimal("951"), s) == "run"
    assert loss_limit(Decimal("950"), s) == "pause"
    assert loss_limit(Decimal("901"), s) == "pause"
    assert loss_limit(Decimal("900"), s) == "shutdown"
    assert loss_limit(Decimal("900"), None) == "run" and loss_limit(Decimal("900"), Decimal(0)) == "run"
    assert loss_limit(None, s) == "run"


def test_evaluate_takes_the_strictest_of_divergence_and_loss():
    t = KillThresholds(pause=Decimal("0.5"), shutdown=Decimal("1.0"))
    flat = {"equity": Decimal(1000), "start_equity": Decimal(1000)}
    assert evaluate(live_sharpe=-0.1, backtest_sharpe=1.2, mode="live", thresholds=t, **flat) == "shutdown"
    assert evaluate(live_sharpe=1.0, backtest_sharpe=1.2, mode="live", thresholds=t,
                    equity=Decimal(940), start_equity=Decimal(1000)) == "pause"
    assert evaluate(live_sharpe=None, backtest_sharpe=None, mode="paper",
                    equity=Decimal(890), start_equity=Decimal(1000)) == "shutdown"
    assert evaluate(live_sharpe=None, backtest_sharpe=None, mode="live", **flat) == "pause"   # None thresholds
    assert evaluate(live_sharpe=None, backtest_sharpe=None, mode="paper", **flat) == "run"
