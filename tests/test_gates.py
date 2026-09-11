import pytest

from polyperps.gates import ExecutionMode, GateDecision, live_orders_allowed


def _decide(mode, env, validated):
    return live_orders_allowed(1, modes={1: mode}, env=env, signal_validated=validated)


def test_default_mode_is_manual_review_for_unknown_instrument():
    d = live_orders_allowed(
        99, modes={}, env={"POLYMARKET_LIVE_TRADING": "true"}, signal_validated=True
    )
    assert d.allowed is False
    assert "manual_review" in d.reason


def test_blocked_when_mode_not_auto():
    d = _decide(ExecutionMode.PAPER, {"POLYMARKET_LIVE_TRADING": "true"}, True)
    assert d.allowed is False
    assert "paper" in d.reason


def test_blocked_when_env_flag_missing():
    d = _decide(ExecutionMode.AUTO, {}, True)
    assert d.allowed is False
    assert "POLYMARKET_LIVE_TRADING" in d.reason


def test_blocked_when_env_flag_not_exactly_true():
    d = _decide(ExecutionMode.AUTO, {"POLYMARKET_LIVE_TRADING": "1"}, True)
    assert d.allowed is False


def test_blocked_when_signal_not_validated():
    d = _decide(ExecutionMode.AUTO, {"POLYMARKET_LIVE_TRADING": "true"}, False)
    assert d.allowed is False
    assert "SIGNAL_VALIDATED" in d.reason


def test_allowed_only_when_all_three_agree():
    d = _decide(ExecutionMode.AUTO, {"POLYMARKET_LIVE_TRADING": "true"}, True)
    assert d == GateDecision(allowed=True, reason="all gates passed")


def test_signal_stub_is_not_validated_and_raises():
    from polyperps.signal import base

    assert base.SIGNAL_VALIDATED is False
    with pytest.raises(NotImplementedError):
        base.generate_signal(market_state=None)
