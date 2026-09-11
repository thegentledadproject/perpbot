from decimal import Decimal

from polyperps.backtest.costs import fill_cost


def test_fill_cost_hand_computed():
    # delta 50 of notional 100: fee 0.0005*50 = 0.025; half-spread 100bps/2 -> 0.005*50 = 0.25;
    # impact 5bps * (50/100) = 2.5bps -> 0.00025*50 = 0.0125; total 0.2875
    c = fill_cost(notional_delta=Decimal("50"), notional=Decimal("100"), spread_bps=Decimal("100"),
                  taker_fee_rate=Decimal("0.0005"), impact_bps=Decimal("5"))
    assert c == Decimal("0.2875")


def test_fill_cost_zero_delta_is_free():
    assert fill_cost(notional_delta=Decimal("0"), notional=Decimal("100"), spread_bps=Decimal("100"),
                     taker_fee_rate=Decimal("0.0005"), impact_bps=Decimal("5")) == 0


def test_fill_cost_full_turnover_pays_full_impact():
    c = fill_cost(notional_delta=Decimal("100"), notional=Decimal("100"), spread_bps=Decimal("0"),
                  taker_fee_rate=Decimal("0"), impact_bps=Decimal("5"))
    assert c == Decimal("0.05")
