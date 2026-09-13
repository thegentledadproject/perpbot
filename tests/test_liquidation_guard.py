from datetime import datetime, timezone
from decimal import Decimal

from polyperps.execution.types import AccountSnapshot, Intent, PositionView
from polyperps.risk.liquidation_guard import (
    LIMITS, Allow, Reject, Resize, RiskLimits, check_open, funding_exit_due, stop_price, verdict_label, vet_entry,
)

T0 = datetime(2026, 9, 12, 12, 0, tzinfo=timezone.utc)


def test_limits_pinned():
    assert LIMITS == RiskLimits(max_leverage=3, min_liq_distance=Decimal("0.25"), stop_distance=Decimal("0.15"),
                                notional_usd=Decimal("100"), max_funding_cost=Decimal("0.02"),
                                maintenance_rate=Decimal("0.02"))


def snap(equity="1000", positions=()):
    return AccountSnapshot(equity=Decimal(equity), positions=tuple(positions), open_orders=(), stops={},
                           in_liquidation=False, ts=T0)


def pos(iid, notional, size_sign=1, liq=None, entry="100", funding="0"):
    n = Decimal(notional)
    return PositionView(instrument_id=iid, size=Decimal(size_sign) * n / Decimal(entry), entry_price=Decimal(entry),
                        notional=n, leverage=3, liquidation_price=Decimal(liq) if liq else None,
                        unrealised_pnl=Decimal(0), cumulative_funding=Decimal(funding))


def intent(notional, mark="100"):
    n = Decimal(notional)
    return Intent(instrument_id=6, side="buy", quantity=n / Decimal(mark), notional=n)


def test_entry_allowed_within_leverage():
    v = vet_entry(intent("100"), mark=Decimal(100), snapshot=snap())
    assert v == Allow()


def test_entry_resized_to_leverage_cap():
    # equity 1000, existing 2500 notional, intent 1000 -> post 3.5x > 3x; allowed = 3000-2500 = 500 -> qty 5
    v = vet_entry(intent("1000"), mark=Decimal(100), snapshot=snap(positions=[pos(7, "2500")]))
    assert v == Resize(quantity=Decimal("5.00000000"))


def test_entry_rejected_when_no_room():
    v = vet_entry(intent("100"), mark=Decimal(100), snapshot=snap(positions=[pos(7, "3000")]))
    assert isinstance(v, Reject) and "leverage" in v.reason


def test_entry_rejected_on_zero_equity():
    assert isinstance(vet_entry(intent("100"), mark=Decimal(100), snapshot=snap(equity="0")), Reject)


def test_liq_distance_floor_is_not_binding_under_3x():
    # at 3x: 1/3 - 0.02 = 0.313 >= 0.25 -> the leverage cap binds first, never the floor
    limits = RiskLimits(max_leverage=10, min_liq_distance=Decimal("0.25"), stop_distance=Decimal("0.15"),
                        notional_usd=Decimal(100), max_funding_cost=Decimal("0.02"), maintenance_rate=Decimal("0.02"))
    # equity 1000, intent 5000 -> 5x -> distance 0.18 < 0.25 -> resize to 1/(0.27) = 3.7037x -> 3703.70 notional
    v = vet_entry(intent("5000"), mark=Decimal(100), snapshot=snap(), limits=limits)
    assert isinstance(v, Resize) and Decimal("37.03") < v.quantity < Decimal("37.04")


def test_check_open_flatten_when_liquidation_close():
    assert check_open(pos(6, "100", liq="80"), mark=Decimal(100)) == "flatten"   # 20% away
    assert check_open(pos(6, "100", liq="70"), mark=Decimal(100)) == "hold"      # 30% away
    assert check_open(pos(6, "100", liq=None), mark=Decimal(100)) == "hold"


def test_stop_price():
    assert stop_price(side="long", entry=Decimal(100)) == Decimal("85.00")
    assert stop_price(side="short", entry=Decimal(100)) == Decimal("115.00")


def test_funding_exit_due():
    assert funding_exit_due(pos(6, "100", funding="-2")) is True     # paid 2% of notional
    assert funding_exit_due(pos(6, "100", funding="-1.99")) is False
    assert funding_exit_due(pos(6, "100", funding="3")) is False     # received, not paid


def test_verdict_labels():
    assert verdict_label(Allow()) == "allow"
    assert verdict_label(Resize(quantity=Decimal("1.5"))) == "resize:1.5"
    assert verdict_label(Reject(reason="x")) == "reject:x"


# --- final review minors: the short side goes through the same guards -----------------------


def test_short_entry_vetted_by_gross_notional_like_a_long():
    # an existing SHORT counts as gross notional too: equity 1000, short 2500, intent 1000 -> resize to 500
    v = vet_entry(intent("1000"), mark=Decimal(100), snapshot=snap(positions=[pos(7, "2500", size_sign=-1)]))
    assert v == Resize(quantity=Decimal("5.00000000"))
    assert isinstance(vet_entry(intent("100"), mark=Decimal(100),
                                snapshot=snap(positions=[pos(7, "3000", size_sign=-1)])), Reject)


def test_check_open_short_side_liquidation_distance():
    # short from 100: liquidation ABOVE the mark; distance measured the same way
    assert check_open(pos(6, "100", size_sign=-1, liq="120"), mark=Decimal(100)) == "flatten"   # 20 % away
    assert check_open(pos(6, "100", size_sign=-1, liq="130"), mark=Decimal(100)) == "hold"      # 30 % away
    assert check_open(pos(6, "100", size_sign=-1, liq="120"), mark=Decimal(90)) == "hold"       # rallied away: 33 %
    assert check_open(pos(6, "100", size_sign=-1, liq="120"), mark=Decimal(110)) == "flatten"   # squeezed: 9 %


def test_funding_exit_due_short_side():
    # a short PAYS when funding is negative: cumulative_funding is what we received, so paid = -it
    assert funding_exit_due(pos(6, "100", size_sign=-1, funding="-2")) is True
    assert funding_exit_due(pos(6, "100", size_sign=-1, funding="-1.99")) is False
    assert funding_exit_due(pos(6, "100", size_sign=-1, funding="3")) is False   # shorts collecting positive funding
