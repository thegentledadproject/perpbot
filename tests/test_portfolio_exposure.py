from decimal import Decimal

from polyperps.execution.types import Intent, PositionView
from polyperps.risk.liquidation_guard import Allow, Reject, Resize
from polyperps.risk.portfolio_exposure import EXPOSURE, ExposureLimits, cluster_of, vet_exposure

CATS = {6: "crypto", 7: "crypto", 9: "equity"}


def pos(iid, notional, sign=1):
    n = Decimal(notional)
    return PositionView(instrument_id=iid, size=Decimal(sign) * n / 100, entry_price=Decimal(100), notional=n,
                        leverage=3, liquidation_price=None, unrealised_pnl=Decimal(0), cumulative_funding=Decimal(0))


def intent(iid, notional, side="buy"):
    n = Decimal(notional)
    return Intent(instrument_id=iid, side=side, quantity=n / 100, notional=n)


def test_limits_pinned_and_clusters():
    assert EXPOSURE == ExposureLimits(gross=Decimal("1.0"), cluster_net=Decimal("0.6"))
    assert cluster_of("crypto") == "crypto" and cluster_of("weird") == "other"


def test_spec_scenario_btc_plus_eth_same_direction_resized_by_cluster_net():
    # equity 1000; BTC long 500; ETH long 500 intent -> gross 1000 ok, cluster net 1000 > 600 -> allowed 100 -> qty 1
    v = vet_exposure(intent(7, "500"), positions=[pos(6, "500")], equity=Decimal(1000), categories=CATS)
    assert v == Resize(quantity=Decimal("1.00000000"))


def test_opposite_direction_in_cluster_is_allowed():
    v = vet_exposure(intent(7, "500", side="sell"), positions=[pos(6, "500")], equity=Decimal(1000), categories=CATS)
    assert v == Allow()


def test_gross_cap_binds_across_clusters():
    # BTC 500 + equity 400 = 900; intent 300 equity -> gross 1200 > 1000 -> allowed 100
    v = vet_exposure(intent(9, "300"), positions=[pos(6, "500"), pos(9, "400")], equity=Decimal(1000), categories=CATS)
    assert v == Resize(quantity=Decimal("1.00000000"))


def test_reject_when_no_room():
    v = vet_exposure(intent(7, "100"), positions=[pos(6, "600")], equity=Decimal(1000), categories=CATS)
    assert isinstance(v, Reject) and "cluster" in v.reason
    v = vet_exposure(intent(9, "100"), positions=[pos(6, "1000")], equity=Decimal(1000), categories=CATS)
    assert isinstance(v, Reject) and "gross" in v.reason


def test_reject_when_equity_non_positive():
    v = vet_exposure(intent(6, "100"), positions=[], equity=Decimal(0), categories=CATS)
    assert v == Reject(reason="equity <= 0")
