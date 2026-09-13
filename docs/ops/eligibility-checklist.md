# Eligibility / jurisdiction checklist (spec 0.6)

Re-run on the 1st of every month and after any Polymarket terms/product
announcement. Record each pass in the log table. This is never "done".

## Checks

1. **Geo access** - From the trading host's egress IP, load the perps UI and
   run `scripts/run_feed.py --list-instruments`. Both must succeed without a
   geo-block notice. Record the egress IP and region.
2. **Terms of use** - Re-read Polymarket's Terms and any perps-specific
   addendum for changes to eligible jurisdictions, leverage limits, or
   automated-trading clauses. Record the document date.
3. **Product status** - Confirm perps are still live and not in a
   restricted/beta state that excludes API trading. Note SDK version and
   whether `polymarket-client` marks perps APIs experimental (it does at
   0.10.0).
4. **Fee / funding schedule** - `fetch_perps_fees()` output compared to what
   `risk/sizing.py` assumes (Phase 2+). Any change is a Phase 3.3 re-check
   trigger.
5. **Personal eligibility** - Your own residency/tax status has not changed
   in a way that alters the above.
6. **Sufficiency re-check** - run `scripts/sufficiency.py`; record days/periods
   per instrument. Earliest possible native pass: ~2026-10-11 (60 days after
   the first stored native funding row, 2026-08-12; re-check with
   `scripts/sufficiency.py`).

## Log

| Date | Egress IP / region | Terms date | SDK ver | Result | Notes |
|------|--------------------|------------|---------|--------|-------|
| 2026-09-12 | Home ISP (Malaysia); egress IP not recorded — see note | not re-read | 0.10.0 | **FAIL (geo)** | All Polymarket hosts (`api.perpetuals`, `clob`, `gamma-api`) timed out; `api.perpetuals.polymarket.com` resolved to 175.139.142.25 (Malaysian ISP range, not Polymarket) — consistent with an ISP-level DNS sinkhole. Hyperliquid and general internet fine. The same calls succeeded earlier the same day (feed run, fee fetch, 2-day backfill), so the working path differs from this one (VPN / other connection / intermittent poisoning). Backfill of instrument 7 not done. Action: only run Polymarket-facing scripts from the path that worked; treat this as the geo-restriction the parent spec warned about and confirm eligibility from the intended EC2 egress before Phase 3. |
| 2026-09-13 | EC2 56.69.199.187, ap-southeast-5 (Malaysia) | not re-read | 0.10.0 | **PASS (reachability)** | First deploy of `polyperps-feed`/`polyperps-paper` under systemd: `GET /v1/info/instruments` and `/v1/info/book` returned HTTP 200 from the box; `store_fees.py` stored `equity` (taker 0.0004); paper run booted with a clean recovery report; 31-day 1h backfill for instruments 6,7 started from this egress. Terms/eligibility text still not re-read — reachability only, not a legal-eligibility conclusion. Box is Ubuntu 26.04 / Python 3.14.4 (not the 24.04/3.12 assumed in the deploy notes; all 45 deps installed as wheels). |
