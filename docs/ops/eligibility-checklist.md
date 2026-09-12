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
|      |                    |            |         |        |       |
