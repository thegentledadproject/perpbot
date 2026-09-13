# NautilusTrader Perps adapter re-check (spec 2.0)

Lookup performed 2026-09-13 via the GitHub REST API (`api.github.com`), read-only,
no Polymarket endpoint touched.

The brief's suggested command,
`curl -s https://api.github.com/repos/nautechsystems/nautilus_trader/contents/nautilus_trader/adapters`,
404s: the repository was restructured for the 2.0 rewrite and no longer has a
top-level `nautilus_trader/` Python package directory (`.../contents/` now shows
`crates/`, `python/`, `docs/`, `ADAPTERS.md`, `MIGRATION_V2.md`, `version.json`,
etc. instead). Adapters now live under `crates/adapters/<name>/` (Rust) and are
also summarized in `ADAPTERS.md`.

| Date | Adapters listed (`crates/adapters/`, Official per `ADAPTERS.md`) | Polymarket **perps** adapter? | Decision |
|------|--------------------------------------------------------------|-------------------------------|----------|
| 2026-09-13 | architect_ax, betfair, binance, bitmex, blockchain, bybit, coinbase, databento, deribit, derive, dydx, hyperliquid, interactive_brokers, kraken, lighter, okx, polymarket, sandbox, tardis | **No.** `polymarket` is listed as Official (Data/Execution), but its README (`crates/adapters/polymarket/README.md`) states it targets **"the Polymarket CLOB API for trading binary option contracts"** (REST `clob.polymarket.com`, WS `ws-subscriptions-clob.polymarket.com`, Gamma) — i.e. prediction-market CLOB trading, not the Perps API our bot uses (`polymarket-client`'s `PerpsSession`, chain-signed WS trading commands). No `perp`/`perps` module appears under `crates/adapters/polymarket/src/` (common, config, data, data_types, execution, factories, filters, http, models, positions, providers, python, resolve, rtds, signing, websocket). | Keep our ExchangeClient/Executor boundary; re-check before Phase 3 (migration only pre-live). |

## Supporting findings

- **No stable 2.0 release yet.** `GET /repos/nautechsystems/nautilus_trader/releases` shows
  the newest tagged release is `v2.0.0rc4` (published 2026-09-02T03:26:46Z, a release
  candidate), preceded by `v2.0.0rc3` (2026-08-21) and the `v1.23x.0` Beta line before that.
  The repo's `version.json` on the `develop` branch already reads `v2.0.0rc5`
  (in-progress, unreleased). So "2.0" exists as release candidates, not as a final
  stable release, as of 2026-09-13.
- `ADAPTERS.md` (root of the repo) lists Polymarket as one of 18 **Official**
  adapters maintained in-repo, last updated 2026-08-24 — confirming Polymarket
  support is real and current, just scoped to CLOB/binary-options, not Perps.
- Nothing here required calling any Polymarket endpoint; only `api.github.com`
  was queried.
