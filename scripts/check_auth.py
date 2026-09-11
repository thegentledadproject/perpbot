"""Spec 0.1: prove an authenticated perps call works.

Run by a human with their own key. Places no orders. Prints balances and
the delegated credential's expiry; never prints any secret.

    POLYPERPS_ALLOW_ENV_SECRETS=1 POLYMARKET_PRIVATE_KEY=0x... \
        .venv/Scripts/python scripts/check_auth.py

The spec names "/v1/account/balances"; that raw path is UNVERIFIED. The
SDK-level equivalent used here is PerpsSession.fetch_balances().
"""

from __future__ import annotations

import asyncio
from datetime import timedelta

from polyperps.security.key_management import load_secret, redact


async def main() -> None:
    from polymarket import AsyncSecureClient

    pk = load_secret("POLYMARKET_PRIVATE_KEY")
    print(f"loaded POLYMARKET_PRIVATE_KEY {redact(pk)}")
    client = await AsyncSecureClient.create(private_key=pk)
    try:
        session = await client.open_perps_session(
            expires_in=timedelta(hours=1), label="polyperps-auth-check"
        )
        try:
            creds = session.credentials
            print(f"delegated proxy={creds.proxy} expires_at={creds.expires_at.isoformat()}")
            balances = await session.fetch_balances()
            print(f"fetch_balances -> {len(balances)} entries")
            for b in balances:
                print(f"  {b!r}")
        finally:
            await session.close()
    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())
