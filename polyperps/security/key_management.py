"""Secret resolution. Nothing here ever logs, reprs, or raises a secret value.

Resolution order:
  1. $CREDENTIALS_DIRECTORY/<name>   - systemd LoadCredential= (EC2 target).
     Put secrets in /etc/credstore/<name> (root:root 0600) and add
     LoadCredential=<name>:/etc/credstore/<name> to the unit. They never
     appear in the unit file, the environment, or `systemctl show`.
  2. OS keyring (service "polyperps")  - developer workstations.
  3. Environment variable               - ONLY if POLYPERPS_ALLOW_ENV_SECRETS=1.
     Dev convenience. Never set that flag on the trading host.

RESIDUAL RISK (spec 0.5) - read before deploying:
  Polymarket perps auth uses delegated credentials (polymarket.models.perps
  PerpsCredentials: proxy/private_key/secret, default TTL 1 week, revocable
  via AsyncSecureClient.revoke_perps_credentials). The PerpsSession object
  has no withdrawal method, so a leaked *delegated* credential can trade
  but not withdraw. However the public SDK API only opens/resumes a session
  through AsyncSecureClient.create(private_key=<wallet key>), so the wallet
  key is still on-host. Mitigations, in order of importance:
    a. Dedicated wallet that holds only the capital you are willing to lose.
    b. Deliver the wallet key via systemd LoadCredential=, never env/.env.
    c. Short-TTL delegated credentials (expires_in=timedelta(days=1)).
  Phase 2 may evaluate constructing polymarket._internal.perps_session.
  PerpsSession directly from stored PerpsCredentials so the runtime process
  holds no wallet key at all - that is private SDK API and must be re-verified
  on every SDK bump. There is NO perps-scoped session key in the SDK today
  (SessionKeyKnownScope = ALL | CLOB | COMBOSRFQ).
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from pathlib import Path
from typing import Protocol

SERVICE = "polyperps"
ALLOW_ENV_VAR = "POLYPERPS_ALLOW_ENV_SECRETS"
CREDENTIALS_DIR_VAR = "CREDENTIALS_DIRECTORY"


class SecretUnavailable(RuntimeError):
    pass


class KeyringLike(Protocol):
    def get_password(self, service: str, name: str) -> str | None: ...
    def set_password(self, service: str, name: str, value: str) -> None: ...


def redact(value: str) -> str:
    return f"<redacted:len={len(value)}>"


def _default_keyring() -> KeyringLike:
    import keyring  # imported lazily so tests never touch a real backend

    return keyring


def _from_credentials_dir(name: str, env: Mapping[str, str]) -> str | None:
    directory = env.get(CREDENTIALS_DIR_VAR)
    if not directory:
        return None
    path = Path(directory) / name
    if not path.is_file():
        return None
    value = path.read_text(encoding="utf-8").strip()
    if not value:
        raise SecretUnavailable(
            f"credentials file for {name!r} in {CREDENTIALS_DIR_VAR} exists but is empty"
        )
    return value


def load_secret(
    name: str,
    *,
    env: Mapping[str, str] = os.environ,
    keyring_backend: KeyringLike | None = None,
) -> str:
    value = _from_credentials_dir(name, env)
    if value:
        return value

    kr = keyring_backend if keyring_backend is not None else _default_keyring()
    try:
        value = kr.get_password(SERVICE, name)
    except Exception:  # no backend available on a headless host is normal
        value = None
    if value:
        return value

    if env.get(ALLOW_ENV_VAR) == "1" and env.get(name):
        return env[name]

    raise SecretUnavailable(
        f"secret {name!r} not found in {CREDENTIALS_DIR_VAR}, keyring service "
        f"{SERVICE!r}, or env (env requires {ALLOW_ENV_VAR}=1)"
    )


def store_secret(name: str, value: str, *, keyring_backend: KeyringLike | None = None) -> None:
    kr = keyring_backend if keyring_backend is not None else _default_keyring()
    kr.set_password(SERVICE, name, value)
