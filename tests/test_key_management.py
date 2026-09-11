import pytest

from polyperps.security.key_management import (
    ALLOW_ENV_VAR,
    SERVICE,
    SecretUnavailable,
    load_secret,
    redact,
    store_secret,
)


class FakeKeyring:
    def __init__(self):
        self.store = {}

    def get_password(self, service, name):
        return self.store.get((service, name))

    def set_password(self, service, name, value):
        self.store[(service, name)] = value


def test_systemd_credentials_dir_wins(tmp_path):
    (tmp_path / "POLYMARKET_PRIVATE_KEY").write_text("0xfromsystemd\n")
    kr = FakeKeyring()
    kr.set_password(SERVICE, "POLYMARKET_PRIVATE_KEY", "0xfromkeyring")
    env = {"CREDENTIALS_DIRECTORY": str(tmp_path), "POLYMARKET_PRIVATE_KEY": "0xfromenv"}
    assert load_secret("POLYMARKET_PRIVATE_KEY", env=env, keyring_backend=kr) == "0xfromsystemd"


def test_keyring_used_when_no_credentials_dir():
    kr = FakeKeyring()
    kr.set_password(SERVICE, "POLYMARKET_PRIVATE_KEY", "0xfromkeyring")
    assert load_secret("POLYMARKET_PRIVATE_KEY", env={}, keyring_backend=kr) == "0xfromkeyring"


def test_env_refused_unless_explicitly_allowed():
    env = {"POLYMARKET_PRIVATE_KEY": "0xfromenv"}
    with pytest.raises(SecretUnavailable) as exc:
        load_secret("POLYMARKET_PRIVATE_KEY", env=env, keyring_backend=FakeKeyring())
    assert "0xfromenv" not in str(exc.value)


def test_env_allowed_with_dev_flag():
    env = {"POLYMARKET_PRIVATE_KEY": "0xfromenv", ALLOW_ENV_VAR: "1"}
    assert load_secret("POLYMARKET_PRIVATE_KEY", env=env, keyring_backend=FakeKeyring()) == "0xfromenv"


def test_missing_everywhere_raises_without_leaking():
    with pytest.raises(SecretUnavailable, match="POLYMARKET_PRIVATE_KEY"):
        load_secret("POLYMARKET_PRIVATE_KEY", env={}, keyring_backend=FakeKeyring())


def test_store_secret_round_trip():
    kr = FakeKeyring()
    store_secret("X", "secret-value", keyring_backend=kr)
    assert load_secret("X", env={}, keyring_backend=kr) == "secret-value"


def test_redact_never_echoes_value():
    out = redact("0xdeadbeef")
    assert "dead" not in out
    assert out == "<redacted:len=10>"
