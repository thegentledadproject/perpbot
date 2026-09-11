import pytest


@pytest.fixture
def no_env():
    """An environment mapping with nothing set."""
    return {}
