from datetime import timedelta

from polyperps.data_ingest.intervals import parse_interval


def test_parses_hours():
    assert parse_interval("1h") == timedelta(hours=1)


def test_parses_eight_hours():
    assert parse_interval("8h") == timedelta(hours=8)


def test_parses_minutes():
    assert parse_interval("15m") == timedelta(minutes=15)


def test_parses_days():
    assert parse_interval("1d") == timedelta(days=1)


def test_parses_weeks():
    assert parse_interval("1w") == timedelta(weeks=1)


def test_parses_seconds():
    assert parse_interval("30s") == timedelta(seconds=30)


def test_rejects_word_form():
    assert parse_interval("hourly") is None


def test_rejects_empty_string():
    assert parse_interval("") is None


def test_rejects_unknown_unit():
    assert parse_interval("1x") is None


def test_rejects_reversed_order():
    assert parse_interval("h1") is None
