"""Unit tests for the typed exception hierarchy."""

from __future__ import annotations

import pytest

from cambrian.errors import (
    CambrianError,
    NotInitializedError,
    SidecarVersionAheadError,
)


def test_not_initialized_default_message_mentions_init() -> None:
    err = NotInitializedError()
    assert isinstance(err, CambrianError)
    assert "cambrian init" in str(err)


def test_not_initialized_custom_message_overrides_default() -> None:
    err = NotInitializedError("bespoke message")
    assert str(err) == "bespoke message"


def test_version_ahead_carries_versions_and_friendly_message() -> None:
    err = SidecarVersionAheadError(found_version=5, expected_version=2)
    assert isinstance(err, CambrianError)
    assert err.found_version == 5
    assert err.expected_version == 2
    msg = str(err)
    assert "5" in msg
    assert "2" in msg
    assert "upgrade cambrian" in msg.lower()


def test_errors_are_raisable() -> None:
    with pytest.raises(NotInitializedError):
        raise NotInitializedError()
    with pytest.raises(SidecarVersionAheadError):
        raise SidecarVersionAheadError(1, 0)
    with pytest.raises(CambrianError):
        raise NotInitializedError()
