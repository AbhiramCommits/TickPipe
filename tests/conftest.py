"""Shared test fixtures."""

from __future__ import annotations

import os

import pytest


@pytest.fixture(scope="session")
def database_url(tmp_path_factory: pytest.TempPathFactory) -> str:
    """Registry database URL: TICKPIPE_TEST_DATABASE_URL (CI: postgres) or sqlite."""
    from_environment = os.environ.get("TICKPIPE_TEST_DATABASE_URL")
    if from_environment:
        return from_environment
    return f"sqlite:///{tmp_path_factory.mktemp('registry')}/experiments.db"
