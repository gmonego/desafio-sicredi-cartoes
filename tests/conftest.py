"""Fixtures compartilhadas pela suite."""

import pytest

from src.config import get_spark


@pytest.fixture(scope="session")
def spark():
    """SparkSession unica para toda a execucao do pytest."""
    yield get_spark()