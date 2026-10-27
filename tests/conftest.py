import pytest


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers",
        "integration: requiere el stack Docker local (PostgreSQL/S3) y datos publicados",
    )
