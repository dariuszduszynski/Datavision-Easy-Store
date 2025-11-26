import pytest


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line("markers", "unit: fast unit tests")
    config.addinivalue_line("markers", "integration: tests that require external services or are slower (S3, Redis, DB, etc.)")
    config.addinivalue_line("markers", "s3: tests that interact with S3 or moto S3")
    config.addinivalue_line("markers", "slow: slow-running tests")


@pytest.fixture
def redis_client():
    """
    Provide fake Redis client for testing.
    Uses fakeredis if available, otherwise skips tests requiring Redis.
    """
    try:
        import fakeredis
        return fakeredis.FakeRedis()
    except ImportError:
        pytest.skip("fakeredis not installed (pip install fakeredis)")
