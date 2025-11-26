import asyncio
import inspect
import random
import time
from unittest.mock import AsyncMock

import pytest

from des.utils.retry import async_retry

pytestmark = pytest.mark.asyncio


async def test_retry_success_first_attempt(monkeypatch: pytest.MonkeyPatch) -> None:
    func = AsyncMock(return_value="ok")
    sleep_mock = AsyncMock()
    monkeypatch.setattr(asyncio, "sleep", sleep_mock)

    decorated = async_retry(max_attempts=3, jitter=False)(func)

    result = await decorated("input")

    assert result == "ok"
    func.assert_awaited_once_with("input")
    sleep_mock.assert_not_called()


async def test_retry_success_after_failures(monkeypatch: pytest.MonkeyPatch) -> None:
    func = AsyncMock(side_effect=[RuntimeError("boom1"), RuntimeError("boom2"), "done"])
    sleep_mock = AsyncMock()
    monkeypatch.setattr(asyncio, "sleep", sleep_mock)

    decorated = async_retry(max_attempts=3, backoff_base=1.0, jitter=False)(func)

    result = await decorated()

    assert result == "done"
    assert func.await_count == 3
    assert sleep_mock.await_count == 2
    waits = [call.args[0] for call in sleep_mock.await_args_list]
    assert waits == [1.0, 1.0]


async def test_retry_max_attempts_exceeded(monkeypatch: pytest.MonkeyPatch) -> None:
    func = AsyncMock(side_effect=ValueError("fail"))
    sleep_mock = AsyncMock()
    monkeypatch.setattr(asyncio, "sleep", sleep_mock)

    decorated = async_retry(max_attempts=2, backoff_base=0.0, jitter=False)(func)

    with pytest.raises(ValueError):
        await decorated()

    assert func.await_count == 2
    sleep_mock.assert_awaited_once_with(0.0)


async def test_retry_backoff_timing(monkeypatch: pytest.MonkeyPatch) -> None:
    call_times: list[float] = []

    async def flaky() -> str:
        call_times.append(time.perf_counter())
        if len(call_times) < 3:
            raise RuntimeError("transient")
        return "ok"

    scale = 0.01
    real_sleep = asyncio.sleep

    async def scaled_sleep(delay: float, *args, **kwargs) -> None:
        await real_sleep(delay * scale)

    monkeypatch.setattr(asyncio, "sleep", scaled_sleep)

    decorated = async_retry(max_attempts=3, backoff_base=2.0, jitter=False)(flaky)
    result = await decorated()

    assert result == "ok"
    assert len(call_times) == 3
    intervals = [call_times[i + 1] - call_times[i] for i in range(2)]
    expected = [2.0 * scale, 4.0 * scale]
    assert intervals[0] == pytest.approx(expected[0], rel=0.5, abs=0.02)
    assert intervals[1] == pytest.approx(expected[1], rel=0.5, abs=0.02)
    assert intervals[1] > intervals[0]


async def test_retry_jitter(monkeypatch: pytest.MonkeyPatch) -> None:
    jitter_values = iter([0.5, 1.5])
    monkeypatch.setattr(random, "uniform", lambda _a, _b: next(jitter_values))

    func = AsyncMock(side_effect=[RuntimeError("first"), RuntimeError("second"), "ok"])
    sleep_mock = AsyncMock()
    monkeypatch.setattr(asyncio, "sleep", sleep_mock)

    decorated = async_retry(max_attempts=3, backoff_base=2.0, jitter=True)(func)
    result = await decorated()

    assert result == "ok"
    waits = [call.args[0] for call in sleep_mock.await_args_list]
    assert len(waits) == 2
    assert waits == [1.0, 6.0]
    assert waits[0] != waits[1]


async def test_retry_preserves_exception_info() -> None:
    raise_line: dict[str, int] = {}

    async def failing() -> None:
        frame = inspect.currentframe()
        assert frame is not None
        raise_line["line"] = frame.f_lineno
        raise ValueError("boom")

    decorated = async_retry(max_attempts=1, jitter=False)(failing)

    with pytest.raises(ValueError) as excinfo:
        await decorated()

    assert excinfo.value.args[0] == "boom"
    assert excinfo.traceback[-1].lineno == raise_line["line"]
    assert excinfo.traceback[-1].name == "failing"


async def test_retry_zero_max_attempts(monkeypatch: pytest.MonkeyPatch) -> None:
    func = AsyncMock(side_effect=RuntimeError("fail-fast"))
    sleep_mock = AsyncMock()
    monkeypatch.setattr(asyncio, "sleep", sleep_mock)

    decorated = async_retry(max_attempts=0, jitter=False)(func)

    with pytest.raises(RuntimeError):
        await decorated()

    func.assert_awaited_once()
    sleep_mock.assert_not_called()


async def test_retry_negative_backoff(monkeypatch: pytest.MonkeyPatch) -> None:
    func = AsyncMock(side_effect=[RuntimeError("first"), "ok"])
    sleep_mock = AsyncMock()
    monkeypatch.setattr(asyncio, "sleep", sleep_mock)

    decorated = async_retry(max_attempts=2, backoff_base=-2.0, jitter=False)(func)
    result = await decorated()

    assert result == "ok"
    assert func.await_count == 2
    sleep_mock.assert_awaited_once()
    assert sleep_mock.await_args_list[0].args[0] == -2.0
