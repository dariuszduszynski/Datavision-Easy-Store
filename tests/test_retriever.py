from __future__ import annotations

import pytest


@pytest.mark.asyncio
async def test_retriever_get_file_success():
    pytest.skip("Retriever integration requires S3/moto setup")


@pytest.mark.asyncio
async def test_retriever_file_not_found():
    pytest.skip("Retriever integration requires S3/moto setup")


@pytest.mark.asyncio
async def test_retriever_cache_hit():
    pytest.skip("Retriever integration requires S3/moto setup")


@pytest.mark.asyncio
async def test_retriever_external_big_file():
    pytest.skip("Retriever integration requires S3/moto setup")
