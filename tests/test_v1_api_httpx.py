import asyncio
from datetime import datetime

import httpx
import pytest
import pytest_asyncio
from asgi_lifespan import LifespanManager
from httpx import ASGITransport
from fastapi import status

from main import app


@pytest_asyncio.fixture
async def test_client():
    async with LifespanManager(app):
        async with httpx.AsyncClient(transport=ASGITransport(app=app), base_url="http://localhost:8000/v1/api") as test_client:
            yield test_client


@pytest.mark.asyncio
async def test_hello(test_client: httpx.AsyncClient):
    response = await test_client.get("/hello")
    assert response.status_code == status.HTTP_200_OK

    json = response.json()
    assert "message" in json
    assert json["message"] == "hello from api v1!!"
    assert "current_time" in json

    # Validate current_time is a valid datetime string
    try:
        datetime.fromisoformat(json["current_time"])
    except ValueError:
        pytest.fail("current_time is not a valid datetime string")