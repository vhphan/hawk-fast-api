from fastapi.testclient import TestClient
from main import app

import pytest

class TestRoot:

    @pytest.mark.asyncio
    async def test_root_endpoint_status_code(self):
        client = TestClient(app)
        response = client.get("/")
        assert response.status_code == 200

    @pytest.mark.asyncio
    async def test_root_endpoint_unexpected_methods(self):
        client = TestClient(app)
        response = client.post("/")
        assert response.status_code == 405
