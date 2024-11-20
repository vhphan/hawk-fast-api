import pytest
from fastapi.testclient import TestClient
from routes.v1.api import router

class TestHello:

    @classmethod
    def setup_class(cls):
        cls.client = TestClient(router)

    def test_get_hello_status_code(self):
        response = self.client.get("/hello")
        assert response.status_code == 200

    def test_unexpected_http_methods(self):
        post_response = self.client.post("/hello")
        delete_response = self.client.delete("/hello")
        assert post_response.status_code == 405
        assert delete_response.status_code == 405