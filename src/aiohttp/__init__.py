class ClientResponse:
    def __init__(self, status: int = 200, headers: dict | None = None, json_data=None):
        self.status = status
        self.headers = headers or {}
        self._json = json_data or {}

    async def json(self):
        return self._json


class ClientSession:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        pass

    async def get(self, *args, **kwargs):  # pragma: no cover - replaced in tests
        return ClientResponse()

    async def post(self, *args, **kwargs):  # pragma: no cover - replaced in tests
        return ClientResponse()
