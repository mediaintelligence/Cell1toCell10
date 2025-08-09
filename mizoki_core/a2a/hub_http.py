import httpx
from .models import A2AMessage

class HttpA2A:
    def __init__(self, hub_url: str, timeout: float = 30.0):
        self.hub_url = hub_url.rstrip('/')
        self.timeout = timeout

    async def send(self, msg: A2AMessage) -> dict:
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            r = await client.post(f"{self.hub_url}/a2a/send", json=msg.model_dump())
            r.raise_for_status()
            return r.json()