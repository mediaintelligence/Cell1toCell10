import httpx
from typing import Dict, Any, List
from ..a2a.models import TaskSpec, ExpertSpec

class HttpClients:
    def __init__(self, base_urls: Dict[str, str], timeout: float = 30.0):
        self.base = {k: v.rstrip('/') for k, v in base_urls.items()}
        self.timeout = timeout

    async def call_moe(self, task: TaskSpec, experts: List[ExpertSpec]):
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            r = await client.post(self.base["cell18"] + "/moe/route",
                                  json={"task": task.model_dump(),
                                        "experts": [e.model_dump() for e in experts]})
            r.raise_for_status()
            return r.json()

    async def call_moa(self, candidates: List[Dict[str, Any]]):
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            r = await client.post(self.base["cell19"] + "/agg/score", json=candidates)
            r.raise_for_status()
            return r.json()