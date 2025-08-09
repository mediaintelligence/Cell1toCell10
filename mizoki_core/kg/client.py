from typing import Dict, Any

class KGClient:
    def __init__(self, endpoint: str = "http://cell3-service"):
        self.endpoint = endpoint

    async def query(self, q: Dict[str, Any]):
        # TODO: httpx call to Cell3
        return {"hits": [{"id": "n1", "score": 0.82}]}