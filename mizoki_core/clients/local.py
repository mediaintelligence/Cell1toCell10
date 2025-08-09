from typing import Dict, Any, List
from ..a2a.models import TaskSpec, ExpertSpec
from ..policy.router_moe import route as moe_route
from ..policy.aggregate_moa import aggregate as moa_aggregate

class LocalClients:
    def __init__(self, hub=None, tools=None, kg=None):
        self.hub = hub
        self.tools = tools or {}
        self.kg = kg

    async def call_moe(self, task: TaskSpec, experts: List[ExpertSpec]):
        selected = moe_route(task, experts, k=3)
        return {"selected": [e.id for e in selected]}

    async def call_moa(self, candidates: List[Dict[str, Any]]):
        return moa_aggregate(candidates)

    async def kg_query(self, q: Dict[str, Any]):
        if self.kg:
            return await self.kg.query(q)
        return {"hits": []}

    async def tool_call(self, name: str, payload: Dict[str, Any]):
        fn = self.tools.get(name)
        if not fn:
            return {"error": f"tool {name} not found"}
        return await fn(payload)