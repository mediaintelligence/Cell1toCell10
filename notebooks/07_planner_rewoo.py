#%% planner demo
import nest_asyncio; nest_asyncio.apply()
import asyncio
from mizoki_core.orchestration.react_planner import plan_and_execute
from mizoki_core.clients.local import LocalClients
from mizoki_core.a2a.hub_inmemory import InMemoryA2A
from mizoki_core.kg.client import KGClient

hub = InMemoryA2A()
clients = LocalClients(hub=hub, tools={"default_tool": lambda p: {"ok": True, **p}}, kg=KGClient())

payload = {"goal": "optimize campaign budget"}
print(asyncio.run(plan_and_execute(payload, clients)))