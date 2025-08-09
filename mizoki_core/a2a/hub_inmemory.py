from typing import Dict, Callable, Awaitable
from .models import A2AMessage, AgentCard

class InMemoryA2A:
    def __init__(self):
        self.agents: Dict[str, AgentCard] = {}
        self.handlers: Dict[str, Callable[[A2AMessage], Awaitable[dict]]] = {}

    def register(self, card: AgentCard, handler):
        self.agents[card.name] = card
        self.handlers[card.name] = handler

    async def send(self, msg: A2AMessage) -> dict:
        handler = self.handlers.get(msg.receiver)
        if not handler:
            raise KeyError(f"receiver {msg.receiver} not registered")
        return await handler(msg)