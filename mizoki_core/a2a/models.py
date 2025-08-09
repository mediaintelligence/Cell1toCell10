from pydantic import BaseModel, Field
from typing import Any, Dict, List, Optional
from datetime import datetime

class A2AMessage(BaseModel):
    v: str = "2025.08"
    trace_id: str
    sender: str
    receiver: str
    intent: str
    payload: Dict[str, Any]
    ttl: int = 300
    ts: datetime = Field(default_factory=datetime.utcnow)

class AgentCard(BaseModel):
    name: str
    version: str
    description: str
    capabilities: List[str]
    endpoint: Optional[str] = None

class TaskSpec(BaseModel):
    goal: str
    context: Dict[str, Any] = {}
    budget: Dict[str, Any] = {"usd": 0.01, "latency_ms": 1000}

class ExpertSpec(BaseModel):
    id: str
    skills: List[str]
    meta: Dict[str, Any] = {}