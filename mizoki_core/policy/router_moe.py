from typing import List
from ..a2a.models import TaskSpec, ExpertSpec

def _score(expert: ExpertSpec, task: TaskSpec) -> float:
    skill_fit = 1.0 if any(s in task.goal.lower() for s in [sk.lower() for sk in expert.skills]) else 0.4
    success_rate = float(expert.meta.get("success_rate", 0.8))
    cost = float(task.budget.get("usd", 0.01))
    latency = float(task.budget.get("latency_ms", 1000)) / 1000.0
    return 0.45*skill_fit + 0.35*success_rate - 0.10*cost - 0.10*latency

def route(task: TaskSpec, experts: List[ExpertSpec], k: int = 3) -> List[ExpertSpec]:
    return sorted(experts, key=lambda e: _score(e, task), reverse=True)[:k]