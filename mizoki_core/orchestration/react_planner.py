import uuid
from typing import Dict, Any, List
from ..a2a.models import TaskSpec, ExpertSpec

async def plan_and_execute(payload: Dict[str, Any], clients, svc_name: str = 'planner') -> Dict[str, Any]:
    trace_id = payload.get('trace_id', str(uuid.uuid4()))
    goal = payload.get('goal', 'analyze')

    kg_hits = await clients.kg_query({"q": goal})

    experts: List[ExpertSpec] = [
        ExpertSpec(id='exp_rag', skills=['retrieval','analysis']),
        ExpertSpec(id='exp_code', skills=['code','analysis']),
        ExpertSpec(id='exp_tool', skills=['tool','action']),
    ]
    moe = await clients.call_moe(TaskSpec(goal=goal), experts)
    selected = moe.get('selected', [])

    candidates = []
    for exp_id in selected:
        candidates.append({
            'output': {'expert': exp_id, 'answer': f"{exp_id} suggests action for {goal}"},
            'quality': 0.78,
            'validation_pass': True,
            'kg_consistency': 0.8,
        })

    agg = await clients.call_moa(candidates)
    winner = agg.get('winner', {})

    tool_name = 'default_tool'
    action_res = await clients.tool_call(tool_name, {'goal': goal, 'answer': winner.get('output', {})})

    return {
        'trace_id': trace_id,
        'kg_hits': kg_hits,
        'experts_selected': selected,
        'aggregation': agg,
        'action_result': action_res,
    }