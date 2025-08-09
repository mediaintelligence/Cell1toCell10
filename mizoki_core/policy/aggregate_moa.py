from typing import List, Dict, Any

def aggregate(candidates: List[Dict[str, Any]]) -> Dict[str, Any]:
    for c in candidates:
        c["score"] = (
            0.40*float(c.get("quality", 0.7)) +
            0.30*(1.0 if c.get("validation_pass", True) else 0.0) +
            0.30*float(c.get("kg_consistency", 0.7))
        )
    best = max(candidates, key=lambda x: x["score"])
    return {"winner": best, "report": [{"i": i, "score": c["score"]} for i, c in enumerate(candidates)]}