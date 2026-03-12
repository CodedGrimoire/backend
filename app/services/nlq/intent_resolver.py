from typing import Dict
import re


def resolve_query_intent(question: str) -> Dict:
    q = question.lower()
    intent: Dict = {}

    if any(k in q for k in ["average", "avg", "mean"]):
        intent["metric"] = "avg"
        intent["type"] = "aggregation"
    if any(k in q for k in ["total", "sum"]):
        intent["metric"] = "sum"
        intent["type"] = "aggregation"
    if any(k in q for k in ["count", "how many"]):
        intent["metric"] = "count"
        intent["type"] = "aggregation"
    if any(k in q for k in ["maximum", "highest", "max"]):
        intent["metric"] = "max"
        intent["type"] = "aggregation"
    if any(k in q for k in ["minimum", "lowest", "min"]):
        intent["metric"] = "min"
        intent["type"] = "aggregation"

    if any(k in q for k in [" by ", " per ", "grouped by"]):
        intent["group_by"] = True
        intent["type"] = intent.get("type", "aggregation_group")

    m = re.search(r"top\\s+(\\d+)", q)
    if m:
        intent["limit"] = int(m.group(1))
        intent["type"] = intent.get("type", "ranking")
    if any(k in q for k in ["top", "highest", "lowest", "best", "worst"]):
        intent["type"] = intent.get("type", "ranking")

    return intent
