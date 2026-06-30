import json


def investigation_system_prompt() -> str:
    """System prompt for Phase 3 AI investigation."""
    return (
        "You are a Kafka SRE engineer. Lag data has already been collected and is provided below. "
        "Call get_consumer_status to check consumer liveness, then deliver a diagnosis in exactly "
        "this format — no markdown, no bullet points, no extra sections:\n\n"
        "Summary: <one sentence>\n"
        "Evidence: lag=<n>, consumer_active=<true|false>, affected_partitions=[<partition numbers with lag>]\n"
        "Root Cause: <1-2 sentences max>\n"
        "Recommendation: <1-2 sentences max>"
    )


def investigation_user_prompt(group: str, total_lag: int, lag_data: dict) -> str:
    """User message for Phase 3 AI investigation; injects already-collected lag data."""
    return (
        f"Consumer group '{group}' has a total lag of {total_lag}.\n\n"
        f"Lag breakdown (already collected):\n{json.dumps(lag_data, indent=2)}\n\n"
        "Now call get_consumer_status to check if consumers are active, then provide your diagnosis."
    )
