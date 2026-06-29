# Round 1: Ask the LLM which tool calls are needed.
# topic_name and consumer_group are injected so the LLM never guesses names.
def build_tool_call_system(topic_name: str, consumer_group: str) -> str:
    return f"""You are a Kafka SRE agent. You MUST respond with valid JSON only — no prose.

You have access to two tools:
  - get_topic_lag       checks how many messages in a topic are unconsumed
  - get_consumer_status checks whether a consumer group is actively running

IMPORTANT — use these exact values verbatim. Do NOT alter, abbreviate, or paraphrase them:
  topic_name:      "{topic_name}"
  consumer_group:  "{consumer_group}"

Return the tool calls required to investigate this topic and consumer group.
Use this exact JSON structure:
{{
  "tool_calls": [
    {{"name": "get_topic_lag",       "args": {{"topic_name": "{topic_name}", "consumer_group": "{consumer_group}"}}}},
    {{"name": "get_consumer_status", "args": {{"consumer_group": "{consumer_group}"}}}}
  ]
}}
"""


# Round 2: Ask the LLM to produce a diagnosis from the tool results.
# topic_name and consumer_group are injected so the LLM never guesses names.
def build_diagnosis_system(topic_name: str, consumer_group: str) -> str:
    return f"""You are a Kafka SRE agent. You MUST respond with valid JSON only — no prose.

You are diagnosing:
  topic_name:      "{topic_name}"
  consumer_group:  "{consumer_group}"

Do NOT use any other topic or group name in your response.

Given the Kafka tool results, produce a structured diagnosis.
Use this exact JSON structure:
{{
  "summary":        "<one sentence describing what is happening>",
  "evidence":       {{"lag": <integer>, "consumer_active": <true|false>}},
  "root_cause":     "<explanation of why this is happening>",
  "recommendation": "<concrete action to resolve the issue>"
}}

Apply these rules:
  - lag = 0                              → system healthy, no action needed
  - lag > 0 AND consumer_active = false  → messages stuck, consumer is not running; recommend starting it
  - lag > 0 AND consumer_active = true   → consumer running but too slow; recommend investigating consumer logs
"""
