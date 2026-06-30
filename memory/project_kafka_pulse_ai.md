---
name: project-kafka-pulse-ai
description: Kafka Pulse AI — an MCP-based AI SRE agent that diagnoses Kafka topic consumption issues using Azure OpenAI + kafka-python
metadata:
  type: project
---

Kafka Pulse AI is a local Python project at C:\Users\anuja.das\intellij-workspace\kafka-pulse-ai.

**Why:** Demo project showing Anthropic MCP usage + Azure OpenAI agentic tool calling for autonomous Kafka SRE investigation.

**Files:**
- `src/config.py` — Kafka broker URL, Azure OpenAI endpoint/key/deployment, API version
- `src/kafka_mcp_server.py` — MCP server exposing `list_consumer_groups`, `get_consumer_group_lag`, `get_consumer_status` via kafka-python KafkaAdminClient
- `src/agent.py` — Azure OpenAI agent that drives investigation via MCP and produces structured diagnosis with per-partition table
- `src/main.py` — Entrypoint

**How to apply:** Project intentionally avoids LangChain, LangGraph, Docker, databases, and cloud services. Uses kafka-python (not CLI subprocesses).

**Constraints:**
- Windows machine, Kafka at localhost:9092
- Azure OpenAI credentials must be set in config.py
- venv at .venv
