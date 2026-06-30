# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Running the agent

```powershell
# Activate venv first
.\.venv\Scripts\Activate.ps1

# Run the agent
python src/main.py
```

## Installing dependencies

```powershell
pip install -r requirements.txt
```

## Environment setup

Copy `.env.example` to `.env` and fill in Azure OpenAI credentials:

```
API_KEY=
AZURE_ENDPOINT=
API_VERSION=2024-02-01
AZURE_DEPLOYMENT=gpt-4o
```

Kafka broker address is set in `resources/application.yml` (`kafka_broker: localhost:9092`). Change it there if your broker is elsewhere.

## Architecture

Two Python processes communicate over MCP (stdio):

```
python src/main.py
  └─► agent.py  (MCP client + Azure OpenAI orchestrator)
          │  MCP protocol over stdio
          └─► kafka_mcp_server.py  (MCP server: "kafka-pulse")
                  │  kafka-python KafkaAdminClient (TCP)
                  └─► Kafka Broker  localhost:9092
```

`agent.py` spawns `kafka_mcp_server.py` as a child process via `StdioServerParameters`. All Kafka access goes through `KafkaAdminClient` — no CLI, no subprocess calls to Kafka shell scripts.

## Investigation phases

`agent.py` runs three phases in sequence:

| Phase | Tool called | By whom |
|-------|-------------|---------|
| 1 — Discover groups | `list_consumer_groups` | agent directly (no AI) |
| 2 — Check lag | `get_consumer_group_lag` | agent directly (no AI) |
| 3 — AI investigation | `get_consumer_status` | Azure OpenAI tool-calling loop |

In Phase 3 the lag data collected in Phase 2 is injected into the AI's user message directly. The AI only has `get_consumer_status` available as a callable tool; it does not re-fetch lag. The diagnosis is printed as plain text followed by a per-partition table rendered from the already-collected `lag_data`.

## Config loading

`src/util/config_loader.py` loads `resources/application.yml` and resolves `${ENV_VAR}` placeholders from `.env`. `src/config.py` imports from it and exposes flat constants (`API_KEY`, `AZURE_ENDPOINT`, etc.) consumed by `agent.py` and `kafka_mcp_server.py`.

```
.env  →  config_loader.py (YAML + ${} substitution)  →  config.py  →  agent / server
```

## Prompts

All AI prompts live in `src/prompts.py`:

- `investigation_system_prompt()` — instructs the AI to act as a Kafka SRE and specifies the exact diagnosis output format
- `investigation_user_prompt(group, total_lag, lag_data)` — per-group user message that injects the already-collected lag data
