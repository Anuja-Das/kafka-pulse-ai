# Kafka Pulse AI — Application Flow

## What it does

Automatically discovers every Kafka consumer group on the broker, checks which ones are lagging behind, and uses an AI (Azure OpenAI) to diagnose and explain the problem — without you having to specify a topic or group name upfront.

---

## Components

### Agent

| Name | File | Role |
|---|---|---|
| Kafka Pulse AI SRE Agent | `src/agent.py` | MCP client + Azure OpenAI orchestrator — drives all three investigation phases |

### MCP Server

| Name | File | Role |
|---|---|---|
| `kafka-pulse` | `src/kafka_mcp_server.py` | Exposes Kafka data as callable tools over the MCP protocol (stdio) |

### MCP Tools (exposed by `kafka-pulse` server)

| Tool name | What it returns | kafka-python calls used |
|---|---|---|
| `list_consumer_groups` | All consumer group names on the broker | `KafkaAdminClient.list_groups()` |
| `get_consumer_group_lag` | Total lag + per-partition breakdown with consumer_id, host, client_id | `list_group_offsets()` · `list_partition_offsets(OffsetSpec.LATEST)` · `describe_groups()` |
| `get_consumer_status` | Group state, active member count, member details | `KafkaAdminClient.describe_groups()` |

> **Phase 1 (scan)** uses `list_consumer_groups` directly (not via AI).
> **Phase 2 (lag check)** uses `get_consumer_group_lag` directly (not via AI).
> **Phase 3 (AI investigation)** gives the AI access to `get_consumer_group_lag` and `get_consumer_status` only.

### Kafka Client

| Library | Class | Used in |
|---|---|---|
| `kafka-python` | `KafkaAdminClient` | `src/kafka_mcp_server.py` — all broker communication, no CLI or subprocess |

### Supporting Files

| File | Role |
|---|---|
| `src/main.py` | Entry point — calls `run_investigation()` |
| `src/config.py` | Kafka broker address and Azure OpenAI credentials |
| `src/util/config_loader.py` | Loads `resources/application.yml` + `.env` into config |
| `src/util/llm_adapter.py` | Thin wrapper around the Azure OpenAI client |
| `src/prompts.py` | System prompt builders |
| `resources/application.yml` | YAML config with `${ENV_VAR}` placeholders |

---

## Two-process architecture

When you run the app, **two Python processes** start:

```
┌─────────────────────────────┐        MCP protocol (stdio)       ┌──────────────────────────────┐
│  agent.py  (MCP client)     │  ──────────────────────────────►  │  kafka_mcp_server.py         │
│                             │                                    │  (MCP server: kafka-pulse)   │
│  - Drives the investigation │  ◄──────────────────────────────  │  - Uses KafkaAdminClient     │
│  - Calls Azure OpenAI       │        JSON tool results           │  - Returns structured JSON   │
└─────────────────────────────┘                                    └──────────────────────────────┘
                                                                              │
                                                                   kafka-python (TCP)
                                                                              │
                                                                   ┌──────────────────┐
                                                                   │  Kafka Broker    │
                                                                   │  localhost:9092  │
                                                                   └──────────────────┘
```

`agent.py` is the client that asks questions. `kafka_mcp_server.py` is the server that queries the Kafka broker using the kafka-python `KafkaAdminClient` and returns structured JSON — no CLI commands, no subprocess.

---

## Flow step-by-step

### Phase 1 — Discover all consumer groups

```
python src/main.py
    │
    └─► agent.py calls tool: list_consumer_groups
            │
            └─► kafka_mcp_server.py:
                    KafkaAdminClient.list_groups()
                    │
                    └─► returns: ["order-processor", "payment-consumer", ...]
```

Console output:
```
[scan] Discovering consumer groups ...
[scan] Found 2 group(s): order-processor, payment-consumer
```

---

### Phase 2 — Check lag for each group

For every group found, the agent calls `get_consumer_group_lag`:

```
agent.py calls tool: get_consumer_group_lag("order-processor")
    │
    └─► kafka_mcp_server.py:
            KafkaAdminClient.list_group_offsets("order-processor")    → committed offsets per partition
            KafkaAdminClient.list_partition_offsets(OffsetSpec.LATEST) → end offsets per partition
            lag = end_offset − committed_offset  (per partition)
            │
            └─► returns: { total_lag: 42, partitions: [...] }
```

Groups with **lag = 0** are marked healthy and skipped. Groups with **lag > 0** are queued for AI investigation.

```
[scan] Checking lag ...
  ✓  payment-consumer                         lag=0   HEALTHY
  !  order-processor                          lag=42  NEEDS INVESTIGATION
```

---

### Phase 3 — AI investigates laggy groups

For each group with lag, the agent opens a tool-calling loop with Azure OpenAI:

```
agent.py → Azure OpenAI
    "Consumer group 'order-processor' has lag of 42. Use tools to investigate."

Azure OpenAI decides to call tools:
    → get_consumer_group_lag("order-processor")   ← per-partition lag + consumer_id/host/client_id
    → get_consumer_status("order-processor")      ← group state, active members

Each tool call is routed to kafka_mcp_server.py and returns formatted JSON.

Azure OpenAI sees the data and produces a plain-text diagnosis:
    ┌──────────────────────────────────────────────────────┐
    │  DIAGNOSIS: order-processor                          │
    │  Summary: Consumer group has lag, no active members. │
    │  Evidence: lag=42, consumer_active=false             │
    │  Root Cause: No consumer is running for this group.  │
    │  Recommendation: Restart the consumer process.       │
    └──────────────────────────────────────────────────────┘
```

The AI loop continues until the model stops requesting tool calls and delivers a final diagnosis.

---

## The 3 outcomes the AI diagnoses

| Lag | Consumer active? | Diagnosis |
|-----|-----------------|-----------|
| 0 | Any | Healthy — no action needed |
| > 0 | No (`state: Empty`) | Consumer not running — restart it |
| > 0 | Yes (`state: Stable`) | Consumer alive but too slow — scale out or debug |

---

## Config and credentials

- **Kafka broker** address is set in `src/config.py` (`KAFKA_BROKER`)
- **Azure OpenAI credentials** (`API_KEY`, `AZURE_ENDPOINT`, `API_VERSION`, `AZURE_DEPLOYMENT`) are loaded from `.env` via `src/util/config_loader.py`
- The YAML file `resources/application.yml` holds config with `${ENV_VAR}` placeholders resolved at startup
