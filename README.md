# Kafka Pulse AI — SRE Agent

An autonomous AI agent that discovers all Kafka consumer groups, detects which ones are lagging, and uses Azure OpenAI to deliver a structured diagnosis — no manual topic or group name required.

---

## Architecture

```
python src/main.py
       │
       ▼
 agent.py  (MCP client + Azure OpenAI)
       │  MCP protocol over stdio
       ▼
 kafka_mcp_server.py  (MCP server: "kafka-pulse")
 ├── list_consumer_groups
 ├── get_consumer_group_lag
 └── get_consumer_status
       │  kafka-python KafkaAdminClient (TCP)
       ▼
 Kafka Broker  localhost:9092
```

No Kafka CLI. No subprocess. All broker communication goes through the `kafka-python` client library directly.

---

## Prerequisites

| Requirement | Notes |
|---|---|
| Python 3.10+ | `python --version` |
| Apache Kafka running locally | Broker at `localhost:9092` |
| Azure OpenAI deployment | GPT-4o or equivalent |

---

## Project Structure

```
kafka-pulse-ai/
├── src/
│   ├── main.py              ← entry point
│   ├── agent.py             ← MCP client + Azure OpenAI orchestrator
│   ├── kafka_mcp_server.py  ← MCP server exposing 3 Kafka tools
│   ├── config.py            ← flat constants for broker address + Azure OpenAI credentials
│   ├── prompts.py           ← AI prompt functions used in Phase 3 investigation
│   └── util/
│       └── config_loader.py ← loads application.yml + .env, resolves ${ENV_VAR} placeholders
├── resources/
│   └── application.yml      ← YAML config with ${ENV_VAR} placeholders
├── .env                     ← secrets (never commit this)
├── requirements.txt
├── APPLICATION-FLOW.md      ← detailed architecture and flow walkthrough
└── README.md
```

---

## Step 1 — Install Dependencies

```powershell
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

---

## Step 2 — Configure Azure OpenAI

Create a `.env` file in the project root:

```
API_KEY=<your-azure-openai-key>
AZURE_ENDPOINT=https://<your-resource>.openai.azure.com/
API_VERSION=2024-02-01
AZURE_DEPLOYMENT=gpt-4o
```

---

## Step 3 — Configure Kafka Broker

Open `resources/application.yml` and verify the broker address:

```yaml
kafka_broker: localhost:9092   # change if your broker is elsewhere
```

---

## Step 4 — Verify Kafka Is Running

```powershell
# List topics
C:\kafka\bin\windows\kafka-topics.bat --bootstrap-server localhost:9092 --list

# List consumer groups
C:\kafka\bin\windows\kafka-consumer-groups.bat --bootstrap-server localhost:9092 --list
```

---

## Step 5 — Create a Test Topic (if needed)

```powershell
# Create topic
C:\kafka\bin\windows\kafka-topics.bat `
  --bootstrap-server localhost:9092 `
  --create --topic orders `
  --partitions 2 --replication-factor 1

# Produce some messages
C:\kafka\bin\windows\kafka-console-producer.bat `
  --bootstrap-server localhost:9092 --topic orders
```

Type a few lines, press Enter after each, then Ctrl+C.

---

## Step 6 — Run the Agent

```powershell
python src/main.py
```

The agent auto-discovers all consumer groups, checks lag, and investigates any group with lag > 0.

---

## Example Output

```
============================================================
  Kafka Pulse AI — SRE Agent
============================================================
  Broker: localhost:9092
============================================================

[scan] Discovering consumer groups ...
[scan] Found 2 group(s): order-processor, payment-consumer

[scan] Checking lag ...
  ✓  payment-consumer                         lag=0   HEALTHY
  !  order-processor                          lag=42  NEEDS INVESTIGATION

[ai] 1 group(s) require investigation.

[ai] Investigating 'order-processor' (lag=42) ...
[tool call]   get_consumer_status({"consumer_group": "order-processor"})
[tool result]
{
  "consumer_group": "order-processor",
  "state": "Empty",
  "active": false,
  "active_consumers": 0,
  "members": []
}

============================================================
  DIAGNOSIS: order-processor
============================================================
Summary: Consumer group 'order-processor' has lag of 42 with no active consumers.
Evidence: lag=42, consumer_active=false, affected_partitions=[0]
Root Cause: No consumer processes are connected — the group state is Empty, meaning messages are accumulating unconsumed.
Recommendation: Restart the consumer process for 'order-processor' and verify it reconnects to the broker.

Partition Status:
  TOPIC                          PART    COMMITTED     LOG-END    LAG  CONSUMER
  -------------------------------------------------------------------------------
  orders                            0           10          52     42  - <-- LAG
============================================================
```

---

## The 3 Diagnoses

| Lag | State | What it means |
|-----|-------|---------------|
| 0 | Any | Healthy — no action needed |
| > 0 | `Empty` | Consumer not running — restart it |
| > 0 | `Stable` | Consumer running but too slow — scale out or debug |

---

## Troubleshooting

**No consumer groups found**

No group has ever committed an offset to this broker. Start a consumer with `--group <name>` at least once.

**Lag shows 0 but CLI shows lag**

The broker state changed between runs. The tool always reports the live state at the moment it runs.

**Azure OpenAI error**

Check `.env` for correct `API_KEY`, `AZURE_ENDPOINT`, `API_VERSION`, and `AZURE_DEPLOYMENT` values. These are substituted into `resources/application.yml` at startup — a missing variable will raise a `KeyError` in `config_loader.py`.

**Group stays `Stable` after stopping consumer**

Kafka's session timeout defaults to 45 seconds. Wait ~60 seconds after stopping the consumer before re-running.
