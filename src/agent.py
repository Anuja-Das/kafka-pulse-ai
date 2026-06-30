import asyncio
import json
import sys
from pathlib import Path

from mcp import ClientSession
from mcp.client.stdio import StdioServerParameters, stdio_client
from openai import AzureOpenAI
from openai.types.chat import ChatCompletionMessageParam

import config

# Only get_consumer_status is exposed to the AI — lag data is passed in directly from Phase 2.
_INVESTIGATION_TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "get_consumer_status",
            "description": "Check whether a consumer group has active member processes.",
            "parameters": {
                "type": "object",
                "properties": {
                    "consumer_group": {"type": "string"}
                },
                "required": ["consumer_group"],
            },
        },
    },
]


async def _call(session: ClientSession, tool: str, args: dict) -> dict:
    result = await session.call_tool(tool, args)
    return json.loads(result.content[0].text)


async def _investigate(session: ClientSession, group: str, lag_data: dict):
    """Invoke Azure OpenAI to diagnose a consumer group that has lag > 0."""
    ai = AzureOpenAI(
        api_key=config.API_KEY,
        azure_endpoint=config.AZURE_ENDPOINT,
        api_version=config.API_VERSION,
    )

    total_lag = lag_data.get("total_lag", 0)

    messages: list[ChatCompletionMessageParam] = [
        {
            "role": "system",
            "content": (
                "You are a Kafka SRE engineer. Lag data has already been collected and is provided below. "
                "Call get_consumer_status to check consumer liveness, then deliver a diagnosis in exactly "
                "this format — no markdown, no bullet points, no extra sections:\n\n"
                "Summary: <one sentence>\n"
                "Evidence: lag=<n>, consumer_active=<true|false>, affected_partitions=[<partition numbers with lag>]\n"
                "Root Cause: <1-2 sentences max>\n"
                "Recommendation: <1-2 sentences max>"
            ),
        },
        {
            "role": "user",
            "content": (
                f"Consumer group '{group}' has a total lag of {total_lag}.\n\n"
                f"Lag breakdown (already collected):\n{json.dumps(lag_data, indent=2)}\n\n"
                "Now call get_consumer_status to check if consumers are active, then provide your diagnosis."
            ),
        },
    ]

    print(f"\n[ai] Investigating '{group}' (lag={total_lag}) ...")

    while True:
        response = ai.chat.completions.create(
            model=config.AZURE_DEPLOYMENT,
            messages=messages,
            tools=_INVESTIGATION_TOOLS,
            tool_choice="auto",
        )
        choice = response.choices[0]
        messages.append(choice.message)

        if choice.finish_reason == "tool_calls":
            for tc in choice.message.tool_calls:
                fn = tc.function.name
                fn_args = json.loads(tc.function.arguments)
                print(f"[tool call]   {fn}({json.dumps(fn_args)})")
                tool_result = await _call(session, fn, fn_args)
                print(f"[tool result]\n{json.dumps(tool_result, indent=2)}")
                messages.append({
                    "role": "tool",
                    "tool_call_id": tc.id,
                    "content": json.dumps(tool_result),
                })
        else:
            print("\n" + "=" * 60)
            print(f"  DIAGNOSIS: {group}")
            print("=" * 60)
            print(choice.message.content)

            partitions = lag_data.get("partitions", [])
            if partitions:
                print()
                print("Partition Status:")
                hdr = f"  {'TOPIC':<30} {'PART':>4}  {'COMMITTED':>10}  {'LOG-END':>10}  {'LAG':>5}  CONSUMER"
                print(hdr)
                print("  " + "-" * (len(hdr) - 2))
                for p in partitions:
                    flag = " <-- LAG" if p["lag"] > 0 else ""
                    print(
                        f"  {p['topic']:<30} {p['partition']:>4}  "
                        f"{p['current_offset']:>10}  {p['log_end_offset']:>10}  "
                        f"{p['lag']:>5}  {p['consumer_id']}{flag}"
                    )

            print("=" * 60)
            break


async def run_investigation():
    server_params = StdioServerParameters(
        command=sys.executable,
        args=[str(Path(__file__).parent / "kafka_mcp_server.py")],
    )

    print("=" * 60)
    print("  Kafka Pulse AI — SRE Agent")
    print("=" * 60)
    print(f"  Broker: {config.KAFKA_BROKER}")
    print("=" * 60)

    async with stdio_client(server_params) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()

            # ── Phase 1: discover all consumer groups ──────────────────────
            print("\n[scan] Discovering consumer groups ...")
            groups_result = await _call(session, "list_consumer_groups", {})
            groups = groups_result.get("consumer_groups", [])

            if not groups:
                print("[scan] No consumer groups found on the broker.")
                return

            print(f"[scan] Found {len(groups)} group(s): {', '.join(groups)}")

            # ── Phase 2: check lag for every group ─────────────────────────
            print("\n[scan] Checking lag ...")
            needs_investigation = []

            for group in groups:
                lag_data = await _call(session, "get_consumer_group_lag", {"consumer_group": group})
                total_lag = lag_data.get("total_lag", 0)
                if total_lag == 0:
                    print(f"  ✓  {group:<40} lag=0   HEALTHY")
                else:
                    print(f"  !  {group:<40} lag={total_lag}  NEEDS INVESTIGATION")
                    needs_investigation.append((group, lag_data))

            if not needs_investigation:
                print("\n[result] All consumer groups are healthy. No AI investigation needed.")
                return

            # ── Phase 3: AI investigation only for groups with lag > 0 ─────
            print(f"\n[ai] {len(needs_investigation)} group(s) require investigation.")
            for group, lag_data in needs_investigation:
                await _investigate(session, group, lag_data)


if __name__ == "__main__":
    asyncio.run(run_investigation())
