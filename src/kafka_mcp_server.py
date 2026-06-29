import asyncio
import json

from kafka import KafkaAdminClient, TopicPartition
from kafka.admin import OffsetSpec
from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp import types

import config

app = Server("kafka-pulse")


def _admin() -> KafkaAdminClient:
    return KafkaAdminClient(bootstrap_servers=config.KAFKA_BROKER)


def _member_to_partition_map(members: list) -> dict:
    """Build {TopicPartition -> {consumer_id, client_id, host}} from describe_groups members."""
    tp_map = {}
    for m in members:
        info = {
            "consumer_id": m.get("member_id") or "-",
            "client_id": m.get("client_id") or "-",
            "host": m.get("client_host") or "-",
        }
        assignment = m.get("member_assignment")
        if not assignment:
            continue
        try:
            if hasattr(assignment, "partitions"):
                # ConsumerProtocolAssignment object (not yet dict-ified)
                tps = assignment.partitions()
            elif isinstance(assignment, dict):
                # Already converted to dict: {'assigned_partitions': [{'topic': ..., 'partitions': [...]}]}
                tps = [
                    TopicPartition(ap["topic"], p)
                    for ap in assignment.get("assigned_partitions", [])
                    for p in ap.get("partitions", [])
                ]
            else:
                continue
            for tp in tps:
                tp_map[tp] = info
        except Exception:
            pass
    return tp_map


@app.list_tools()
async def handle_list_tools() -> list[types.Tool]:
    return [
        types.Tool(
            name="list_consumer_groups",
            description="List all consumer groups registered with the Kafka broker.",
            inputSchema={"type": "object", "properties": {}, "required": []},
        ),
        types.Tool(
            name="get_consumer_group_lag",
            description="Get total lag and per-partition breakdown for a consumer group.",
            inputSchema={
                "type": "object",
                "properties": {
                    "consumer_group": {"type": "string", "description": "Consumer group name"}
                },
                "required": ["consumer_group"],
            },
        ),
        types.Tool(
            name="get_consumer_status",
            description="Check whether a consumer group has active member processes.",
            inputSchema={
                "type": "object",
                "properties": {
                    "consumer_group": {"type": "string", "description": "Consumer group name"}
                },
                "required": ["consumer_group"],
            },
        ),
    ]


@app.call_tool()
async def handle_call_tool(name: str, arguments: dict) -> list[types.TextContent]:
    admin = _admin()
    try:
        if name == "list_consumer_groups":
            groups = admin.list_groups()
            group_ids = [
                g["group_id"] for g in groups
                if not g["group_id"].startswith("_")
            ]
            result = {"consumer_groups": group_ids, "count": len(group_ids)}

        elif name == "get_consumer_group_lag":
            group = arguments["consumer_group"]

            committed = admin.list_group_offsets(group).get(group, {})

            end_offsets = {}
            if committed:
                end_offsets = {
                    tp: ots.offset
                    for tp, ots in admin.list_partition_offsets(
                        {tp: OffsetSpec.LATEST for tp in committed}
                    ).items()
                }

            desc = admin.describe_groups([group])
            members = desc.get(group, {}).get("members", [])
            tp_member = _member_to_partition_map(members)

            partitions = []
            for tp, om in committed.items():
                committed_offset = om.offset if om.offset >= 0 else 0
                end = end_offsets.get(tp, committed_offset)
                lag = max(0, end - committed_offset)
                member = tp_member.get(tp, {})
                partitions.append({
                    "group": group,
                    "topic": tp.topic,
                    "partition": tp.partition,
                    "current_offset": committed_offset,
                    "log_end_offset": end,
                    "lag": lag,
                    "consumer_id": member.get("consumer_id", "-"),
                    "host": member.get("host", "-"),
                    "client_id": member.get("client_id", "-"),
                })

            result = {
                "consumer_group": group,
                "total_lag": sum(p["lag"] for p in partitions),
                "partitions": partitions,
            }

        elif name == "get_consumer_status":
            group = arguments["consumer_group"]
            desc = admin.describe_groups([group])
            group_info = desc.get(group, {})
            members = group_info.get("members", [])
            member_details = [
                {
                    "consumer_id": m.get("member_id") or "-",
                    "client_id": m.get("client_id") or "-",
                    "host": m.get("client_host") or "-",
                }
                for m in members
            ]
            result = {
                "consumer_group": group,
                "state": group_info.get("group_state") or "unknown",
                "active": len(member_details) > 0,
                "active_consumers": len(member_details),
                "members": member_details,
            }

        else:
            result = {"error": f"Unknown tool: {name}"}

    finally:
        admin.close()

    return [types.TextContent(type="text", text=json.dumps(result, indent=2))]


async def main():
    async with stdio_server() as (read_stream, write_stream):
        await app.run(read_stream, write_stream, app.create_initialization_options())


if __name__ == "__main__":
    asyncio.run(main())
