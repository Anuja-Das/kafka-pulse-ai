def banner(topic: str, group: str) -> None:
    width = 60
    print("=" * width)
    print("  Kafka Pulse AI — SRE Agent")
    print("=" * width)
    print(f"  Topic:          {topic}")
    print(f"  Consumer Group: {group}")
    print("=" * width)
    print()


def print_diagnosis(d: dict) -> None:
    print("\n" + "=" * 60)
    print("  DIAGNOSIS")
    print("=" * 60)
    print(f"\nSummary:\n{d.get('summary', 'N/A')}")
    evidence = d.get("evidence", {})
    print(f"\nEvidence:")
    print(f"  Lag              = {evidence.get('lag', 'N/A')}")
    print(f"  Consumer active  = {evidence.get('consumer_active', 'N/A')}")
    print(f"\nRoot Cause:\n{d.get('root_cause', 'N/A')}")
    print(f"\nRecommendation:\n{d.get('recommendation', 'N/A')}")
    print("\n" + "=" * 60 + "\n")
