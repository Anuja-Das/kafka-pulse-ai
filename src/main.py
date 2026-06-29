#!/usr/bin/env python3
# Entry point
#
# Usage:
#   python src/main.py

import asyncio
import sys
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.agent import run_investigation


def main() -> None:
    asyncio.run(run_investigation())


if __name__ == "__main__":
    main()
