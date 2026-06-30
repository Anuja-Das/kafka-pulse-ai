---
name: feedback-diagnosis-output
description: User prefers diagnosis output to include per-partition breakdown table with a LAG marker — validated approach
metadata:
  type: feedback
---

Print per-partition status directly from already-collected `lag_data` rather than making an extra tool call or asking the AI to format it.

**Why:** The `get_consumer_group_lag` result already contains `partitions` with all needed fields; adding a table in the print block is zero-cost and makes the diagnosis immediately actionable.

**How to apply:** When adding new diagnostic detail to the DIAGNOSIS block, prefer rendering it from data already in hand (e.g., `lag_data`) rather than introducing new tool calls or AI formatting. Use a `<-- LAG` style inline flag to highlight the offending row.
