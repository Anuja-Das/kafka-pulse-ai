---
name: feedback-env-file
description: Never read .env — always read .env.example for environment variable reference
metadata:
  type: feedback
---

Never read the `.env` file. Always read `.env.example` instead when checking what environment variables are needed or expected.

**Why:** `.env` contains real secrets and should not be read or referenced. `.env.example` is the safe, committed reference copy.

**How to apply:** Any time a task involves checking env vars, credential keys, or config structure — open `.env.example`, not `.env`.
