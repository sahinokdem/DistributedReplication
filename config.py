#!/usr/bin/env python3
"""
Shared connection settings for the replication experiments.

Values are read from environment variables. For local runs, copy
.env.example to .env and fill it in -- .env is git-ignored.
No external dependency: the tiny loader below is enough.
"""
import os
from pathlib import Path


def _load_dotenv():
    """Load KEY=VALUE lines from a .env file next to this script."""
    env_file = Path(__file__).with_name(".env")
    if not env_file.exists():
        return
    for line in env_file.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip("'\""))


_load_dotenv()


def _require(name):
    value = os.environ.get(name)
    if not value:
        raise SystemExit(
            f"Missing environment variable: {name}\n"
            f"Copy .env.example to .env and fill it in, or export the variable."
        )
    return value


LEADER = dict(
    host=_require("LEADER_HOST"),
    dbname=os.environ.get("PG_DBNAME", "postgres"),
    user=_require("PG_USER"),
    password=_require("PG_PASSWORD"),
)

FOLLOWER = dict(
    host=_require("FOLLOWER_HOST"),
    dbname=os.environ.get("PG_DBNAME", "postgres"),
    user=_require("PG_USER"),
    password=_require("PG_PASSWORD"),
)