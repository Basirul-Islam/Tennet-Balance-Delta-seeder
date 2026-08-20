"""
Non-secret configuration. Safe to commit.

Hosts and credentials are NOT here. They live in `secrets.local.json` (gitignored), loaded at
import time. `secrets.example.json` is the committed template showing the shape.

Only `Local` is built in, pointing at a passwordless localhost StarRocks, so a fresh clone runs
without any setup. Every other environment comes from the secrets file — if the file is absent,
those environments simply do not appear in the UI rather than failing at connect time.

Point `SEEDER_SECRETS` at a different file to override the location.
"""

from __future__ import annotations

import json
import os

APP_DIR = os.path.dirname(os.path.abspath(__file__))
SECRETS_FILE = os.environ.get("SEEDER_SECRETS") or os.path.join(APP_DIR, "secrets.local.json")

# ── Source ────────────────────────────────────────────────────────────────────────────

# Only the starting default — the folder is chosen in the UI and remembered in
# .seeder-state.json. Override with "source_dir" in the secrets file if you want a different
# one on first run.
SOURCE_DIR = os.path.join(APP_DIR, "data")

DATABASE = "ScalarStarrocks"
TABLE = "TennetBalanceDelta"

# ── Run tuning ────────────────────────────────────────────────────────────────────────

# Rows per Stream Load request. Small and sequential by design: each batch completes
# before the next is built, so the destination never sees concurrent load pressure.
BATCH_ROWS = 1_000

# Breather between batches, seconds.
BATCH_PAUSE_SECONDS = 0.1

# Stream Load server-side timeout, seconds (also used as the HTTP client timeout).
STREAM_LOAD_TIMEOUT = 600

# Per-batch retries on connection errors / timeouts / HTTP 5xx. The label is reused
# across retries so a lost response replays as "Label Already Exists" rather than
# double-loading.
MAX_BATCH_RETRIES = 3

# Batch sizes offered in the UI.
BATCH_SIZE_CHOICES = [1_000, 5_000, 10_000, 25_000]

DEFAULT_ENVIRONMENT = "Local"

# ── Environments ──────────────────────────────────────────────────────────────────────
#
# fe_host / http_port  -> StarRocks FE, used for the Stream Load PUT.
# be_host              -> substituted into the FE's 307 redirect, because the FE hands back
#                         an internal BE hostname that is not resolvable from here.
# mysql_port           -> MySQL protocol, used ONLY for read-only queries (see starrocks.py).
# confirm_phrase       -> when set, the UI requires the user to type it before Start Sync.

_ENVIRONMENT_DEFAULTS = {
    "http_port": 8030,
    "mysql_port": 9030,
    "user": "root",
    "password": "",
    "database": DATABASE,
    "confirm_phrase": None,
    "note": "",
}

_BUILTIN_ENVIRONMENTS = {
    "Local": dict(
        _ENVIRONMENT_DEFAULTS,
        fe_host="127.0.0.1",
        be_host="127.0.0.1",
        note="Local docker stack (starrocks-fe / starrocks-be). Built in - no secrets needed.",
    ),
}

# Populated by _load(): describes what happened, for the UI to show.
SECRETS_STATUS: dict = {}


def _load() -> dict:
    """
    Merge the secrets file over the built-in environments.

    A missing file is normal, not an error: it means only Local is available. A malformed file
    IS an error worth surfacing, because silently falling back to Local would look like the
    secrets simply had not been picked up.
    """
    environments = {name: dict(cfg) for name, cfg in _BUILTIN_ENVIRONMENTS.items()}
    status = {"file": SECRETS_FILE, "loaded": False, "error": None, "from_file": []}

    if not os.path.isfile(SECRETS_FILE):
        status["error"] = None
        SECRETS_STATUS.update(status)
        return environments

    try:
        with open(SECRETS_FILE, "r", encoding="utf-8") as fh:
            payload = json.load(fh)
    except (OSError, ValueError) as exc:
        status["error"] = f"Could not read {os.path.basename(SECRETS_FILE)}: {exc}"
        SECRETS_STATUS.update(status)
        return environments

    global SOURCE_DIR
    if payload.get("source_dir"):
        SOURCE_DIR = os.path.expandvars(os.path.expanduser(str(payload["source_dir"])))

    for name, raw in (payload.get("environments") or {}).items():
        if not isinstance(raw, dict):
            continue
        cfg = dict(_ENVIRONMENT_DEFAULTS)
        cfg.update(environments.get(name, {}))
        cfg.update(raw)
        cfg.setdefault("be_host", cfg.get("fe_host"))
        if not cfg.get("fe_host"):
            status["error"] = f"Environment '{name}' in the secrets file has no fe_host."
            continue
        environments[name] = cfg
        status["from_file"].append(name)

    status["loaded"] = True
    SECRETS_STATUS.update(status)
    return environments


ENVIRONMENTS = _load()
