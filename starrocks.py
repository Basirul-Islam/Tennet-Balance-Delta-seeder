"""
StarRocks access for the seeder: a read-only MySQL-protocol helper and a Stream Load client.

Two hard rules are enforced here rather than merely intended:

1. The MySQL side cannot write. `query()` refuses any statement that is not SELECT / SHOW /
   DESCRIBE, so there is no DELETE, UPDATE, INSERT, TRUNCATE, ALTER or DROP path anywhere in
   this tool. The only thing that writes is `stream_load()`.

2. Stream Load returns HTTP 200 on a rejected load. The body's `Status` is the real answer,
   so it is always parsed and anything outside the accepted set raises.

The wire format mirrors Scalar.StarRocks/LoadApi/StarRocksStreamLoadApiClient.cs so seeded rows
are byte-identical in form to rows the ingestion pipeline writes.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from urllib.parse import urlparse, urlunparse

import pymysql
import requests
from requests.auth import HTTPBasicAuth

import config

# ── CSV wire format ───────────────────────────────────────────────────────────────────
# SOH (U+0001) as the column separator, matching the repo. StarRocks does not honour
# RFC4180 quoting unless `enclose` is set — it splits a quoted field on the separator anyway
# and rejects the row. A separator that cannot occur in the data removes the need to quote or
# escape at all, so the `\N` NULL sentinel also survives untouched.
#
# The two constants must stay in sync: the header tells StarRocks how to split, the delimiter
# controls what we emit. StarRocks decodes the literal 4-character string "\x01" as 0x01.
CSV_COLUMN_SEPARATOR_HEADER = r"\x01"
CSV_DELIMITER = ""

NULL_SENTINEL = r"\N"

# Stream Load body statuses that mean the rows are safe.
#   Success              — loaded and visible.
#   Publish Timeout      — committed; visibility is still catching up. The data WILL appear.
#   Label Already Exists — this exact label already committed. Because the label is stable
#                          across the retries of one batch, a first attempt that loaded the
#                          data but lost the response lands here on retry. Rows are in.
ACCEPTED_STATUSES = {"success", "publish timeout", "label already exists"}

_READ_ONLY_PREFIXES = ("select", "show", "describe", "desc")


class ReadOnlyViolation(RuntimeError):
    """Raised when something tries to run a non-read statement over the MySQL connection."""


class StreamLoadError(RuntimeError):
    pass


@dataclass
class ConnectionInfo:
    environment: str
    host: str
    mysql_port: int
    database: str
    table_rows: int
    min_start: str | None
    max_start: str | None
    grants: list[str]
    # True = INSERT is clearly held, False = grants look read-only, None = inconclusive.
    can_insert: bool | None


def _infer_insert_capability(grants: list[str]) -> bool | None:
    """
    Best-effort read of SHOW GRANTS.

    Deliberately tri-state. StarRocks reports a superuser as `GRANT 'root' TO 'root'@'%'` with no
    privilege list at all, so "no INSERT in the text" is not evidence of a read-only login. Only
    say False when the grants name SELECT and nothing that implies writing; otherwise say "don't
    know" and let the first batch be the real test.
    """
    blob = " ".join(grants).upper()
    if not blob.strip():
        return None
    if any(tok in blob for tok in ("INSERT", "ALL PRIVILEGES", "'ROOT'", "DB_ADMIN", "ROOT'@")):
        return True
    if "SELECT" in blob:
        return False
    return None


class StarRocksClient:
    def __init__(self, environment: str):
        if environment not in config.ENVIRONMENTS:
            raise KeyError(f"Unknown environment '{environment}'")
        self.environment = environment
        self.cfg = config.ENVIRONMENTS[environment]

    # ── read-only MySQL side ─────────────────────────────────────────────────────────

    def _connect(self, timeout: int = 10):
        return pymysql.connect(
            host=self.cfg["fe_host"],
            port=self.cfg["mysql_port"],
            user=self.cfg["user"],
            password=self.cfg["password"],
            database=self.cfg["database"],
            connect_timeout=timeout,
            read_timeout=120,
            write_timeout=120,
            charset="utf8mb4",
            autocommit=True,
        )

    def query(self, sql: str, params: tuple | None = None) -> list[tuple]:
        """Run a read-only statement. Anything else is refused before it reaches the server."""
        first = sql.strip().split(None, 1)[0].lower() if sql.strip() else ""
        if first not in _READ_ONLY_PREFIXES:
            raise ReadOnlyViolation(
                f"This tool is insert-only. Refused to run a '{first.upper()}' statement: {sql[:120]}"
            )
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute(sql, params or ())
                return list(cur.fetchall())
        finally:
            conn.close()

    def connection_test(self) -> ConnectionInfo:
        """Probe the destination. Raises on any failure; the caller turns that into a red toast."""
        self.query("SELECT 1")

        try:
            cols = [r[0] for r in self.query(f"DESCRIBE `{self.cfg['database']}`.`{config.TABLE}`")]
        except Exception as exc:  # noqa: BLE001
            if "unknown table" in str(exc).lower():
                raise RuntimeError(
                    f"Connected to {self.cfg['database']} on {self.cfg['fe_host']}, but the table "
                    f"{config.TABLE} does not exist there yet. This tool only inserts - it never "
                    "creates tables. Create it from the Scalar.DAL model first "
                    "(the Ingestion service does this at startup), then reconnect."
                ) from exc
            raise
        missing = [c for c in EXPECTED_COLUMNS if c not in cols]
        if missing:
            raise RuntimeError(
                f"{config.TABLE} is missing expected column(s): {', '.join(missing)}. "
                "Refusing to load against a schema that has drifted."
            )

        rows = self.query(
            f"SELECT COUNT(*), MIN(`Start`), MAX(`Start`) FROM `{self.cfg['database']}`.`{config.TABLE}`"
        )
        total, mn, mx = rows[0] if rows else (0, None, None)

        # SHOW GRANTS returns (UserIdentity, Catalog, Grants) — the privileges live in the last
        # column, so the whole row has to be read, not just the identity.
        try:
            grants = [" | ".join("" if v is None else str(v) for v in row)
                      for row in self.query("SHOW GRANTS")]
        except Exception as exc:  # noqa: BLE001 - a missing SHOW GRANTS must not block a probe
            grants = [f"(could not read grants: {exc})"]

        can_insert = _infer_insert_capability(grants)

        return ConnectionInfo(
            environment=self.environment,
            host=self.cfg["fe_host"],
            mysql_port=self.cfg["mysql_port"],
            database=self.cfg["database"],
            table_rows=int(total or 0),
            min_start=str(mn) if mn else None,
            max_start=str(mx) if mx else None,
            grants=grants,
            can_insert=can_insert,
        )

    def count_range(self, start: str, end: str) -> int:
        """Rows whose Start falls in the inclusive [start, end] window."""
        rows = self.query(
            f"SELECT COUNT(*) FROM `{self.cfg['database']}`.`{config.TABLE}` "
            "WHERE `Start` >= %s AND `Start` <= %s",
            (start, end),
        )
        return int(rows[0][0]) if rows else 0

    # ── the only write path ──────────────────────────────────────────────────────────

    def _stream_load_url(self) -> str:
        return (
            f"http://{self.cfg['fe_host']}:{self.cfg['http_port']}"
            f"/api/{self.cfg['database']}/{config.TABLE}/_stream_load"
        )

    def _resolve_redirect(self, location: str) -> str:
        """
        The FE 307s to a BE using an internal hostname that is not resolvable from here.
        Keep the redirect's port and path; swap the host for the configured be_host.
        """
        parsed = urlparse(location)
        port = parsed.port or 8040
        netloc = f"{self.cfg['be_host']}:{port}"
        return urlunparse((parsed.scheme or "http", netloc, parsed.path, parsed.params, parsed.query, parsed.fragment))

    def stream_load(self, rows: list[list[str]], label: str) -> int:
        """
        Load one batch. Returns the number of rows StarRocks reports as loaded.

        `rows` is a list of already-formatted field lists in EXPECTED_COLUMNS order; NULLs must
        already be the `\\N` sentinel.
        """
        if not rows:
            return 0

        body = "\n".join(CSV_DELIMITER.join(r) for r in rows).encode("utf-8")
        headers = {
            # Mandatory. StarRocks rejects the load outright with
            # "DdlException: There is no 100-continue header" if this is absent — it is a
            # precondition of the FE's Stream Load handler, not an optimisation. urllib3 sends
            # headers and body together rather than waiting, and http.client transparently skips
            # the interim 100 response, so the handshake needs nothing further from us.
            "Expect": "100-continue",
            "label": label,
            "format": "CSV",
            "column_separator": CSV_COLUMN_SEPARATOR_HEADER,
            "columns": ",".join(f"`{c}`" for c in EXPECTED_COLUMNS),
            "max_filter_ratio": "0",
            "timeout": str(config.STREAM_LOAD_TIMEOUT),
        }
        auth = HTTPBasicAuth(self.cfg["user"], self.cfg["password"])
        url = self._stream_load_url()

        last_error: Exception | None = None
        for attempt in range(config.MAX_BATCH_RETRIES + 1):
            try:
                resp = requests.put(
                    url,
                    headers=headers,
                    data=body,
                    auth=auth,
                    allow_redirects=False,
                    timeout=(15, config.STREAM_LOAD_TIMEOUT),
                )

                if resp.status_code == 307 and resp.headers.get("Location"):
                    resp = requests.put(
                        self._resolve_redirect(resp.headers["Location"]),
                        headers=headers,
                        data=body,
                        auth=auth,
                        allow_redirects=False,
                        timeout=(15, config.STREAM_LOAD_TIMEOUT),
                    )

                if resp.status_code >= 500:
                    raise requests.HTTPError(f"HTTP {resp.status_code}: {resp.text[:300]}")

                if not resp.ok:
                    # 4xx is a request-shape problem; retrying the same request will not fix it.
                    raise StreamLoadError(
                        f"Stream Load failed: HTTP {resp.status_code} — {resp.text[:500]}"
                    )

                return self._parse_response(resp.text, len(rows))

            except (requests.ConnectionError, requests.Timeout, requests.HTTPError) as exc:
                last_error = exc
                if attempt >= config.MAX_BATCH_RETRIES:
                    break
                time.sleep(1.5 * (2**attempt))

        raise StreamLoadError(
            f"Stream Load failed after {config.MAX_BATCH_RETRIES + 1} attempts: {last_error}"
        )

    @staticmethod
    def _parse_response(text: str, expected_rows: int) -> int:
        try:
            payload = json.loads(text)
        except json.JSONDecodeError as exc:
            raise StreamLoadError(f"Could not parse Stream Load response: {text[:500]}") from exc

        status = str(payload.get("Status", "")).strip()
        if status.lower() not in ACCEPTED_STATUSES:
            raise StreamLoadError(
                "Stream Load rejected by StarRocks. "
                f"Status: {status}, "
                f"Loaded: {payload.get('NumberLoadedRows')}/{payload.get('NumberTotalRows')}, "
                f"Filtered: {payload.get('NumberFilteredRows')}, "
                f"Message: {payload.get('Message') or 'none'}, "
                f"ErrorURL: {payload.get('ErrorURL') or 'none'}"
            )

        loaded = payload.get("NumberLoadedRows")
        if isinstance(loaded, int) and loaded > 0:
            return loaded
        # "Label Already Exists" reports 0 loaded because this call did not do the loading —
        # the rows are nonetheless in, from the attempt whose response was lost.
        return expected_rows


# Column order must match the table DDL, which matches the C# property order.
EXPECTED_COLUMNS = [
    "Start",
    "End",
    "Resolution",
    "PowerAfrrIn",
    "PowerAfrrOut",
    "PowerIgccIn",
    "PowerIgccOut",
    "PowerMfrrdaIn",
    "PowerMfrrdaOut",
    "PowerPicassoIn",
    "PowerPicassoOut",
    "PowerMariIn",
    "PowerMariOut",
    "MaxUpwRegulationPrice",
    "MinDownwRegulationPrice",
    "MidPrice",
]
