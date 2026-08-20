# TenneT Balance Delta — historical seeder

A standalone local tool that loads TenneT's `balance_delta` CSV exports into the
`TennetBalanceDelta` StarRocks table, because the historical acquisition pipeline in `scalar-mono`
is disabled. Not part of that repo, and not meant to be deployed anywhere — run it from your own
machine, pick a destination, press the button.

```
cp secrets.example.json secrets.local.json    # fill in your hosts + credentials
run.bat                                       →  http://127.0.0.1:5057
```

First run creates a venv and installs dependencies. `tzdata` is one of them and is **not
optional**: Windows ships no IANA zone database, and every timestamp here depends on
`Europe/Amsterdam` resolving.

`secrets.local.json` is gitignored and is the only place hosts and passwords live — see
[Environments and secrets](#environments-and-secrets). Skip it and you still get a working `Local`
environment pointing at `127.0.0.1`.

---

## What it does

1. Scans every `.csv` in the chosen source folder, deriving each file's shape from its content.
2. Converts each row's Amsterdam wall-clock timestamp to UTC.
3. Maps it onto the `TennetBalanceDelta` columns.
4. Loads it via the StarRocks Stream Load API, 1,000 rows per request, one request at a time.
5. Verifies the row count in range and reports per month.

Currently in scope: **19 files, 2,230,260 rows, 2024-12-31T23:00:00Z → 2026-07-31T21:59:48Z.**

---

## Choosing the source folder

`config.SOURCE_DIR` is only the starting default. The **Source folder** row at the top of the page
lets you change it per run:

- **Browse…** opens a native folder dialog. It opens on the machine running the process, not in the
  browser — a browser cannot hand back a real directory path (`webkitdirectory` yields file objects,
  not a path), and this tool only ever runs locally, so the dialog belongs server-side. Tk is
  created and destroyed inside the request thread, which is what makes it safe to call from a Flask
  worker. If `tkinter` is missing the button disables itself and you type the path instead.
- The text box accepts a typed or pasted path, Enter to apply. Quotes are stripped, so Explorer's
  **Copy as path** works as-is, and `%VAR%` / `~` are expanded.
- **Default** goes back to `config.SOURCE_DIR`.

The choice is remembered in `.seeder-state.json` next to the app, so it survives a restart. Changing
the folder rescans immediately, and is refused outright while a sync is running — the run pins the
folder at the moment you press Start Sync, so it can never follow a change made underneath it.

---

## Light and dark

The header toggle switches themes and remembers the choice in `localStorage`. Until you pick one it
follows the OS, and it is applied before first paint so there is no flash of the wrong theme.

Every colour is a CSS custom property defined in the `:root` blocks — there are no hardcoded colours
anywhere else in the stylesheet, which is what stops a stale value surviving the switch. Both
palettes clear WCAG AA (4.5:1) on every text/background pair, including the log panel.

---

## Insert-only

The tool cannot delete, update, or create anything. That is enforced, not just intended:

- The only write path is `stream_load()`.
- Every other statement goes through `StarRocksClient.query()`, which refuses anything that is not
  `SELECT` / `SHOW` / `DESCRIBE` and raises `ReadOnlyViolation`.
- It does not create the table. If `TennetBalanceDelta` is missing, the connection test says so and
  stops — create it from the `Scalar.DAL` model (the Ingestion service does this at startup).

### The consequence, and how it is handled

`TennetBalanceDelta` is `DUPLICATE KEY(Start)`, which **appends** — it does not upsert. Loading a
month twice writes it twice. Since the tool cannot delete, it decides skip-vs-load by comparing
counts rather than testing existence:

| Pre-flight | What happens |
|---|---|
| range is empty | loads it |
| range holds exactly the file's row count | `2025-10 is already synced and skipped (44,700 rows already present)` |
| range holds fewer rows than the file | **refuses**, marks the month `Partial`, prints the `DELETE` a human must run |
| range holds more rows than the file | **refuses**, marks it `Unexpected` |

A plain existence test would mark a half-loaded month done forever. Comparing counts is what makes
pressing Start Sync twice safe.

If a month does need redoing — after a crash, or after pressing Stop mid-month — the tool prints
the exact statement and you run it yourself:

```sql
DELETE FROM ScalarStarrocks.TennetBalanceDelta
WHERE Start >= '2025-09-30 22:00:00' AND Start <= '2025-10-31 22:59:00';
```

---

## The two things that are easy to get wrong

### 1. "CET" in the filename is the Dutch zone, not UTC+1

Source timestamps are Europe/Amsterdam wall-clock (`Loc` in the column headers), and the offset
moves with DST. The filenames prove it: winter files end at `2300`, summer files at `2200`. Reading
`_CET` as a fixed +01:00 would put every summer row an hour out. Conversion goes through
`zoneinfo`, never through a hardcoded offset.

### 2. UTC comes from the `Isp` index, not from the label

On the autumn change-over the Dutch clock runs 02:00–03:00 twice and TenneT publishes no offset, so
the two passes are labelled **identically**. In the October 2025 file:

```
isp=121  2025-10-26T02:00  →  first pass,  +02:00  →  2025-10-26 00:00:00Z
isp=181  2025-10-26T02:00  →  second pass, +01:00  →  2025-10-26 01:00:00Z
```

Converting from the label collapses 60 rows onto 60 others, leaving one UTC hour empty and
double-writing the next — silently, on an append-only table. `Isp` counts buckets from local
midnight, which is never ambiguous, so anchoring there and adding real elapsed time separates them
exactly. This is a port of `TennetTimestamp.TryResolveIndexedToUtc` in `scalar-mono`.

Verified against the live table: those two rows land an hour apart with their own distinct values
(aFRR out 102.0 vs 445.0, price −10.06 vs −102.10).

`End` is derived the same way — `Start + bucket`, never from the label pair. The label span is
wrong at both DST edges: 3660 s / 3612 s at the spring gap, and **minus** 3540 s at the autumn fold.

The label→UTC comparison is kept as a tripwire. It disagrees exactly 60 times, all in the October
2025 file, all inside the fold. Anything beyond that baseline is a real signal.

---

## Two source shapes

| | 2025-01 → 2025-11 (`balance_delta_*`) | 2025-12 → 2026-07 (`balance_delta_high_res_*`) |
|---|---|---|
| Columns | 14 — **no Mari in/out** | 16 |
| Delimiter | `;` | `,` |
| Label | `2025-01-01T00:00` | `2025-12-01T00:00:00` |
| `Isp` | 1…1440 (1380 spring / 1500 autumn) | 1…7200 (6900 spring) |
| Bucket | 60 s → `PT1M` | 12 s → `PT12S` |
| Rows | 480,960 | 1,749,300 |

Nothing about this is hardcoded — delimiter, columns, bucket length and resolution are all detected
per file from the header and the first 1,000 rows, and the filename is used only for display. The
export set has already changed shape once.

**Bucket length is the smallest positive label span in the sample**, deliberately: the maximum is
polluted by the spring-gap row and the autumn-fold row is negative.

**`Resolution` is a line-for-line port of `Utils.ScalarDynamicResolution.ToIso8601()`**, so a seeded
row carries whatever the ingestion mappers would have produced. 60 s → `PT1M`, 12 s → `PT12S`.

---

## Blank and absent are NULL, never 0

A blank price means that direction was not activated in the bucket. An absent Mari column means
MARI was not published in that era. Neither is the same fact as an activation at zero, and the #371
state machine depends on the difference. Both become `\N`.

Where Mari *is* published (all 1.75M high-res rows) every value is 0.0 — so NULL vs 0 is the only
thing separating "not published" from "published as zero".

---

## Environments and secrets

**No hosts or credentials live in tracked files.** `config.py` holds only non-secret settings and a
loader; everything environment-specific comes from `secrets.local.json`, which is gitignored.

```
cp secrets.example.json secrets.local.json     # then fill in your own hosts and passwords
```

`Local` is built in and points at a passwordless `127.0.0.1`, so a fresh clone runs with no setup at
all. Every other environment comes from the secrets file — and if that file is absent, those
environments simply don't appear in the dropdown, rather than failing later at connect time. The UI
says so on load instead of leaving you wondering where DEV and PROD went. A malformed file is
reported as an error, because silently falling back to Local would look like the secrets just hadn't
been picked up.

Set `SEEDER_SECRETS` to load the file from somewhere else.

Per environment: `fe_host` (required), `be_host` (defaults to `fe_host`), `http_port` (8030),
`mysql_port` (9030), `user` (`root`), `password` (empty), `database` (`ScalarStarrocks`),
`confirm_phrase`, `note`. An optional top-level `source_dir` sets the initial folder.

`/api/environments` returns host and user so the status pill can render, but **never** a password.
The server binds to `127.0.0.1` only.

### The connection probe

Selecting an environment immediately probes it: `SELECT 1`, a `DESCRIBE` assertion on the 16
expected columns, a row count, and `SHOW GRANTS`.

- Success → **"Destination DB connected"**, and Start Sync enables.
- Failure → **"Couldn't connect the db"** with the underlying error, and Start Sync stays disabled.

Start Sync is disabled on page load and re-disabled on every environment change until the new probe
returns. An environment with a `confirm_phrase` additionally requires typing it, so it needs both a
live connection and the phrase.

If your DEV/PROD are private addresses, a timeout there almost always means the VPN is down, and the
error says so.

---

## Batching

1,000 rows per Stream Load request, strictly sequential — each batch finishes its 307 FE→BE hop and
its status check before the next is built. That is 45 requests for a `PT1M` month, 224 for a full
`PT12S` month, ~2,230 for the whole backfill. Measured locally at ~23 s per 44,700-row month, so
roughly 20 minutes end to end. Batch size is selectable (1k / 5k / 10k / 25k) if a run needs to go
faster.

Stop is checked between batches. It lands on a clean boundary but still leaves the month in flight
partially loaded, so it confirms with that warning first and marks the month red rather than done.

### Wire format

Mirrors `Scalar.StarRocks/LoadApi/StarRocksStreamLoadApiClient.cs`:

- SOH (`\x01`) column separator. StarRocks ignores RFC4180 quoting unless `enclose` is set, so a
  separator that cannot occur in the data removes the need to quote or escape at all — and `\N`
  survives untouched.
- `Expect: 100-continue` is **mandatory**. Without it StarRocks rejects the load outright with
  `DdlException: There is no 100-continue header`.
- `max_filter_ratio: 0` — one bad row fails the whole batch, which is what you want here.
- The FE 307s to an internal BE hostname; the redirect's port and path are kept and the host is
  swapped for the configured `be_host`.
- **HTTP 200 does not mean success.** The body's `Status` is the real answer; only `Success`,
  `Publish Timeout` and `Label Already Exists` are accepted.
- Batch labels are `TBD_{env}_{yyyymm}_b{n}_{run_id}`, stable across the retries of one batch (so a
  lost response replays as "Label Already Exists" rather than double-loading) but distinct across
  runs.

---

## Out of scope

- **The derived preliminary imbalance price (#371) is not backfilled.** In the live pipeline
  `BalanceDeltaConsumer` calls `TennetImbalancePriceTriggerService` after the write to produce
  `SystemImbalancePrice` rows with `Source = 'Scalar'`. This tool writes `TennetBalanceDelta` only.
  It could not be backfilled from the 2025 data as things stand anyway: `IspCompletenessGate`
  hardcodes a 12-second bucket and requires 75 per PT15M ISP, so a `PT1M` quarter (15 buckets) can
  never satisfy it.
- No `ScalarIngestionLog` rows are written — this run is invisible to the ingestion retry machinery.
- Nothing is interpolated or gap-filled. All 19 files are complete as delivered.

---

## Files

| | |
|---|---|
| `app.py` | Flask routes, SSE log stream, single-worker run state |
| `seeder.py` | Discovery, shape detection, time mapping, batch loop |
| `starrocks.py` | Stream Load client + the read-only MySQL helper |
| `config.py` | Non-secret settings and the secrets loader. Safe to commit. |
| `secrets.example.json` | Committed template — copy to `secrets.local.json` |
| `secrets.local.json` | **Your real hosts and passwords. Gitignored.** |
| `.seeder-state.json` | Last-chosen source folder. Generated; gitignored. |
| `templates/index.html` | The whole UI |
