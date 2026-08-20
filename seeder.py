"""
CSV discovery, mapping and the sync run loop.

Everything about a source file is derived from its content — delimiter, column set, bucket
length, resolution, month. The filename is used only for display and sort order, because the
export set has already changed shape once.

Three rules carry the weight:

1. UTC from the within-day index, not from the label. TenneT publishes Europe/Amsterdam
   wall-clock with no offset, so on the autumn change-over the two passes through 02:00-03:00
   are labelled identically. `Isp` counts buckets from local midnight, which is never ambiguous,
   so anchoring there and adding real elapsed time separates them. This is a port of
   TennetTimestamp.TryResolveIndexedToUtc in scalar-mono.

2. `End` comes from the bucket, never from the label pair. The label span is wrong at both DST
   edges — 3660s/3612s at the spring gap, MINUS 3540s at the autumn fold — while the real
   elapsed time is one bucket in every case.

3. Blank or absent is NULL, never 0. A blank price means that direction was not activated in
   the bucket; an absent Mari column means MARI was not published in that era. Neither is the
   same fact as an activation at zero.
"""

from __future__ import annotations

import csv
import os
import threading
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import config
from starrocks import NULL_SENTINEL, EXPECTED_COLUMNS, StarRocksClient

AMSTERDAM = ZoneInfo("Europe/Amsterdam")
UTC = timezone.utc

# CSV header -> StarRocks column. Mapped by name, never by position: the CSV order differs from
# the table order, and the older 14-column export omits the two Mari columns entirely.
CSV_TO_COLUMN = {
    "Power In Activated Afrr": "PowerAfrrIn",
    "Power Out Activated Afrr": "PowerAfrrOut",
    "Power In Igcc": "PowerIgccIn",
    "Power Out Igcc": "PowerIgccOut",
    "Power In Mfrrda": "PowerMfrrdaIn",
    "Power Out Mfrrda": "PowerMfrrdaOut",
    "Picasso Contribution Power In": "PowerPicassoIn",
    "Picasso Contribution Power Out": "PowerPicassoOut",
    "Mari Contribution Power In": "PowerMariIn",
    "Mari Contribution Power Out": "PowerMariOut",
    "Highest Upward Regulation Price": "MaxUpwRegulationPrice",
    "Lowest Downward Regulation Price": "MinDownwRegulationPrice",
    "Mid Price": "MidPrice",
}

START_HEADER = "Timeinterval Start Loc"
END_HEADER = "Timeinterval End Loc"
ISP_HEADER = "Isp"

# Columns the tool fills itself rather than reading from the CSV.
DERIVED_COLUMNS = {"Start", "End", "Resolution"}


def to_iso8601(seconds: int) -> str:
    """
    Port of Utils.ScalarDynamicResolution.ToIso8601() in scalar-mono, so a seeded row's
    Resolution is whatever the ingestion mappers would have produced for the same bucket.
    """
    if seconds % 86400 == 0:
        return f"P{seconds // 86400}D"
    if seconds % 3600 == 0:
        return f"PT{seconds // 3600}H"
    if seconds % 60 == 0:
        return f"PT{seconds // 60}M"
    return f"PT{seconds}S"


_midnight_cache: dict[date, datetime] = {}


def midnight_utc(day: date) -> datetime:
    """
    Local midnight of `day` in Europe/Amsterdam, as a naive UTC datetime.

    Midnight is the one instant on a change-over day that is neither ambiguous nor missing,
    which is exactly why the index is anchored there.
    """
    hit = _midnight_cache.get(day)
    if hit is None:
        local = datetime(day.year, day.month, day.day, tzinfo=AMSTERDAM)
        hit = local.astimezone(UTC).replace(tzinfo=None)
        _midnight_cache[day] = hit
    return hit


def label_to_utc(local_naive: datetime) -> datetime:
    """The naive label read as Amsterdam wall-clock. Used only as a tripwire, never to place a row."""
    return local_naive.replace(tzinfo=AMSTERDAM).astimezone(UTC).replace(tzinfo=None)


@dataclass
class FileShape:
    path: str
    filename: str
    delimiter: str
    header: list[str]
    has_mari: bool
    bucket_seconds: int
    resolution: str
    start_idx: int
    end_idx: int
    isp_idx: int
    # StarRocks column -> index in the CSV row, for every mapped column present in this file.
    column_idx: dict[str, int]


@dataclass
class FileScan:
    shape: FileShape
    month: str
    row_count: int
    utc_min: datetime | None
    utc_max: datetime | None
    index_label_mismatches: int
    first_mismatch: str | None
    day_counts: dict[str, int] = field(default_factory=dict)
    error: str | None = None

    @property
    def start_sql(self) -> str:
        return self.utc_min.strftime("%Y-%m-%d %H:%M:%S") if self.utc_min else ""

    @property
    def end_sql(self) -> str:
        return self.utc_max.strftime("%Y-%m-%d %H:%M:%S") if self.utc_max else ""

    def to_dict(self) -> dict:
        return {
            "filename": self.shape.filename,
            "month": self.month,
            "resolution": self.shape.resolution,
            "bucket_seconds": self.shape.bucket_seconds,
            "columns": len(self.shape.header),
            "has_mari": self.shape.has_mari,
            "row_count": self.row_count,
            "utc_min": self.start_sql,
            "utc_max": self.end_sql,
            "index_label_mismatches": self.index_label_mismatches,
            "first_mismatch": self.first_mismatch,
            "error": self.error,
        }


def _sniff_delimiter(header_line: str) -> str:
    return ";" if ";" in header_line else ","


def detect_shape(path: str) -> FileShape:
    """Read the header and a sample of rows to work out how this file is laid out."""
    with open(path, "r", encoding="utf-8-sig", newline="") as fh:
        first_line = fh.readline()
        if not first_line.strip():
            raise ValueError("file is empty")
        delimiter = _sniff_delimiter(first_line)

    with open(path, "r", encoding="utf-8-sig", newline="") as fh:
        reader = csv.reader(fh, delimiter=delimiter)
        header = [h.strip() for h in next(reader)]

        for required in (START_HEADER, END_HEADER, ISP_HEADER):
            if required not in header:
                raise ValueError(f"header is missing '{required}'")

        start_idx = header.index(START_HEADER)
        end_idx = header.index(END_HEADER)
        isp_idx = header.index(ISP_HEADER)

        column_idx: dict[str, int] = {}
        for csv_name, column in CSV_TO_COLUMN.items():
            if csv_name in header:
                column_idx[column] = header.index(csv_name)

        # Bucket length = the SMALLEST positive label span in the sample. The maximum is
        # polluted by the spring-gap row (3660s / 3612s) and the autumn-fold row is negative.
        smallest: int | None = None
        sampled = 0
        for row in reader:
            if not row or len(row) <= max(start_idx, end_idx):
                continue
            try:
                span = int(
                    (
                        datetime.fromisoformat(row[end_idx].strip())
                        - datetime.fromisoformat(row[start_idx].strip())
                    ).total_seconds()
                )
            except ValueError:
                continue
            if span > 0 and (smallest is None or span < smallest):
                smallest = span
            sampled += 1
            if sampled >= 1000:
                break

        if smallest is None:
            raise ValueError("could not determine a bucket length from the first 1000 rows")

    return FileShape(
        path=path,
        filename=os.path.basename(path),
        delimiter=delimiter,
        header=header,
        has_mari="PowerMariIn" in column_idx,
        bucket_seconds=smallest,
        resolution=to_iso8601(smallest),
        start_idx=start_idx,
        end_idx=end_idx,
        isp_idx=isp_idx,
        column_idx=column_idx,
    )


def scan_file(path: str) -> FileScan:
    """Full pass over one file: row count, UTC range, per-day counts, index/label tripwire."""
    shape = detect_shape(path)
    bucket = timedelta(seconds=shape.bucket_seconds)

    row_count = 0
    utc_min: datetime | None = None
    utc_max: datetime | None = None
    mismatches = 0
    first_mismatch: str | None = None
    day_counts: dict[str, int] = {}
    month: str | None = None

    with open(path, "r", encoding="utf-8-sig", newline="") as fh:
        reader = csv.reader(fh, delimiter=shape.delimiter)
        next(reader, None)
        for row in reader:
            if not row or len(row) <= shape.isp_idx:
                continue
            label_raw = row[shape.start_idx].strip()
            if not label_raw:
                continue

            local = datetime.fromisoformat(label_raw)
            isp = int(row[shape.isp_idx])
            start = midnight_utc(local.date()) + bucket * (isp - 1)

            if month is None:
                month = f"{local.year:04d}-{local.month:02d}"

            row_count += 1
            day_key = local.date().isoformat()
            day_counts[day_key] = day_counts.get(day_key, 0) + 1

            if utc_min is None or start < utc_min:
                utc_min = start
            if utc_max is None or start > utc_max:
                utc_max = start

            if start != label_to_utc(local):
                mismatches += 1
                if first_mismatch is None:
                    first_mismatch = f"'{label_raw}' isp={isp} -> {start:%Y-%m-%d %H:%M:%S}Z from the index"

    return FileScan(
        shape=shape,
        month=month or "?",
        row_count=row_count,
        utc_min=utc_min,
        utc_max=utc_max,
        index_label_mismatches=mismatches,
        first_mismatch=first_mismatch,
        day_counts=day_counts,
    )


# Scans are expensive (a full parse of ~2.2M rows), so cache on identity + mtime + size.
_scan_cache: dict[str, tuple[tuple[float, int], FileScan]] = {}
_scan_lock = threading.Lock()


def scan_all(source_dir: str | None = None, force: bool = False) -> list[FileScan]:
    """Scan every CSV in the source folder, newest cache entry reused where the file is unchanged."""
    directory = source_dir or config.SOURCE_DIR
    if not os.path.isdir(directory):
        raise FileNotFoundError(f"Source folder not found: {directory}")

    scans: list[FileScan] = []
    for name in sorted(os.listdir(directory)):
        if not name.lower().endswith(".csv"):
            continue
        path = os.path.join(directory, name)
        stat = os.stat(path)
        key = (stat.st_mtime, stat.st_size)

        with _scan_lock:
            cached = _scan_cache.get(path)
        if cached and cached[0] == key and not force:
            scans.append(cached[1])
            continue

        try:
            scan = scan_file(path)
        except Exception as exc:  # noqa: BLE001 - a bad file must not sink the whole listing
            shape = FileShape(
                path=path, filename=name, delimiter=",", header=[], has_mari=False,
                bucket_seconds=0, resolution="", start_idx=-1, end_idx=-1, isp_idx=-1,
                column_idx={},
            )
            scan = FileScan(
                shape=shape, month="?", row_count=0, utc_min=None, utc_max=None,
                index_label_mismatches=0, first_mismatch=None, error=str(exc),
            )

        with _scan_lock:
            _scan_cache[path] = (key, scan)
        scans.append(scan)

    # Order by the window they cover, so the table reads chronologically regardless of naming.
    scans.sort(key=lambda s: (s.utc_min is None, s.utc_min or datetime.max))
    return scans


def iter_batches(scan: FileScan, batch_rows: int):
    """
    Stream the file and yield lists of already-formatted rows, `batch_rows` at a time.

    Never materialises a whole month: a 223k-row file goes out in 224 batches of 1,000 with only
    one batch resident at a time.
    """
    shape = scan.shape
    bucket = timedelta(seconds=shape.bucket_seconds)
    resolution = shape.resolution
    # Resolve the per-column CSV positions once, outside the row loop.
    plan = [(col, shape.column_idx.get(col)) for col in EXPECTED_COLUMNS if col not in DERIVED_COLUMNS]

    batch: list[list[str]] = []
    with open(shape.path, "r", encoding="utf-8-sig", newline="") as fh:
        reader = csv.reader(fh, delimiter=shape.delimiter)
        next(reader, None)
        for row in reader:
            if not row or len(row) <= shape.isp_idx:
                continue
            label_raw = row[shape.start_idx].strip()
            if not label_raw:
                continue

            local = datetime.fromisoformat(label_raw)
            isp = int(row[shape.isp_idx])
            start = midnight_utc(local.date()) + bucket * (isp - 1)
            end = start + bucket

            fields = [
                start.strftime("%Y-%m-%d %H:%M:%S"),
                end.strftime("%Y-%m-%d %H:%M:%S"),
                resolution,
            ]
            for _col, idx in plan:
                if idx is None:
                    # Column absent from this export — MARI before it went live.
                    fields.append(NULL_SENTINEL)
                    continue
                raw = row[idx].strip() if idx < len(row) else ""
                fields.append(raw if raw else NULL_SENTINEL)

            batch.append(fields)
            if len(batch) >= batch_rows:
                yield batch
                batch = []

    if batch:
        yield batch


# ── Run loop ──────────────────────────────────────────────────────────────────────────

# Pre-flight verdicts.
LOAD = "load"
ALREADY_SYNCED = "already_synced"
PARTIAL = "partial"
UNEXPECTED = "unexpected"
EMPTY = "empty"


def classify(db_count: int, file_rows: int) -> str:
    """
    Decide skip-vs-load from the counts.

    Comparing counts rather than testing existence is what makes an insert-only tool safe: a
    plain "any rows? then skip" test cannot tell a finished month from one that died halfway,
    and would mark the half-loaded month done forever.
    """
    if file_rows == 0:
        return EMPTY
    if db_count == 0:
        return LOAD
    if db_count == file_rows:
        return ALREADY_SYNCED
    if db_count < file_rows:
        return PARTIAL
    return UNEXPECTED


def manual_delete_sql(database: str, scan: FileScan) -> str:
    return (
        f"DELETE FROM {database}.{config.TABLE}\n"
        f"WHERE Start >= '{scan.start_sql}' AND Start <= '{scan.end_sql}';"
    )


def run_sync(environment, months, batch_rows, emit, should_stop, run_id, source_dir=None):
    """
    Seed the selected months into `environment`.

    `emit(level, message, **extra)` publishes a log line; `should_stop()` is polled between
    batches. Returns the per-month summary.
    """
    client = StarRocksClient(environment)
    database = client.cfg["database"]

    scans = {s.month: s for s in scan_all(source_dir)}
    selected = [scans[m] for m in months if m in scans]

    total_rows = sum(s.row_count for s in selected)
    emit(
        "info",
        f"Starting sync of {len(selected)} month(s), {total_rows:,} rows, to {environment} "
        f"({client.cfg['fe_host']}:{client.cfg['http_port']}) in batches of {batch_rows:,}.",
        overall_total=total_rows,
    )

    summary = []
    loaded_overall = 0
    synced = skipped = failed = 0

    for scan in selected:
        if should_stop():
            emit("warn", "Stopped before starting the next month.")
            break

        month = scan.month
        emit(
            "info",
            f"Syncing historical data from {scan.start_sql}Z to {scan.end_sql}Z to {environment}",
            month=month, status="running",
        )
        emit(
            "detail",
            f"  shape: {len(scan.shape.header)} cols, {scan.shape.resolution}, {scan.row_count:,} rows"
            + ("" if scan.shape.has_mari else " - Mari columns absent, writing NULL"),
            month=month,
        )
        if scan.index_label_mismatches:
            emit(
                "detail",
                f"  note: {scan.index_label_mismatches} index/label disagreement(s) - the DST fold, "
                f"placed from the index. First: {scan.first_mismatch}",
                month=month,
            )

        try:
            db_count = client.count_range(scan.start_sql, scan.end_sql)
        except Exception as exc:  # noqa: BLE001
            emit("error", f"  {month} pre-flight count failed: {exc}", month=month, status="failed")
            summary.append(dict(month=month, status="Failed", read=scan.row_count, loaded=0,
                                verified=0, detail=str(exc)))
            failed += 1
            continue

        verdict = classify(db_count, scan.row_count)

        if verdict == EMPTY:
            emit("warn", f"  {month} has no data rows - nothing to sync.", month=month, status="empty")
            summary.append(dict(month=month, status="Empty", read=0, loaded=0, verified=db_count, detail=""))
            skipped += 1
            continue

        if verdict == ALREADY_SYNCED:
            emit(
                "skip",
                f"{month} is already synced and skipped ({db_count:,} rows already present)",
                month=month, status="already_synced",
            )
            summary.append(dict(month=month, status="Already synced - skipped", read=scan.row_count,
                                loaded=0, verified=db_count, detail=""))
            skipped += 1
            continue

        if verdict in (PARTIAL, UNEXPECTED):
            if verdict == PARTIAL:
                headline = (
                    f"  {month} is PARTIAL - {db_count:,} of {scan.row_count:,} rows present. "
                    "Not loading: this table appends, so writing now would stack a second copy on "
                    "top of what is already there."
                )
            else:
                headline = (
                    f"  {month} has MORE rows than expected - {db_count:,} present, "
                    f"{scan.row_count:,} in the file. Not loading."
                )
            emit("error", headline, month=month, status=verdict)
            emit(
                "detail",
                "  This tool cannot delete. To redo this month, run the following by hand first:\n"
                + manual_delete_sql(database, scan),
                month=month,
            )
            summary.append(dict(
                month=month,
                status="Partial - needs manual cleanup" if verdict == PARTIAL else "Unexpected row count",
                read=scan.row_count, loaded=0, verified=db_count,
                detail=manual_delete_sql(database, scan),
            ))
            failed += 1
            continue

        # verdict == LOAD
        expected_batches = (scan.row_count + batch_rows - 1) // batch_rows
        month_loaded = 0
        batch_no = 0
        interrupted = False

        try:
            for batch in iter_batches(scan, batch_rows):
                if should_stop():
                    interrupted = True
                    break

                batch_no += 1
                label = f"TBD_{environment}_{month.replace('-', '')}_b{batch_no}_{run_id}"
                month_loaded += client.stream_load(batch, label)
                loaded_overall += len(batch)

                emit(
                    "progress",
                    f"  batch {batch_no}/{expected_batches} - {month_loaded:,}/{scan.row_count:,} rows loaded",
                    month=month, month_loaded=month_loaded, month_total=scan.row_count,
                    overall_loaded=loaded_overall, overall_total=total_rows,
                )

                if config.BATCH_PAUSE_SECONDS:
                    time.sleep(config.BATCH_PAUSE_SECONDS)

        except Exception as exc:  # noqa: BLE001
            emit("error", f"  {month} failed after {month_loaded:,} rows: {exc}", month=month, status="failed")
            emit(
                "detail",
                "  That leaves this month partially loaded. This tool cannot delete; to retry, run:\n"
                + manual_delete_sql(database, scan),
                month=month,
            )
            summary.append(dict(month=month, status="Failed - partially loaded", read=scan.row_count,
                                loaded=month_loaded, verified=0, detail=str(exc)))
            failed += 1
            continue

        try:
            verified = client.count_range(scan.start_sql, scan.end_sql)
        except Exception as exc:  # noqa: BLE001
            verified = -1
            emit("warn", f"  {month} verification query failed: {exc}", month=month)

        if interrupted:
            if month_loaded == 0:
                # Stopped before the first batch went out, so nothing was written and the next
                # run's pre-flight will simply see an empty range and load it normally.
                emit(
                    "warn",
                    f"  {month} STOPPED before any rows were written - nothing to clean up.",
                    month=month, status="load",
                )
                summary.append(dict(month=month, status="Stopped before writing", read=scan.row_count,
                                    loaded=0, verified=verified, detail=""))
            else:
                emit(
                    "warn",
                    f"  {month} STOPPED part-way - {month_loaded:,} of {scan.row_count:,} rows loaded. "
                    "Run the printed DELETE by hand before retrying this month.",
                    month=month, status="partial",
                )
                emit("detail", manual_delete_sql(database, scan), month=month)
                summary.append(dict(month=month, status="Partial - stopped by user", read=scan.row_count,
                                    loaded=month_loaded, verified=verified,
                                    detail=manual_delete_sql(database, scan)))
            failed += 1
            break

        ok = verified == scan.row_count
        emit(
            "ok" if ok else "warn",
            f"{month} done - {scan.row_count:,} read, {month_loaded:,} loaded, "
            f"{verified:,} verified in range" + ("" if ok else "  counts disagree"),
            month=month, status="synced" if ok else "warn",
        )
        summary.append(dict(month=month, status="Synced" if ok else "Synced - count mismatch",
                            read=scan.row_count, loaded=month_loaded, verified=verified, detail=""))
        synced += 1

    emit(
        "info",
        f"Finished: {synced} month(s) synced, {skipped} already synced and skipped, {failed} needing attention.",
        done=True,
    )
    return summary
