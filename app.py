"""
Flask front end for the TenneT balance-delta seeder.

One background worker thread at a time, guarded by a lock. Log lines and progress events go to
every connected browser over SSE, and a ring buffer replays recent lines to a late joiner.
"""

from __future__ import annotations

import json
import os
import queue
import threading
import traceback
from datetime import datetime, timezone

from flask import Flask, Response, jsonify, render_template, request

import config
import seeder
from starrocks import StarRocksClient

app = Flask(__name__)

# ── source folder ─────────────────────────────────────────────────────────────────────
#
# config.SOURCE_DIR is only the initial default. The folder in use is chosen at runtime and
# remembered here, so switching between export drops does not mean editing config.py.

_STATE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".seeder-state.json")


def _load_source_dir() -> str:
    try:
        with open(_STATE_FILE, "r", encoding="utf-8") as fh:
            saved = json.load(fh).get("source_dir")
        if saved and os.path.isdir(saved):
            return saved
    except (OSError, ValueError):
        pass
    return config.SOURCE_DIR


def _save_source_dir(path: str) -> None:
    try:
        with open(_STATE_FILE, "w", encoding="utf-8") as fh:
            json.dump({"source_dir": path}, fh, indent=2)
    except OSError:
        # Not being able to remember the folder is a nuisance, not a failure.
        pass


_source_dir = _load_source_dir()


def current_source_dir() -> str:
    with _state_lock:
        return _source_dir


def describe_source(path: str) -> dict:
    """What the UI needs to render the folder row, without paying for a full scan."""
    exists = os.path.isdir(path)
    csv_count = 0
    if exists:
        try:
            csv_count = sum(1 for n in os.listdir(path) if n.lower().endswith(".csv"))
        except OSError:
            exists = False
    return {"path": path, "exists": exists, "csv_count": csv_count,
            "is_default": os.path.normcase(path) == os.path.normcase(config.SOURCE_DIR)}

# ── run state ─────────────────────────────────────────────────────────────────────────

_state_lock = threading.Lock()
_subscribers: list[queue.Queue] = []
_history: list[dict] = []
_HISTORY_MAX = 500

_run = {
    "active": False,
    "environment": None,
    "run_id": None,
    "stop": False,
    "summary": [],
}


def _publish(event: dict) -> None:
    event.setdefault("ts", datetime.now().strftime("%H:%M:%S"))
    with _state_lock:
        _history.append(event)
        if len(_history) > _HISTORY_MAX:
            del _history[: len(_history) - _HISTORY_MAX]
        targets = list(_subscribers)
    for q in targets:
        try:
            q.put_nowait(event)
        except queue.Full:
            pass


def emit(level: str, message: str, **extra) -> None:
    _publish({"level": level, "message": message, **extra})


def _should_stop() -> bool:
    with _state_lock:
        return _run["stop"]


# ── pages and API ─────────────────────────────────────────────────────────────────────


@app.get("/")
def index():
    return render_template(
        "index.html",
        environments=list(config.ENVIRONMENTS.keys()),
        default_environment=config.DEFAULT_ENVIRONMENT,
        batch_sizes=config.BATCH_SIZE_CHOICES,
        default_batch=config.BATCH_ROWS,
        source_dir=current_source_dir(),
        table=config.TABLE,
    )


@app.get("/api/environments")
def api_environments():
    """
    Never returns passwords. Host and user are fine — they are needed to render the status pill,
    and this server only ever listens on localhost.
    """
    out = []
    for name, cfg in config.ENVIRONMENTS.items():
        out.append({
            "name": name,
            "host": cfg["fe_host"],
            "mysql_port": cfg["mysql_port"],
            "http_port": cfg["http_port"],
            "database": cfg["database"],
            "user": cfg["user"],
            "requires_confirm": bool(cfg.get("confirm_phrase")),
            "confirm_phrase": cfg.get("confirm_phrase"),
            "note": cfg.get("note", ""),
            "from_secrets": name in config.SECRETS_STATUS.get("from_file", []),
        })

    status = config.SECRETS_STATUS
    return jsonify({
        "environments": out,
        "secrets": {
            "file": os.path.basename(status.get("file", "")),
            "loaded": status.get("loaded", False),
            "error": status.get("error"),
            # Only Local is built in; anything else has to come from the secrets file.
            "only_builtin": not status.get("from_file"),
        },
    })


@app.post("/api/connect")
def api_connect():
    name = (request.json or {}).get("environment")
    if name not in config.ENVIRONMENTS:
        return jsonify(ok=False, error=f"Unknown environment '{name}'"), 400

    try:
        info = StarRocksClient(name).connection_test()
    except Exception as exc:  # noqa: BLE001
        cfg = config.ENVIRONMENTS[name]
        hint = ""
        text = str(exc).lower()
        if "timed out" in text or "can't connect" in text or "refused" in text:
            hint = (
                " This is a private address - check the VPN is up."
                if cfg["fe_host"].startswith("10.")
                else " Check the StarRocks FE is running and reachable."
            )
        return jsonify(
            ok=False,
            environment=name,
            error=f"{type(exc).__name__}: {exc}{hint}",
        )

    return jsonify(
        ok=True,
        environment=name,
        host=info.host,
        mysql_port=info.mysql_port,
        database=info.database,
        table_rows=info.table_rows,
        min_start=info.min_start,
        max_start=info.max_start,
        can_insert=info.can_insert,
        grants=info.grants,
        requires_confirm=bool(config.ENVIRONMENTS[name].get("confirm_phrase")),
        confirm_phrase=config.ENVIRONMENTS[name].get("confirm_phrase"),
    )


@app.get("/api/files")
def api_files():
    environment = request.args.get("environment")
    refresh = request.args.get("refresh") == "1"

    try:
        scans = seeder.scan_all(current_source_dir(), force=refresh)
    except Exception as exc:  # noqa: BLE001
        return jsonify(ok=False, error=str(exc)), 400

    client = None
    if environment in config.ENVIRONMENTS:
        client = StarRocksClient(environment)

    rows = []
    for scan in scans:
        item = scan.to_dict()
        item["db_count"] = None
        item["verdict"] = None
        if client is not None and scan.utc_min and not scan.error:
            try:
                db_count = client.count_range(scan.start_sql, scan.end_sql)
                item["db_count"] = db_count
                item["verdict"] = seeder.classify(db_count, scan.row_count)
            except Exception as exc:  # noqa: BLE001
                item["db_error"] = str(exc)
        rows.append(item)

    return jsonify(
        ok=True,
        source_dir=current_source_dir(),
        files=rows,
        total_rows=sum(r["row_count"] for r in rows),
    )


@app.get("/api/source")
def api_source_get():
    return jsonify(ok=True, **describe_source(current_source_dir()),
                   default=config.SOURCE_DIR, can_browse=_can_browse())


@app.post("/api/source")
def api_source_set():
    global _source_dir
    path = ((request.json or {}).get("path") or "").strip().strip('"')
    if not path:
        return jsonify(ok=False, error="No folder given."), 400

    path = os.path.abspath(os.path.expandvars(os.path.expanduser(path)))
    if not os.path.isdir(path):
        return jsonify(ok=False, error=f"Not a folder: {path}"), 400

    with _state_lock:
        if _run["active"]:
            return jsonify(ok=False, error="A sync is running - cannot change the source folder."), 409
        _source_dir = path
    _save_source_dir(path)

    info = describe_source(path)
    if info["csv_count"] == 0:
        return jsonify(ok=True, warning="That folder holds no .csv files.", **info)
    return jsonify(ok=True, **info)


def _can_browse() -> bool:
    """Whether a native folder dialog is available in this Python."""
    try:
        import tkinter  # noqa: F401
        from tkinter import filedialog  # noqa: F401
    except Exception:  # noqa: BLE001
        return False
    return True


@app.post("/api/browse")
def api_browse():
    """
    Open a native folder picker on the machine running this process.

    A browser cannot hand back a real directory path - `webkitdirectory` yields file objects, not
    a path - and this tool is only ever run locally, so the dialog belongs on the server side.
    Tk is created and destroyed inside this request thread, which is what keeps it safe to call
    from a Flask worker.
    """
    if not _can_browse():
        return jsonify(ok=False, error="tkinter is not available - type or paste the path instead.")

    try:
        import tkinter
        from tkinter import filedialog

        root = tkinter.Tk()
        root.withdraw()
        root.attributes("-topmost", True)   # otherwise it opens behind the browser
        root.update()
        chosen = filedialog.askdirectory(
            title="Choose the folder holding the balance-delta CSV exports",
            initialdir=current_source_dir() if os.path.isdir(current_source_dir()) else None,
            parent=root,
        )
        root.destroy()
    except Exception as exc:  # noqa: BLE001
        return jsonify(ok=False, error=f"Could not open the folder dialog: {exc}")

    if not chosen:
        return jsonify(ok=True, cancelled=True)
    return jsonify(ok=True, cancelled=False, path=os.path.abspath(chosen))


@app.post("/api/sync")
def api_sync():
    payload = request.json or {}
    environment = payload.get("environment")
    months = payload.get("months") or []
    batch_rows = int(payload.get("batch_rows") or config.BATCH_ROWS)
    confirm = (payload.get("confirm") or "").strip()

    if environment not in config.ENVIRONMENTS:
        return jsonify(ok=False, error=f"Unknown environment '{environment}'"), 400
    if not months:
        return jsonify(ok=False, error="No months selected."), 400

    phrase = config.ENVIRONMENTS[environment].get("confirm_phrase")
    if phrase and confirm != phrase:
        return jsonify(ok=False, error=f"Type '{phrase}' to confirm a {environment} run."), 400

    # Pinned at request time: the run must not follow a folder change made while it is going.
    source_dir = current_source_dir()

    with _state_lock:
        if _run["active"]:
            return jsonify(ok=False, error="A sync is already running."), 409
        run_id = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        _run.update(active=True, environment=environment, run_id=run_id, stop=False, summary=[])
        _history.clear()

    def worker():
        try:
            summary = seeder.run_sync(
                environment=environment,
                months=months,
                batch_rows=batch_rows,
                emit=emit,
                should_stop=_should_stop,
                run_id=run_id,
                source_dir=source_dir,
            )
            with _state_lock:
                _run["summary"] = summary
            _publish({"level": "summary", "message": "", "summary": summary, "done": True})
        except Exception as exc:  # noqa: BLE001
            emit("error", f"Run aborted: {type(exc).__name__}: {exc}", done=True)
            emit("detail", traceback.format_exc())
        finally:
            with _state_lock:
                _run["active"] = False
            _publish({"level": "state", "message": "", "active": False})

    threading.Thread(target=worker, name="seeder-run", daemon=True).start()
    return jsonify(ok=True, run_id=run_id)


@app.post("/api/stop")
def api_stop():
    with _state_lock:
        if not _run["active"]:
            return jsonify(ok=False, error="Nothing is running.")
        _run["stop"] = True
    emit("warn", "Stop requested — will halt after the current batch.")
    return jsonify(ok=True)


@app.get("/api/status")
def api_status():
    with _state_lock:
        return jsonify(active=_run["active"], environment=_run["environment"], summary=_run["summary"])


@app.get("/api/stream")
def api_stream():
    q: queue.Queue = queue.Queue(maxsize=2000)
    with _state_lock:
        backlog = list(_history)
        _subscribers.append(q)

    def gen():
        import json as _json

        try:
            for event in backlog:
                yield f"data: {_json.dumps(event)}\n\n"
            while True:
                try:
                    event = q.get(timeout=20)
                    yield f"data: {_json.dumps(event)}\n\n"
                except queue.Empty:
                    yield ": keep-alive\n\n"
        finally:
            with _state_lock:
                if q in _subscribers:
                    _subscribers.remove(q)

    return Response(
        gen(),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no", "Connection": "keep-alive"},
    )


if __name__ == "__main__":
    print(f"  source : {current_source_dir()}")
    print(f"  table  : {config.TABLE}")
    print("  open   : http://127.0.0.1:5057")
    app.run(host="127.0.0.1", port=5057, threaded=True, debug=False)
