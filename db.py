"""
db.py -- Supabase persistence layer for SNR Short Scanner.
Falls back to JSON if Supabase credentials are absent (local dev).

Tables required in Supabase (public schema):
  - signals        (signal_id TEXT PK, symbol, status, direction, entry_ts, data JSONB)
  - scanner_config (key TEXT PK, value JSONB)
  - scanner_health (id INT PK DEFAULT 1, total_cycles, last_scan_at, ...)
"""
from __future__ import annotations
import threading
import json as _json

CREATE_TABLES_SQL = """
-- Run this ONCE in Supabase SQL Editor

CREATE TABLE IF NOT EXISTS signals (
    signal_id TEXT PRIMARY KEY,
    symbol    TEXT,
    status    TEXT,
    direction TEXT,
    entry_ts  TEXT,
    data      JSONB NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_signals_status   ON signals(status);
CREATE INDEX IF NOT EXISTS idx_signals_symbol   ON signals(symbol);
CREATE INDEX IF NOT EXISTS idx_signals_entry_ts ON signals(entry_ts DESC);
ALTER TABLE signals DISABLE ROW LEVEL SECURITY;

CREATE TABLE IF NOT EXISTS scanner_config (
    key   TEXT PRIMARY KEY,
    value JSONB
);
ALTER TABLE scanner_config DISABLE ROW LEVEL SECURITY;

CREATE TABLE IF NOT EXISTS scanner_health (
    id                    INT   PRIMARY KEY DEFAULT 1,
    total_cycles          INT   DEFAULT 0,
    last_scan_at          TEXT,
    last_scan_duration_s  FLOAT DEFAULT 0.0,
    total_api_errors      INT   DEFAULT 0,
    watchlist_size        INT   DEFAULT 0,
    pre_filtered_out      INT   DEFAULT 0,
    deep_scanned          INT   DEFAULT 0,
    CHECK (id = 1)
);
INSERT INTO scanner_health (id) VALUES (1) ON CONFLICT DO NOTHING;
ALTER TABLE scanner_health DISABLE ROW LEVEL SECURITY;
"""

_sb_client     = None
_sb_init_done  = False
_sb_lock       = threading.Lock()
_db_available  = False
_last_db_error = ""
_sb_url        = ""   # stored for diagnostics
_sb_key        = ""   # stored for diagnostics


def _set_error(msg: str):
    global _last_db_error
    _last_db_error = msg
    print(f"[DB] {msg}")


def last_error() -> str:
    return _last_db_error


def _get_client():
    global _sb_client, _sb_init_done, _db_available, _sb_url, _sb_key
    if _sb_init_done:
        return _sb_client
    with _sb_lock:
        if _sb_init_done:
            return _sb_client
        try:
            import streamlit as st
            sb_cfg = st.secrets.get("supabase", {})
            url = sb_cfg.get("url", "").strip().rstrip("/")
            key = sb_cfg.get("key", "").strip()
            _sb_url = url
            _sb_key = key
            if not url or not key:
                _sb_client = None
                _db_available = False
                return None
            from supabase import create_client
            _sb_client = create_client(url, key)
            _db_available = True
            print(f"[DB] Supabase connected. URL={url}")
        except Exception as exc:
            _set_error(f"Supabase init failed -- {type(exc).__name__}: {exc}")
            _sb_client = None
            _db_available = False
        finally:
            _sb_init_done = True
    return _sb_client


def is_db_available() -> bool:
    _get_client()
    return _db_available


def db_status() -> dict:
    if is_db_available():
        return {"available": True,  "label": "Supabase DB", "icon": "green", "error": _last_db_error}
    else:
        return {"available": False, "label": "Local JSON",  "icon": "yellow", "error": _last_db_error}


def test_raw_http() -> dict:
    """
    Test Supabase using raw requests -- bypasses supabase-py library.
    Returns full diagnostic including the exact URL called and HTTP status.
    """
    import requests as _req
    _get_client()   # ensure _sb_url / _sb_key are populated
    url = _sb_url.rstrip("/")
    key = _sb_key
    if not url or not key:
        return {"ok": False, "url": url, "status": None,
                "body": "No URL/key — check Streamlit secrets."}
    headers = {
        "apikey":        key,
        "Authorization": f"Bearer {key}",
        "Content-Type":  "application/json",
        "Prefer":        "return=representation",
    }
    # 1. Try GET on scanner_health
    full_url = f"{url}/rest/v1/scanner_health?id=eq.1"
    try:
        r = _req.get(full_url, headers=headers, timeout=10)
        return {
            "ok":     r.status_code == 200,
            "url":    full_url,
            "status": r.status_code,
            "body":   r.text[:600],
        }
    except Exception as exc:
        return {"ok": False, "url": full_url, "status": None, "body": str(exc)}


def test_write() -> dict:
    """Write a test row via raw HTTP (most reliable path)."""
    result = test_raw_http()
    if result["ok"]:
        return {"ok": True, "error": "",
                "detail": f"GET {result['url']} -> {result['status']} OK\n{result['body']}"}
    else:
        # Try supabase-py as fallback for comparison
        sb = _get_client()
        lib_err = ""
        if sb:
            try:
                sb.table("scanner_health").upsert({
                    "id": 1, "total_cycles": 0, "last_scan_at": None,
                    "last_scan_duration_s": 0.0, "total_api_errors": 0,
                    "watchlist_size": 0, "pre_filtered_out": 0, "deep_scanned": 0,
                }).execute()
            except Exception as exc:
                lib_err = str(exc)
        return {
            "ok":    False,
            "error": f"HTTP {result['status']}: {result['body']}",
            "detail": (
                f"URL tried: {result['url']}\n"
                f"Raw HTTP status: {result['status']}\n"
                f"Raw HTTP body: {result['body']}\n"
                f"supabase-py error: {lib_err or '(not tested)'}"
            ),
        }


# ---------- Config helpers ----------

def load_config_db() -> dict | None:
    sb = _get_client()
    if sb is None:
        return None
    try:
        resp = sb.table("scanner_config").select("key, value").execute()
        if not resp.data:
            return None
        return {row["key"]: row["value"] for row in resp.data}
    except Exception as exc:
        _set_error(f"load_config_db: {type(exc).__name__}: {exc}")
        return None


def save_config_db(cfg: dict) -> bool:
    sb = _get_client()
    if sb is None:
        return False
    try:
        rows = [{"key": k, "value": v} for k, v in cfg.items()]
        for i in range(0, len(rows), 200):
            sb.table("scanner_config").upsert(rows[i: i + 200]).execute()
        return True
    except Exception as exc:
        _set_error(f"save_config_db: {type(exc).__name__}: {exc}")
        return False


# ---------- Log helpers ----------

def _signal_pk(sig: dict) -> str:
    sid = sig.get("id", "").strip()
    if sid:
        return sid
    return f"{sig.get('symbol', 'unknown')}|{sig.get('timestamp', '')}"


def load_log_db() -> dict | None:
    sb = _get_client()
    if sb is None:
        return None
    try:
        sig_resp = (
            sb.table("signals")
            .select("data")
            .order("entry_ts", desc=False)
            .execute()
        )
        signals = [row["data"] for row in sig_resp.data]

        h_resp = sb.table("scanner_health").select("*").eq("id", 1).execute()
        if h_resp.data:
            h = h_resp.data[0]
            health = {
                "total_cycles":          h.get("total_cycles",         0),
                "last_scan_at":          h.get("last_scan_at",         None),
                "last_scan_duration_s":  h.get("last_scan_duration_s", 0.0),
                "total_api_errors":      h.get("total_api_errors",     0),
                "watchlist_size":        h.get("watchlist_size",       0),
                "pre_filtered_out":      h.get("pre_filtered_out",     0),
                "deep_scanned":          h.get("deep_scanned",         0),
            }
        else:
            health = _empty_health()

        return {"health": health, "signals": signals}
    except Exception as exc:
        _set_error(f"load_log_db: {type(exc).__name__}: {exc}")
        return None


def save_log_db(log: dict) -> bool:
    sb = _get_client()
    if sb is None:
        return False
    try:
        signals = log.get("signals", [])
        if signals:
            rows = []
            for sig in signals:
                rows.append({
                    "signal_id": _signal_pk(sig),
                    "symbol":    sig.get("symbol",    ""),
                    "status":    sig.get("status",    ""),
                    "direction": sig.get("direction", "long"),
                    "entry_ts":  sig.get("timestamp", None),
                    "data":      sig,
                })
            for i in range(0, len(rows), 100):
                sb.table("signals").upsert(rows[i: i + 100]).execute()

        h = log.get("health", {})
        sb.table("scanner_health").upsert({
            "id":                   1,
            "total_cycles":         h.get("total_cycles",         0),
            "last_scan_at":         h.get("last_scan_at",         None),
            "last_scan_duration_s": h.get("last_scan_duration_s", 0.0),
            "total_api_errors":     h.get("total_api_errors",     0),
            "watchlist_size":       h.get("watchlist_size",       0),
            "pre_filtered_out":     h.get("pre_filtered_out",     0),
            "deep_scanned":         h.get("deep_scanned",         0),
        }).execute()
        return True
    except Exception as exc:
        _set_error(f"save_log_db: {type(exc).__name__}: {exc}")
        return False


def _empty_health() -> dict:
    return {
        "total_cycles": 0, "last_scan_at": None,
        "last_scan_duration_s": 0.0, "total_api_errors": 0,
        "watchlist_size": 0, "pre_filtered_out": 0, "deep_scanned": 0,
    }
