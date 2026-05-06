"""
db.py — Supabase persistence layer for SNR Short Scanner
=========================================================

Provides DB-backed load/save for config and signal log.
Automatically falls back to JSON file storage when Supabase
credentials are not configured (so local development requires
zero setup).

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
FIRST-TIME SETUP (Streamlit Cloud)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
1. Create a free project at https://supabase.com
2. In the Supabase SQL Editor, run the SQL in CREATE_TABLES_SQL below
3. Copy your project URL and anon/public API key from:
       Project Settings → API → Project URL & anon public key
4. In Streamlit Cloud → your app → Settings → Secrets, add:

       [supabase]
       url = "https://xxxxxxxxxxxx.supabase.co"
       key = "eyJ..."

5. (For local dev) create .streamlit/secrets.toml with the same block,
   OR just leave it blank — JSON files work fine locally.
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

from __future__ import annotations

import threading
import traceback
from typing import Any

# ── SQL to create tables (run once in Supabase SQL Editor) ───────────────────
CREATE_TABLES_SQL = """
-- ============================================================
-- Run this ONCE in your Supabase project → SQL Editor
-- ============================================================

-- 1. Signals table
CREATE TABLE IF NOT EXISTS signals (
    signal_id   TEXT PRIMARY KEY,          -- sig["id"] or derived key
    symbol      TEXT,
    status      TEXT,
    direction   TEXT,
    entry_ts    TEXT,                      -- ISO timestamp string
    data        JSONB NOT NULL             -- full signal dict
);
CREATE INDEX IF NOT EXISTS idx_signals_status    ON signals(status);
CREATE INDEX IF NOT EXISTS idx_signals_symbol    ON signals(symbol);
CREATE INDEX IF NOT EXISTS idx_signals_entry_ts  ON signals(entry_ts DESC);

-- 2. Config table  (one row per config key)
CREATE TABLE IF NOT EXISTS config (
    key   TEXT PRIMARY KEY,
    value JSONB                            -- stored as JSON so all types survive
);

-- 3. Health table  (single row, always id=1)
CREATE TABLE IF NOT EXISTS health (
    id                    INT  PRIMARY KEY DEFAULT 1,
    total_cycles          INT  DEFAULT 0,
    last_scan_at          TEXT,
    last_scan_duration_s  FLOAT DEFAULT 0.0,
    total_api_errors      INT  DEFAULT 0,
    watchlist_size        INT  DEFAULT 0,
    pre_filtered_out      INT  DEFAULT 0,
    deep_scanned          INT  DEFAULT 0,
    CHECK (id = 1)
);
-- Ensure the single health row exists
INSERT INTO health (id) VALUES (1) ON CONFLICT DO NOTHING;
"""

# ── Lazy singleton Supabase client ───────────────────────────────────────────
_sb_client      = None
_sb_init_done   = False
_sb_lock        = threading.Lock()
_db_available   = False   # cached reachability flag


def _get_client():
    """Return the Supabase client, or None if not configured / unreachable."""
    global _sb_client, _sb_init_done, _db_available

    if _sb_init_done:
        return _sb_client  # already attempted — return cached result

    with _sb_lock:
        if _sb_init_done:
            return _sb_client

        try:
            import streamlit as st
            sb_cfg = st.secrets.get("supabase", {})
            url    = sb_cfg.get("url", "").strip()
            key    = sb_cfg.get("key", "").strip()

            if not url or not key:
                # Credentials absent — silent fallback to JSON
                _sb_client    = None
                _db_available = False
                return None

            from supabase import create_client  # type: ignore
            _sb_client    = create_client(url, key)
            _db_available = True
            print("[DB] Supabase connected.")
        except Exception as exc:
            print(f"[DB] Supabase init failed ({type(exc).__name__}: {exc}) — using JSON fallback.")
            _sb_client    = None
            _db_available = False
        finally:
            _sb_init_done = True

    return _sb_client


def is_db_available() -> bool:
    """True if Supabase is configured and the client was created successfully."""
    _get_client()           # ensure init has run
    return _db_available


def db_status() -> dict:
    """
    Return a status dict for display in the UI.
    Keys: 'available' (bool), 'label' (str), 'icon' (str)
    """
    if is_db_available():
        return {"available": True,  "label": "Supabase DB",  "icon": "🟢"}
    else:
        return {"available": False, "label": "Local JSON",   "icon": "🟡"}


# ─────────────────────────────────────────────────────────────────────────────
# Config helpers
# ─────────────────────────────────────────────────────────────────────────────

def load_config_db() -> dict | None:
    """
    Load all config keys from Supabase.
    Returns a dict on success, or None if DB is not available / query fails.
    The caller should merge this into DEFAULT_CONFIG (same as JSON path).
    """
    sb = _get_client()
    if sb is None:
        return None
    try:
        resp = sb.table("config").select("key, value").execute()
        if not resp.data:
            return None     # table empty — first run, fall through to defaults
        cfg: dict = {}
        for row in resp.data:
            cfg[row["key"]] = row["value"]
        return cfg
    except Exception as exc:
        print(f"[DB] load_config_db error: {exc}")
        return None


def save_config_db(cfg: dict) -> bool:
    """
    Upsert every key in cfg to the config table.
    Returns True on success, False on any error.
    """
    sb = _get_client()
    if sb is None:
        return False
    try:
        rows = [{"key": k, "value": v} for k, v in cfg.items()]
        # Batch in groups of 200 (well within Supabase limits)
        for i in range(0, len(rows), 200):
            sb.table("config").upsert(rows[i : i + 200]).execute()
        return True
    except Exception as exc:
        print(f"[DB] save_config_db error: {exc}")
        return False


# ─────────────────────────────────────────────────────────────────────────────
# Log (signals + health) helpers
# ─────────────────────────────────────────────────────────────────────────────

def _signal_pk(sig: dict) -> str:
    """
    Derive a stable primary key for a signal.
    Prefer sig["id"] (8-char UUID prefix added in recent versions).
    Fall back to <symbol>|<timestamp> for older signals that lack "id".
    """
    sid = sig.get("id", "").strip()
    if sid:
        return sid
    symbol = sig.get("symbol", "unknown")
    ts     = sig.get("timestamp", "")
    return f"{symbol}|{ts}"


def load_log_db() -> dict | None:
    """
    Load full log (signals + health) from Supabase.
    Returns the same dict shape as load_log() / the in-memory _bsc_log,
    or None if DB is not available / query fails.
    """
    sb = _get_client()
    if sb is None:
        return None
    try:
        # ── Signals ──────────────────────────────────────────────────────────
        sig_resp = (
            sb.table("signals")
            .select("data")
            .order("entry_ts", desc=False)
            .execute()
        )
        signals = [row["data"] for row in sig_resp.data]

        # ── Health ───────────────────────────────────────────────────────────
        h_resp = sb.table("health").select("*").eq("id", 1).execute()
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
        print(f"[DB] load_log_db error: {exc}")
        return None


def save_log_db(log: dict) -> bool:
    """
    Upsert all signals + health row to Supabase.
    Returns True on success, False on any error.

    Called every scan cycle and after every trade event — Supabase
    handles upsert efficiently; only changed rows are rewritten.
    """
    sb = _get_client()
    if sb is None:
        return False
    try:
        signals = log.get("signals", [])

        # ── Signals ──────────────────────────────────────────────────────────
        if signals:
            rows = []
            for sig in signals:
                rows.append({
                    "signal_id":  _signal_pk(sig),
                    "symbol":     sig.get("symbol",    ""),
                    "status":     sig.get("status",    ""),
                    "direction":  sig.get("direction", "long"),
                    "entry_ts":   sig.get("timestamp", None),
                    "data":       sig,         # full dict stored as JSONB
                })
            # Upsert in batches of 100
            for i in range(0, len(rows), 100):
                sb.table("signals").upsert(rows[i : i + 100]).execute()

        # ── Health ───────────────────────────────────────────────────────────
        h = log.get("health", {})
        sb.table("health").upsert({
            "id":                    1,
            "total_cycles":          h.get("total_cycles",         0),
            "last_scan_at":          h.get("last_scan_at",         None),
            "last_scan_duration_s":  h.get("last_scan_duration_s", 0.0),
            "total_api_errors":      h.get("total_api_errors",     0),
            "watchlist_size":        h.get("watchlist_size",       0),
            "pre_filtered_out":      h.get("pre_filtered_out",     0),
            "deep_scanned":          h.get("deep_scanned",         0),
        }).execute()

        return True

    except Exception as exc:
        print(f"[DB] save_log_db error: {exc}")
        return False


# ── Utility ───────────────────────────────────────────────────────────────────

def _empty_health() -> dict:
    return {
        "total_cycles":         0,
        "last_scan_at":         None,
        "last_scan_duration_s": 0.0,
        "total_api_errors":     0,
        "watchlist_size":       0,
        "pre_filtered_out":     0,
        "deep_scanned":         0,
    }
