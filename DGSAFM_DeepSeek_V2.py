#!/usr/bin/env python3
"""
OKX Futures Scanner — SHORT ONLY — Complete Full Feature Port
===============================================================
Complete port of the original LONG scanner with ALL features inverted for SHORT trades.

KEY INVERSIONS FOR SHORT:
  • Entry: SELL first (side="sell"), close with BUY (side="buy")
  • TP: Below entry (price drops to profit), SL: Above entry (price rises to stop)
  • PDZ: Premium zone (sell high), not Discount
  • Super Setup = 15m + 1h both in Premium zone (was Discount for Long)
  • RSI: Overbought (high values) — RSI ≤ max threshold
  • EMA: Price must be BELOW EMA (bearish trend)
  • MACD: Bearish condition (MACD < 0, signal < 0, histogram decreasing)
  • SAR: Must be ABOVE price (bearish)
  • EMA Cross: Death cross (fast < slow)
  • DCA: Average UP as price rises (add more shorts when price moves against you)
  • FC-B: fc_trigger_px = avg_entry - ($0.50 / total_coins) — fixed profit
  • Watcher: TP when low ≤ tp_price, DCA when high ≥ trigger, SL when high ≥ sl_price
  • Full UI with Market Condition Analyser, Diagnostics, Filter Funnel, Manual Trade panel
"""

import base64, hashlib, hmac, json, math, os, pathlib, threading, time, uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

import requests
import streamlit as st
import plotly.graph_objects as go
import plotly.express as px

# ============================================================================
# NETWORK CONSTANTS
# ============================================================================
BASE = "https://www.okx.com"
OKX_INTERVALS = {"30m": "30m", "3m": "3m", "5m": "5m", "15m": "15m", "1h": "1H"}

_api_sem = threading.Semaphore(20)
_RATE_INTERVAL = 1.0 / 15
_rate_lock = threading.Lock()
_rate_last_grant = 0.0

def _rate_wait():
    global _rate_last_grant
    while True:
        with _rate_lock:
            now = time.time()
            wait = _rate_last_grant + _RATE_INTERVAL - now
            if wait <= 0:
                _rate_last_grant = now
                return
        time.sleep(max(0.001, wait))

_SYMBOL_CACHE_TTL = 6 * 3600
PRE_FILTER_MIN_VOL_USDT = 100_000
PRE_FILTER_LOW_BUFFER = 1.005

DUBAI_TZ = timezone(timedelta(hours=4))

def dubai_now() -> datetime:
    return datetime.now(DUBAI_TZ)

def to_dubai(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(DUBAI_TZ)

def fmt_dubai(iso_str: str, fmt: str = "%m/%d %H:%M") -> str:
    try:
        dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
        return to_dubai(dt).strftime(fmt)
    except Exception:
        return iso_str[:16] if iso_str else "—"

# ============================================================================
# DEFAULT CONFIGURATION — SHORT MODE (fully inverted)
# ============================================================================
DEFAULT_CONFIG: dict = {
    "tp_pct": 1.5,
    "sl_pct": 3.0,
    "use_pre_filter": True,
    "use_rsi_5m": True,
    "rsi_5m_max": 70,
    "use_rsi_1h": True,
    "rsi_1h_min": 65,
    "rsi_1h_max": 95,
    "loop_minutes": 4,
    "cooldown_minutes": 2,
    "use_ema_3m": False,
    "ema_period_3m": 12,
    "use_ema_5m": False,
    "ema_period_5m": 12,
    "use_ema_15m": True,
    "ema_period_15m": 12,
    "use_macd_3m": False,
    "use_macd_5m": False,
    "use_macd_15m": True,
    "use_sar_3m": False,
    "use_sar_5m": True,
    "use_sar_15m": True,
    "use_vol_spike": False,
    "vol_spike_mult": 2.0,
    "vol_spike_lookback": 20,
    "use_pdz_5m": False,
    "use_pdz_15m": True,
    "use_atr_filter": False,
    "atr_mode": "Normal",
    "use_ema_cross_15m": True,
    "ema_cross_fast_15m": 12,
    "ema_cross_slow_15m": 21,
    "max_open_trades": 7,
    "max_super_trades": 2,
    "sl_cooldown_hours": 4,
    "trade_enabled": False,
    "demo_mode": True,
    "api_key": "",
    "api_secret": "",
    "api_passphrase": "",
    "trade_usdt_amount": 5.0,
    "trade_leverage": 20,
    "trade_margin_mode": "isolated",
    "trade_max_dca": 3,
    "dca_iso_distance_pct": 80.0,
    "dca_cross_rise_pct": 7.0,
    "dca_tp_usd": 0.50,
    "dca_sl_usd": 5.00,
    "use_dca_sl": True,
    "watcher_minutes": 1,
    "reconcile_t1_minutes": 2,
    "reconcile_t2_minutes": 10,
    "scan_hour_enabled": False,
    "scan_hour_start": 0,
    "scan_hour_end": 23,
    "watchlist": [
        "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT", "DOGEUSDT",
        "ADAUSDT", "LTCUSDT", "AVAXUSDT", "DOTUSDT", "LINKUSDT", "MATICUSDT",
    ],
}

# ============================================================================
# SECTOR TAGS (Enhanced)
# ============================================================================
SECTORS: dict = {
    "BTCUSDT": "BTC",
    "ETHUSDT": "L1", "SOLUSDT": "L1", "BNBUSDT": "L1", "XRPUSDT": "L1",
    "DOGEUSDT": "Meme", "ADAUSDT": "L1", "AVAXUSDT": "L1",
    "ARBUSDT": "L2", "OPUSDT": "L2", "UNIUSDT": "DeFi", "AAVEUSDT": "DeFi",
    "LINKUSDT": "Oracle", "MATICUSDT": "L2", "DOTUSDT": "L1", "LTCUSDT": "L1",
    "APTUSDT": "L1", "SUIUSDT": "L1", "TONUSDT": "L1", "NEARUSDT": "L1",
    "FETUSDT": "AI", "RENDERUSDT": "AI", "GRTUSDT": "AI", "VIRTUALUSDT": "AI",
    "TAOUSDT": "AI", "WLDUSDT": "AI", "FETUSDT": "AI",
    "DASHUSDT": "Privacy", "ZECUSDT": "Privacy", "XMRUSDT": "Privacy",
    "SANDUSDT": "Gaming", "MANAUSDT": "Gaming", "GALAUSDT": "Gaming",
    "AXSUSDT": "Gaming", "APEUSDT": "Gaming",
}

MAX_LEVERAGE: dict = {
    "BTCUSDT": 125, "ETHUSDT": 100, "SOLUSDT": 75, "BNBUSDT": 75,
    "XRPUSDT": 75, "DOGEUSDT": 75, "ADAUSDT": 75, "LTCUSDT": 75,
    "AVAXUSDT": 75, "DOTUSDT": 75, "MATICUSDT": 75, "LINKUSDT": 75,
}

def get_max_leverage(sym: str) -> int:
    return MAX_LEVERAGE.get(sym, 20)

# ============================================================================
# SL REASON ANALYZER (SHORT VERSION)
# ============================================================================
def analyze_sl_reason_short(sig: dict) -> str:
    criteria = sig.get("criteria", {})
    reasons = []
    improve = []

    def _f(key, default=None):
        try:
            v = criteria.get(key, default)
            return float(v) if v not in (None, "—", "") else default
        except (TypeError, ValueError):
            return default

    duration_mins = None
    try:
        if sig.get("timestamp") and sig.get("close_time"):
            t_open = datetime.fromisoformat(sig["timestamp"].replace("Z", "+00:00"))
            t_close = datetime.fromisoformat(sig["close_time"].replace("Z", "+00:00"))
            duration_mins = int((t_close - t_open).total_seconds() / 60)
    except Exception:
        pass

    if duration_mins is not None:
        if duration_mins <= 5:
            reasons.append(f"SL hit in {duration_mins} min — entry was at a local price bottom.")
            improve.append("Add 1–2 candle confirmation delay")
        elif duration_mins <= 15:
            reasons.append(f"SL hit in {duration_mins} min — very fast reversal upward.")
        elif duration_mins >= 120:
            reasons.append(f"Trade held {duration_mins} min before SL — sustained trend reversal.")

    if sig.get("is_super_setup"):
        reasons.append("Super Setup — 15m Premium zone bypassed all filters.")

    pdz_5m = str(criteria.get("pdz_zone_5m", "") or "")
    pdz_15m = str(criteria.get("pdz_zone_15m", "") or "")
    for zone_label, zone_val in [("5m", pdz_5m), ("15m", pdz_15m)]:
        if "BandA" in zone_val:
            reasons.append(f"PDZ {zone_label} zone was {zone_val} — near Premium boundary.")
        elif "BandB" in zone_val:
            reasons.append(f"PDZ {zone_label} zone was {zone_val} — borderline zone.")
        elif "Discount" in zone_val:
            reasons.append(f"PDZ {zone_label} zone was Discount — oversold, bounce risk.")

    rsi_5m = _f("rsi_5m")
    rsi_1h = _f("rsi_1h")

    if rsi_5m is not None:
        if rsi_5m > 85:
            reasons.append(f"RSI 5m extremely high ({rsi_5m:.1f}) — blow-off top risk.")
            improve.append("Lower RSI 5m maximum threshold")
        elif rsi_5m < 55:
            reasons.append(f"RSI 5m only {rsi_5m:.1f} — weak overbought condition.")

    if rsi_1h is not None and rsi_1h < 60:
        reasons.append(f"RSI 1h weak ({rsi_1h:.1f}) — no bearish higher-TF support.")

    macd_flags = {"3m": criteria.get("macd_3m"), "5m": criteria.get("macd_5m"), "15m": criteria.get("macd_15m")}
    false_bullish = [tf for tf, v in macd_flags.items() if v not in (None, "—") and str(v) not in ("", "—") and float(v) > 0]
    if false_bullish:
        reasons.append(f"MACD was bullish on {', '.join(false_bullish)} timeframe(s) at entry.")

    vol_ratio = _f("vol_ratio")
    if vol_ratio is not None and vol_ratio < 1.5:
        reasons.append(f"Volume spike only {vol_ratio:.1f}× average — low conviction.")

    if not reasons:
        reasons.append("All entry filters were healthy — reversal caused by external market event.")

    lines = [f"• {r}" for r in reasons]
    if improve:
        lines.append("")
        lines.append("💡 Improvement suggestions:")
        lines.extend(f"  → {i}" for i in improve)

    return "\n".join(lines)

# ============================================================================
# CONFIG PERSISTENCE (Short-specific files)
# ============================================================================
def _resolve_data_dir() -> pathlib.Path:
    candidates = []
    try:
        candidates.append(pathlib.Path(__file__).parent.absolute())
    except Exception:
        pass
    candidates.append(pathlib.Path.home() / "Documents" / "CryptoDemoTrades_Short")
    for path in candidates:
        try:
            path.mkdir(parents=True, exist_ok=True)
            probe = path / ".write_probe"
            probe.touch()
            probe.unlink()
            return path
        except OSError:
            continue
    import tempfile
    fallback = pathlib.Path(tempfile.gettempdir()) / "CryptoDemoTrades_Short"
    fallback.mkdir(parents=True, exist_ok=True)
    return fallback

_SCRIPT_DIR = _resolve_data_dir()
CONFIG_FILE = _SCRIPT_DIR / "scanner_config_short.json"
LOG_FILE = _SCRIPT_DIR / "scanner_log_short.json"
_config_lock = threading.Lock()

def load_config() -> dict:
    cfg = dict(DEFAULT_CONFIG)
    if CONFIG_FILE.exists():
        try:
            saved = json.loads(CONFIG_FILE.read_text(encoding="utf-8"))
            for k in DEFAULT_CONFIG:
                if k in saved:
                    cfg[k] = saved[k]
        except (json.JSONDecodeError, OSError):
            pass
    import os as _os
    for _cfg_k, _env_k in (("api_key", "OKX_API_KEY"),
                           ("api_secret", "OKX_API_SECRET"),
                           ("api_passphrase", "OKX_API_PASSPHRASE")):
        _v = _os.environ.get(_env_k, "").strip()
        if _v:
            cfg[_cfg_k] = _v
    return cfg

def save_config(cfg: dict):
    import os as _os
    _cfg_to_save = dict(cfg)
    for _cfg_k, _env_k in (("api_key", "OKX_API_KEY"),
                           ("api_secret", "OKX_API_SECRET"),
                           ("api_passphrase", "OKX_API_PASSPHRASE")):
        if _os.environ.get(_env_k, "").strip():
            _cfg_to_save[_cfg_k] = ""
    try:
        with _config_lock:
            CONFIG_FILE.write_text(json.dumps(_cfg_to_save, indent=2), encoding="utf-8")
            try:
                _os.chmod(CONFIG_FILE, 0o600)
            except (OSError, NotImplementedError):
                pass
    except (OSError, TypeError, ValueError):
        pass

def load_log():
    if LOG_FILE.exists():
        try:
            return json.loads(LOG_FILE.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            pass
    return {"health": {"total_cycles": 0, "last_scan_at": None,
                       "last_scan_duration_s": 0.0, "total_api_errors": 0,
                       "watchlist_size": 0, "pre_filtered_out": 0, "deep_scanned": 0},
            "signals": []}

def save_log(log):
    try:
        LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
        LOG_FILE.write_text(json.dumps(log, indent=2), encoding="utf-8")
    except (OSError, TypeError, ValueError):
        pass

# ============================================================================
# MODULE-LEVEL SHARED STATE
# ============================================================================
if "_scanner_initialised_short" not in st.session_state:
    import builtins
    if not getattr(builtins, "_short_scanner_globals_set", False):
        import builtins as _b
        _b._short_scanner_globals_set = True
        _b._ss_cfg = load_config()
        _b._ss_log = load_log()
        _b._ss_log_lock = threading.Lock()
        _b._ss_running = threading.Event()
        _b._ss_running.set()
        _b._ss_thread = None
        _b._ss_filter_counts = {}
        _b._ss_filter_lock = threading.Lock()
        _b._ss_last_error = ""
        _b._ss_rescan_event = threading.Event()
        _b._ss_watcher_thread = None
        _b._ss_watcher_event = threading.Event()
        _b._ss_watcher_last_ts = 0
        _b._ss_watcher_last_dur = 0.0
        _b._ss_sl_paused = False
        _b._ss_api_conn_status = {"status": "untested", "message": "", "tested_at": None,
                                   "demo_mode": None, "uid": "", "pos_mode": "net_mode", "acct_lv": "2"}
        _b._ss_last_trade_raw = {}
        _b._ss_error_log = []
        _b._ss_error_log_lock = threading.Lock()
        _b._ss_symbol_cache = {"symbols": [], "fetched_at": 0, "wl_key": "", "ct_val": {}, "ct_raw": {}}
        _b._ss_reconcile_t1_last = 0
        _b._ss_reconcile_t1_runs = 0
        _b._ss_reconcile_t1_actions = 0
        _b._ss_reconcile_t2_last = 0
        _b._ss_reconcile_t2_runs = 0
        _b._ss_reconcile_t2_actions = 0
    st.session_state["_scanner_initialised_short"] = True

import builtins as _b
if not hasattr(_b, "_ss_watcher_thread"):
    _b._ss_watcher_thread = None
if not hasattr(_b, "_ss_watcher_event"):
    _b._ss_watcher_event = threading.Event()

_cfg = _b._ss_cfg
_log = _b._ss_log
_log_lock = _b._ss_log_lock
_scanner_running = _b._ss_running
_filter_lock = _b._ss_filter_lock
_filter_counts = _b._ss_filter_counts
_rescan_event = _b._ss_rescan_event

# ============================================================================
# HTTP HELPERS
# ============================================================================
HEADERS = {"User-Agent": "Mozilla/5.0", "Accept": "application/json", "Accept-Language": "en-US,en;q=0.9"}
_local = threading.local()

def get_session():
    if not hasattr(_local, "session"):
        s = requests.Session()
        s.headers.update(HEADERS)
        from requests.adapters import HTTPAdapter
        adapter = HTTPAdapter(pool_connections=4, pool_maxsize=25, max_retries=0)
        s.mount("https://", adapter)
        s.mount("http://", adapter)
        _local.session = s
    return _local.session

def safe_get(url, params=None, _retries=4):
    for attempt in range(_retries):
        try:
            _rate_wait()
            with _api_sem:
                r = get_session().get(url, params=params, timeout=10)
            if r.status_code == 429:
                wait = min(int(r.headers.get("Retry-After", 5)), 5)
                time.sleep(wait)
                continue
            if r.status_code in (418, 403, 451):
                raise RuntimeError(f"HTTP {r.status_code}: OKX is blocking this IP")
            r.raise_for_status()
            data = r.json()
            if isinstance(data, dict) and "code" in data and data["code"] != "0":
                raise RuntimeError(f"OKX API error {data['code']}: {data.get('msg', '')}")
            return data
        except requests.exceptions.ConnectionError:
            if attempt < _retries - 1:
                time.sleep(1)
                continue
            raise
    raise RuntimeError(f"Failed after {_retries} retries: {url}")

def _append_error(err_type: str, message: str, symbol: str = "", endpoint: str = ""):
    entry = {"ts": dubai_now().isoformat(), "type": err_type, "symbol": symbol,
             "endpoint": endpoint, "message": message[:400]}
    try:
        lock = getattr(_b, "_ss_error_log_lock", None)
        log = getattr(_b, "_ss_error_log", None)
        if lock is None or log is None:
            return
        with lock:
            log.append(entry)
            if len(log) > 500:
                del log[:len(log) - 500]
    except Exception:
        pass

# ============================================================================
# OKX SYMBOL HELPERS
# ============================================================================
def _to_okx(sym: str) -> str:
    return f"{sym[:-4]}-USDT-SWAP" if sym.endswith("USDT") else sym

def _from_okx(inst_id: str) -> str:
    return inst_id.replace("-USDT-SWAP", "USDT") if inst_id.endswith("-USDT-SWAP") else inst_id

def get_symbols(watchlist: list) -> tuple:
    active = set()
    ct_vals = {}
    data = safe_get(f"{BASE}/api/v5/public/instruments", {"instType": "SWAP"})
    for s in data.get("data", []):
        inst_id = s.get("instId", "")
        if inst_id.endswith("-USDT-SWAP") and s.get("state") == "live":
            sym = _from_okx(inst_id)
            active.add(sym)
            try:
                _cv = float(s.get("ctVal") or 0)
                _cmul = float(s.get("ctMult") or 1)
                _eff = _cv if _cv > 0 else 0.0
                ct_vals[sym] = _eff
                _b._ss_symbol_cache.setdefault("ct_raw", {})[sym] = (_cv, _cmul, _eff)
            except (TypeError, ValueError):
                ct_vals[sym] = 0.0
    return [s for s in watchlist if s in active], ct_vals

def get_symbols_cached(watchlist: list) -> list:
    now = time.time()
    cache = _b._ss_symbol_cache
    wl_key = ",".join(sorted(watchlist))
    if (not cache["symbols"] or now - cache["fetched_at"] > _SYMBOL_CACHE_TTL or
            cache["wl_key"] != wl_key):
        syms, ct_vals = get_symbols(watchlist)
        cache["symbols"] = syms
        cache["ct_val"] = ct_vals
        cache["fetched_at"] = now
        cache["wl_key"] = wl_key
    return list(cache["symbols"])

# ============================================================================
# OKX AUTO-TRADING — SHORT VERSION
# ============================================================================
def _okx_sign(timestamp: str, method: str, request_path: str, body: str, secret: str) -> str:
    msg = timestamp + method + request_path + body
    return base64.b64encode(hmac.new(secret.encode("utf-8"), msg.encode("utf-8"), hashlib.sha256).digest()).decode()

def _trade_post(path: str, body: dict, cfg: dict) -> dict:
    ts = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    body_str = json.dumps(body)
    sign = _okx_sign(ts, "POST", path, body_str, cfg["api_secret"])
    headers = {"OK-ACCESS-KEY": cfg["api_key"], "OK-ACCESS-SIGN": sign,
               "OK-ACCESS-TIMESTAMP": ts, "OK-ACCESS-PASSPHRASE": cfg["api_passphrase"],
               "Content-Type": "application/json"}
    if cfg.get("demo_mode", True):
        headers["x-simulated-trading"] = "1"
    sess = get_session()
    merged = {**dict(sess.headers), **headers}
    with _api_sem:
        r = sess.post(f"{BASE}{path}", headers=merged, data=body_str, timeout=20)
    r.raise_for_status()
    try:
        return r.json()
    except (json.JSONDecodeError, ValueError) as _je:
        _snippet = r.text[:200].replace("\n", " ") if r.text else ""
        raise RuntimeError(f"OKX POST {path} returned non-JSON: {_snippet!r}") from _je

def _trade_get(path: str, params: dict, cfg: dict) -> dict:
    qs = ("?" + "&".join(f"{k}={v}" for k, v in params.items())) if params else ""
    ts = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    sign = _okx_sign(ts, "GET", path + qs, "", cfg["api_secret"])
    headers = {"OK-ACCESS-KEY": cfg["api_key"], "OK-ACCESS-SIGN": sign,
               "OK-ACCESS-TIMESTAMP": ts, "OK-ACCESS-PASSPHRASE": cfg["api_passphrase"],
               "Content-Type": "application/json"}
    if cfg.get("demo_mode", True):
        headers["x-simulated-trading"] = "1"
    sess = get_session()
    merged = {**dict(sess.headers), **headers}
    with _api_sem:
        r = sess.get(f"{BASE}{path}", params=params, headers=merged, timeout=20)
    r.raise_for_status()
    try:
        return r.json()
    except (json.JSONDecodeError, ValueError) as _je:
        _snippet = r.text[:200].replace("\n", " ") if r.text else ""
        raise RuntimeError(f"OKX GET {path} returned non-JSON: {_snippet!r}") from _je

def test_api_connection(cfg: dict) -> dict:
    if not cfg.get("api_key") or not cfg.get("api_secret") or not cfg.get("api_passphrase"):
        return {"status": "error", "message": "API credentials are incomplete.", "uid": "", "pos_mode": "net_mode"}
    try:
        resp = _trade_get("/api/v5/account/balance", {}, cfg)
        if resp.get("code") != "0":
            return {"status": "error", "message": f"OKX error {resp.get('code')}: {resp.get('msg', 'unknown')}",
                    "uid": "", "pos_mode": "net_mode"}
        cfg_resp = _trade_get("/api/v5/account/config", {}, cfg)
        uid = ""
        pos_mode = "net_mode"
        acct_lv = "2"
        if cfg_resp.get("code") == "0":
            acct = cfg_resp.get("data", [{}])[0]
            uid = acct.get("uid", "")
            pos_mode = acct.get("posMode", "net_mode")
            acct_lv = str(acct.get("acctLv", "2"))
        if acct_lv == "1":
            return {"status": "error",
                    "message": "Account in Simple mode — SWAP trading disabled. Switch to Single-currency margin.",
                    "uid": uid, "pos_mode": pos_mode, "acct_lv": acct_lv}
        env = "Demo" if cfg.get("demo_mode", True) else "Live"
        return {"status": "ok", "message": f"Connected ({env}) · Mode: {pos_mode}", "uid": uid,
                "pos_mode": pos_mode, "acct_lv": acct_lv}
    except Exception as exc:
        return {"status": "error", "message": str(exc), "uid": "", "pos_mode": "net_mode"}

def _get_ct_val(sym: str) -> float:
    ct = _b._ss_symbol_cache.get("ct_val", {}).get(sym)
    if ct and ct > 0:
        return ct
    try:
        data = safe_get(f"{BASE}/api/v5/public/instruments",
                        {"instType": "SWAP", "instId": _to_okx(sym)})
        for s in data.get("data", []):
            raw = s.get("ctVal", "")
            val = float(raw) if raw not in ("", None) else 0.0
            if val > 0:
                _b._ss_symbol_cache.setdefault("ct_val", {})[sym] = val
                return val
    except Exception as _e:
        raise ValueError(f"ctVal for {_to_okx(sym)} could not be fetched: {_e}") from _e
    raise ValueError(f"ctVal for {_to_okx(sym)} not found")

def _set_leverage_okx(sym: str, cfg: dict) -> dict:
    lev = str(min(int(cfg.get("trade_leverage", 10)), get_max_leverage(sym)))
    resp = _trade_post("/api/v5/account/set-leverage", {
        "instId": _to_okx(sym), "lever": lev, "mgnMode": cfg.get("trade_margin_mode", "isolated"),
    }, cfg)
    if resp.get("code") != "0":
        raise RuntimeError(f"OKX rejected set-leverage {lev}x for {_to_okx(sym)}: {resp.get('msg', '')}")
    return resp

_OKX_KNOWN_ERRORS = {
    "51010": "Account in Simple mode — SWAP trading disabled.",
    "51000": "Parameter error — check instId, tdMode, or sz.",
    "51001": "Instrument doesn't exist on OKX — coin may be delisted.",
    "51006": "Order price out of limit.",
    "51008": "Insufficient balance.",
    "51020": "Order quantity below minimum.",
    "58001": "Invalid API key — check credentials.",
}

def _okx_err(resp: dict) -> str:
    top = f"OKX {resp.get('code', '?')}: {resp.get('msg', 'unknown')}"
    items = resp.get("data") or []
    if items:
        d = items[0]
        s_code = d.get("sCode", "")
        s_msg = d.get("sMsg", "").strip()
        if s_code and s_code not in ("", "0"):
            friendly = _OKX_KNOWN_ERRORS.get(s_code)
            detail = friendly if friendly else (s_msg or s_code)
            return f"[{s_code}] {detail}"
    return top

def _okx_log_entry(sig: dict, label: str, **fields) -> None:
    ts = dubai_now().strftime("%m/%d %H:%M:%S")
    parts = [f"[{ts}] {label}"]
    for k, v in fields.items():
        parts.append(f"{k}: {v}")
    entry = " | ".join(parts)
    if not isinstance(sig.get("okx_log"), list):
        sig["okx_log"] = []
    sig["okx_log"].append(entry)

def _pround(x, sig=6):
    try:
        x = float(x)
        if x == 0:
            return 0.0
        magnitude = math.floor(math.log10(abs(x)))
        decimals = max(2, sig - int(magnitude))
        return round(x, decimals)
    except Exception:
        return x

# ============================================================================
# BULK TICKER FETCH + PRE-FILTER
# ============================================================================
def get_bulk_tickers() -> dict:
    data = safe_get(f"{BASE}/api/v5/market/tickers", {"instType": "SWAP"})
    result = {}
    for t in data.get("data", []):
        inst_id = t.get("instId", "")
        if not inst_id.endswith("-USDT-SWAP"):
            continue
        sym = _from_okx(inst_id)
        try:
            result[sym] = {"last": float(t.get("last", 0) or 0), "open24h": float(t.get("open24h", 0) or 0),
                           "high24h": float(t.get("high24h", 0) or 0), "low24h": float(t.get("low24h", 0) or 0),
                           "volCcy24h": float(t.get("volCcy24h", 0) or 0)}
        except Exception:
            pass
    return result

def pre_filter_by_ticker(symbols: list, tickers: dict) -> list:
    kept = []
    for sym in symbols:
        t = tickers.get(sym)
        if not t:
            continue
        if t["volCcy24h"] < PRE_FILTER_MIN_VOL_USDT:
            continue
        if t["low24h"] > 0 and t["last"] < t["low24h"] * PRE_FILTER_LOW_BUFFER:
            continue
        kept.append(sym)
    return kept

# ============================================================================
# CANDLE FETCH
# ============================================================================
def get_klines(sym: str, interval: str, limit: int) -> list:
    okx_iv = OKX_INTERVALS.get(interval, interval)
    inst_id = _to_okx(sym)
    all_bars = []
    after = None
    while len(all_bars) < limit:
        batch = min(300, limit - len(all_bars))
        params = {"instId": inst_id, "bar": okx_iv, "limit": batch}
        if after:
            params["after"] = after
        data = safe_get(f"{BASE}/api/v5/market/candles", params)
        bars = data.get("data", [])
        if not bars:
            break
        all_bars.extend(bars)
        after = bars[-1][0]
        if len(bars) < batch:
            break
    all_bars.reverse()
    return [{"time": int(b[0]), "open": float(b[1]), "high": float(b[2]),
             "low": float(b[3]), "close": float(b[4]), "volume": float(b[5])}
            for b in all_bars]

# ============================================================================
# TECHNICAL INDICATORS
# ============================================================================
def calc_atr(candles: list, period: int = 14) -> list:
    if len(candles) < period + 1:
        return []
    trs = []
    for i in range(1, len(candles)):
        h = candles[i]["high"]
        l = candles[i]["low"]
        pc = candles[i - 1]["close"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    if len(trs) < period:
        return []
    atr = [sum(trs[:period]) / period]
    for tr in trs[period:]:
        atr.append((atr[-1] * (period - 1) + tr) / period)
    return atr

def calc_rsi_series(closes, period=14):
    if len(closes) < period + 2:
        return []
    deltas = [closes[i] - closes[i-1] for i in range(1, len(closes))]
    gains = [max(d, 0.) for d in deltas]
    losses = [max(-d, 0.) for d in deltas]
    ag = sum(gains[:period]) / period
    al = sum(losses[:period]) / period
    rsi = [100. if al == 0 else 100 - 100 / (1 + ag / al)]
    for i in range(period, len(deltas)):
        ag = (ag * (period - 1) + gains[i]) / period
        al = (al * (period - 1) + losses[i]) / period
        rsi.append(100. if al == 0 else 100 - 100 / (1 + ag / al))
    return rsi

def calc_ema(values: list, period: int) -> list:
    if len(values) < period:
        return []
    k = 2.0 / (period + 1)
    result = [sum(values[:period]) / period]
    for v in values[period:]:
        result.append(v * k + result[-1] * (1 - k))
    return result

def calc_macd(closes: list, fast=12, slow=26, signal_period=9):
    if len(closes) < slow + signal_period:
        return [], [], []
    ema_f = calc_ema(closes, fast)
    ema_s = calc_ema(closes, slow)
    trim = len(ema_f) - len(ema_s)
    ema_f = ema_f[trim:]
    macd_line = [f - s for f, s in zip(ema_f, ema_s)]
    if len(macd_line) < signal_period:
        return [], [], []
    sig_line = calc_ema(macd_line, signal_period)
    trim2 = len(macd_line) - len(sig_line)
    macd_aligned = macd_line[trim2:]
    histogram = [m - s for m, s in zip(macd_aligned, sig_line)]
    return macd_aligned, sig_line, histogram

def macd_bearish_and_value(closes: list, crossover_lookback: int = 12):
    """SHORT version: MACD bearish condition - MACD < 0, signal < 0, histogram decreasing."""
    macd_line, sig_line, histogram = calc_macd(closes)
    _last_val = macd_line[-1] if macd_line else None
    if not histogram or len(histogram) < 2:
        return False, _last_val
    if macd_line[-1] >= 0 or sig_line[-1] >= 0:
        return False, _last_val
    if histogram[-1] >= 0 or histogram[-1] >= histogram[-2]:
        return False, _last_val
    n = min(crossover_lookback + 1, len(macd_line))
    for i in range(1, n):
        prev, curr = -(i + 1), -i
        if (len(macd_line) + prev >= 0 and
                macd_line[prev] >= sig_line[prev] and
                macd_line[curr] < sig_line[curr]):
            return True, _last_val
    return False, _last_val

def calc_parabolic_sar(candles: list, af_start=0.02, af_step=0.02, af_max=0.20):
    if not candles:
        return []
    if len(candles) < 2:
        return [(candles[0]["close"], True)]
    highs = [c["high"] for c in candles]
    lows = [c["low"] for c in candles]
    closes = [c["close"] for c in candles]
    bullish = closes[1] >= closes[0]
    ep, sar, af = (highs[0], lows[0], af_start) if bullish else (lows[0], highs[0], af_start)
    result = [(sar, bullish)]
    for i in range(1, len(candles)):
        new_sar = sar + af * (ep - sar)
        if bullish:
            new_sar = min(new_sar, lows[i-1])
            if i >= 2:
                new_sar = min(new_sar, lows[i-2])
            if lows[i] < new_sar:
                bullish, new_sar, ep, af = False, ep, lows[i], af_start
            else:
                if highs[i] > ep:
                    ep = highs[i]
                    af = min(af + af_step, af_max)
        else:
            new_sar = max(new_sar, highs[i-1])
            if i >= 2:
                new_sar = max(new_sar, highs[i-2])
            if highs[i] > new_sar:
                bullish, new_sar, ep, af = True, ep, highs[i], af_start
            else:
                if lows[i] < ep:
                    ep = lows[i]
                    af = min(af + af_step, af_max)
        sar = new_sar
        result.append((sar, bullish))
    return result

# ============================================================================
# PDZ ZONE — SHORT VERSION (Premium zone is target)
# ============================================================================
def calc_pdz_zone_short(candles: list, price: float, buffer_pct: float = 0.015) -> tuple:
    if not candles or len(candles) < 50:
        return False, "insufficient_data"
    lookback = candles[-290:]
    H = max(c["high"] for c in lookback)
    L = min(c["low"] for c in lookback)
    if H <= L:
        return False, "flat_range"
    premium_bottom = 0.95 * H + 0.05 * L
    equil_top = 0.525 * H + 0.475 * L
    equil_bottom = 0.475 * H + 0.525 * L
    discount_top = 0.05 * H + 0.95 * L
    band_a_ceil = premium_bottom * (1 - buffer_pct)
    if price >= premium_bottom:
        return True, "Premium"
    elif price <= discount_top:
        return False, "Discount"
    elif equil_bottom <= price <= equil_top:
        return False, "Equilibrium"
    elif equil_top < price < premium_bottom:
        dist_pct = (premium_bottom - price) / premium_bottom * 100
        label = f"BandA({dist_pct:.1f}%↓Prem)"
        return (price <= band_a_ceil), label
    else:
        dist_pct = (equil_bottom - price) / equil_bottom * 100
        label = f"BandB({dist_pct:.1f}%↓Equil)"
        return False, label

# ============================================================================
# FILTER FUNNEL COUNTERS
# ============================================================================
def _reset_filter_counts():
    counts = {
        "total_watchlist": 0, "pre_filtered_out": 0, "checked": 0,
        "f2_pdz15m": 0, "f3_pdz5m": 0, "f4_rsi5m": 0, "f5_rsi1h": 0,
        "f5b_atr": 0, "f6_ema_3m": 0, "f6_ema_5m": 0, "f6_ema_15m": 0,
        "f7_macd_3m": 0, "f7_macd_5m": 0, "f7_macd_15m": 0,
        "f8_sar_3m": 0, "f8_sar_5m": 0, "f8_sar_15m": 0,
        "f9_vol": 0, "f10_ema_cross": 0, "f_empty_data": 0,
        "passed": 0, "super_setup": 0, "super_cap_demoted": 0,
        "f_sl_cooldown": 0, "errors": 0, "scan_cfg": {},
        "pre_filter_passed_syms": [], "checked_syms": [],
        "f2_elim_syms": [], "f3_elim_syms": [], "f4_elim_syms": [],
        "f5_elim_syms": [], "f5b_elim_syms": [],
        "f6_ema_3m_elim_syms": [], "f6_ema_5m_elim_syms": [], "f6_ema_15m_elim_syms": [],
        "f7_macd_3m_elim_syms": [], "f7_macd_5m_elim_syms": [], "f7_macd_15m_elim_syms": [],
        "f8_sar_3m_elim_syms": [], "f8_sar_5m_elim_syms": [], "f8_sar_15m_elim_syms": [],
        "f9_elim_syms": [], "f10_elim_syms": [], "f_empty_data_syms": [],
        "passed_syms": [], "super_setup_syms": [], "super_cap_demoted_syms": [],
        "f_sl_cooldown_syms": [], "blocked_by_sl_cooldown_syms": [],
        "flushed_at": 0.0, "scan_completed_at": 0.0,
    }
    with _filter_lock:
        _filter_counts.clear()
        _filter_counts.update(counts)
        _b._ss_filter_counts = _filter_counts

_tl_counts = threading.local()

def _tl_delta() -> dict:
    _d = getattr(_tl_counts, "delta", None)
    if _d is None:
        _d = {}
        _tl_counts.delta = _d
    return _d

def _incr_filter(key: str, n: int = 1) -> None:
    _d = _tl_delta()
    _d[key] = _d.get(key, 0) + n

def _record_elim(count_key: str, sym_list_key: str, sym: str) -> None:
    _incr_filter(count_key)
    _lst = _filter_counts.get(sym_list_key)
    if _lst is not None:
        _lst.append(sym)

def _flush_tl_counts() -> None:
    _d = getattr(_tl_counts, "delta", None)
    if not _d:
        _tl_counts.delta = {}
        return
    with _filter_lock:
        for _k, _v in _d.items():
            _filter_counts[_k] = _filter_counts.get(_k, 0) + _v
    _tl_counts.delta = {}

# ============================================================================
# SHORT ORDER PLACEMENT
# ============================================================================
def place_okx_order_short(sig: dict, cfg: dict) -> dict:
    try:
        sym = sig.get("symbol", "")
        if not sym:
            return {"ordId": "", "algoId": "", "sz": 0, "status": "error",
                    "error": "Signal missing 'symbol' field"}
        try:
            entry = float(sig["entry"])
            tp = float(sig["tp"])
            sl = float(sig["sl"])
            usdt = float(cfg.get("trade_usdt_amount", 10))
            _lev_cfg = int(cfg.get("trade_leverage", 10))
        except (TypeError, ValueError, KeyError) as _valexc:
            return {"ordId": "", "algoId": "", "sz": 0, "status": "error",
                    "error": f"Malformed signal: {_valexc}"}
        if entry <= 0 or tp <= 0 or sl <= 0 or usdt <= 0 or _lev_cfg <= 0:
            return {"ordId": "", "algoId": "", "sz": 0, "status": "error",
                    "error": "Invalid numeric values"}

        if sl <= entry:
            return {"ordId": "", "algoId": "", "sz": 0, "status": "error",
                    "error": f"SHORT SL ({sl}) must be above entry ({entry})"}
        if tp >= entry:
            return {"ordId": "", "algoId": "", "sz": 0, "status": "error",
                    "error": f"SHORT TP ({tp}) must be below entry ({entry})"}

        lev = min(_lev_cfg, get_max_leverage(sym))
        mode = cfg.get("trade_margin_mode", "isolated")

        ct_cache = _b._ss_symbol_cache.get("ct_val", {})
        if ct_cache and sym not in ct_cache:
            return {"ordId": "", "algoId": "", "sz": 0, "status": "error",
                    "error": f"Instrument {_to_okx(sym)} not found in OKX live SWAP list"}

        try:
            ct_val = _get_ct_val(sym)
        except ValueError as _ctv_err:
            return {"ordId": "", "algoId": "", "sz": 0, "status": "error", "error": str(_ctv_err)}
        notional = usdt * lev
        raw_contracts = notional / (ct_val * entry)
        contracts = max(1, int(raw_contracts))
        if contracts > 100_000:
            return {"ordId": "", "algoId": "", "sz": contracts, "status": "error",
                    "error": f"Contract count too large ({contracts})"}

        _base_info = {"ct_val": ct_val, "notional": notional, "tdMode": mode, "is_hedge": False}

        is_hedge = False
        try:
            _pm_resp = _trade_get("/api/v5/account/config", {}, cfg)
            if _pm_resp.get("code") == "0":
                _pm = _pm_resp.get("data", [{}])[0].get("posMode", "net_mode")
                is_hedge = (_pm == "long_short_mode")
                _cs = getattr(_b, "_ss_api_conn_status", None)
                if _cs is not None:
                    _cs["pos_mode"] = _pm
        except Exception:
            pass
        _base_info["is_hedge"] = is_hedge

        try:
            _set_leverage_okx(sym, cfg)
        except Exception as lev_exc:
            return {"ordId": "", "algoId": "", "sz": 0, "status": "error",
                    "error": f"set-leverage failed: {lev_exc}", **_base_info}

        order_body = {"instId": _to_okx(sym), "tdMode": mode, "side": "sell", "ordType": "market", "sz": str(contracts)}
        if is_hedge:
            order_body["posSide"] = "short"

        resp = _trade_post("/api/v5/trade/order", order_body, cfg)
        _b._ss_last_trade_raw = {"endpoint": "/api/v5/trade/order", "body_sent": order_body,
                                  "response": resp, "is_hedge": is_hedge, "contracts": contracts, "ct_val": ct_val}
        d0 = (resp.get("data") or [{}])[0]
        ord_id = d0.get("ordId", "")

        if resp.get("code") != "0":
            err = _okx_err(resp)
            return {"ordId": "", "algoId": "", "sz": contracts, "status": "error", "error": err, **_base_info}
        if d0.get("sCode", "0") != "0":
            err = f"Entry order: {d0.get('sCode')}: {d0.get('sMsg', '')}"
            return {"ordId": ord_id, "algoId": "", "sz": contracts, "status": "error", "error": err, **_base_info}

        actual_entry = entry
        time.sleep(0.3)
        try:
            fill_resp = _trade_get("/api/v5/trade/order", {"instId": _to_okx(sym), "ordId": ord_id}, cfg)
            if fill_resp.get("code") == "0":
                fill_d = fill_resp.get("data", [{}])[0]
                avg_px = float(fill_d.get("avgPx", 0) or 0)
                if avg_px > 0:
                    actual_entry = avg_px
        except Exception:
            pass

        tp_pct = float(cfg.get("tp_pct", 1.5)) / 100
        actual_tp = _pround(actual_entry * (1 - tp_pct))
        if mode == "isolated":
            actual_sl = _pround(actual_entry * (1 + 1 / lev))
        else:
            sl_pct = float(cfg.get("sl_pct", 3.0)) / 100
            actual_sl = _pround(actual_entry * (1 + sl_pct))

        if mode == "cross":
            _tp_sig = _place_tp_only_order_short(sig, cfg, actual_tp, contracts)
            if not _tp_sig:
                return {"ordId": ord_id, "algoId": "", "sz": contracts, "status": "partial",
                        "error": "Entry ✅ · TP algo ❌ (cross mode, no SL)",
                        "actual_entry": actual_entry, "actual_tp": actual_tp, "actual_sl": actual_sl,
                        **_base_info}
            return {"ordId": ord_id, "algoId": _tp_sig, "sz": contracts, "status": "placed",
                    "error": "", "actual_entry": actual_entry, "actual_tp": actual_tp,
                    "actual_sl": actual_sl, "tp_algo_id": _tp_sig, **_base_info}

        algo_body = {"instId": _to_okx(sym), "tdMode": mode, "side": "buy", "ordType": "oco",
                     "sz": str(contracts), "tpTriggerPx": str(actual_tp), "tpTriggerPxType": "mark",
                     "tpOrdPx": "-1", "slTriggerPx": str(actual_sl), "slTriggerPxType": "mark", "slOrdPx": "-1"}
        if is_hedge:
            algo_body["posSide"] = "short"
        else:
            algo_body["reduceOnly"] = "true"

        algo_resp = _trade_post("/api/v5/trade/order-algo", algo_body, cfg)
        ad = (algo_resp.get("data") or [{}])[0]
        algo_id = ad.get("algoId", "")

        if algo_resp.get("code") != "0" or (ad.get("sCode", "0") != "0" and ad.get("sCode", "")):
            algo_err = _okx_err(algo_resp) if algo_resp.get("code") != "0" \
                       else f"OCO algo: {ad.get('sCode')}: {ad.get('sMsg', '')}"
            return {"ordId": ord_id, "algoId": "", "sz": contracts, "status": "partial",
                    "error": f"Entry ✅ · TP/SL ❌ {algo_err}",
                    "actual_entry": actual_entry, "actual_tp": actual_tp, "actual_sl": actual_sl,
                    **_base_info}

        return {"ordId": ord_id, "algoId": algo_id, "sz": contracts, "status": "placed",
                "error": "", "actual_entry": actual_entry, "actual_tp": actual_tp,
                "actual_sl": actual_sl, **_base_info}

    except Exception as exc:
        _append_error("trade", str(exc), symbol=sig.get("symbol", ""))
        return {"ordId": "", "algoId": "", "sz": 0, "status": "error", "error": str(exc)}

def _place_tp_only_order_short(sig: dict, cfg: dict, tp_price: float, total_contracts: int) -> str:
    try:
        sym = sig.get("symbol", "")
        mode = (sig.get("order_margin_mode") or cfg.get("trade_margin_mode", "isolated")).strip().lower()
        is_hedge = bool(sig.get("order_is_hedge", False))
        algo_body = {"instId": _to_okx(sym), "tdMode": mode, "side": "buy", "ordType": "conditional",
                     "sz": str(int(max(1, total_contracts))), "tpTriggerPx": str(_pround(tp_price)),
                     "tpTriggerPxType": "mark", "tpOrdPx": "-1"}
        if is_hedge:
            algo_body["posSide"] = "short"
        else:
            algo_body["reduceOnly"] = "true"
        resp = _trade_post("/api/v5/trade/order-algo", algo_body, cfg)
        ad = (resp.get("data") or [{}])[0]
        if resp.get("code") != "0":
            return ""
        return ad.get("algoId", "")
    except Exception as exc:
        _append_error("trade", f"TP-only algo exception: {exc}", symbol=sig.get("symbol", ""))
        return ""

# ============================================================================
# DCA FOR SHORT (average UP as price rises)
# ============================================================================
def _dca_compute_trigger_short(sig: dict, cfg: dict = None) -> float:
    avg_entry = float(sig.get("avg_entry", sig.get("entry", 0)) or 0)
    if avg_entry <= 0:
        return 0.0
    mode = (sig.get("order_margin_mode") or "isolated").strip().lower()
    lev = int(sig.get("trade_lev", 0) or 0)
    if lev <= 0:
        lev = 10
    cfg = cfg or {}
    if mode == "isolated":
        iso_dist = float(sig.get("dca_iso_distance_pct", cfg.get("dca_iso_distance_pct", 80.0)) or 80.0)
        iso_dist = max(10.0, min(95.0, iso_dist))
        rise_pct = (1.0 / lev) * (iso_dist / 100.0)
        return _pround(avg_entry * (1.0 + rise_pct))
    else:
        cross_rise = float(sig.get("dca_cross_rise_pct", cfg.get("dca_cross_rise_pct", 7.0)) or 7.0)
        cross_rise = max(0.1, min(50.0, cross_rise))
        rise_pct = cross_rise / 100.0
        return _pround(avg_entry * (1.0 + rise_pct))

def _dca_next_usdt(sig: dict) -> float:
    fills = sig.get("dca_fills") or []
    base = float(sig.get("trade_usdt", 0) or 0)
    if base <= 0:
        return 0.0
    dca_count = max(0, len(fills) - 1)
    next_idx = dca_count + 1
    return base * (2 ** (next_idx - 1))

def place_okx_dca_order_short(sig: dict, cfg: dict, dca_usdt: float) -> dict:
    try:
        sym = sig.get("symbol", "")
        if not sym:
            return {"ordId": "", "sz": 0, "status": "error", "error": "Missing symbol"}
        if dca_usdt <= 0:
            return {"ordId": "", "sz": 0, "status": "error", "error": f"Invalid DCA size: {dca_usdt}"}

        lev = int(sig.get("trade_lev", 0) or 0)
        if lev <= 0:
            lev = int(cfg.get("trade_leverage", 10) or 10)
        lev = min(lev, get_max_leverage(sym))
        mode = (sig.get("order_margin_mode") or cfg.get("trade_margin_mode", "isolated")).strip().lower()

        ref_price = float(sig.get("latest_price", sig.get("avg_entry", sig.get("entry", 0))) or 0)
        if ref_price <= 0:
            return {"ordId": "", "sz": 0, "status": "error", "error": "No reference price"}

        try:
            ct_val = _get_ct_val(sym)
        except ValueError as _cv:
            return {"ordId": "", "sz": 0, "status": "error", "error": f"ct_val: {_cv}"}
        if ct_val <= 0:
            return {"ordId": "", "sz": 0, "status": "error", "error": f"Invalid ct_val: {ct_val}"}

        notional = dca_usdt * lev
        contracts = max(1, int(notional / (ct_val * ref_price)))
        if contracts > 100_000:
            return {"ordId": "", "sz": contracts, "status": "error", "error": f"Contract count too large ({contracts})"}

        is_hedge = bool(sig.get("order_is_hedge", False))
        try:
            _pm_resp = _trade_get("/api/v5/account/config", {}, cfg)
            if _pm_resp.get("code") == "0":
                _pm = _pm_resp.get("data", [{}])[0].get("posMode", "net_mode")
                is_hedge = (_pm == "long_short_mode")
        except Exception:
            pass

        try:
            _set_leverage_okx(sym, cfg)
        except Exception as lev_exc:
            _append_error("trade", f"DCA set-leverage warning: {lev_exc}", symbol=sym)

        order_body = {"instId": _to_okx(sym), "tdMode": mode, "side": "sell", "ordType": "market", "sz": str(contracts)}
        if is_hedge:
            order_body["posSide"] = "short"

        resp = _trade_post("/api/v5/trade/order", order_body, cfg)
        _b._ss_last_trade_raw = {"endpoint": "/api/v5/trade/order (DCA add)", "body_sent": order_body,
                                  "response": resp, "is_hedge": is_hedge, "contracts": contracts,
                                  "ct_val": ct_val, "dca_usdt": dca_usdt}
        d0 = (resp.get("data") or [{}])[0]
        ord_id = d0.get("ordId", "")

        if resp.get("code") != "0":
            err = _okx_err(resp)
            return {"ordId": "", "sz": contracts, "status": "error", "error": err}
        if d0.get("sCode", "0") != "0":
            err = f"DCA order: {d0.get('sCode')}: {d0.get('sMsg', '')}"
            return {"ordId": ord_id, "sz": contracts, "status": "error", "error": err}

        actual_entry = ref_price
        time.sleep(0.3)
        try:
            fill_resp = _trade_get("/api/v5/trade/order", {"instId": _to_okx(sym), "ordId": ord_id}, cfg)
            if fill_resp.get("code") == "0":
                fill_d = fill_resp.get("data", [{}])[0]
                avg_px = float(fill_d.get("avgPx", 0) or 0)
                if avg_px > 0:
                    actual_entry = avg_px
        except Exception:
            pass

        return {"ordId": ord_id, "sz": contracts, "status": "placed", "error": "",
                "actual_entry": actual_entry, "ct_val": ct_val, "notional": notional,
                "tdMode": mode, "is_hedge": is_hedge}

    except Exception as exc:
        _append_error("trade", f"DCA exception: {exc}", symbol=sig.get("symbol", ""))
        return {"ordId": "", "sz": 0, "status": "error", "error": str(exc)}

def _cancel_algo_best_effort_short(algo_id: str, sym: str, cfg: dict) -> None:
    if not algo_id or not sym:
        return
    try:
        _trade_post("/api/v5/trade/cancel-algos", [{"algoId": algo_id, "instId": _to_okx(sym)}], cfg)
    except Exception as exc:
        _append_error("trade", f"cancel-algo failed: {exc}", symbol=sym)

def _init_signal_trade_snapshot_short(sig: dict, cfg: dict) -> None:
    _dca_max_snap = int(cfg.get("trade_max_dca", 0) or 0)
    _dca_max_snap = max(0, min(6, _dca_max_snap))
    _entry_px = float(sig.get("entry", 0) or 0)
    _entry_usdt = float(sig.get("trade_usdt", cfg.get("trade_usdt_amount", 0)) or 0)
    _entry_lev = int(sig.get("trade_lev", cfg.get("trade_leverage", 10)) or 10)
    _entry_notnl = _entry_usdt * _entry_lev if _entry_lev > 0 else 0.0

    sig.setdefault("trade_usdt", _entry_usdt)
    sig.setdefault("trade_lev", _entry_lev)
    sig.setdefault("demo_mode", bool(cfg.get("demo_mode", True)))
    sig.setdefault("order_margin_mode", cfg.get("trade_margin_mode", "isolated") or "isolated")

    sig["dca_enabled"] = _dca_max_snap > 0
    sig["dca_max"] = _dca_max_snap
    sig["dca_count"] = 0
    sig["dca_iso_distance_pct"] = float(cfg.get("dca_iso_distance_pct", 80.0) or 80.0)
    sig["dca_cross_rise_pct"] = float(cfg.get("dca_cross_rise_pct", 7.0) or 7.0)

    _entry_tp_snap = float(sig.get("tp", 0) or 0)
    _entry_sl_snap = float(sig.get("sl", 0) or 0)
    sig["dca_fills"] = [{
        "price": _entry_px, "usdt": _entry_usdt, "leverage": _entry_lev,
        "notional": _entry_notnl, "ts": sig.get("timestamp", ""),
        "order_id": sig.get("order_id", "") or "", "dca_idx": 0,
        "tp": _entry_tp_snap, "sl": _entry_sl_snap,
    }] if _entry_px > 0 else []
    sig["avg_entry"] = _entry_px
    sig["total_usdt"] = _entry_usdt
    sig["total_notional"] = _entry_notnl
    sig["original_entry"] = _entry_px
    sig["final_sl_price"] = None

    _mode = (sig.get("order_margin_mode") or "isolated").strip().lower()
    if _mode == "isolated" and _entry_lev > 0:
        _sl_dist = 1.0 / _entry_lev
    else:
        _sl_dist = float(cfg.get("sl_pct", 3.0) or 3.0) / 100.0
    if _entry_px > 0 and _entry_sl_snap > _entry_px:
        _sl_dist = (_entry_sl_snap - _entry_px) / _entry_px
    sig["sl_distance_pct"] = float(_sl_dist)

    # DCA ladder pre-calculation for cross margin
    if _dca_max_snap > 0 and _mode == "cross" and _entry_px > 0:
        _tp_pct_l = float(cfg.get("tp_pct", 1.5) or 1.5)
        _rise_l = float(cfg.get("dca_cross_rise_pct", 7.0) or 7.0)
        sig["dca_ladder"] = _calc_cross_dca_ladder_short(
            _entry_px, _entry_usdt, _entry_lev,
            _dca_max_snap, _rise_l, _tp_pct_l
        )
    else:
        sig.setdefault("dca_ladder", [])

    try:
        sig["next_dca_px"] = float(_dca_compute_trigger_short(sig, cfg) or 0.0)
    except Exception:
        sig["next_dca_px"] = 0.0

def _calc_cross_dca_ladder_short(entry_px: float, base_usdt: float, leverage: int,
                                  max_dca: int, rise_pct: float, tp_pct: float) -> list:
    """Pre-calculate the full DCA price cascade for cross margin mode (SHORT)."""
    if entry_px <= 0 or base_usdt <= 0 or leverage <= 0 or max_dca <= 0:
        return []
    rise_frac = rise_pct / 100.0
    tp_frac = tp_pct / 100.0
    lev = max(1, int(leverage))

    fills = [{"price": entry_px, "notional": base_usdt * lev}]
    ladder = []

    for i in range(1, max_dca + 1):
        tot_not = sum(f["notional"] for f in fills)
        tot_unit = sum(f["notional"] / f["price"] for f in fills if f["price"] > 0)
        blended_before = tot_not / tot_unit if tot_unit > 0 else fills[-1]["price"]

        trigger_px = _pround(blended_before * (1.0 + rise_frac))
        dca_usdt = base_usdt * (2 ** (i - 1))
        dca_not = dca_usdt * lev

        fills_after = fills + [{"price": trigger_px, "notional": dca_not}]
        tot_not_a = sum(f["notional"] for f in fills_after)
        tot_unit_a = sum(f["notional"] / f["price"] for f in fills_after if f["price"] > 0)
        blend_after = tot_not_a / tot_unit_a if tot_unit_a > 0 else trigger_px
        tp_after = _pround(blend_after * (1.0 - tp_frac))

        ladder.append({
            "level": i,
            "trigger_px": trigger_px,
            "usdt": dca_usdt,
            "blended_avg": _pround(blend_after),
            "tp_px": tp_after,
        })
        fills.append({"price": trigger_px, "notional": dca_not})

    return ladder

def _force_close_position_short(sig: dict, cfg: dict) -> dict:
    sym = sig.get("symbol", "?")
    is_paper = not sig.get("order_id")

    try:
        if is_paper:
            _close_px = float(sig.get("latest_price") or sig.get("avg_entry") or sig.get("entry") or 0)
            with _log_lock:
                for _s in _b._ss_log["signals"]:
                    if _s.get("timestamp") == sig.get("timestamp") and _s.get("symbol") == sym:
                        _s["status"] = "closed_okx"
                        _s["close_price"] = _close_px
                        _s["close_time"] = dubai_now().isoformat()
                        break
                save_log(_b._ss_log)
            return {"success": True, "message": f"✅ {sym} paper short closed at {_close_px}."}

        _pos_resp = _trade_get("/api/v5/account/positions", {"instType": "SWAP", "instId": _to_okx(sym)}, cfg)
        if _pos_resp.get("code") != "0":
            return {"success": False, "message": f"Position fetch failed: {_pos_resp.get('msg', 'unknown')}"}

        _live = [p for p in _pos_resp.get("data", []) if abs(float(p.get("pos", 0) or 0)) != 0]
        if not _live:
            with _log_lock:
                for _s in _b._ss_log["signals"]:
                    if _s.get("timestamp") == sig.get("timestamp") and _s.get("symbol") == sym:
                        _s["status"] = "closed_okx"
                        _s["close_price"] = float(sig.get("latest_price") or sig.get("avg_entry") or sig.get("entry") or 0)
                        _s["close_time"] = dubai_now().isoformat()
                        break
                save_log(_b._ss_log)
            return {"success": True, "message": f"✅ {sym}: No open position on OKX — log updated."}

        _pos = _live[0]
        contracts = abs(int(float(_pos.get("pos", 0) or 0)))
        mode = (sig.get("order_margin_mode") or cfg.get("trade_margin_mode", "isolated")).strip().lower()
        is_hedge = bool(sig.get("order_is_hedge", False))

        for _aid_key in ("algo_id", "tp_algo_id"):
            _aid = sig.get(_aid_key, "")
            if _aid:
                _cancel_algo_best_effort_short(_aid, sym, cfg)

        _buy_body = {"instId": _to_okx(sym), "tdMode": mode, "side": "buy", "ordType": "market", "sz": str(contracts)}
        if is_hedge:
            _buy_body["posSide"] = "short"

        _buy_resp = _trade_post("/api/v5/trade/order", _buy_body, cfg)
        _sd0 = (_buy_resp.get("data") or [{}])[0]

        if _buy_resp.get("code") != "0" or _sd0.get("sCode", "0") != "0":
            _err = (_okx_err(_buy_resp) if _buy_resp.get("code") != "0"
                    else f"{_sd0.get('sCode')}: {_sd0.get('sMsg', '')}")
            return {"success": False, "message": f"Buy order rejected: {_err}"}

        _close_px = float(sig.get("latest_price") or sig.get("avg_entry") or sig.get("entry") or 0)
        with _log_lock:
            for _s in _b._ss_log["signals"]:
                if _s.get("timestamp") == sig.get("timestamp") and _s.get("symbol") == sym:
                    _s["status"] = "closed_okx"
                    _s["close_price"] = _close_px
                    _s["close_time"] = dubai_now().isoformat()
                    break
            save_log(_b._ss_log)

        return {"success": True, "message": f"✅ {sym} short force closed ({contracts} contracts at market)."}

    except Exception as exc:
        _append_error("trade", f"Force close exception: {exc}", symbol=sym)
        return {"success": False, "message": f"Exception: {exc}"}

# ============================================================================
# PER-COIN PROCESSING — SHORT VERSION
# ============================================================================
def process_short(sym, cfg: dict, super_counter: dict = None, super_lock=None):
    _tl_counts.delta = {}
    try:
        _incr_filter("checked")
        _filter_counts["checked_syms"].append(sym)

        with ThreadPoolExecutor(max_workers=3) as pool:
            f_5m_q = pool.submit(get_klines, sym, "5m", 300)
            f_15m_q = pool.submit(get_klines, sym, "15m", 300)
            f_1h_q = pool.submit(get_klines, sym, "1h", 300)
            m5_quick = f_5m_q.result()[:-1]
            m15_quick = f_15m_q.result()[:-1]
            m1h_quick = f_1h_q.result()[:-1]

        closes_5m_q = [c["close"] for c in m5_quick]
        entry_q = _pround(m5_quick[-1]["close"])

        pdz_zone_15m = "—"
        pdz_zone_1h = "—"
        is_super_eligible = False
        if cfg.get("use_pdz_15m", True):
            pdz_pass_15m, pdz_zone_15m = calc_pdz_zone_short(m15_quick, entry_q,
                                float(cfg.get("tp_pct", 1.2)) / 100.0)
            if pdz_zone_15m == "Premium":
                if m1h_quick:
                    _, pdz_zone_1h = calc_pdz_zone_short(m1h_quick, entry_q,
                                        float(cfg.get("tp_pct", 1.2)) / 100.0)
                if pdz_zone_1h == "Premium":
                    is_super_eligible = True
            elif not pdz_pass_15m:
                _record_elim("f2_pdz15m", "f2_elim_syms", sym)
                return None

        if is_super_eligible:
            _take_super_slot = True
            if super_counter is not None and super_lock is not None:
                with super_lock:
                    if super_counter.get("slots", 0) > 0:
                        super_counter["slots"] -= 1
                    else:
                        _take_super_slot = False
            if _take_super_slot:
                tp = _pround(entry_q * (1 - cfg["tp_pct"] / 100))
                _ss_lev = max(1, int(cfg.get("trade_leverage", 10)))
                if cfg.get("trade_margin_mode", "isolated") == "isolated":
                    sl = _pround(entry_q * (1 + 1 / _ss_lev))
                else:
                    sl = _pround(entry_q * (1 + cfg["sl_pct"] / 100))
                sec = SECTORS.get(sym, "Other")
                max_lev = get_max_leverage(sym)
                _incr_filter("passed")
                _incr_filter("super_setup")
                _filter_counts["passed_syms"].append(sym)
                _filter_counts["super_setup_syms"].append(sym)
                return {
                    "id": str(uuid.uuid4())[:8],
                    "timestamp": dubai_now().isoformat(),
                    "symbol": sym,
                    "entry": entry_q,
                    "tp": tp,
                    "sl": sl,
                    "sector": sec,
                    "status": "open",
                    "close_price": None,
                    "close_time": None,
                    "max_lev": max_lev,
                    "is_super_setup": True,
                    "criteria": {
                        "rsi_5m": "—", "rsi_1h": "—",
                        "ema_3m": "—", "ema_5m": "—", "ema_15m": "—",
                        "macd_3m": "—", "macd_5m": "—", "macd_15m": "—",
                        "sar_3m": "—", "sar_5m": "—", "sar_15m": "—",
                        "vol_ratio": "—", "pdz_zone_5m": "—",
                        "pdz_zone_15m": pdz_zone_15m, "pdz_zone_1h": pdz_zone_1h,
                        "ema_cross_12_15m": "—", "ema_cross_21_15m": "—",
                        "atr_15m": "—", "atr_ratio": "—",
                    },
                }
            _record_elim("super_cap_demoted", "super_cap_demoted_syms", sym)

        pdz_zone_5m = "—"
        if cfg.get("use_pdz_5m", True):
            pdz_pass_5m, pdz_zone_5m = calc_pdz_zone_short(m5_quick, entry_q,
                                float(cfg.get("tp_pct", 1.2)) / 100.0)
            if not pdz_pass_5m:
                _record_elim("f3_pdz5m", "f3_elim_syms", sym)
                return None

        rsi5_q = (calc_rsi_series(closes_5m_q) or [0])[-1]
        if cfg.get("use_rsi_5m", True) and rsi5_q > cfg.get("rsi_5m_max", 70):
            _record_elim("f4_rsi5m", "f4_elim_syms", sym)
            return None

        _need_3m_detail = (cfg.get("use_ema_3m") or cfg.get("use_macd_3m") or cfg.get("use_sar_3m"))
        candle_limit_3m = 80 if _need_3m_detail else 30
        m3_candles = get_klines(sym, "3m", candle_limit_3m)[:-1]
        m5 = m5_quick
        m15 = m15_quick
        m1h_candles = m1h_quick

        _missing_tf = [tf for tf, bars in (("5m", m5), ("15m", m15), ("1h", m1h_candles), ("3m", m3_candles))
                       if not bars]
        if _missing_tf:
            _incr_filter("f_empty_data")
            _filter_counts["f_empty_data_syms"].append(f"{sym}(no {','.join(_missing_tf)})")
            return None

        closes_5m = [c["close"] for c in m5]
        closes_15m = [c["close"] for c in m15]
        closes_3m = [c["close"] for c in m3_candles]
        closes_1h = [c["close"] for c in m1h_candles]
        entry = _pround(m5[-1]["close"])

        rsi1h = (calc_rsi_series(closes_1h) or [0])[-1]
        if cfg.get("use_rsi_1h", True) and not (cfg["rsi_1h_min"] <= rsi1h <= cfg["rsi_1h_max"]):
            _record_elim("f5_rsi1h", "f5_elim_syms", sym)
            return None

        atr_15m_val = None
        atr_ratio_val = None
        _atr_series = calc_atr(m15, 14)
        if _atr_series and entry > 0:
            atr_15m_val = _atr_series[-1]
            _tp_dist_pct = float(cfg.get("tp_pct", 1.5))
            _atr_pct = (atr_15m_val / entry) * 100.0
            atr_ratio_val = (_tp_dist_pct / _atr_pct) if _atr_pct > 0 else None
        if cfg.get("use_atr_filter", False) and atr_ratio_val is not None:
            _atr_thresh = {"Strict": 1.5, "Normal": 2.0, "Relaxed": 3.0}.get(cfg.get("atr_mode", "Normal"), 2.0)
            if atr_ratio_val > _atr_thresh:
                _record_elim("f5b_atr", "f5b_elim_syms", sym)
                return None

        ema_3m_val = ema_5m_val = ema_15m_val = None
        if cfg.get("use_ema_3m"):
            ema = calc_ema(closes_3m, max(2, int(cfg.get("ema_period_3m", 12))))
            if not ema or entry > ema[-1]:
                _record_elim("f6_ema_3m", "f6_ema_3m_elim_syms", sym)
                return None
            ema_3m_val = _pround(ema[-1])
        if cfg.get("use_ema_5m"):
            ema = calc_ema(closes_5m, max(2, int(cfg.get("ema_period_5m", 12))))
            if not ema or entry > ema[-1]:
                _record_elim("f6_ema_5m", "f6_ema_5m_elim_syms", sym)
                return None
            ema_5m_val = _pround(ema[-1])
        if cfg.get("use_ema_15m"):
            ema = calc_ema(closes_15m, max(2, int(cfg.get("ema_period_15m", 12))))
            if not ema or entry > ema[-1]:
                _record_elim("f6_ema_15m", "f6_ema_15m_elim_syms", sym)
                return None
            ema_15m_val = _pround(ema[-1])

        macd_3m_val = macd_5m_val = macd_15m_val = None
        _macd_3m_on = cfg.get("use_macd_3m", True)
        _macd_5m_on = cfg.get("use_macd_5m", True)
        _macd_15m_on = cfg.get("use_macd_15m", True)
        if _macd_3m_on:
            _ok_3m, _ml3 = macd_bearish_and_value(closes_3m)
            if _ml3 is not None:
                macd_3m_val = round(_ml3, 8)
            if not _ok_3m:
                _record_elim("f7_macd_3m", "f7_macd_3m_elim_syms", sym)
                return None
        if _macd_5m_on:
            _ok_5m, _ml5 = macd_bearish_and_value(closes_5m)
            if _ml5 is not None:
                macd_5m_val = round(_ml5, 8)
            if not _ok_5m:
                _record_elim("f7_macd_5m", "f7_macd_5m_elim_syms", sym)
                return None
        if _macd_15m_on:
            _ok_15m, _ml15 = macd_bearish_and_value(closes_15m)
            if _ml15 is not None:
                macd_15m_val = round(_ml15, 8)
            if not _ok_15m:
                _record_elim("f7_macd_15m", "f7_macd_15m_elim_syms", sym)
                return None

        sar_3m_val = sar_5m_val = sar_15m_val = None
        _sar_3m_on = cfg.get("use_sar_3m", True)
        _sar_5m_on = cfg.get("use_sar_5m", True)
        _sar_15m_on = cfg.get("use_sar_15m", True)
        if _sar_3m_on:
            sar_3m = calc_parabolic_sar(m3_candles)
            if not (sar_3m and not sar_3m[-1][1]):
                _record_elim("f8_sar_3m", "f8_sar_3m_elim_syms", sym)
                return None
            sar_3m_val = _pround(sar_3m[-1][0])
        if _sar_5m_on:
            sar_5m = calc_parabolic_sar(m5)
            if not (sar_5m and not sar_5m[-1][1]):
                _record_elim("f8_sar_5m", "f8_sar_5m_elim_syms", sym)
                return None
            sar_5m_val = _pround(sar_5m[-1][0])
        if _sar_15m_on:
            sar_15m = calc_parabolic_sar(m15)
            if not (sar_15m and not sar_15m[-1][1]):
                _record_elim("f8_sar_15m", "f8_sar_15m_elim_syms", sym)
                return None
            sar_15m_val = _pround(sar_15m[-1][0])

        vol_ratio = None
        if cfg.get("use_vol_spike"):
            lookback = max(2, int(cfg.get("vol_spike_lookback", 20)))
            mult = float(cfg.get("vol_spike_mult", 2.0))
            vols_15m = [c["volume"] for c in m15]
            if len(vols_15m) >= lookback + 1:
                window = vols_15m[-(lookback + 1):-1]
                avg_vol = sum(window) / len(window)
                if avg_vol <= 0 or vols_15m[-1] < mult * avg_vol:
                    _record_elim("f9_vol", "f9_elim_syms", sym)
                    return None
                vol_ratio = round(vols_15m[-1] / avg_vol, 2) if avg_vol > 0 else None

        ema_cross_12_15m_val = ema_cross_21_15m_val = None
        if cfg.get("use_ema_cross_15m", True):
            _fast_p = max(2, int(cfg.get("ema_cross_fast_15m", 12)))
            _slow_p = max(_fast_p + 1, int(cfg.get("ema_cross_slow_15m", 21)))
            ema_fast_15m = calc_ema(closes_15m, _fast_p)
            ema_slow_15m = calc_ema(closes_15m, _slow_p)
            if not ema_fast_15m or not ema_slow_15m or ema_fast_15m[-1] >= ema_slow_15m[-1]:
                _record_elim("f10_ema_cross", "f10_elim_syms", sym)
                return None
            ema_cross_12_15m_val = _pround(ema_fast_15m[-1])
            ema_cross_21_15m_val = _pround(ema_slow_15m[-1])

        tp = _pround(entry * (1 - cfg["tp_pct"] / 100))
        _lev_sl = max(1, int(cfg.get("trade_leverage", 10)))
        if cfg.get("trade_margin_mode", "isolated") == "isolated":
            sl = _pround(entry * (1 + 1 / _lev_sl))
        else:
            sl = _pround(entry * (1 + cfg["sl_pct"] / 100))
        sec = SECTORS.get(sym, "Other")
        max_lev = get_max_leverage(sym)

        _incr_filter("passed")
        _filter_counts["passed_syms"].append(sym)

        rsi5 = (calc_rsi_series(closes_5m) or [rsi5_q])[-1]
        criteria = {
            "rsi_5m": round(rsi5, 1),
            "rsi_1h": round(rsi1h, 1),
            "ema_3m": ema_3m_val if cfg.get("use_ema_3m") else "—",
            "ema_5m": ema_5m_val if cfg.get("use_ema_5m") else "—",
            "ema_15m": ema_15m_val if cfg.get("use_ema_15m") else "—",
            "macd_3m": macd_3m_val if cfg.get("use_macd_3m", True) else "—",
            "macd_5m": macd_5m_val if cfg.get("use_macd_5m", True) else "—",
            "macd_15m": macd_15m_val if cfg.get("use_macd_15m", True) else "—",
            "sar_3m": sar_3m_val if cfg.get("use_sar_3m", True) else "—",
            "sar_5m": sar_5m_val if cfg.get("use_sar_5m", True) else "—",
            "sar_15m": sar_15m_val if cfg.get("use_sar_15m", True) else "—",
            "vol_ratio": vol_ratio if cfg.get("use_vol_spike") else "—",
            "pdz_zone_5m": pdz_zone_5m if cfg.get("use_pdz_5m", True) else "—",
            "pdz_zone_15m": pdz_zone_15m if cfg.get("use_pdz_15m", True) else "—",
            "pdz_zone_1h": pdz_zone_1h if cfg.get("use_pdz_15m", True) else "—",
            "ema_cross_12_15m": ema_cross_12_15m_val if cfg.get("use_ema_cross_15m", True) else "—",
            "ema_cross_21_15m": ema_cross_21_15m_val if cfg.get("use_ema_cross_15m", True) else "—",
            "atr_15m": round(atr_15m_val, 8) if atr_15m_val is not None else "—",
            "atr_ratio": round(atr_ratio_val, 3) if atr_ratio_val is not None else "—",
        }

        return {
            "id": str(uuid.uuid4())[:8],
            "timestamp": dubai_now().isoformat(),
            "symbol": sym,
            "entry": entry,
            "tp": tp,
            "sl": sl,
            "sector": sec,
            "status": "open",
            "close_price": None,
            "close_time": None,
            "max_lev": max_lev,
            "is_super_setup": False,
            "criteria": criteria,
        }

    except Exception as _proc_exc:
        _incr_filter("errors")
        _append_error("scan", str(_proc_exc), symbol=sym)
        return "error"
    finally:
        _flush_tl_counts()

def _parse_iso_safe(ts_str: str):
    s = str(ts_str).strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt

# ============================================================================
# SCAN FUNCTION (SHORT VERSION)
# ============================================================================
_PRICE_ALERT_PCT = 3.0

def scan_short(cfg: dict, super_slots_remaining: int = None, skip_symbols: set = None):
    _reset_filter_counts()
    with _filter_lock:
        _filter_counts["scan_cfg"] = dict(cfg)

    symbols = get_symbols_cached(cfg["watchlist"])
    with _filter_lock:
        _filter_counts["total_watchlist"] = len(symbols)

    tickers = get_bulk_tickers()
    if cfg.get("use_pre_filter", True):
        pre_filtered = pre_filter_by_ticker(symbols, tickers)
    else:
        pre_filtered = list(symbols)
    with _filter_lock:
        _filter_counts["pre_filtered_out"] = len(symbols) - len(pre_filtered)
        _filter_counts["pre_filter_passed_syms"] = list(pre_filtered)

    skip_set = set(skip_symbols) if skip_symbols else set()
    if skip_set:
        pre_filtered = [s for s in pre_filtered if s not in skip_set]

    if super_slots_remaining is None:
        super_slots_remaining = max(0, int(cfg.get("max_super_trades", 5)))
    super_counter = {"slots": max(0, int(super_slots_remaining))}
    super_lock = threading.Lock()

    results = []
    with ThreadPoolExecutor(max_workers=10) as exe:
        futs = [exe.submit(process_short, s, cfg, super_counter, super_lock)
                for s in pre_filtered]
        for f in as_completed(futs):
            r = f.result()
            if r and r != "error":
                results.append(r)

    with _filter_lock:
        _filter_counts["scan_completed_at"] = time.time()
    return sorted(results, key=lambda x: x["symbol"]), _filter_counts.get("errors", 0)

# ============================================================================
# OPEN SIGNAL UPDATE FUNCTIONS (for watcher)
# ============================================================================

def _update_one_signal_short(sig: dict) -> None:
    _sym = sig.get("symbol", "?")
    try:
        with _config_lock:
            _cfg_snap = dict(_b._ss_cfg)
        sig_dt = _parse_iso_safe(sig["timestamp"])
        sig_ts_ms = int(sig_dt.timestamp() * 1000)
        candles = get_klines(sig["symbol"], "5m", 200)
        if not candles:
            return
        post = [c for c in candles if c["time"] >= sig_ts_ms]

        _dca_enabled = bool(sig.get("dca_enabled", False))
        _dca_max = int(sig.get("dca_max", 0) or 0)
        _dca_count = int(sig.get("dca_count", 0) or 0)
        _ladder_slots_left = _dca_enabled and _dca_count < _dca_max

        if _ladder_slots_left:
            _dca_trigger_price = _dca_compute_trigger_short(sig, _cfg_snap)
            _last_fill_ts_ms = sig_ts_ms
            _fills = sig.get("dca_fills") or []
            if _fills:
                _last_fill = _fills[-1]
                try:
                    _last_fill_ts_ms = int(_parse_iso_safe(_last_fill.get("ts") or sig["timestamp"]).timestamp() * 1000)
                except Exception:
                    _last_fill_ts_ms = sig_ts_ms
            _post_dca = [c for c in post if c["time"] >= _last_fill_ts_ms]
            tp_time = None
            dca_time = None
            sl_time = None
            _dca_sl_px = float(sig.get("sl", 0) or 0)
            _use_dca_sl_w = _cfg_snap.get("use_dca_sl", True)
            for c in _post_dca:
                if tp_time is None and c["low"] <= sig["tp"]:
                    tp_time = c["time"]
                if (dca_time is None and _dca_trigger_price > 0 and c["high"] >= _dca_trigger_price):
                    dca_time = c["time"]
                if (_use_dca_sl_w and sl_time is None and _dca_sl_px > 0 and c["high"] >= _dca_sl_px):
                    sl_time = c["time"]
            _fc_px_w = float(sig.get("fc_trigger_px", 0) or 0)
            fc_time = None
            if _fc_px_w > 0:
                for _c in _post_dca:
                    if fc_time is None and _c["low"] <= _fc_px_w:
                        fc_time = _c["time"]
                        break

            _sl_wins = (sl_time is not None and (tp_time is None or sl_time <= tp_time) and
                        (fc_time is None or sl_time <= fc_time) and (dca_time is None or sl_time <= dca_time))
            _fc_wins = (not _sl_wins and fc_time is not None and (dca_time is None or fc_time <= dca_time) and
                        (tp_time is None or fc_time <= tp_time))
            _tp_wins = (not _sl_wins and not _fc_wins and tp_time is not None and
                        (dca_time is None or tp_time <= dca_time))

            if _sl_wins:
                sig.update(status="dca_sl_hit", close_price=_dca_sl_px,
                           close_time=to_dubai(datetime.fromtimestamp(sl_time/1000, tz=timezone.utc)).isoformat())
                sig.pop("price_alert", None)
                sig.pop("_dca_pending", None)
            elif _fc_wins:
                sig.update(status="fc_hit", close_price=_fc_px_w,
                           close_time=to_dubai(datetime.fromtimestamp(fc_time/1000, tz=timezone.utc)).isoformat())
                sig.pop("price_alert", None)
                sig.pop("_dca_pending", None)
            elif _tp_wins:
                sig.update(status="tp_hit", close_price=sig["tp"],
                           close_time=to_dubai(datetime.fromtimestamp(tp_time/1000, tz=timezone.utc)).isoformat())
                sig.pop("price_alert", None)
                sig.pop("_dca_pending", None)
            elif dca_time is not None:
                sig["_dca_pending"] = True
                sig["_dca_trigger_px"] = _dca_trigger_price
                sig["_dca_trigger_time"] = to_dubai(datetime.fromtimestamp(dca_time/1000, tz=timezone.utc)).isoformat()
                if candles:
                    latest_price = candles[-1]["close"]
                    sig["latest_price"] = float(latest_price)
                    _avg = float(sig.get("avg_entry", sig.get("entry", 0)) or 0)
                    if _avg > 0:
                        rise_pct = (latest_price - _avg) / _avg * 100
                        sig["price_alert"] = rise_pct >= _PRICE_ALERT_PCT
                        sig["price_alert_pct"] = round(rise_pct, 2)
            else:
                if candles:
                    latest_price = candles[-1]["close"]
                    sig["latest_price"] = float(latest_price)
                    _avg = float(sig.get("avg_entry", sig.get("entry", 0)) or 0)
                    if _avg > 0:
                        rise_pct = (latest_price - _avg) / _avg * 100
                        sig["price_alert"] = rise_pct >= _PRICE_ALERT_PCT
                        sig["price_alert_pct"] = round(rise_pct, 2)
                    else:
                        sig["price_alert"] = False
                        sig["price_alert_pct"] = 0.0
            return

        _ladder_full = _dca_enabled and _dca_max > 0 and _dca_count >= _dca_max
        _use_sl_here = _cfg_snap.get("use_dca_sl", True)
        tp_time = sl_time = None
        for c in post:
            if tp_time is None and c["low"] <= sig["tp"]:
                tp_time = c["time"]
            if _use_sl_here and sl_time is None and c["high"] >= sig["sl"]:
                sl_time = c["time"]
        if tp_time is not None or sl_time is not None:
            if tp_time is not None and (sl_time is None or tp_time <= sl_time):
                sig.update(status="tp_hit", close_price=sig["tp"],
                           close_time=to_dubai(datetime.fromtimestamp(tp_time/1000, tz=timezone.utc)).isoformat())
                sig.pop("price_alert", None)
            else:
                _close_status = "dca_sl_hit" if _ladder_full else "sl_hit"
                sig.update(status=_close_status, close_price=sig["sl"],
                           close_time=to_dubai(datetime.fromtimestamp(sl_time/1000, tz=timezone.utc)).isoformat())
                sig.pop("price_alert", None)
        else:
            if candles:
                latest_price = candles[-1]["close"]
                _ref = float(sig.get("avg_entry", sig.get("entry", 0)) or 0)
                sig["latest_price"] = float(latest_price)
                if _ref > 0:
                    rise_pct = (latest_price - _ref) / _ref * 100
                    sig["price_alert"] = rise_pct >= _PRICE_ALERT_PCT
                    sig["price_alert_pct"] = round(rise_pct, 2)
                else:
                    sig["price_alert"] = False
                    sig["price_alert_pct"] = 0.0

    except Exception as _upd_exc:
        _append_error("signal_update", f"{_sym}: {_upd_exc}", symbol=_sym)

def update_open_signals_short(signals):
    open_sigs = [s for s in signals if s["status"] == "open"]
    if not open_sigs:
        return signals
    with ThreadPoolExecutor(max_workers=min(len(open_sigs), 15)) as pool:
        futs = [pool.submit(_update_one_signal_short, s) for s in open_sigs]
        for f in as_completed(futs):
            f.result()
    return signals

# ============================================================================
# TWO-TIER RECONCILIATION (for Short positions)
# ============================================================================
def _reconcile_tier1_short(cfg: dict) -> int:
    _actions = 0
    try:
        if not cfg.get("api_key"):
            return 0

        _pos_resp = _trade_get("/api/v5/account/positions", {"instType": "SWAP"}, cfg)
        if _pos_resp.get("code") != "0":
            return 0

        _okx_pos = {}
        for _p in (_pos_resp.get("data") or []):
            if abs(float(_p.get("pos", 0) or 0)) != 0:
                _okx_pos[_p.get("instId", "")] = _p

        _now = dubai_now()
        _GRACE_SECS = 90
        _RECENTLY_CLOSED_MINS = 15

        with _log_lock:
            _signals_copy = list(_b._ss_log["signals"])

        for _sig in _signals_copy:
            if _sig.get("status") != "open":
                continue
            if not _sig.get("order_id"):
                continue

            _sym = _sig.get("symbol", "")
            _inst_id = _to_okx(_sym)

            _entry_ts = _sig.get("timestamp", "")
            _age_secs = 9999
            if _entry_ts:
                try:
                    _entry_dt = datetime.fromisoformat(_entry_ts.replace("Z", "+00:00"))
                    _age_secs = (_now - _entry_dt).total_seconds()
                except Exception:
                    pass
            if _age_secs < _GRACE_SECS:
                continue

            if _inst_id not in _okx_pos:
                _close_px = float(_sig.get("latest_price") or _sig.get("avg_entry") or _sig.get("entry") or 0)
                with _log_lock:
                    for _s in _b._ss_log["signals"]:
                        if _s.get("timestamp") == _sig.get("timestamp") and _s.get("symbol") == _sym:
                            _s["status"] = "closed_okx"
                            _s["close_price"] = _close_px
                            _s["close_time"] = _now.isoformat()
                            break
                    save_log(_b._ss_log)
                _actions += 1

        for _sig in _signals_copy:
            if _sig.get("status") not in ("sl_hit", "dca_sl_hit"):
                continue
            _close_t = _sig.get("close_time", "")
            if not _close_t:
                continue
            try:
                _close_dt = datetime.fromisoformat(_close_t.replace("Z", "+00:00"))
                _mins_ago = (_now - _close_dt).total_seconds() / 60
            except Exception:
                continue
            if _mins_ago > _RECENTLY_CLOSED_MINS:
                continue

            _sym = _sig.get("symbol", "")
            _inst_id = _to_okx(_sym)
            if _inst_id not in _okx_pos:
                continue

            for _aid_key in ("algo_id", "tp_algo_id"):
                _aid = _sig.get(_aid_key, "")
                if _aid:
                    _cancel_algo_best_effort_short(_aid, _sym, cfg)

            _live_sz = abs(float(_okx_pos[_inst_id].get("pos", 0) or 0))
            if _live_sz > 0:
                try:
                    _buy_resp = _trade_post("/api/v5/trade/order", {
                        "instId": _inst_id, "tdMode": "cross", "side": "buy",
                        "ordType": "market", "sz": str(int(_live_sz)), "posSide": "short",
                    }, cfg)
                except Exception:
                    pass
            _actions += 1

    except Exception as _t1_exc:
        _append_error("watcher", f"Tier-1 reconcile error: {_t1_exc}")

    _b._ss_reconcile_t1_last = int(time.time())
    _b._ss_reconcile_t1_runs = getattr(_b, "_ss_reconcile_t1_runs", 0) + 1
    _b._ss_reconcile_t1_actions = getattr(_b, "_ss_reconcile_t1_actions", 0) + _actions
    return _actions

def _reconcile_tier2_short(cfg: dict) -> int:
    _actions = 0
    try:
        if not cfg.get("api_key"):
            return 0

        _algo_resp = _trade_get("/api/v5/trade/orders-algo-pending",
                                {"instType": "SWAP", "ordType": "conditional,oco"}, cfg)
        if _algo_resp.get("code") != "0":
            return 0

        _pending_algos_by_id = {}
        for _a in (_algo_resp.get("data") or []):
            _aid = _a.get("algoId", "")
            if _aid:
                _pending_algos_by_id[_aid] = _a

        _pos_resp = _trade_get("/api/v5/account/positions", {"instType": "SWAP"}, cfg)
        _okx_pos = {}
        if _pos_resp.get("code") == "0":
            for _p in (_pos_resp.get("data") or []):
                if abs(float(_p.get("pos", 0) or 0)) != 0:
                    _okx_pos[_p.get("instId", "")] = _p

        _now = dubai_now()
        _RECENTLY_CLOSED_MINS = 30

        with _log_lock:
            _signals_copy = list(_b._ss_log["signals"])

        for _sig in _signals_copy:
            if _sig.get("status") not in ("tp_hit", "sl_hit", "dca_sl_hit", "closed_okx"):
                continue
            _close_t = _sig.get("close_time", "")
            if not _close_t:
                continue
            try:
                _close_dt = datetime.fromisoformat(_close_t.replace("Z", "+00:00"))
                _mins_ago = (_now - _close_dt).total_seconds() / 60
            except Exception:
                continue
            if _mins_ago > _RECENTLY_CLOSED_MINS:
                continue

            _sym = _sig.get("symbol", "")
            for _aid_key in ("algo_id", "tp_algo_id"):
                _aid = _sig.get(_aid_key, "")
                if _aid and _aid in _pending_algos_by_id:
                    _cancel_algo_best_effort_short(_aid, _sym, cfg)
                    _actions += 1

        for _sig in _signals_copy:
            if _sig.get("status") != "open":
                continue
            if not _sig.get("order_id"):
                continue

            _sym = _sig.get("symbol", "")
            _inst_id = _to_okx(_sym)
            if _inst_id not in _okx_pos:
                continue

            _algo_id = _sig.get("algo_id", "")
            _tp_algo = _sig.get("tp_algo_id", "")
            _has_algo = (_algo_id and _algo_id in _pending_algos_by_id) or (_tp_algo and _tp_algo in _pending_algos_by_id)
            if _has_algo:
                continue

            _entry_ts = _sig.get("timestamp", "")
            _age_secs = 9999
            if _entry_ts:
                try:
                    _entry_dt = datetime.fromisoformat(_entry_ts.replace("Z", "+00:00"))
                    _age_secs = (_now - _entry_dt).total_seconds()
                except Exception:
                    pass
            if _age_secs < 120:
                continue

            _dca_count = int(_sig.get("dca_count", 0) or 0)
            _live_pos = _okx_pos[_inst_id]
            _live_sz = abs(float(_live_pos.get("pos", 0) or 0))

            if _live_sz <= 0:
                continue

            _current_tp = float(_sig.get("tp") or 0)
            _current_sl = float(_sig.get("sl") or 0)
            if not _current_tp:
                continue

            try:
                if _dca_count > 0 and _current_sl:
                    _new_result = _place_dca_oco_algo_short(_sig, cfg, _current_tp, _current_sl, int(_live_sz))
                else:
                    _new_result = _place_tp_only_order_short(_sig, cfg, _current_tp, int(_live_sz))
                if _new_result:
                    _new_algo = _new_result if isinstance(_new_result, str) else _new_result.get("algoId", "")
                    if _new_algo:
                        with _log_lock:
                            for _s in _b._ss_log["signals"]:
                                if _s.get("timestamp") == _sig.get("timestamp") and _s.get("symbol") == _sym:
                                    _s["algo_id"] = _new_algo
                                    _s["tp_algo_id"] = _new_algo
                                    break
                            save_log(_b._ss_log)
                        _actions += 1
            except Exception:
                pass

    except Exception as _t2_exc:
        _append_error("watcher", f"Tier-2 reconcile error: {_t2_exc}")

    _b._ss_reconcile_t2_last = int(time.time())
    _b._ss_reconcile_t2_runs = getattr(_b, "_ss_reconcile_t2_runs", 0) + 1
    _b._ss_reconcile_t2_actions = getattr(_b, "_ss_reconcile_t2_actions", 0) + _actions
    return _actions

def _place_dca_oco_algo_short(sig: dict, cfg: dict, new_tp: float, new_sl: float, total_contracts: int) -> str:
    try:
        sym = sig.get("symbol", "")
        mode = (sig.get("order_margin_mode") or cfg.get("trade_margin_mode", "isolated")).strip().lower()
        is_hedge = bool(sig.get("order_is_hedge", False))
        algo_body = {"instId": _to_okx(sym), "tdMode": mode, "side": "buy", "ordType": "oco",
                     "sz": str(int(total_contracts)), "tpTriggerPx": str(_pround(new_tp)),
                     "tpTriggerPxType": "mark", "tpOrdPx": "-1",
                     "slTriggerPx": str(_pround(new_sl)), "slTriggerPxType": "mark", "slOrdPx": "-1"}
        if is_hedge:
            algo_body["posSide"] = "short"
        else:
            algo_body["reduceOnly"] = "true"
        algo_resp = _trade_post("/api/v5/trade/order-algo", algo_body, cfg)
        ad = (algo_resp.get("data") or [{}])[0]
        if algo_resp.get("code") != "0":
            return ""
        return ad.get("algoId", "")
    except Exception as exc:
        _append_error("trade", f"DCA OCO exception: {exc}", symbol=sig.get("symbol", ""))
        return ""

def _execute_dca_fill_paper_short(sig: dict, cfg: dict) -> bool:
    sym = sig.get("symbol", "?")
    try:
        dca_usdt = _dca_next_usdt(sig)
        if dca_usdt <= 0:
            sig.pop("_dca_pending", None)
            return False

        fill_px = float(sig.get("_dca_trigger_px", 0) or 0)
        if fill_px <= 0:
            fill_px = _dca_compute_trigger_short(sig, cfg)
        if fill_px <= 0:
            sig.pop("_dca_pending", None)
            return False

        lev = int(sig.get("trade_lev", 0) or 0)
        if lev <= 0:
            lev = int(cfg.get("trade_leverage", 10) or 10)
        fill_not = dca_usdt * lev

        fills = sig.get("dca_fills") or []
        next_idx = len(fills)
        fills.append({"price": _pround(fill_px), "usdt": dca_usdt, "leverage": lev,
                      "notional": fill_not, "ts": dubai_now().isoformat(),
                      "order_id": "", "dca_idx": next_idx, "paper": True})
        sig["dca_fills"] = fills

        total_notional = 0.0
        total_units = 0.0
        total_usdt = 0.0
        for f in fills:
            px = float(f.get("price", 0) or 0)
            nt = float(f.get("notional", 0) or 0)
            ud = float(f.get("usdt", 0) or 0)
            if px > 0 and nt > 0:
                total_notional += nt
                total_units += nt / px
                total_usdt += ud
        if total_units > 0:
            avg_entry = total_notional / total_units
        else:
            prices = [float(f.get("price", 0) or 0) for f in fills if float(f.get("price", 0) or 0) > 0]
            avg_entry = sum(prices) / len(prices) if prices else float(sig.get("avg_entry", 0) or 0)

        sig["avg_entry"] = _pround(avg_entry)
        sig["total_usdt"] = total_usdt
        sig["total_notional"] = total_notional
        sig["dca_count"] = next_idx

        _dca_tp_usd_p = float(cfg.get("dca_tp_usd", 0.50) or 0.50)
        _dca_sl_usd_p = float(cfg.get("dca_sl_usd", 5.00) or 5.00)
        _avg_p = float(sig["avg_entry"])
        _tot_not_p = float(sig.get("total_notional", 0) or 0)
        if _avg_p > 0 and _tot_not_p > 0:
            _total_coins_p = _tot_not_p / _avg_p
            new_tp = _pround(_avg_p - _dca_tp_usd_p / _total_coins_p)
            new_sl = _pround(_avg_p + _dca_sl_usd_p / _total_coins_p)
        else:
            _tp_pct_fb = float(cfg.get("tp_pct", 1.5)) / 100.0
            new_tp = _pround(_avg_p * (1.0 - _tp_pct_fb))
            _sl_d_fb = float(sig.get("sl_distance_pct", 0.03))
            new_sl = _pround(_avg_p * (1.0 + _sl_d_fb))
        sig["tp"] = new_tp
        if cfg.get("use_dca_sl", True):
            sig["sl"] = new_sl
        sig["fc_trigger_px"] = new_tp

        dca_max = int(sig.get("dca_max", 0) or 0)
        if sig["dca_count"] >= dca_max:
            sig["final_sl_price"] = new_sl if cfg.get("use_dca_sl", True) else None
            sig["next_dca_px"] = 0.0
        else:
            sig["final_sl_price"] = None
            try:
                sig["next_dca_px"] = float(_dca_compute_trigger_short(sig, cfg) or 0.0)
            except Exception:
                sig["next_dca_px"] = 0.0

        try:
            fills[-1]["entry"] = float(sig.get("avg_entry", 0) or 0)
            fills[-1]["tp"] = float(sig.get("tp", 0) or 0)
            fills[-1]["sl"] = float(sig.get("sl", 0) or 0)
        except Exception:
            pass

        sig["order_status"] = "paper_filled"

        _okx_log_entry(sig, f"DCA-{next_idx} [PAPER SHORT]",
                       instId=_to_okx(sym), ordType="market (simulated)",
                       fill_px=_pround(fill_px), collateral=f"${dca_usdt:.2f}",
                       avg_entry=sig.get("avg_entry", ""))
        _okx_log_entry(sig, f"DCA-{next_idx} TP/SL [PAPER SHORT]",
                       new_tp=sig.get("tp", ""), new_sl=sig.get("sl", ""),
                       tp_usd=f"+${_dca_tp_usd_p:.2f}", sl_usd=f"-${_dca_sl_usd_p:.2f}")

        sig.pop("_dca_pending", None)
        sig.pop("_dca_trigger_px", None)
        sig.pop("_dca_trigger_time", None)
        return True

    except Exception as exc:
        _append_error("trade", f"Paper DCA exception: {exc}", symbol=sym)
        sig.pop("_dca_pending", None)
        return False

def _execute_dca_fill_short(sig: dict, cfg: dict) -> bool:
    sym = sig.get("symbol", "?")
    try:
        _entry_ts_str = sig.get("timestamp", "")
        _now_utc = dubai_now()
        _age_secs = 9999
        if _entry_ts_str:
            try:
                _entry_dt = datetime.fromisoformat(_entry_ts_str.replace("Z", "+00:00"))
                _age_secs = (_now_utc - _entry_dt).total_seconds()
            except Exception:
                pass

        _GRACE_SECS = 90
        if _age_secs >= _GRACE_SECS:
            try:
                _pos_chk = _trade_get("/api/v5/account/positions",
                                      {"instType": "SWAP", "instId": _to_okx(sym)}, cfg)
                if _pos_chk.get("code") == "0":
                    _live_pos = [p for p in _pos_chk.get("data", [])
                                 if abs(float(p.get("pos", 0) or 0)) != 0]
                    if not _live_pos:
                        try:
                            _ph_resp = _trade_get("/api/v5/account/positions-history",
                                                  {"instType": "SWAP", "instId": _to_okx(sym), "limit": "10"}, cfg)
                            if _ph_resp.get("code") == "0" and _ph_resp.get("data"):
                                try:
                                    _entry_ms = int(_parse_iso_safe(sig["timestamp"]).timestamp() * 1000)
                                except Exception:
                                    _entry_ms = 0
                                _ph_match = None
                                for _ph_rec in _ph_resp["data"]:
                                    try:
                                        _ph_rec_ms = int(_ph_rec.get("uTime", 0) or 0)
                                    except Exception:
                                        _ph_rec_ms = 0
                                    if _ph_rec_ms > _entry_ms:
                                        if (_ph_match is None or _ph_rec_ms > int(_ph_match.get("uTime", 0) or 0)):
                                            _ph_match = _ph_rec
                                if _ph_match is not None:
                                    _ph_type = str(_ph_match.get("type", ""))
                                    _ph_close_px = float(_ph_match.get("closeAvgPx", 0) or 0) or None
                                    sig["status"] = "closed_okx"
                                    sig["close_price"] = _ph_close_px if _ph_close_px else float(sig.get("latest_price", 0) or 0)
                                    sig["close_time"] = dubai_now().isoformat()
                                    sig.pop("_dca_pending", None)
                                    return False
                        except Exception:
                            pass
                        sig.pop("_dca_pending", None)
                        return False
            except Exception as _pos_exc:
                _append_error("trade", f"DCA position pre-check failed: {_pos_exc}", symbol=sym)

        dca_usdt = _dca_next_usdt(sig)
        if dca_usdt <= 0:
            sig.pop("_dca_pending", None)
            return False

        result = place_okx_dca_order_short(sig, cfg, dca_usdt)
        if result.get("status") != "placed":
            sig.pop("_dca_pending", None)
            sig["_dca_last_error"] = result.get("error", "")
            return False

        fill_px = float(result.get("actual_entry", 0) or 0)
        fill_sz = int(result.get("sz", 0) or 0)
        fill_ordid = result.get("ordId", "")

        fills = sig.get("dca_fills") or []
        next_idx = len(fills)
        fills.append({"price": fill_px, "usdt": dca_usdt,
                      "leverage": int(sig.get("trade_lev", 0) or 0),
                      "notional": dca_usdt * int(sig.get("trade_lev", 10) or 10),
                      "ts": dubai_now().isoformat(), "order_id": fill_ordid, "dca_idx": next_idx})
        sig["dca_fills"] = fills

        total_notional = 0.0
        total_units = 0.0
        total_usdt = 0.0
        for f in fills:
            px = float(f.get("price", 0) or 0)
            nt = float(f.get("notional", 0) or 0)
            ud = float(f.get("usdt", 0) or 0)
            if px > 0 and nt > 0:
                total_notional += nt
                total_units += nt / px
                total_usdt += ud
        if total_units > 0:
            avg_entry = total_notional / total_units
        else:
            prices = [float(f.get("price", 0) or 0) for f in fills if float(f.get("price", 0) or 0) > 0]
            avg_entry = sum(prices) / len(prices) if prices else float(sig.get("avg_entry", 0) or 0)

        sig["avg_entry"] = _pround(avg_entry)
        sig["total_usdt"] = total_usdt
        sig["total_notional"] = total_notional
        sig["dca_count"] = next_idx

        _dca_tp_usd_l = float(cfg.get("dca_tp_usd", 0.50) or 0.50)
        _dca_sl_usd_l = float(cfg.get("dca_sl_usd", 5.00) or 5.00)
        _avg_l = float(sig["avg_entry"])
        if _avg_l > 0 and total_notional > 0:
            _total_coins_l = total_notional / _avg_l
            new_tp = _pround(_avg_l - _dca_tp_usd_l / _total_coins_l)
            new_sl = _pround(_avg_l + _dca_sl_usd_l / _total_coins_l)
        else:
            _tp_pct_fb = float(cfg.get("tp_pct", 1.5)) / 100.0
            new_tp = _pround(_avg_l * (1.0 - _tp_pct_fb))
            _sl_d_fb = float(sig.get("sl_distance_pct", 0.03))
            new_sl = _pround(_avg_l * (1.0 + _sl_d_fb))
        sig["tp"] = new_tp
        if cfg.get("use_dca_sl", True):
            sig["sl"] = new_sl
        sig["fc_trigger_px"] = new_tp

        dca_max = int(sig.get("dca_max", 0) or 0)
        if sig["dca_count"] >= dca_max:
            sig["final_sl_price"] = new_sl if cfg.get("use_dca_sl", True) else None
            sig["next_dca_px"] = 0.0
        else:
            sig["final_sl_price"] = None
            try:
                sig["next_dca_px"] = float(_dca_compute_trigger_short(sig, cfg) or 0.0)
            except Exception:
                sig["next_dca_px"] = 0.0

        try:
            fills[-1]["entry"] = float(sig.get("avg_entry", 0) or 0)
            fills[-1]["tp"] = float(sig.get("tp", 0) or 0)
            fills[-1]["sl"] = float(sig.get("sl", 0) or 0)
        except Exception:
            pass

        sig["order_id"] = fill_ordid or sig.get("order_id", "")
        sig["order_sz"] = fill_sz
        sig["order_status"] = "placed"

        _ct_val = float(sig.get("order_ct_val", 0) or 0)
        total_contracts = 0
        for f in fills:
            _fz = int(f.get("sz", 0) or 0)
            if _fz > 0:
                total_contracts += _fz
            elif _ct_val > 0:
                _fp = float(f.get("price", 0) or 0)
                _fn = float(f.get("notional", 0) or 0)
                if _fp > 0 and _fn > 0:
                    total_contracts += max(1, int(_fn / (_ct_val * _fp)))
        if total_contracts <= 0:
            total_contracts = fill_sz

        _old_algo = sig.get("algo_id", "") or sig.get("tp_algo_id", "")
        if _old_algo:
            _cancel_algo_best_effort_short(_old_algo, sym, cfg)

        _use_dca_sl_live = cfg.get("use_dca_sl", True)
        if _use_dca_sl_live:
            new_algo = _place_dca_oco_algo_short(sig, cfg, sig["tp"], sig["sl"], total_contracts)
            if new_algo:
                sig["algo_id"] = new_algo
                sig["tp_algo_id"] = new_algo
        else:
            new_algo = _place_tp_only_order_short(sig, cfg, sig["tp"], total_contracts)
            if new_algo:
                sig["algo_id"] = new_algo
                sig["tp_algo_id"] = new_algo

        sig.pop("_dca_pending", None)
        sig.pop("_dca_trigger_px", None)
        sig.pop("_dca_trigger_time", None)
        return True

    except Exception as exc:
        _append_error("trade", f"DCA execute exception: {exc}", symbol=sym)
        sig.pop("_dca_pending", None)
        return False

# ============================================================================
# WATCHER THREAD (SHORT VERSION)
# ============================================================================

def _watcher_update_one_signal_short(sig: dict) -> None:
    _sym = sig.get("symbol", "?")
    try:
        with _config_lock:
            _cfg_snap = dict(_b._ss_cfg)
        sig_dt = _parse_iso_safe(sig["timestamp"])
        sig_ts_ms = int(sig_dt.timestamp() * 1000)
        candles = get_klines(sig["symbol"], "1m", 120)
        if not candles:
            return
        post = [c for c in candles if c["time"] >= sig_ts_ms]

        _dca_enabled = bool(sig.get("dca_enabled", False))
        _dca_max = int(sig.get("dca_max", 0) or 0)
        _dca_count = int(sig.get("dca_count", 0) or 0)
        _ladder_slots_left = _dca_enabled and _dca_count < _dca_max

        if _ladder_slots_left:
            _dca_trigger_price = _dca_compute_trigger_short(sig, _cfg_snap)
            _last_fill_ts_ms = sig_ts_ms
            _fills = sig.get("dca_fills") or []
            if _fills:
                _last_fill = _fills[-1]
                try:
                    _last_fill_ts_ms = int(_parse_iso_safe(_last_fill.get("ts") or sig["timestamp"]).timestamp() * 1000)
                except Exception:
                    _last_fill_ts_ms = sig_ts_ms
            _post_dca = [c for c in post if c["time"] >= _last_fill_ts_ms]
            tp_time = None
            dca_time = None
            sl_time = None
            _dca_sl_px = float(sig.get("sl", 0) or 0)
            _use_dca_sl_w = _cfg_snap.get("use_dca_sl", True)
            for c in _post_dca:
                if tp_time is None and c["low"] <= sig["tp"]:
                    tp_time = c["time"]
                if (dca_time is None and _dca_trigger_price > 0 and c["high"] >= _dca_trigger_price):
                    dca_time = c["time"]
                if (_use_dca_sl_w and sl_time is None and _dca_sl_px > 0 and c["high"] >= _dca_sl_px):
                    sl_time = c["time"]
            _fc_px_w = float(sig.get("fc_trigger_px", 0) or 0)
            fc_time = None
            if _fc_px_w > 0:
                for _c in _post_dca:
                    if fc_time is None and _c["low"] <= _fc_px_w:
                        fc_time = _c["time"]
                        break

            if sl_time is not None and (tp_time is None or sl_time <= tp_time):
                sig.update(status="dca_sl_hit", close_price=_dca_sl_px,
                           close_time=to_dubai(datetime.fromtimestamp(sl_time/1000, tz=timezone.utc)).isoformat())
                sig.pop("price_alert", None)
                sig.pop("_dca_pending", None)
            elif fc_time is not None:
                sig.update(status="fc_hit", close_price=_fc_px_w,
                           close_time=to_dubai(datetime.fromtimestamp(fc_time/1000, tz=timezone.utc)).isoformat())
                sig.pop("price_alert", None)
                sig.pop("_dca_pending", None)
            elif tp_time is not None and (dca_time is None or tp_time <= dca_time):
                sig.update(status="tp_hit", close_price=sig["tp"],
                           close_time=to_dubai(datetime.fromtimestamp(tp_time/1000, tz=timezone.utc)).isoformat())
                sig.pop("price_alert", None)
                sig.pop("_dca_pending", None)
            elif dca_time is not None:
                sig["_dca_pending"] = True
                sig["_dca_trigger_px"] = _dca_trigger_price
                sig["_dca_trigger_time"] = to_dubai(datetime.fromtimestamp(dca_time/1000, tz=timezone.utc)).isoformat()
                if candles:
                    latest_price = candles[-1]["close"]
                    sig["latest_price"] = float(latest_price)
                    _avg = float(sig.get("avg_entry", sig.get("entry", 0)) or 0)
                    if _avg > 0:
                        rise_pct = (latest_price - _avg) / _avg * 100
                        sig["price_alert"] = rise_pct >= _PRICE_ALERT_PCT
                        sig["price_alert_pct"] = round(rise_pct, 2)
            else:
                if candles:
                    latest_price = candles[-1]["close"]
                    sig["latest_price"] = float(latest_price)
                    _avg = float(sig.get("avg_entry", sig.get("entry", 0)) or 0)
                    if _avg > 0:
                        rise_pct = (latest_price - _avg) / _avg * 100
                        sig["price_alert"] = rise_pct >= _PRICE_ALERT_PCT
                        sig["price_alert_pct"] = round(rise_pct, 2)
                    else:
                        sig["price_alert"] = False
                        sig["price_alert_pct"] = 0.0
            return

        _ladder_full = _dca_enabled and _dca_max > 0 and _dca_count >= _dca_max
        tp_time = sl_time = None
        for c in post:
            if tp_time is None and c["low"] <= sig["tp"]:
                tp_time = c["time"]
            if sl_time is None and c["high"] >= sig["sl"]:
                sl_time = c["time"]
        if tp_time is not None or sl_time is not None:
            if tp_time is not None and (sl_time is None or tp_time <= sl_time):
                sig.update(status="tp_hit", close_price=sig["tp"],
                           close_time=to_dubai(datetime.fromtimestamp(tp_time/1000, tz=timezone.utc)).isoformat())
                sig.pop("price_alert", None)
            else:
                _close_status = "dca_sl_hit" if _ladder_full else "sl_hit"
                sig.update(status=_close_status, close_price=sig["sl"],
                           close_time=to_dubai(datetime.fromtimestamp(sl_time/1000, tz=timezone.utc)).isoformat())
                sig.pop("price_alert", None)
        else:
            if candles:
                latest_price = candles[-1]["close"]
                _ref = float(sig.get("avg_entry", sig.get("entry", 0)) or 0)
                sig["latest_price"] = float(latest_price)
                if _ref > 0:
                    rise_pct = (latest_price - _ref) / _ref * 100
                    sig["price_alert"] = rise_pct >= _PRICE_ALERT_PCT
                    sig["price_alert_pct"] = round(rise_pct, 2)
                else:
                    sig["price_alert"] = False
                    sig["price_alert_pct"] = 0.0

    except Exception as _upd_exc:
        _append_error("signal_update", f"[watcher] {_sym}: {_upd_exc}", symbol=_sym)

def _watcher_loop_short():
    while True:
        try:
            if not _scanner_running.is_set() or getattr(_b, "_ss_sl_paused", False):
                time.sleep(2)
                continue

            with _config_lock:
                cfg = dict(_b._ss_cfg)

            _watcher_min = int(cfg.get("watcher_minutes", 1) or 0)
            _loop_min = int(cfg.get("loop_minutes", 5) or 5)

            if _watcher_min <= 0 or _watcher_min >= _loop_min:
                _b._ss_watcher_event.wait(timeout=10)
                _b._ss_watcher_event.clear()
                continue

            t0 = time.time()
            try:
                with _log_lock:
                    _open_snapshot = [s for s in _b._ss_log["signals"] if s.get("status") == "open"]
                if _open_snapshot:
                    with ThreadPoolExecutor(max_workers=min(len(_open_snapshot), 15)) as pool:
                        futs = [pool.submit(_watcher_update_one_signal_short, s) for s in _open_snapshot]
                        for f in as_completed(futs):
                            f.result()

                    _sync_updated = _reconcile_tier1_short(cfg)
                    if _sync_updated:
                        with _log_lock:
                            save_log(_b._ss_log)

                    _dca_pending_sigs = [s for s in _open_snapshot if s.get("_dca_pending")]
                    if _dca_pending_sigs:
                        for _dsig in _dca_pending_sigs:
                            if cfg.get("trade_enabled") and not _dsig.get("order_id"):
                                if _dsig.get("order_status") == "error":
                                    _dsig["status"] = "entry_failed"
                                _append_error("trade", f"DCA skipped for {_dsig.get('symbol', '?')} — trade_enabled ON but order_id empty")
                                _dsig.pop("_dca_pending", None)
                                continue
                            _is_paper = (not cfg.get("trade_enabled") or not _dsig.get("order_id"))
                            if _is_paper:
                                _execute_dca_fill_paper_short(_dsig, cfg)
                            else:
                                _execute_dca_fill_short(_dsig, cfg)
                        with _log_lock:
                            save_log(_b._ss_log)
                    else:
                        if any(s.get("status") != "open" for s in _open_snapshot):
                            with _log_lock:
                                save_log(_b._ss_log)

            except Exception as e:
                _append_error("watcher", str(e))

            if cfg.get("trade_enabled") and cfg.get("api_key"):
                _t1_interval = int(cfg.get("reconcile_t1_minutes", 2) or 2) * 60
                _t2_interval = int(cfg.get("reconcile_t2_minutes", 10) or 10) * 60
                _now_ts = int(time.time())

                if _t1_interval > 0:
                    _t1_last = getattr(_b, "_ss_reconcile_t1_last", 0)
                    if _now_ts - _t1_last >= _t1_interval:
                        try:
                            _reconcile_tier1_short(cfg)
                        except Exception as _t1_e:
                            _append_error("watcher", f"T1 reconcile outer: {_t1_e}")

                if _t2_interval > 0:
                    _t2_last = getattr(_b, "_ss_reconcile_t2_last", 0)
                    if _now_ts - _t2_last >= _t2_interval:
                        try:
                            _reconcile_tier2_short(cfg)
                        except Exception as _t2_e:
                            _append_error("watcher", f"T2 reconcile outer: {_t2_e}")

            elapsed = time.time() - t0
            _b._ss_watcher_last_ts = int(time.time())
            _b._ss_watcher_last_dur = round(elapsed, 2)
            sleep_sec = max(0, _watcher_min * 60 - elapsed)
            _b._ss_watcher_event.wait(timeout=sleep_sec)
            _b._ss_watcher_event.clear()

        except Exception as e:
            _append_error("watcher", f"outer loop: {e}")
            time.sleep(5)

# ============================================================================
# BACKGROUND SCANNER THREAD
# ============================================================================
def _check_sl_circuit_breaker_short():
    with _log_lock:
        _closed = sorted([s for s in _b._ss_log["signals"]
                         if s.get("status") in ("tp_hit", "sl_hit") and s.get("close_time")],
                         key=lambda x: x.get("close_time", ""))
    return len(_closed) >= 3 and all(s["status"] == "sl_hit" for s in _closed[-3:])

def _bg_loop_short():
    while True:
        if not _scanner_running.is_set() or getattr(_b, "_ss_sl_paused", False):
            time.sleep(2)
            continue
        with _config_lock:
            cfg = dict(_b._ss_cfg)
        t0 = time.time()
        try:
            _watcher_min = int(cfg.get("watcher_minutes", 1) or 0)
            _loop_min = int(cfg.get("loop_minutes", 5) or 5)
            _watcher_active = 0 < _watcher_min < _loop_min

            with _log_lock:
                _open_snapshot = [s for s in _b._ss_log["signals"] if s.get("status") == "open"]
            if _open_snapshot and not _watcher_active:
                update_open_signals_short(_open_snapshot)

                _dca_pending_sigs = [s for s in _open_snapshot if s.get("_dca_pending")]
                if _dca_pending_sigs:
                    for _dsig in _dca_pending_sigs:
                        if cfg.get("trade_enabled") and not _dsig.get("order_id"):
                            if _dsig.get("order_status") == "error":
                                _dsig["status"] = "entry_failed"
                            _append_error("trade", f"DCA skipped for {_dsig.get('symbol', '?')} — trade_enabled ON but order_id empty")
                            _dsig.pop("_dca_pending", None)
                            continue
                        _is_paper = (not cfg.get("trade_enabled") or not _dsig.get("order_id"))
                        if _is_paper:
                            _execute_dca_fill_paper_short(_dsig, cfg)
                        else:
                            _execute_dca_fill_short(_dsig, cfg)
                    with _log_lock:
                        save_log(_b._ss_log)

            if not _watcher_active and cfg.get("trade_enabled") and cfg.get("api_key"):
                _t1_interval = int(cfg.get("reconcile_t1_minutes", 2) or 2) * 60
                _t2_interval = int(cfg.get("reconcile_t2_minutes", 10) or 10) * 60
                _now_ts = int(time.time())
                if _t1_interval > 0:
                    _t1_last = getattr(_b, "_ss_reconcile_t1_last", 0)
                    if _now_ts - _t1_last >= _t1_interval:
                        try:
                            _reconcile_tier1_short(cfg)
                        except Exception as _t1_e:
                            _append_error("watcher", f"T1 reconcile (bg): {_t1_e}")
                if _t2_interval > 0:
                    _t2_last = getattr(_b, "_ss_reconcile_t2_last", 0)
                    if _now_ts - _t2_last >= _t2_interval:
                        try:
                            _reconcile_tier2_short(cfg)
                        except Exception as _t2_e:
                            _append_error("watcher", f"T2 reconcile (bg): {_t2_e}")

            if _check_sl_circuit_breaker_short():
                _b._ss_sl_paused = True
                continue

            with _log_lock:
                _current_supers = sum(1 for s in _b._ss_log["signals"]
                                      if s.get("status") == "open" and s.get("is_super_setup"))
            _super_cap = max(0, int(cfg.get("max_super_trades", 5)))
            _super_slots_left = max(0, _super_cap - _current_supers)

            now_dubai = dubai_now()
            tp_cutoff = now_dubai - timedelta(minutes=int(cfg["cooldown_minutes"]))
            sl_cd_hours = max(1, int(cfg.get("sl_cooldown_hours", 24)))
            sl_cutoff = now_dubai - timedelta(hours=sl_cd_hours)

            with _log_lock:
                cooled_tp = {s["symbol"] for s in _b._ss_log["signals"]
                             if s.get("close_time") and s.get("status") == "tp_hit"
                             and datetime.fromisoformat(s["close_time"].replace("Z", "+00:00")) >= tp_cutoff}
                cooled_sl = {s["symbol"] for s in _b._ss_log["signals"]
                             if s.get("close_time") and s.get("status") == "sl_hit"
                             and datetime.fromisoformat(s["close_time"].replace("Z", "+00:00")) >= sl_cutoff}
                cooled = cooled_tp | cooled_sl
                active = {s["symbol"] for s in _b._ss_log["signals"] if s["status"] == "open"}

            if cfg.get("scan_hour_enabled", False):
                _now_h = dubai_now().hour
                _h_start = int(cfg.get("scan_hour_start", 0))
                _h_end = int(cfg.get("scan_hour_end", 23))
                if _h_start <= _h_end:
                    _in_window = _h_start <= _now_h <= _h_end
                else:
                    _in_window = _now_h >= _h_start or _now_h <= _h_end
                if not _in_window:
                    _rescan_event.wait(timeout=60)
                    _rescan_event.clear()
                    continue

            _pre_scan_skip = cooled | active
            new_sigs, errors = scan_short(cfg, super_slots_remaining=_super_slots_left,
                                           skip_symbols=_pre_scan_skip)

            with _log_lock:
                open_trade_count = len(active)
                MAX_OPEN_TRADES = max(1, int(cfg.get("max_open_trades", 15)))
                _new_sig_syms = [s["symbol"] for s in new_sigs]
                _blocked_active = sorted(active & set(_new_sig_syms))
                _blocked_cooldown = sorted(cooled_tp & set(_new_sig_syms))
                _blocked_sl_cooldown = sorted(cooled_sl & set(_new_sig_syms))
                _filter_counts["f_sl_cooldown"] = len(_blocked_sl_cooldown)
                _filter_counts["f_sl_cooldown_syms"] = list(_blocked_sl_cooldown)
                skip = cooled | active
                _added_syms = []
                _queued_syms = []
                sigs_to_trade = []
                for sig in new_sigs:
                    if sig["symbol"] not in skip:
                        if open_trade_count >= MAX_OPEN_TRADES:
                            queued_sig = dict(sig)
                            queued_sig["status"] = "queue_limit"
                            _b._ss_log["signals"].append(queued_sig)
                            _queued_syms.append(sig["symbol"])
                            skip.add(sig["symbol"])
                        else:
                            _init_signal_trade_snapshot_short(sig, cfg)
                            _b._ss_log["signals"].append(sig)
                            skip.add(sig["symbol"])
                            _added_syms.append(sig["symbol"])
                            open_trade_count += 1
                            if (cfg.get("trade_enabled") and cfg.get("api_key") and
                                cfg.get("api_secret") and cfg.get("api_passphrase")):
                                sigs_to_trade.append(sig)
                _filter_counts["new_signal_syms"] = _added_syms
                _filter_counts["queued_syms"] = _queued_syms
                _filter_counts["blocked_by_active_syms"] = _blocked_active
                _filter_counts["blocked_by_cooldown_syms"] = _blocked_cooldown
                _filter_counts["blocked_by_sl_cooldown_syms"] = _blocked_sl_cooldown
                elapsed = time.time() - t0
                _b._ss_log["health"].update(
                    total_cycles=_b._ss_log["health"].get("total_cycles", 0) + 1,
                    last_scan_at=dubai_now().isoformat(),
                    last_scan_duration_s=round(elapsed, 1),
                    total_api_errors=_b._ss_log["health"].get("total_api_errors", 0) + errors,
                    watchlist_size=len(cfg["watchlist"]),
                    pre_filtered_out=_filter_counts.get("pre_filtered_out", 0),
                    deep_scanned=_filter_counts.get("checked", 0),
                )
                save_log(_b._ss_log)

            if sigs_to_trade:
                for sig in sigs_to_trade:
                    result = place_okx_order_short(sig, cfg)
                    sig["order_id"] = result.get("ordId", "")
                    sig["algo_id"] = result.get("algoId", "")
                    sig["order_sz"] = result.get("sz", 0)
                    sig["order_status"] = result.get("status", "")
                    sig["order_error"] = result.get("error", "")
                    if result.get("status") == "error":
                        sig["status"] = "entry_failed"
                    sig["trade_usdt"] = float(cfg.get("trade_usdt_amount", 0))
                    sig["trade_lev"] = int(cfg.get("trade_leverage", 10))
                    sig["demo_mode"] = bool(cfg.get("demo_mode", True))
                    sig["order_ct_val"] = float(result.get("ct_val", 0) or 0)
                    sig["order_notional"] = float(result.get("notional", 0) or 0)
                    sig["order_margin_mode"] = result.get("tdMode") or cfg.get("trade_margin_mode", "isolated")
                    sig["order_is_hedge"] = bool(result.get("is_hedge", False))
                    sig["tp_algo_id"] = result.get("tp_algo_id", "")
                    if result.get("actual_entry"):
                        sig["entry"] = result["actual_entry"]
                        sig["tp"] = result["actual_tp"]
                        sig["sl"] = result["actual_sl"]
                        sig["signal_entry"] = sig.get("entry")
                    _init_signal_trade_snapshot_short(sig, cfg)
                    if sig.get("dca_fills") and sig.get("order_sz"):
                        sig["dca_fills"][0]["sz"] = int(sig["order_sz"])
                    if result.get("tp_algo_id"):
                        sig["tp_algo_id"] = result["tp_algo_id"]
                    _env_tag = "DEMO" if sig.get("demo_mode") else "LIVE"
                    if result.get("status") in ("placed", "partial", "error"):
                        _okx_log_entry(sig, f"ENTRY SHORT [{_env_tag}]",
                                       instId=_to_okx(sig.get("symbol", "")),
                                       ordType="market", tdMode=sig.get("order_margin_mode", ""),
                                       sz=f"{sig.get('order_sz', 0)} contracts",
                                       ordId=result.get("ordId", "—"),
                                       entry_px=result.get("actual_entry", sig.get("entry", "")),
                                       status=result.get("status", ""))
                    _algo_placed = result.get("algoId", "") or result.get("tp_algo_id", "")
                    if _algo_placed:
                        _is_cross_log = (sig.get("order_margin_mode", "").strip().lower() == "cross")
                        if _is_cross_log:
                            _okx_log_entry(sig, f"TP ALGO SHORT [{_env_tag}]",
                                           ordType="conditional",
                                           tpTriggerPx=result.get("actual_tp", sig.get("tp", "")),
                                           tpTriggerPxType="mark", algoId=_algo_placed)
                        else:
                            _okx_log_entry(sig, f"OCO ALGO SHORT [{_env_tag}]",
                                           ordType="oco",
                                           tpTriggerPx=result.get("actual_tp", sig.get("tp", "")),
                                           slTriggerPx=result.get("actual_sl", sig.get("sl", "")),
                                           tpTriggerPxType="mark", slTriggerPxType="mark", algoId=_algo_placed)
                with _log_lock:
                    save_log(_b._ss_log)
            _b._ss_last_error = ""

        except Exception as e:
            _b._ss_last_error = str(e)
            _append_error("loop", str(e))

        elapsed = time.time() - t0
        sleep_sec = max(0, cfg["loop_minutes"] * 60 - elapsed)
        _rescan_event.wait(timeout=sleep_sec)
        _rescan_event.clear()

def _ensure_scanner_short():
    if _b._ss_thread is None or not _b._ss_thread.is_alive():
        t = threading.Thread(target=_bg_loop_short, daemon=True, name="okx-scanner-short")
        t.start()
        _b._ss_thread = t
    if _b._ss_watcher_thread is None or not _b._ss_watcher_thread.is_alive():
        wt = threading.Thread(target=_watcher_loop_short, daemon=True, name="okx-watcher-short")
        wt.start()
        _b._ss_watcher_thread = wt

# ============================================================================
# MARKET CONDITION ANALYSER — SHORT VERSION
# ============================================================================
def _analyze_market_conditions_short(cfg: dict, symbols: list, progress_bar, status_text) -> dict:
    import statistics as _stats

    n = len(symbols)
    if n == 0:
        return {}

    tp_pct = float(cfg.get("tp_pct", 1.5))

    _d = {
        "f2_pdz15m": {"pass": 0, "fail": 0, "zones": []},
        "f3_pdz5m": {"pass": 0, "fail": 0, "zones": []},
        "f4_rsi5m": {"pass": 0, "fail": 0, "values": []},
        "f5_rsi1h": {"pass": 0, "fail": 0, "values": []},
        "f5b_atr": {"pass": 0, "fail": 0, "values": []},
        "f6_ema_3m": {"pass": 0, "fail": 0},
        "f6_ema_5m": {"pass": 0, "fail": 0},
        "f6_ema_15m": {"pass": 0, "fail": 0},
        "f7_macd_3m": {"pass": 0, "fail": 0},
        "f7_macd_5m": {"pass": 0, "fail": 0},
        "f7_macd_15m": {"pass": 0, "fail": 0},
        "f8_sar_3m": {"pass": 0, "fail": 0},
        "f8_sar_5m": {"pass": 0, "fail": 0},
        "f8_sar_15m": {"pass": 0, "fail": 0},
        "f9_vol": {"pass": 0, "fail": 0, "values": []},
        "f10_ema_cross": {"pass": 0, "fail": 0},
    }
    errors = 0

    for i, sym in enumerate(symbols):
        progress_bar.progress((i + 1) / n)
        status_text.text(f"Analysing {sym}  [{i + 1} / {n}]")
        try:
            m15 = get_klines(sym, "15m", 55)[:-1]
            m5 = get_klines(sym, "5m", 55)[:-1]
            m1h = get_klines(sym, "1h", 55)[:-1]
            m3 = get_klines(sym, "3m", 85)[:-1]

            if not m5 or not m15 or not m1h or not m3:
                errors += 1
                continue

            entry = float(m5[-1]["close"])
            if entry <= 0:
                errors += 1
                continue

            closes_5m = [c["close"] for c in m5]
            closes_15m = [c["close"] for c in m15]
            closes_1h = [c["close"] for c in m1h]
            closes_3m = [c["close"] for c in m3]

            _p15, _z15 = calc_pdz_zone_short(m15, entry, tp_pct / 100.0)
            _d["f2_pdz15m"]["zones"].append(_z15)
            _d["f2_pdz15m"]["pass" if _z15 == "Premium" else "fail"] += 1

            _p5, _z5 = calc_pdz_zone_short(m5, entry, tp_pct / 100.0)
            _d["f3_pdz5m"]["zones"].append(_z5)
            _d["f3_pdz5m"]["pass" if _z5 == "Premium" else "fail"] += 1

            _rsi5 = (calc_rsi_series(closes_5m) or [None])[-1]
            if _rsi5 is not None:
                _d["f4_rsi5m"]["values"].append(_rsi5)
                _d["f4_rsi5m"]["pass" if _rsi5 <= float(cfg.get("rsi_5m_max", 70)) else "fail"] += 1

            _rsi1h = (calc_rsi_series(closes_1h) or [None])[-1]
            if _rsi1h is not None:
                _d["f5_rsi1h"]["values"].append(_rsi1h)
                _rlo = float(cfg.get("rsi_1h_min", 65))
                _rhi = float(cfg.get("rsi_1h_max", 95))
                _d["f5_rsi1h"]["pass" if _rlo <= _rsi1h <= _rhi else "fail"] += 1

            _atr_s = calc_atr(m15, 14)
            if _atr_s and entry > 0:
                _atr_pct = (_atr_s[-1] / entry) * 100.0
                if _atr_pct > 0:
                    _ratio = tp_pct / _atr_pct
                    _d["f5b_atr"]["values"].append(_ratio)
                    _thresh = {"Strict": 1.5, "Normal": 2.0, "Relaxed": 3.0}.get(cfg.get("atr_mode", "Normal"), 2.0)
                    _d["f5b_atr"]["pass" if _ratio <= _thresh else "fail"] += 1

            for _fk, _cl, _pk in [("f6_ema_3m", closes_3m, "ema_period_3m"),
                                   ("f6_ema_5m", closes_5m, "ema_period_5m"),
                                   ("f6_ema_15m", closes_15m, "ema_period_15m")]:
                _ema = calc_ema(_cl, max(2, int(cfg.get(_pk, 12))))
                if _ema:
                    _d[_fk]["pass" if entry < _ema[-1] else "fail"] += 1

            for _fk, _cl in [("f7_macd_3m", closes_3m), ("f7_macd_5m", closes_5m), ("f7_macd_15m", closes_15m)]:
                _ok, _ = macd_bearish_and_value(_cl)
                _d[_fk]["pass" if _ok else "fail"] += 1

            for _fk, _bars in [("f8_sar_3m", m3), ("f8_sar_5m", m5), ("f8_sar_15m", m15)]:
                _sar = calc_parabolic_sar(_bars)
                _d[_fk]["pass" if (_sar and not _sar[-1][1]) else "fail"] += 1

            _lkb = max(2, int(cfg.get("vol_spike_lookback", 20)))
            _vols = [c["volume"] for c in m15]
            if len(_vols) >= _lkb + 1:
                _avg_v = sum(_vols[-(_lkb + 1):-1]) / _lkb
                if _avg_v > 0:
                    _vr = _vols[-1] / _avg_v
                    _d["f9_vol"]["values"].append(_vr)
                    _d["f9_vol"]["pass" if _vr >= float(cfg.get("vol_spike_mult", 2.0)) else "fail"] += 1

            _fp = max(2, int(cfg.get("ema_cross_fast_15m", 12)))
            _sp = max(_fp + 1, int(cfg.get("ema_cross_slow_15m", 21)))
            _ef = calc_ema(closes_15m, _fp)
            _es = calc_ema(closes_15m, _sp)
            if _ef and _es:
                _d["f10_ema_cross"]["pass" if _ef[-1] < _es[-1] else "fail"] += 1

        except Exception:
            errors += 1
            continue

    def _pr(key: str) -> float:
        p = _d[key]["pass"]
        f = _d[key]["fail"]
        t = p + f
        return (p / t * 100) if t > 0 else 0.0

    def _icon(rate: float) -> str:
        return "🟢" if rate >= 35 else ("🟡" if rate >= 15 else "🔴")

    def _bool_rec(rate: float, label_on: str) -> str:
        if rate < 15:
            return "Consider OFF"
        if rate < 35:
            return f"{label_on} (tight)"
        if rate > 75:
            return f"{label_on} (permissive)"
        return f"{label_on} ✓"

    recs = {}

    for _fk, _fl, _use_key, _tf_label in [("f2_pdz15m", "F2 — PDZ 15m", "use_pdz_15m", "15m"),
                                            ("f3_pdz5m", "F3 — PDZ 5m", "use_pdz_5m", "5m")]:
        _rate = _pr(_fk)
        _zones = _d[_fk]["zones"]
        _prem = (_zones.count("Premium") / len(_zones) * 100) if _zones else 0
        _disc = (_zones.count("Discount") / len(_zones) * 100) if _zones else 0
        if _rate < 15:
            _rec = "Consider OFF"
            _reason = f"Only {_rate:.0f}% qualify — market is mostly Discount ({_disc:.0f}%), only {_prem:.0f}% in Premium zone"
        elif _rate < 30:
            _rec = f"ON – tight ({_tf_label})"
            _reason = f"{_rate:.0f}% qualify — limited Premium zones ({_prem:.0f}%)"
        else:
            _rec = "ON ✓"
            _reason = f"{_rate:.0f}% qualify — healthy zone distribution ({_prem:.0f}% Premium)"
        recs[_fk] = {"filter": _fl, "current": "ON" if cfg.get(_use_key, True) else "OFF",
                     "current_pass_rate": _rate, "rec": _rec, "rec_pass_rate": _rate,
                     "reason": _reason, "icon": _icon(_rate)}

    _r4 = _pr("f4_rsi5m")
    _v5 = _d["f4_rsi5m"]["values"]
    _cur_r5 = int(cfg.get("rsi_5m_max", 70))
    _med_r5 = round(_stats.median(_v5)) if _v5 else _cur_r5
    _best_r5 = _cur_r5
    for _t in range(50, 96):
        _pr_t = sum(1 for v in _v5 if v <= _t) / len(_v5) * 100 if _v5 else 0
        if abs(_pr_t - 35) < abs((sum(1 for v in _v5 if v <= _best_r5) / len(_v5) * 100 if _v5 else 0) - 35):
            _best_r5 = _t
    recs["f4_rsi5m"] = {"filter": "F4 — RSI 5m max", "current": f"≤{_cur_r5}",
                        "current_pass_rate": _r4, "rec": f"≤{_best_r5}",
                        "rec_pass_rate": (sum(1 for v in _v5 if v <= _best_r5) / len(_v5) * 100) if _v5 else _r4,
                        "reason": f"Median 5m RSI {_med_r5}", "icon": _icon(_r4)}

    _r5 = _pr("f5_rsi1h")
    _v1h = _d["f5_rsi1h"]["values"]
    _cur_lo = int(cfg.get("rsi_1h_min", 65))
    _cur_hi = int(cfg.get("rsi_1h_max", 95))
    _med_1h = round(_stats.median(_v1h)) if _v1h else 80
    if _v1h:
        _sv = sorted(_v1h)
        _lo_idx = max(0, int(0.20 * len(_sv)))
        _hi_idx = min(len(_sv) - 1, int(0.80 * len(_sv)))
        _rec_lo = max(50, round(_sv[_lo_idx]))
        _rec_hi = min(95, round(_sv[_hi_idx]))
    else:
        _rec_lo, _rec_hi = _cur_lo, _cur_hi
    recs["f5_rsi1h"] = {"filter": "F5 — RSI 1h range", "current": f"{_cur_lo}–{_cur_hi}",
                        "current_pass_rate": _r5, "rec": f"{_rec_lo}–{_rec_hi}",
                        "rec_pass_rate": (sum(1 for v in _v1h if _rec_lo <= v <= _rec_hi) / len(_v1h) * 100) if _v1h else _r5,
                        "reason": f"Median 1h RSI {_med_1h}", "icon": _icon(_r5)}

    _r5b = _pr("f5b_atr")
    _vatr = _d["f5b_atr"]["values"]
    _cur_atr = cfg.get("atr_mode", "Normal")
    _atr_rates = {}
    for _mode, _thr in [("Strict", 1.5), ("Normal", 2.0), ("Relaxed", 3.0)]:
        _atr_rates[_mode] = (sum(1 for v in _vatr if v <= _thr) / len(_vatr) * 100) if _vatr else 0
    _best_atr = min(_atr_rates, key=lambda m: abs(_atr_rates[m] - 50))
    recs["f5b_atr"] = {"filter": "F5b — ATR mode", "current": _cur_atr,
                       "current_pass_rate": _r5b, "rec": _best_atr,
                       "rec_pass_rate": _atr_rates.get(_best_atr, _r5b),
                       "reason": f"Mode '{_best_atr}' gives best pass rate", "icon": _icon(_r5b)}

    for _fk, _fl, _uk, _pk in [("f6_ema_3m", "F6 — EMA 3m", "use_ema_3m", "ema_period_3m"),
                                ("f6_ema_5m", "F6 — EMA 5m", "use_ema_5m", "ema_period_5m"),
                                ("f6_ema_15m", "F6 — EMA 15m", "use_ema_15m", "ema_period_15m")]:
        _rate = _pr(_fk)
        _per = int(cfg.get(_pk, 12))
        recs[_fk] = {"filter": _fl, "current": "ON" if cfg.get(_uk, False) else "OFF",
                     "current_pass_rate": _rate, "rec": _bool_rec(_rate, "ON"),
                     "rec_pass_rate": _rate, "reason": f"{_rate:.0f}% below EMA{_per}", "icon": _icon(_rate)}

    for _fk, _fl, _uk in [("f7_macd_3m", "F7 — MACD 3m (bearish)", "use_macd_3m"),
                           ("f7_macd_5m", "F7 — MACD 5m (bearish)", "use_macd_5m"),
                           ("f7_macd_15m", "F7 — MACD 15m (bearish)", "use_macd_15m")]:
        _rate = _pr(_fk)
        recs[_fk] = {"filter": _fl, "current": "ON" if cfg.get(_uk, True) else "OFF",
                     "current_pass_rate": _rate, "rec": _bool_rec(_rate, "ON"),
                     "rec_pass_rate": _rate, "reason": f"{_rate:.0f}% bearish", "icon": _icon(_rate)}

    for _fk, _fl, _uk in [("f8_sar_3m", "F8 — SAR 3m (bearish)", "use_sar_3m"),
                           ("f8_sar_5m", "F8 — SAR 5m (bearish)", "use_sar_5m"),
                           ("f8_sar_15m", "F8 — SAR 15m (bearish)", "use_sar_15m")]:
        _rate = _pr(_fk)
        recs[_fk] = {"filter": _fl, "current": "ON" if cfg.get(_uk, True) else "OFF",
                     "current_pass_rate": _rate, "rec": _bool_rec(_rate, "ON"),
                     "rec_pass_rate": _rate, "reason": f"{_rate:.0f}% below SAR", "icon": _icon(_rate)}

    _r9 = _pr("f9_vol")
    _vvol = _d["f9_vol"]["values"]
    _cur_mlt = float(cfg.get("vol_spike_mult", 2.0))
    _cur_lkb = int(cfg.get("vol_spike_lookback", 20))
    _best_mlt = _cur_mlt
    for _tm in [1.5, 1.8, 2.0, 2.5, 3.0]:
        _rt = sum(1 for v in _vvol if v >= _tm) / len(_vvol) * 100 if _vvol else 0
        _rb = sum(1 for v in _vvol if v >= _best_mlt) / len(_vvol) * 100 if _vvol else 0
        if abs(_rt - 25) < abs(_rb - 25):
            _best_mlt = _tm
    _med_vol = round(_stats.median(_vvol), 2) if _vvol else "?"
    recs["f9_vol"] = {"filter": f"F9 — Vol ≥Nx / {_cur_lkb}", "current": f"≥{_cur_mlt}×",
                      "current_pass_rate": _r9, "rec": f"≥{_best_mlt}×",
                      "rec_pass_rate": (sum(1 for v in _vvol if v >= _best_mlt) / len(_vvol) * 100) if _vvol else _r9,
                      "reason": f"Median vol ratio {_med_vol}×", "icon": _icon(_r9)}

    _r10 = _pr("f10_ema_cross")
    _fp10 = int(cfg.get("ema_cross_fast_15m", 12))
    _sp10 = int(cfg.get("ema_cross_slow_15m", 21))
    recs["f10_ema_cross"] = {"filter": f"F10 — EMA{_fp10}<EMA{_sp10} 15m",
                             "current": "ON" if cfg.get("use_ema_cross_15m", True) else "OFF",
                             "current_pass_rate": _r10, "rec": _bool_rec(_r10, "ON"),
                             "rec_pass_rate": _r10, "reason": f"{_r10:.0f}% pass", "icon": _icon(_r10)}

    return {"recommendations": recs, "total_coins": n, "valid_coins": n - errors, "errors": errors}

# ============================================================================
# STREAMLIT UI — COMPLETE SHORT SCANNER
# ============================================================================
st.set_page_config(page_title="S&R — Short Scanner (Full)", page_icon="📉",
                   layout="wide", initial_sidebar_state="expanded")

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Sora:wght@300;400;600;700&family=JetBrains+Mono:wght@400;500&display=swap');
html, body, [class*="css"] { font-family: 'Sora', sans-serif !important; color: #F9FAFB !important; }
.stApp { background-color: #0A0F1E !important; }
[data-testid="stSidebar"] { background-color: #0D1424 !important; border-right: 1px solid #FF6B6B33 !important; }
[data-testid="stSidebar"] .stMarkdown h2 { color: #FF6B6B !important; font-weight: 700 !important; border-bottom: 1px solid #FF6B6B55 !important; }
h1 { font-weight: 700 !important; color: #FF6B6B !important; border-bottom: 2px solid #FF6B6B55 !important; }
[data-testid="stMetricLabel"] { color: #FF6B6B !important; }
.stButton > button:hover { background-color: #FF6B6B !important; color: #0A0F1E !important; border-color: #FF6B6B !important; }
.stButton > button[kind="primary"] { background-color: #FF6B6B !important; color: #0A0F1E !important; }
[data-testid="stMetric"] { background-color: #111827 !important; border: 1px solid #FF6B6B44 !important; border-radius: 8px !important; }
.js-plotly-plot .plotly .bg { fill: #111827 !important; }
</style>
""", unsafe_allow_html=True)

_ensure_scanner_short()

with _log_lock:
    _snap_log = json.loads(json.dumps(_b._ss_log))
with _config_lock:
    _snap_cfg = dict(_b._ss_cfg)

health = _snap_log.get("health", {})
signals = _snap_log.get("signals", [])

# ============================================================================
# SIDEBAR — Complete with all filter controls
# ============================================================================
with st.sidebar:
    st.markdown("## 📉 Short Scanner Config")
    running = _scanner_running.is_set()
    if st.button("⏹ Stop Scanner" if running else "▶️ Start Scanner", use_container_width=True):
        if running:
            _scanner_running.clear()
        else:
            _scanner_running.set()
        st.rerun()
    st.caption(f"{'🟢' if running else '🔴'} Scanner {'running' if running else 'stopped'}")

    _sl_paused = getattr(_b, "_ss_sl_paused", False)
    if _sl_paused:
        st.warning("🔴 **Paused — 3 consecutive SL hit**\n\nReview market conditions before resuming.")
        if st.button("▶️ Resume Scanning", key="resume_sl_circuit", type="primary", use_container_width=True):
            _b._ss_sl_paused = False
            _rescan_event.set()
            st.rerun()
    else:
        st.success("✅ Circuit Breaker: OK — No consecutive SL pause")
    st.divider()

    st.markdown("**🤖 Auto-Trading (SHORT)**")
    new_trade_enabled = st.checkbox("Enable Short Trading", value=bool(_snap_cfg.get("trade_enabled", False)))
    new_demo_mode = st.radio("Environment", ["Demo", "Live"], index=0 if _snap_cfg.get("demo_mode", True) else 1, horizontal=True)

    import os as _os_env
    _env_has = {"api_key": bool(_os_env.environ.get("OKX_API_KEY", "").strip()),
                "api_secret": bool(_os_env.environ.get("OKX_API_SECRET", "").strip()),
                "api_passphrase": bool(_os_env.environ.get("OKX_API_PASSPHRASE", "").strip())}
    _all_env = all(_env_has.values())
    if _all_env:
        st.success("🔐 Credentials loaded from environment variables (not stored on disk)")

    new_api_key = st.text_input("API Key", value=_snap_cfg.get("api_key", ""), type="password", disabled=_env_has["api_key"])
    new_api_secret = st.text_input("API Secret", value=_snap_cfg.get("api_secret", ""), type="password", disabled=_env_has["api_secret"])
    new_api_passphrase = st.text_input("API Passphrase", value=_snap_cfg.get("api_passphrase", ""), type="password", disabled=_env_has["api_passphrase"])

    st.divider()
    st.markdown("**📊 Trade Settings (SHORT)**")
    ta1, ta2 = st.columns(2)
    new_trade_usdt = ta1.number_input("Size (USDT)", min_value=1.0, value=float(_snap_cfg.get("trade_usdt_amount", 5.0)), key="cfg_trade_usdt")
    new_trade_lev = ta2.number_input("Leverage ×", min_value=1, max_value=125, value=int(_snap_cfg.get("trade_leverage", 20)), key="cfg_trade_lev")
    new_margin_mode = st.selectbox("Margin Mode", ["isolated", "cross"], index=0 if _snap_cfg.get("trade_margin_mode", "isolated") == "isolated" else 1, key="cfg_margin_mode")

    c1, c2 = st.columns(2)
    new_tp = c1.number_input("TP % (below entry)", min_value=0.1, max_value=20.0, step=0.1, value=float(_snap_cfg.get("tp_pct", 1.5)), key="cfg_tp")
    _isolated_active = _snap_cfg.get("trade_margin_mode", "isolated") == "isolated"
    new_sl = c2.number_input("SL % (above entry, cross only)", min_value=0.1, max_value=20.0, step=0.1, value=float(_snap_cfg.get("sl_pct", 3.0)), key="cfg_sl", disabled=_isolated_active)

    st.markdown("**📊 DCA Settings (SHORT — Average UP)**")
    _dca_opts = [0, 1, 2, 3, 4, 5, 6]
    _cur_dca = int(_snap_cfg.get("trade_max_dca", 1))
    new_trade_max_dca = st.selectbox("Max DCA per Trade", _dca_opts, index=_dca_opts.index(_cur_dca) if _cur_dca in _dca_opts else 1, key="cfg_trade_max_dca")

    if new_margin_mode == "isolated":
        _iso_options = list(range(10, 100, 5))
        _cur_iso_pct = float(_snap_cfg.get("dca_iso_distance_pct", 80.0))
        _cur_iso_int = max(10, min(95, int(round(_cur_iso_pct / 5.0) * 5)))
        _cur_iso_idx = _iso_options.index(_cur_iso_int) if _cur_iso_int in _iso_options else 14
        new_dca_iso_distance_pct = float(st.selectbox("DCA Trigger — Isolated (% distance from liquidation)", options=_iso_options, index=_cur_iso_idx, format_func=lambda v: f"{v}%", key="cfg_dca_iso_dist"))
        new_dca_cross_rise_pct = float(_snap_cfg.get("dca_cross_rise_pct", 7.0))
    else:
        new_dca_cross_rise_pct = st.number_input("DCA Trigger — Cross (% rise above avg)", min_value=0.1, max_value=50.0, value=float(_snap_cfg.get("dca_cross_rise_pct", 7.0)), step=0.5, key="cfg_dca_cross_rise")
        new_dca_iso_distance_pct = float(_snap_cfg.get("dca_iso_distance_pct", 80.0))

    new_dca_tp_usd = st.number_input("DCA TP — Fixed $ profit (after DCA)", min_value=0.10, max_value=50.0, value=float(_snap_cfg.get("dca_tp_usd", 0.50)), step=0.10, key="cfg_dca_tp_usd")
    new_dca_sl_usd = st.number_input("DCA SL — Fixed $ loss limit (after DCA)", min_value=0.50, max_value=500.0, value=float(_snap_cfg.get("dca_sl_usd", 5.00)), step=0.50, key="cfg_dca_sl_usd")
    new_use_dca_sl = st.checkbox("Enable DCA SL (fixed $ stop-loss)", value=bool(_snap_cfg.get("use_dca_sl", True)), key="cfg_use_dca_sl")

    notional_usdt = new_trade_usdt * new_trade_lev
    st.caption(f"📐 Notional per trade: ~${notional_usdt:,.0f} USDT | TP +${notional_usdt * new_tp/100:.2f} | SL -${notional_usdt * new_sl/100:.2f}")

    st.divider()

    _conn = getattr(_b, "_ss_api_conn_status", {"status": "untested", "message": ""})
    _has_creds = bool(_snap_cfg.get("api_key") and _snap_cfg.get("api_secret") and _snap_cfg.get("api_passphrase"))
    if st.button("🔌 Test Connection", use_container_width=True, disabled=not _has_creds):
        with st.spinner("Testing…"):
            _result = test_api_connection(dict(_snap_cfg))
        _b._ss_api_conn_status = {"status": _result["status"], "message": _result["message"], "tested_at": dubai_now().isoformat(),
                                   "demo_mode": _snap_cfg.get("demo_mode", True), "uid": _result.get("uid", ""),
                                   "pos_mode": _result.get("pos_mode", "net_mode"), "acct_lv": _result.get("acct_lv", "2")}
        st.rerun()
    _conn_status = _b._ss_api_conn_status.get("status", "untested")
    if _conn_status == "ok":
        st.success(f"🟢 {_b._ss_api_conn_status.get('message')}")
    elif _conn_status == "error":
        st.error(f"🔴 {_b._ss_api_conn_status.get('message')}")

    st.divider()

    st.markdown("**⚡ Filters (BEARISH)**")
    new_use_pre_filter = st.checkbox("F1 — Bulk Pre-filter", value=bool(_snap_cfg.get("use_pre_filter", True)), key="cfg_use_pre_filter")
    new_use_pdz_15m = st.checkbox("F2 — PDZ 15m (Premium zone)", value=bool(_snap_cfg.get("use_pdz_15m", True)), key="cfg_use_pdz_15m")
    new_use_pdz_5m = st.checkbox("F3 — PDZ 5m (Premium zone)", value=bool(_snap_cfg.get("use_pdz_5m", False)), key="cfg_use_pdz_5m")
    new_use_rsi_5m = st.checkbox("F4 — RSI 5m (overbought)", value=bool(_snap_cfg.get("use_rsi_5m", True)), key="cfg_use_rsi_5m")
    new_rsi5_max = st.number_input("RSI 5m max", min_value=0, max_value=100, step=1, value=int(_snap_cfg.get("rsi_5m_max", 70)), key="cfg_rsi5_max", disabled=not new_use_rsi_5m)
    new_use_rsi_1h = st.checkbox("F5 — RSI 1h (upper range)", value=bool(_snap_cfg.get("use_rsi_1h", True)), key="cfg_use_rsi_1h")
    cr1, cr2 = st.columns(2)
    new_rsi1h_min = cr1.number_input("1h min", min_value=0, max_value=100, step=1, value=int(_snap_cfg.get("rsi_1h_min", 65)), key="cfg_rsi1h_min", disabled=not new_use_rsi_1h)
    new_rsi1h_max = cr2.number_input("1h max", min_value=0, max_value=100, step=1, value=int(_snap_cfg.get("rsi_1h_max", 95)), key="cfg_rsi1h_max", disabled=not new_use_rsi_1h)
    new_use_atr_filter = st.checkbox("F5b — ATR Filter (TP reachability)", value=bool(_snap_cfg.get("use_atr_filter", False)), key="cfg_use_atr_filter")
    new_atr_mode = st.selectbox("ATR Mode", ["Strict", "Normal", "Relaxed"], index=["Strict", "Normal", "Relaxed"].index(_snap_cfg.get("atr_mode", "Normal")), key="cfg_atr_mode", disabled=not new_use_atr_filter)

    st.markdown("**📉 Trend Filters (Bearish)**")
    new_use_ema_15m = st.checkbox("F6 — EMA 15m (price below)", value=bool(_snap_cfg.get("use_ema_15m", True)), key="cfg_use_ema_15m")
    new_ema_period_15m = st.number_input("EMA period 15m", min_value=2, max_value=500, step=1, value=int(_snap_cfg.get("ema_period_15m", 12)), key="cfg_ema_period_15m", disabled=not new_use_ema_15m)
    new_use_macd_15m = st.checkbox("F7 — MACD 15m (bearish)", value=bool(_snap_cfg.get("use_macd_15m", True)), key="cfg_use_macd_15m")
    new_use_sar_15m = st.checkbox("F8 — SAR 15m (above price)", value=bool(_snap_cfg.get("use_sar_15m", True)), key="cfg_use_sar_15m")
    new_use_vol_spike = st.checkbox("F9 — Volume Spike", value=bool(_snap_cfg.get("use_vol_spike", False)), key="cfg_use_vol_spike")
    if new_use_vol_spike:
        vx1, vx2 = st.columns(2)
        new_vol_mult = vx1.number_input("Mult (X×)", min_value=1.0, max_value=20.0, step=0.5, value=float(_snap_cfg.get("vol_spike_mult", 2.0)), key="cfg_vol_mult")
        new_vol_lookback = vx2.number_input("Lookback (N)", min_value=2, max_value=100, step=1, value=int(_snap_cfg.get("vol_spike_lookback", 20)), key="cfg_vol_lookback")
    new_use_ema_cross_15m = st.checkbox("F10 — EMA Cross (death cross)", value=bool(_snap_cfg.get("use_ema_cross_15m", True)), key="cfg_use_ema_cross")
    if new_use_ema_cross_15m:
        ex1, ex2 = st.columns(2)
        new_ema_cross_fast = ex1.number_input("Fast EMA", min_value=2, max_value=500, step=1, value=int(_snap_cfg.get("ema_cross_fast_15m", 12)), key="cfg_ema_cross_fast")
        new_ema_cross_slow = ex2.number_input("Slow EMA", min_value=2, max_value=500, step=1, value=int(_snap_cfg.get("ema_cross_slow_15m", 21)), key="cfg_ema_cross_slow")

    st.divider()
    st.markdown("**📊 Queue & Cooldown**")
    new_max_open_trades = st.number_input("Max Open Trades", min_value=1, max_value=50, step=1, value=int(_snap_cfg.get("max_open_trades", 7)), key="cfg_max_open")
    new_max_super_trades = st.number_input("Max Super Trades", min_value=1, max_value=20, step=1, value=int(_snap_cfg.get("max_super_trades", 2)), key="cfg_max_super")
    new_loop = st.number_input("Loop (min)", min_value=1, max_value=60, step=1, value=int(_snap_cfg.get("loop_minutes", 4)), key="cfg_loop")
    new_sl_cooldown = st.number_input("SL Cooldown (hrs)", min_value=1, max_value=720, step=1, value=int(_snap_cfg.get("sl_cooldown_hours", 4)), key="cfg_sl_cooldown")

    st.divider()
    st.markdown("**📋 Watchlist**")
    wl_text = st.text_area("wl", value="\n".join(_snap_cfg["watchlist"]), height=150)

    if st.button("💾 Save & Apply", use_container_width=True, type="primary"):
        new_wl = [s.strip().upper() for s in wl_text.splitlines() if s.strip()]
        new_cfg = {
            "tp_pct": new_tp, "sl_pct": new_sl,
            "use_pre_filter": bool(new_use_pre_filter),
            "use_rsi_5m": bool(new_use_rsi_5m), "rsi_5m_max": int(new_rsi5_max),
            "use_rsi_1h": bool(new_use_rsi_1h), "rsi_1h_min": int(new_rsi1h_min), "rsi_1h_max": int(new_rsi1h_max),
            "loop_minutes": int(new_loop), "cooldown_minutes": 2,
            "use_ema_15m": bool(new_use_ema_15m), "ema_period_15m": int(new_ema_period_15m),
            "use_macd_15m": bool(new_use_macd_15m),
            "use_sar_15m": bool(new_use_sar_15m),
            "use_pdz_5m": bool(new_use_pdz_5m), "use_pdz_15m": bool(new_use_pdz_15m),
            "use_ema_cross_15m": bool(new_use_ema_cross_15m),
            "ema_cross_fast_15m": int(new_ema_cross_fast) if new_use_ema_cross_15m else 12,
            "ema_cross_slow_15m": int(new_ema_cross_slow) if new_use_ema_cross_15m else 21,
            "max_open_trades": max(1, int(new_max_open_trades)),
            "max_super_trades": max(1, int(new_max_super_trades)),
            "sl_cooldown_hours": max(1, int(new_sl_cooldown)),
            "watchlist": new_wl,
            "trade_enabled": bool(new_trade_enabled), "demo_mode": (new_demo_mode == "Demo"),
            "api_key": new_api_key.strip(), "api_secret": new_api_secret.strip(),
            "api_passphrase": new_api_passphrase.strip(),
            "trade_usdt_amount": float(new_trade_usdt), "trade_leverage": int(new_trade_lev),
            "trade_margin_mode": new_margin_mode,
            "trade_max_dca": max(0, min(6, int(new_trade_max_dca))),
            "dca_iso_distance_pct": max(10.0, min(95.0, float(new_dca_iso_distance_pct))),
            "dca_cross_rise_pct": max(0.1, min(50.0, float(new_dca_cross_rise_pct))),
            "dca_tp_usd": max(0.10, min(50.0, float(new_dca_tp_usd))),
            "dca_sl_usd": max(0.50, min(500.0, float(new_dca_sl_usd))),
            "use_dca_sl": bool(new_use_dca_sl),
            "watcher_minutes": 1, "reconcile_t1_minutes": 2, "reconcile_t2_minutes": 10,
            "use_atr_filter": bool(new_use_atr_filter), "atr_mode": new_atr_mode,
            "use_vol_spike": bool(new_use_vol_spike),
            "vol_spike_mult": float(new_vol_mult) if new_use_vol_spike else 2.0,
            "vol_spike_lookback": int(new_vol_lookback) if new_use_vol_spike else 20,
            "scan_hour_enabled": False, "scan_hour_start": 0, "scan_hour_end": 23,
        }
        with _config_lock:
            _b._ss_cfg.clear()
            _b._ss_cfg.update(new_cfg)
        save_config(new_cfg)
        _b._ss_symbol_cache["fetched_at"] = 0
        _b._ss_rescan_event.set()
        _b._ss_watcher_event.set()
        st.success(f"✅ Saved — {len(new_wl)} coins — rescanning now…")
        st.rerun()

# ============================================================================
# MAIN AREA
# ============================================================================
st.title("📉 S&R — Short Scanner (Full)")
st.caption("Bearish-only signals: Premium zones, overbought RSI, price below EMA, bearish MACD, SAR above price. Full DCA, FC-B, two-tier sync.")

# PnL computation for SHORT
def _calc_pnl_usd_short(sig: dict, ref_price, usdt_fb: float, lev_fb: int):
    try:
        _ref = float(ref_price) if ref_price is not None else 0.0
    except (TypeError, ValueError):
        return None
    if _ref <= 0:
        return None
    _dca_count = int(sig.get("dca_count", 0) or 0)
    if _dca_count > 0:
        try:
            _avg = float(sig.get("avg_entry", 0) or 0)
            _tnot = float(sig.get("total_notional", 0) or 0)
        except (TypeError, ValueError):
            _avg = 0.0
            _tnot = 0.0
        if _avg > 0 and _tnot > 0:
            return -(_ref / _avg - 1.0) * _tnot
    try:
        _entry = float(sig.get("entry", 0) or 0)
        _usdt = float(sig.get("trade_usdt", usdt_fb) or 0)
        _lev = int(sig.get("trade_lev", lev_fb) or 0)
    except (TypeError, ValueError):
        return None
    if _entry <= 0 or _usdt <= 0 or _lev <= 0:
        return None
    return -(_ref / _entry - 1.0) * (_usdt * _lev)

_total_pnl = 0.0
_total_pnl_wins = 0.0
_total_pnl_loss = 0.0
_total_tp_ct = 0
_total_sl_ct = 0
_total_dca_sl_ct = 0
_cfg_usdt_fb = float(_snap_cfg.get("trade_usdt_amount", 0) or 0)
_cfg_lev_fb = int(_snap_cfg.get("trade_leverage", 10) or 0)
for _s in signals:
    if _s.get("status") not in ("tp_hit", "sl_hit", "dca_sl_hit", "fc_hit"):
        continue
    _v = _calc_pnl_usd_short(_s, _s.get("close_price"), _cfg_usdt_fb, _cfg_lev_fb)
    if _v is None:
        continue
    _total_pnl += _v
    if _s["status"] == "tp_hit":
        _total_pnl_wins += _v
        _total_tp_ct += 1
    elif _s["status"] == "dca_sl_hit":
        _total_pnl_loss += _v
        _total_dca_sl_ct += 1
    else:
        _total_pnl_loss += _v
        _total_sl_ct += 1

_closed_total = _total_tp_ct + _total_sl_ct + _total_dca_sl_ct
if _closed_total == 0:
    _total_color = "#9CA3AF"
    _total_prefix = "💼"
    _total_sub = "no closed trades yet"
else:
    _total_color = "#22C55E" if _total_pnl >= 0 else "#EF4444"
    _total_prefix = "💼" if _total_pnl >= 0 else "📉"
    _total_sub = f"{_closed_total} closed trades  ·  {_total_tp_ct} TP (+\\${_total_pnl_wins:,.2f})  |  {_total_sl_ct} SL  |  {_total_dca_sl_ct} DCA-SL (\\${_total_pnl_loss:,.2f})"

st.markdown(f"<div style='padding:10px 14px; margin-bottom:10px; border:1px solid {_total_color}33; border-radius:10px;'>"
            f"<span style='opacity:0.75;'>{_total_prefix} Total Realized PnL</span><br>"
            f"<span style='font-size:2.2em; font-weight:700; color:{_total_color};'>{_total_pnl:+,.2f} $</span>  "
            f"<span style='opacity:0.75;'>{_total_sub}</span></div>", unsafe_allow_html=True)

# Metrics
open_count = sum(1 for s in signals if s["status"] == "open")
tp_count = sum(1 for s in signals if s["status"] == "tp_hit")
sl_count = sum(1 for s in signals if s["status"] == "sl_hit")
dca_sl_count = sum(1 for s in signals if s["status"] == "dca_sl_hit")
fc_count = sum(1 for s in signals if s["status"] == "fc_hit")
queue_count = sum(1 for s in signals if s["status"] == "queue_limit")

col1, col2, col3, col4, col5, col6 = st.columns(6)
col1.metric("🔓 Open Shorts", open_count)
col2.metric("✅ TP Hit", tp_count)
col3.metric("❌ SL Hit", sl_count)
col4.metric("❌ DCA-SL", dca_sl_count)
col5.metric("🟣 FC Hit", fc_count)
col6.metric("⏳ Queued", queue_count)

last_scan = health.get("last_scan_at", "never")
if last_scan and last_scan != "never":
    try:
        ts = datetime.fromisoformat(last_scan.replace("Z", "+00:00"))
        ts_dubai = to_dubai(ts)
        ago = int((dubai_now() - ts_dubai).total_seconds() / 60)
        last_scan = f"{ago}m ago ({ts_dubai.strftime('%H:%M')} GST)"
    except Exception:
        pass
st.caption(f"Last scan: {last_scan} | Dubai / GST (UTC+4)")

# Helper functions for row building
def _fmt_px_auto(px) -> str:
    try:
        p = float(px or 0)
    except (TypeError, ValueError):
        return "—"
    if p <= 0:
        return "—"
    if p >= 1:
        return f"{p:.4f}"
    if p >= 0.01:
        return f"{p:.6f}"
    return f"{p:.8f}"

def _cv(v):
    if v is None or v == "—" or v == "✅":
        return "—"
    try:
        return f"{float(v):.6f}".rstrip("0").rstrip(".")
    except (TypeError, ValueError):
        return str(v)

def _fmt_trade_history_short(s: dict) -> str:
    fills = s.get("dca_fills") or []
    lines = []
    if fills:
        _last_idx = len(fills) - 1
        for i, f in enumerate(fills):
            idx = int(f.get("dca_idx", 0) or 0)
            label = "Entry" if idx == 0 else f"DCA {idx}"
            px_s = _fmt_px_auto(f.get("price", 0))
            ts_s = fmt_dubai(f.get("ts", "")) or "—"
            tp_v = f.get("tp")
            sl_v = f.get("sl")
            if i == _last_idx:
                if tp_v is None or _fmt_px_auto(tp_v) == "—":
                    tp_v = s.get("tp")
                if sl_v is None or _fmt_px_auto(sl_v) == "—":
                    sl_v = s.get("sl")
            _has_tp = tp_v is not None and _fmt_px_auto(tp_v) != "—"
            _has_sl = sl_v is not None and _fmt_px_auto(sl_v) != "—"
            if _has_tp and _has_sl:
                lines.append(f"{label:<6}: ${px_s} | TP ${_fmt_px_auto(tp_v)} | SL ${_fmt_px_auto(sl_v)} | {ts_s}")
            else:
                lines.append(f"{label:<6}: ${px_s} | {ts_s}")
        return "\n".join(lines)
    _ent = s.get("original_entry") or s.get("signal_entry") or s.get("entry")
    _tp = s.get("tp")
    _sl = s.get("sl")
    _ts = fmt_dubai(s.get("timestamp", "")) or "—"
    if _ent:
        _px_s = _fmt_px_auto(_ent)
        if _tp and _sl and _fmt_px_auto(_tp) != "—" and _fmt_px_auto(_sl) != "—":
            return f"Entry : ${_px_s} | TP ${_fmt_px_auto(_tp)} | SL ${_fmt_px_auto(_sl)} | {_ts}"
        return f"Entry : ${_px_s} | {_ts}"
    return "—"

def _build_signal_row_short(s: dict, is_open_table: bool = False, show_pnl: bool = False) -> dict:
    status = s.get("status", "open")
    status_icon = {"open": "🔵 Open", "tp_hit": "✅ TP Hit", "sl_hit": "❌ SL Hit",
                   "dca_sl_hit": "❌ DCA SL Hit", "fc_hit": "🟣 FC Hit",
                   "queue_limit": "⏳ Queue Limit", "closed_okx": "🟠 Closed on OKX"}.get(status, status)

    alert_col = ""
    _dca_count_row = int(s.get("dca_count", 0) or 0)
    if status == "open":
        if _dca_count_row > 0:
            alert_col = f"DCA-{_dca_count_row}"
        else:
            _dl = s.get("price_alert", False)
            _tl = False
            if s.get("timestamp"):
                try:
                    _t_open = datetime.fromisoformat(s["timestamp"].replace("Z", "+00:00"))
                    _tl = (dubai_now() - _t_open).total_seconds() >= 7200
                except Exception:
                    pass
            if _dl and _tl:
                alert_col = "🔴 DL / TL"
            elif _dl:
                alert_col = "🔴 DL"
            elif _tl:
                alert_col = "🔴 TL"
    elif status in ("tp_hit", "sl_hit", "dca_sl_hit", "fc_hit") and _dca_count_row > 0:
        alert_col = f"DCA-{_dca_count_row}"

    current_status_col = "—"
    if status == "open":
        _entry_rm = float(s.get("entry", 0) or 0)
        _pa_pct = s.get("price_alert_pct")
        if _pa_pct is not None and _entry_rm > 0:
            try:
                _change = -float(_pa_pct)
                current_status_col = f"{_change:+.2f}%"
            except (TypeError, ValueError):
                current_status_col = "—"

    ts_str = fmt_dubai(s.get("timestamp", ""))
    close_str = fmt_dubai(s["close_time"]) if s.get("close_time") else "—"
    crit = s.get("criteria", {})
    pdz_5m_val = crit.get("pdz_zone_5m", "—") or "—"
    pdz_15m_val = crit.get("pdz_zone_15m", "—") or "—"
    crit_str = (f"• RSI 5m    : {crit.get('rsi_5m','—')}\n• RSI 1h    : {crit.get('rsi_1h','—')}\n"
                f"• EMA 15m   : {_cv(crit.get('ema_15m','—'))}\n• MACD 15m  : {_cv(crit.get('macd_15m'))}\n"
                f"• SAR 15m   : {_cv(crit.get('sar_15m'))}\n• Vol ×avg  : {_cv(crit.get('vol_ratio'))}\n"
                f"• PDZ 5m    : {pdz_5m_val}\n• PDZ 15m   : {pdz_15m_val}\n"
                f"• EMA12 15m : {_cv(crit.get('ema_cross_12_15m'))}\n• EMA21 15m : {_cv(crit.get('ema_cross_21_15m'))}\n"
                f"• ATR ratio : {_cv(crit.get('atr_ratio'))}") if crit else "—"

    setup_type = "⭐ Super" if s.get("is_super_setup") else "Normal"

    _usdt = float(s.get("trade_usdt", _snap_cfg.get("trade_usdt_amount", 0)))
    _lev = int(s.get("trade_lev", _snap_cfg.get("trade_leverage", 10)))
    _entry_p = float(s.get("entry", 0) or 0)
    _tp_p = float(s.get("tp", 0) or 0)
    _sl_p = float(s.get("sl", 0) or 0)
    _dca_n_row = int(s.get("dca_count", 0) or 0)
    if _dca_n_row > 0:
        _avg_row = float(s.get("avg_entry", 0) or 0)
        _pos_row = float(s.get("total_notional", 0) or 0)
        if _avg_row > 0 and _pos_row > 0 and _tp_p > 0 and _sl_p > 0:
            tp_usd_str = f"+${_pos_row * (_avg_row - _tp_p) / _avg_row:.2f}"
            sl_usd_str = f"-${_pos_row * (_sl_p - _avg_row) / _avg_row:.2f}"
        else:
            tp_usd_str = sl_usd_str = "—"
    elif _usdt > 0 and _lev > 0 and _entry_p > 0:
        _pos = _usdt * _lev
        tp_usd_str = f"+${_pos * (_entry_p - _tp_p) / _entry_p:.2f}"
        sl_usd_str = f"-${_pos * (_sl_p - _entry_p) / _entry_p:.2f}"
    else:
        tp_usd_str = sl_usd_str = "—"

    ord_id_str = s.get("order_id", "") or "—"
    ord_env = "🟡 Demo" if s.get("demo_mode") else "🔴 Live"
    ord_status = s.get("order_status", "")
    ord_err = s.get("order_error", "")
    if ord_status == "placed":
        ord_status_str = f"✅ Entry+OCO {ord_env}"
    elif ord_status == "partial":
        ord_status_str = f"⚠️ Entry only {ord_env} · {ord_err[:80]}"
    elif ord_status == "error":
        ord_status_str = f"❌ {ord_err[:80]}" if ord_err else "❌ Error"
    else:
        ord_status_str = "—"

    _okx_log_list = s.get("okx_log")
    if isinstance(_okx_log_list, list) and _okx_log_list:
        okx_cmd_str = "\n########\n".join(_okx_log_list)
    else:
        okx_cmd_str = "—"

    dca_levels_col = "—"
    _dca_en_lv = bool(s.get("dca_enabled", False))
    _dca_max_lv = int(s.get("dca_max", 0) or 0)
    if _dca_en_lv and _dca_max_lv > 0:
        _ladder_lv = s.get("dca_ladder") or []
        _count_lv = int(s.get("dca_count", 0) or 0)
        if _ladder_lv:
            _fills_lv = s.get("dca_fills") or []
            _fill_px_map = {_fi: float(_ff.get("price", 0) or 0) for _fi, _ff in enumerate(_fills_lv) if _fi > 0 and float(_ff.get("price", 0) or 0) > 0}
            _lines = []
            for _item in _ladder_lv:
                _lvl = int(_item.get("level", 0))
                _planned_px = float(_item.get("trigger_px", 0) or 0)
                if _planned_px <= 0:
                    continue
                _filled = _lvl <= _count_lv
                _actual = _fill_px_map.get(_lvl, 0)
                if _filled and _actual > 0:
                    _lines.append(f"✅ DCA-{_lvl}: {_fmt_px_auto(_actual)} (plan {_fmt_px_auto(_planned_px)})")
                else:
                    _icon = "✅" if _filled else "⏳"
                    _lines.append(f"{_icon} DCA-{_lvl}: {_fmt_px_auto(_planned_px)}")
            dca_levels_col = "\n".join(_lines) if _lines else "—"

    next_dca_col = "—"
    if status == "open" and _dca_en_lv and _dca_max_lv > 0 and _dca_count_row < _dca_max_lv:
        try:
            _next_dca_px = float(s.get("next_dca_px", 0) or 0)
            if _next_dca_px <= 0:
                _next_dca_px = _dca_compute_trigger_short(s, _snap_cfg)
            if _next_dca_px and _next_dca_px > 0:
                next_dca_col = f"{_fmt_px_auto(_next_dca_px)} (DCA {_dca_count_row + 1}/{_dca_max_lv})"
        except Exception:
            next_dca_col = "—"

    trade_history_col = _fmt_trade_history_short(s)

    pnl_col = "—"
    if show_pnl and status in ("open", "tp_hit", "sl_hit", "dca_sl_hit", "fc_hit"):
        _ref_pnl = s.get("latest_price") if status == "open" else s.get("close_price")
        _pnl_val = _calc_pnl_usd_short(s, _ref_pnl, _cfg_usdt_fb, _cfg_lev_fb)
        if _pnl_val is not None:
            pnl_col = f"{_pnl_val:+.2f} $"

    est_liq_col = "—"
    if status == "open":
        _margin_mode_rm = (s.get("order_margin_mode") or _snap_cfg.get("trade_margin_mode", "isolated") or "isolated").lower()
        _entry_liq = float(s.get("avg_entry", s.get("entry", 0)) or 0)
        _lev_liq = int(s.get("trade_lev", _snap_cfg.get("trade_leverage", 10)) or 0)
        if _margin_mode_rm == "isolated" and _entry_liq > 0 and _lev_liq > 0:
            _liq_price = _entry_liq * (1.0 + 1.0 / _lev_liq)
            _liq_pct = 100.0 / _lev_liq
            est_liq_col = f"{_fmt_px_auto(_liq_price)} (+{_liq_pct:.2f}%)"

    _fc_px_raw = s.get("fc_trigger_px")
    fc_trig_col = "—"
    if _fc_px_raw:
        try:
            _fcp = float(_fc_px_raw)
            if _fcp > 0:
                fc_trig_col = _fmt_px_auto(_fcp)
                if status == "fc_hit":
                    _avg_fc_disp = float(s.get("avg_entry", 0) or 0)
                    if _avg_fc_disp > 0:
                        _fc_delta = (_avg_fc_disp - _fcp) / _avg_fc_disp * 100.0
                        fc_trig_col += f"  (-{_fc_delta:.3f}%)"
        except (TypeError, ValueError):
            fc_trig_col = "—"

    diff_col = "—"
    if status == "open":
        _atr_ratio_s = crit.get("atr_ratio")
        if _atr_ratio_s not in (None, "—"):
            try:
                _r = float(_atr_ratio_s)
                if _r <= 1.5:
                    diff_col = "🟢 Easy"
                elif _r <= 2.5:
                    diff_col = "🟡 Medium"
                else:
                    diff_col = "🔴 Hard"
            except (TypeError, ValueError):
                diff_col = "—"

    if is_open_table:
        row = {
            "Difficulty": diff_col, "Time (GST)": ts_str, "Symbol": s.get("symbol", ""),
            "Alert": alert_col, "Setup": setup_type, "PnL $": pnl_col,
            "Current Status": current_status_col, "Signal Entry": _fmt_px_auto(s.get("avg_entry", s.get("entry", ""))),
            "Original Entry": _fmt_px_auto(s.get("original_entry", s.get("entry", ""))),
            "Current Price": _fmt_px_auto(s.get("latest_price", "")), "DCA Levels": dca_levels_col,
            "Next DCA": next_dca_col, "FC Trigger Price": fc_trig_col, "TP": _fmt_px_auto(s.get("tp", "")),
            "SL": _fmt_px_auto(s.get("sl", "")), "Trade History": trade_history_col, "Est Liquidity": est_liq_col,
            "Duration": s.get("duration", "—"), "TP $": tp_usd_str, "SL $": sl_usd_str,
            "Status": status_icon, "Close Time": close_str, "Sector": s.get("sector", "Other"),
            "Close $": _fmt_px_auto(s.get("close_price", "")), "Max Lev": f"{s.get('max_lev', get_max_leverage(s.get('symbol', '')))}×",
            "Order": ord_status_str, "OKX Command": okx_cmd_str, "Entry Criteria": crit_str,
            "Order ID": ord_id_str, "⚠️ SL Reason": analyze_sl_reason_short(s) if status == "sl_hit" else "—",
        }
        return row

    row = {"Time (GST)": ts_str, "Symbol": s.get("symbol", ""), "Alert": alert_col}
    if show_pnl:
        row["PnL $"] = pnl_col
    row.update({
        "Setup": setup_type, "DCA Levels": dca_levels_col,
        "Signal Entry": _fmt_px_auto(s.get("avg_entry", s.get("entry", ""))),
        "Original Entry": _fmt_px_auto(s.get("original_entry", s.get("entry", ""))),
        "Fill $": _fmt_px_auto(s.get("entry", "")), "TP": _fmt_px_auto(s.get("tp", "")),
        "TP $": tp_usd_str, "Trade History": trade_history_col,
        "Status": status_icon, "Close Time": close_str, "Close $": _fmt_px_auto(s.get("close_price", "")),
        "Max Lev": f"{s.get('max_lev', get_max_leverage(s.get('symbol', '')))}×",
        "Order": ord_status_str, "OKX Command": okx_cmd_str, "Entry Criteria": crit_str,
    })
    if status == "fc_hit":
        row["FC Trigger Price"] = fc_trig_col
    else:
        row["SL"] = _fmt_px_auto(s.get("sl", ""))
        row["SL $"] = sl_usd_str
        row["⚠️ SL Reason"] = analyze_sl_reason_short(s) if status == "sl_hit" else "—"
    if show_pnl:
        row["Order Size"] = f"${float(s.get('total_notional', 0) or 0):,.2f}" if float(s.get('total_notional', 0) or 0) > 0 else "—"
    return row

_SIG_COL_CFG_SHORT = {
    "Difficulty": st.column_config.TextColumn("🎯 Difficulty", width="small"),
    "DCA Levels": st.column_config.TextColumn("📊 DCA Levels", width="medium"),
    "Alert": st.column_config.TextColumn("🚨 Alert", width="small"),
    "Current Status": st.column_config.TextColumn("📈 Current Status", width="small"),
    "Est Liquidity": st.column_config.TextColumn("💥 Est Liquidity (Short)", width="medium",
                       help="Estimated liquidation price for isolated short trades. Liq ≈ entry × (1 + 1/leverage)"),
    "PnL $": st.column_config.TextColumn("💰 PnL $", width="small"),
    "Signal Entry": st.column_config.TextColumn("Signal Entry", width="medium"),
    "Original Entry": st.column_config.TextColumn("Original Entry", width="medium"),
    "Current Price": st.column_config.TextColumn("Current Price", width="small"),
    "Next DCA": st.column_config.TextColumn("🪜 Next DCA", width="medium",
                       help="Price at which next DCA add triggers (price rises to this level)"),
    "FC Trigger Price": st.column_config.TextColumn("🟣 FC Trigger", width="medium",
                       help="Force-close price = avg_entry - $0.50 / total_coins"),
    "Trade History": st.column_config.TextColumn("📜 Trade History", width="large"),
    "TP $": st.column_config.TextColumn(width="small"), "SL $": st.column_config.TextColumn(width="small"),
    "Order": st.column_config.TextColumn(width="medium"), "OKX Command": st.column_config.TextColumn(width="large"),
    "Entry Criteria": st.column_config.TextColumn(width="medium"), "⚠️ SL Reason": st.column_config.TextColumn(width="medium"),
}

# ============================================================================
# TABLES
# ============================================================================
st.markdown(f"### 📉 Open Short Signals ({open_count})")
_open_sigs = [s for s in signals if s["status"] == "open"]
if _open_sigs:
    _open_rows = [_build_signal_row_short(s, is_open_table=True, show_pnl=True) for s in _open_sigs]
    _open_event = st.dataframe(_open_rows, use_container_width=True, hide_index=True, height=450,
                                column_config=_SIG_COL_CFG_SHORT, selection_mode="single-row", on_select="rerun", key="open_short_table")
    _sel_rows = (_open_event.selection.rows if _open_event and hasattr(_open_event, "selection") else [])
    if _sel_rows and _sel_rows[0] < len(_open_sigs):
        _fc_sig = _open_sigs[_sel_rows[0]]
        _fc_sym = _fc_sig.get("symbol", "?")
        _fc_entry = float(_fc_sig.get("avg_entry") or _fc_sig.get("entry") or 0)
        _fc_price = float(_fc_sig.get("latest_price") or _fc_entry or 0)
        _fc_upnl = ((_fc_entry - _fc_price) / _fc_entry * 100) if _fc_entry > 0 else 0
        _fc_mode = "📄 Paper" if not _fc_sig.get("order_id") else ("🟡 Demo" if _fc_sig.get("demo_mode") else "🔴 Live")
        _fc_color = "🟢" if _fc_upnl >= 0 else "🔴"
        _fca1, _fca2, _fca3 = st.columns([3, 2, 2])
        with _fca1:
            st.info(f"**{_fc_sym}** selected · {_fc_mode}")
        with _fca2:
            st.markdown(f"{_fc_color} uPnL: `{_fc_upnl:+.2f}%`")
        with _fca3:
            if st.button("⚡ Force Close (Buy to Cover)", type="primary", key="fc_execute_short", use_container_width=True):
                with st.spinner(f"Closing {_fc_sym}…"):
                    _fc_result = _force_close_position_short(_fc_sig, _snap_cfg)
                if _fc_result["success"]:
                    st.success(_fc_result["message"])
                    st.rerun()
                else:
                    st.error(_fc_result["message"])
    else:
        st.caption("👆 Click a row to select it, then use ⚡ Force Close.")
else:
    st.info("No open short signals right now.")

st.divider()
st.markdown(f"### ✅ TP Hit ({tp_count})")
_tp_sigs = [s for s in signals if s["status"] == "tp_hit"]
if _tp_sigs:
    st.dataframe([_build_signal_row_short(s, show_pnl=True) for s in _tp_sigs], use_container_width=True, hide_index=True, column_config=_SIG_COL_CFG_SHORT)
else:
    st.info("No TP hits yet.")

st.divider()
st.markdown(f"### ❌ SL Hit ({sl_count})")
_sl_sigs = [s for s in signals if s["status"] == "sl_hit"]
if _sl_sigs:
    st.dataframe([_build_signal_row_short(s, show_pnl=True) for s in _sl_sigs], use_container_width=True, hide_index=True, column_config=_SIG_COL_CFG_SHORT)
else:
    st.info("No SL hits yet.")

st.divider()
st.markdown(f"### ❌ DCA SL Hit (ladder exhausted) ({dca_sl_count})")
_dca_sl_sigs = [s for s in signals if s["status"] == "dca_sl_hit"]
if _dca_sl_sigs:
    st.dataframe([_build_signal_row_short(s, show_pnl=True) for s in _dca_sl_sigs], use_container_width=True, hide_index=True, column_config=_SIG_COL_CFG_SHORT)
else:
    st.info("No DCA SL hits yet.")

st.divider()
st.markdown(f"### 🟣 FC Hit (DCA breakeven close) ({fc_count})")
_fc_sigs = [s for s in signals if s["status"] == "fc_hit"]
if _fc_sigs:
    st.dataframe([_build_signal_row_short(s, show_pnl=True) for s in _fc_sigs], use_container_width=True, hide_index=True, column_config=_SIG_COL_CFG_SHORT)
else:
    st.info("No FC hits yet.")

st.divider()
if queue_count > 0:
    with st.expander(f"⏳ Queue Limit ({queue_count})", expanded=False):
        _queue_sigs = [s for s in signals if s["status"] == "queue_limit"]
        st.dataframe([_build_signal_row_short(s) for s in _queue_sigs], use_container_width=True, hide_index=True, column_config=_SIG_COL_CFG_SHORT)
        if st.button("🗑️ Clear Queue Limit Records", key="clear_queue_short"):
            with _log_lock:
                _b._ss_log["signals"] = [s for s in _b._ss_log["signals"] if s.get("status") != "queue_limit"]
                save_log(_b._ss_log)
            st.success("✅ Queue Limit records cleared.")
            st.rerun()

# ============================================================================
# OKX LIVE POSITIONS PANEL
# ============================================================================
with st.expander("📡 OKX Live Positions (Short)", expanded=False):
    _has_api_creds = bool(_snap_cfg.get("api_key") and _snap_cfg.get("api_secret") and _snap_cfg.get("api_passphrase"))
    if not _has_api_creds:
        st.warning("Enter API credentials in the sidebar to use this panel.")
    else:
        _env_label = "🟡 Demo" if _snap_cfg.get("demo_mode", True) else "🔴 Live"
        if st.button("🔄 Refresh from OKX", key="refresh_short_okx"):
            try:
                _pos_resp = _trade_get("/api/v5/account/positions", {"instType": "SWAP"}, _snap_cfg)
                st.session_state["okx_pos_short"] = _pos_resp
                st.session_state["okx_pos_short_ts"] = dubai_now().strftime("%d %b %Y %H:%M:%S GST")
                st.rerun()
            except Exception as _e:
                st.error(f"❌ Error: {_e}")
        _pos_data = st.session_state.get("okx_pos_short")
        if _pos_data is None:
            st.info("Press Refresh to load live data")
        elif _pos_data.get("code") != "0":
            st.error(f"OKX error: {_pos_data.get('msg')}")
        else:
            _positions = [p for p in _pos_data.get("data", []) if abs(float(p.get("pos", 0) or 0)) != 0]
            if _positions:
                _pos_rows = []
                for _p in _positions:
                    _pos_rows.append({
                        "Symbol": _p.get("instId", ""), "Side": "Short",
                        "Contracts": abs(int(float(_p.get("pos", 0) or 0))),
                        "Avg Entry": float(_p.get("avgPx", 0) or 0),
                        "Mark Price": float(_p.get("markPx", 0) or 0),
                        "Unreal PnL": round(float(_p.get("upl", 0) or 0), 4),
                        "PnL %": f"{float(_p.get('uplRatio', 0) or 0) * 100:+.2f}%",
                        "Leverage": f"{_p.get('lever', '')}×",
                        "Mode": _p.get("mgnMode", "").capitalize(),
                    })
                st.dataframe(_pos_rows, use_container_width=True, hide_index=True)
            else:
                st.info("No open short positions on OKX right now.")

# ============================================================================
# MARKET CONDITION ANALYSER
# ============================================================================
st.divider()
st.markdown("### 🎯 Market Condition Analyser (Short)")
st.caption("Scans all coins live and recommends optimal SHORT filter settings for current market conditions.")
_mkt_symbols = list(_snap_cfg.get("watchlist", []))
if st.button(f"🔍 Analyse Market Now ({len(_mkt_symbols)} coins)", type="primary", key="btn_market_short"):
    _mkt_prog = st.progress(0.0)
    _mkt_stat = st.empty()
    _mkt_result = _analyze_market_conditions_short(dict(_snap_cfg), _mkt_symbols, _mkt_prog, _mkt_stat)
    _mkt_prog.empty()
    _mkt_stat.empty()
    st.session_state["_mkt_short_result"] = _mkt_result
if "_mkt_short_result" in st.session_state:
    _mkt_res = st.session_state["_mkt_short_result"]
    _mkt_recs = _mkt_res.get("recommendations", {})
    _mkt_ok = _mkt_res.get("valid_coins", 0)
    _mkt_err = _mkt_res.get("errors", 0)
    st.success(f"✅ Analysis complete — {_mkt_ok} coins analysed" + (f" · {_mkt_err} skipped" if _mkt_err else ""))
    _MKT_ORDER = ["f2_pdz15m", "f3_pdz5m", "f4_rsi5m", "f5_rsi1h", "f5b_atr",
                  "f6_ema_3m", "f6_ema_5m", "f6_ema_15m", "f7_macd_3m", "f7_macd_5m", "f7_macd_15m",
                  "f8_sar_3m", "f8_sar_5m", "f8_sar_15m", "f9_vol", "f10_ema_cross"]
    _mkt_tbl = []
    for _mk in _MKT_ORDER:
        if _mk in _mkt_recs:
            _mr = _mkt_recs[_mk]
            _mkt_tbl.append({" ": _mr["icon"], "Filter": _mr["filter"], "Current Setting": _mr["current"],
                             "Current Pass Rate": f"{_mr['current_pass_rate']:.0f}%",
                             "Recommended": _mr["rec"], "Rec. Pass Rate": f"{_mr['rec_pass_rate']:.0f}%",
                             "Analysis": _mr["reason"]})
    st.dataframe(_mkt_tbl, use_container_width=True, hide_index=True)

# ============================================================================
# API ERROR LOG
# ============================================================================
st.divider()
with st.expander("⚠️ API Error Log", expanded=False):
    with getattr(_b, "_ss_error_log_lock", threading.Lock()):
        _err_entries = list(getattr(_b, "_ss_error_log", []))[-100:]
    if not _err_entries:
        st.success("✅ No errors recorded yet.")
    else:
        _err_rows = [{"Time": fmt_dubai(e["ts"]), "Type": e["type"], "Symbol": e.get("symbol", "—"),
                      "Endpoint": e.get("endpoint", "—"), "Error": e.get("message", "")[:200]}
                     for e in reversed(_err_entries)]
        st.dataframe(_err_rows, use_container_width=True, hide_index=True)
        if st.button("🗑 Clear error log", key="clear_err_short"):
            with _b._ss_error_log_lock:
                _b._ss_error_log.clear()
            st.rerun()

# ============================================================================
# FILTER FUNNEL
# ============================================================================
with _filter_lock:
    fc = {k: (list(v) if isinstance(v, list) else v) for k, v in _filter_counts.items()}
if fc.get("total_watchlist", 0) > 0 and fc.get("scan_completed_at", 0.0) >= fc.get("flushed_at", 0.0):
    with st.expander("🔬 Last scan filter funnel"):
        total = fc.get("total_watchlist", 0)
        pre_out_n = fc.get("pre_filtered_out", 0)
        after_pre = total - pre_out_n
        checked = fc.get("checked", after_pre)
        after_f2 = checked - fc.get("f2_pdz15m", 0)
        after_f3 = after_f2 - fc.get("f3_pdz5m", 0)
        after_f4 = after_f3 - fc.get("f4_rsi5m", 0)
        after_f5 = after_f4 - fc.get("f5_rsi1h", 0)
        after_f5b = after_f5 - fc.get("f5b_atr", 0)
        after_f6_ema_3m = after_f5b - fc.get("f6_ema_3m", 0)
        after_f6_ema_5m = after_f6_ema_3m - fc.get("f6_ema_5m", 0)
        after_f6_ema_15m = after_f6_ema_5m - fc.get("f6_ema_15m", 0)
        after_f6 = after_f6_ema_15m
        after_f7_macd_3m = after_f6 - fc.get("f7_macd_3m", 0)
        after_f7_macd_5m = after_f7_macd_3m - fc.get("f7_macd_5m", 0)
        after_f7_macd_15m = after_f7_macd_5m - fc.get("f7_macd_15m", 0)
        after_f8_sar_3m = after_f7_macd_15m - fc.get("f8_sar_3m", 0)
        after_f8_sar_5m = after_f8_sar_3m - fc.get("f8_sar_5m", 0)
        after_f8_sar_15m = after_f8_sar_5m - fc.get("f8_sar_15m", 0)
        after_f9 = after_f8_sar_15m - fc.get("f9_vol", 0)
        after_f10 = after_f9 - fc.get("f10_ema_cross", 0)
        after_empty = after_f10 - fc.get("f_empty_data", 0)

        sc = fc.get("scan_cfg") or _snap_cfg

        col_a, col_b, col_c = st.columns(3)
        col_a.metric("Pre-filtered out ⚡", pre_out_n)
        col_b.metric("Deep scanned 🔬", checked)
        col_c.metric("Errors", fc.get("errors", 0))
        super_n = fc.get("super_setup", 0)
        if super_n:
            st.success(f"⭐ {super_n} Super Setup(s) this cycle — 15m+1h Premium instant short!")

st.divider()
st.caption("📉 Short Scanner — sells high (Premium zones, overbought), covers low. Full DCA ladder, FC-B mechanism, two-tier reconciliation, and circuit breaker.")
