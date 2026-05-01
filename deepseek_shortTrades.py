#!/usr/bin/env python3
"""
OKX Futures Scanner — Streamlit Dashboard  v3 (SHORT-ONLY)
============================================================
SHORT-ONLY version - looks for bearish signals only.

Key differences from Long version:
  • Entry signal: Bearish flip (+1 → -1) on ≥2 of F2/F3/F4 indicators
  • TP: Below entry (e.g., -1.5%)
  • SL: Above entry (e.g., +3%)
  • Order side: SELL to open, BUY to close
  • Trend exit: Close when ANY indicator flips bullish (-1 → +1)
  • PnL formula: (1 - close/entry) × notional

All other features (pre-filter, queue system, circuit breaker, etc.) unchanged.
"""

import base64, hashlib, hmac, json, math, os, pathlib, threading, time, uuid, traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

import requests
import streamlit as st
import plotly.graph_objects as go
import plotly.express as px

# ─────────────────────────────────────────────────────────────────────────────
# Network constants
# ─────────────────────────────────────────────────────────────────────────────
BASE          = "https://www.okx.com"
OKX_INTERVALS = {"30m": "30m", "3m": "3m", "5m": "5m", "15m": "15m", "1h": "1H"}

# ─────────────────────────────────────────────────────────────────────────────
# API rate-limiter — hard cap: 20 concurrent HTTP requests at any time
# OKX public-market limit is 20 req / 2 s. Staying well below keeps us safe.
# ─────────────────────────────────────────────────────────────────────────────
_api_sem = threading.Semaphore(20)

_RATE_INTERVAL  = 1.0 / 15          # 66.7 ms minimum between token grants
_rate_lock       = threading.Lock()
_rate_last_grant = 0.0

def _rate_wait():
    """Block the calling thread until its turn within the 15 req/s budget."""
    global _rate_last_grant
    while True:
        with _rate_lock:
            now  = time.time()
            wait = _rate_last_grant + _RATE_INTERVAL - now
            if wait <= 0:
                _rate_last_grant = now
                return
        time.sleep(max(0.001, wait))

# ─────────────────────────────────────────────────────────────────────────────
# Symbol-cache TTL
# ─────────────────────────────────────────────────────────────────────────────
_SYMBOL_CACHE_TTL = 6 * 3600

# ─────────────────────────────────────────────────────────────────────────────
# Bulk pre-filter thresholds
# ─────────────────────────────────────────────────────────────────────────────
PRE_FILTER_MIN_VOL_USDT  =  100_000
PRE_FILTER_LOW_BUFFER    =    1.005
PRE_FILTER_HIGH_BUFFER   =    0.995   # SHORT: price must be ≤ 24h high × 0.995

# ─────────────────────────────────────────────────────────────────────────────
# Dubai Timezone (UTC+4, no DST)
# ─────────────────────────────────────────────────────────────────────────────
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

# ─────────────────────────────────────────────────────────────────────────────
# Default configuration (SHORT-ONLY)
# ─────────────────────────────────────────────────────────────────────────────
DEFAULT_CONFIG: dict = {
    "tp_pct":               1.5,        # SHORT: TP is BELOW entry (e.g., entry -1.5%)
    "sl_pct":               3.0,        # SHORT: SL is ABOVE entry (e.g., entry +3%)
    "use_pre_filter":       True,
    "max_open_trades":       7,
    "sl_cooldown_hours":     4,
    "trade_enabled":         False,
    "demo_mode":             True,
    "api_key":               "",
    "api_secret":            "",
    "api_passphrase":        "",
    "trade_usdt_amount":     5.0,
    "trade_leverage":        20,
    "trade_margin_mode":     "cross",
    "use_trend_exit":        True,
    "f2_supertrend":         True,
    "f3_chandelier":         True,
    "f4_lux":                True,
    "loop_minutes":          4,
    "cooldown_minutes":      2,
    "scan_hour_enabled":     False,
    "scan_hour_start":       0,
    "scan_hour_end":         23,
    "watchlist": [
        "XPDUSDT","WIFUSDT","PIUSDT","EDGEUSDT","RECALLUSDT","SUSHIUSDT","RAVEUSDT","XLMUSDT","DASHUSDT","TRUSTUSDT",
        "GPSUSDT","CROUSDT","ACUUSDT","UNIUSDT","STRKUSDT","NEIROUSDT","ZKPUSDT","APEUSDT","MSTRUSDT","ENJUSDT",
        "INJUSDT","RAYUSDT","OLUSDT","HUSDT","OKBUSDT","APTUSDT","WCTUSDT","NEOUSDT","SNXUSDT","LITUSDT",
        "WUSDT","SYRUPUSDT","AVAXUSDT","LPTUSDT","ACTUSDT","FLOKIUSDT","MSFTUSDT","MEMEUSDT","DOGEUSDT","KGENUSDT",
        "MOODENGUSDT","NOTUSDT","XCUUSDT","AEROUSDT","STXUSDT","GIGGLEUSDT","AUCTIONUSDT","WALUSDT","ETHUSDT","SHIBUSDT",
        "ZROUSDT","GMXUSDT","LAYERUSDT","ARBUSDT","MINAUSDT","IMXUSDT","LINEAUSDT","PUMPUSDT","VANAUSDT","FOGOUSDT",
        "BASEDUSDT","ZBTUSDT","KITEUSDT","XTZUSDT","SUIUSDT","ATHUSDT","AIXBTUSDT","TRIAUSDT","PIPPINUSDT","ANIMEUSDT",
        "PEPEUSDT","LRCUSDT","DYDXUSDT","LAUSDT","GLMUSDT","CHZUSDT","ACHUSDT","INITUSDT","PLUMEUSDT","BCHUSDT",
        "BLURUSDT","SENTUSDT","ALLOUSDT","XPTUSDT","QQQUSDT","YGGUSDT","AAVEUSDT","METISUSDT","ZAMAUSDT","ZKUSDT",
        "MERLUSDT","EGLDUSDT","AVNTUSDT","HMSTRUSDT","AGLDUSDT","ONTUSDT","ALGOUSDT","ADAUSDT","TRUMPUSDT","MEUSDT",
        "NFLXUSDT","GALAUSDT","BONKUSDT","LUNAUSDT","XAGUSDT","TRXUSDT","BEATUSDT","BABYUSDT","EDENUSDT","PNUTUSDT",
        "BICOUSDT","IWMUSDT","ICPUSDT","METAUSDT","BANDUSDT","LDOUSDT","OFCUSDT","SATSUSDT","ZRXUSDT","ZECUSDT",
        "MORPHOUSDT","QTUMUSDT","SPACEUSDT","SIGNUSDT","AMZNUSDT","TRUTHUSDT","YFIUSDT","1INCHUSDT","BRETTUSDT","SOLUSDT",
        "RIVERUSDT","FARTCOINUSDT","API3USDT","DOTUSDT","HOODUSDT","JELLYJELLYUSDT","STABLEUSDT","FUSDT","DOODUSDT","COAIUSDT",
        "WLFIUSDT","USDCUSDT","IPUSDT","ATUSDT","WLDUSDT","LQTYUSDT","IOTAUSDT","TRBUSDT","RVNUSDT","ORCLUSDT",
        "KSMUSDT","CFXUSDT","SOPHUSDT","BARDUSDT","UMAUSDT","ZENUSDT","2ZUSDT","YBUSDT","CRCLUSDT","RENDERUSDT",
        "JUPUSDT","MAGICUSDT","TURBOUSDT","ORDIUSDT","PYTHUSDT","ETCUSDT","MEWUSDT","CRVUSDT","MUBARAKUSDT","BIGTIMEUSDT",
        "ORDERUSDT","VIRTUALUSDT","INTCUSDT","THETAUSDT","ONDOUSDT","LTCUSDT","SPKUSDT","AUSDT","ROBOUSDT","EWJUSDT",
        "ASTERUSDT","BREVUSDT","IOSTUSDT","BTCUSDT","EWYUSDT","BNBUSDT","SAHARAUSDT","MONUSDT","AAPLUSDT","RSRUSDT",
        "SPYUSDT","KAITOUSDT","LINKUSDT","NMRUSDT","CRWVUSDT","TSLAUSDT","XPLUSDT","COMPUSDT","ENAUSDT","CCUSDT",
        "USELESSUSDT","PLTRUSDT","RLSUSDT","HOMEUSDT","GRTUSDT","LIGHTUSDT","KATUSDT","LABUSDT","JTOUSDT","FILUSDT",
        "TIAUSDT","MUUSDT","SKYUSDT","ENSUSDT","NVDAUSDT","BOMEUSDT","PIEVERSEUSDT","0GUSDT","GASUSDT","SEIUSDT",
        "OPUSDT","AMDUSDT","BIOUSDT","COREUSDT","MOVEUSDT","NGUSDT","GRASSUSDT","KMNOUSDT","SAPIENUSDT","OPNUSDT",
        "TONUSDT","ATOMUSDT","ETHWUSDT","ONEUSDT","COINUSDT","ESPUSDT","XAUUSDT","NIGHTUSDT","BSBUSDT","PENGUUSDT",
        "ETHFIUSDT","SSVUSDT","CVXUSDT","RESOLVUSDT","UPUSDT","METUSDT","SANDUSDT","CELOUSDT","SNDKUSDT","MANAUSDT",
        "POPCATUSDT","TAOUSDT","ARUSDT","FLOWUSDT","SUSDT","AZTECUSDT","ARKMUSDT","WETUSDT","HUMAUSDT","APRUSDT",
        "AEVOUSDT","CLUSDT","BATUSDT","ZORAUSDT","BERAUSDT","TSMUSDT","HYPEUSDT","WOOUSDT","PEOPLEUSDT","PENDLEUSDT",
        "SOONUSDT","MMTUSDT","EIGENUSDT","POLUSDT","PROVEUSDT","GMTUSDT","ZILUSDT","PARTIUSDT","MASKUSDT","ENSOUSDT",
        "BZUSDT","NEARUSDT","SHELLUSDT","ZETAUSDT","GOOGLUSDT","XRPUSDT","HBARUSDT","ICXUSDT","SPXUSDT","AXSUSDT",
    ],
}

# ─────────────────────────────────────────────────────────────────────────────
# Sector tags
# ─────────────────────────────────────────────────────────────────────────────
SECTORS: dict = {
    "FETUSDT":"AI","RENDERUSDT":"AI","AIXBTUSDT":"AI","GRTUSDT":"AI",
    "AGLDUSDT":"AI","AIAUSDT":"AI","AINUSDT":"AI","UAIUSDT":"AI",
    "ARKMUSDT":"AI","VIRTUALUSDT":"AI","SKYAIUSDT":"AI","DEEPUSDT":"AI",
    "ZECUSDT":"Privacy","DASHUSDT":"Privacy","XMRUSDT":"Privacy",
    "DUSKUSDT":"Privacy","POLYXUSDT":"Privacy",
    "BTCUSDT":"BTC","ORDIUSDT":"BTC",
    "ETHUSDT":"L1","SOLUSDT":"L1","AVAXUSDT":"L1","ADAUSDT":"L1",
    "DOTUSDT":"L1","NEARUSDT":"L1","APTUSDT":"L1","SUIUSDT":"L1",
    "TONUSDT":"L1","XLMUSDT":"L1","TRXUSDT":"L1","LTCUSDT":"L1",
    "BCHUSDT":"L1","XRPUSDT":"L1","BNBUSDT":"L1","ATOMUSDT":"L1",
    "ARBUSDT":"L2","OPUSDT":"L2","STRKUSDT":"L2","ZKUSDT":"L2",
    "LINEAUSDT":"L2","POLUSDT":"L2","IMXUSDT":"L2",
    "AAVEUSDT":"DeFi","UNIUSDT":"DeFi","CRVUSDT":"DeFi","COMPUSDT":"DeFi",
    "SNXUSDT":"DeFi","DYDXUSDT":"DeFi","PENDLEUSDT":"DeFi","AEROUSDT":"DeFi",
    "MORPHOUSDT":"DeFi","1INCHUSDT":"DeFi","CAKEUSDT":"DeFi","LDOUSDT":"DeFi",
    "DOGEUSDT":"Meme","1000PEPEUSDT":"Meme","1000SHIBUSDT":"Meme",
    "1000BONKUSDT":"Meme","1000FLOKIUSDT":"Meme","FARTCOINUSDT":"Meme",
    "MEMEUSDT":"Meme","BOMEUSDT":"Meme","TURBOUSDT":"Meme","NEIROUSDT":"Meme",
    "SANDUSDT":"Gaming","MANAUSDT":"Gaming","GALAUSDT":"Gaming",
    "AXSUSDT":"Gaming","ALICEUSDT":"Gaming","APEUSDT":"Gaming",
}

# ─────────────────────────────────────────────────────────────────────────────
# Max leverage tiers (same as original)
# ─────────────────────────────────────────────────────────────────────────────
MAX_LEVERAGE: dict = {
    "BTCUSDT":125,"ETHUSDT":100,
    "SOLUSDT":75,"BNBUSDT":75,"XRPUSDT":75,"DOGEUSDT":75,
    "ADAUSDT":75,"LTCUSDT":75,"BCHUSDT":75,"TRXUSDT":75,
    "XLMUSDT":75,"DOTUSDT":75,"AVAXUSDT":75,"LINKUSDT":75,
    "UNIUSDT":75,"ATOMUSDT":75,"ETCUSDT":75,
    "NEARUSDT":50,"APTUSDT":50,"SUIUSDT":50,"ARBUSDT":50,
    "OPUSDT":50,"INJUSDT":50,"TONUSDT":50,"AAVEUSDT":50,
    "LDOUSDT":50,"FILUSDT":50,"IMXUSDT":50,"STXUSDT":50,
    "ORDIUSDT":50,"WLDUSDT":50,"JUPUSDT":50,"PENDLEUSDT":50,
    "CRVUSDT":50,"TIAUSDT":50,"SEIUSDT":50,"TAOUSDT":50,
    "RENDERUSDT":50,"FETUSDT":50,"HBARUSDT":50,"MANTRAUSDT":50,
    "SANDUSDT":50,"GALAUSDT":50,"AXSUSDT":50,"APEUSDT":50,
    "GRTUSDT":50,"ENAUSDT":50,"POLUSDT":50,"STRKUSDT":50,
    "ZKUSDT":50,"DYDXUSDT":50,"SNXUSDT":50,"COMPUSDT":50,
    "ARUSDT":50,"KASUSDT":50,"VETUSDT":50,"ICPUSDT":50,
}

def get_max_leverage(sym: str) -> int:
    return MAX_LEVERAGE.get(sym, 20)

# ─────────────────────────────────────────────────────────────────────────────
# SL reason analyzer
# ─────────────────────────────────────────────────────────────────────────────
def analyze_sl_reason(sig: dict) -> str:
    """Post-mortem analysis of a stopped-out trade (SHORT version)."""
    criteria = sig.get("criteria", {})
    reasons  = []
    improve  = []

    def _f(key, default=None):
        try:
            v = criteria.get(key, default)
            return float(v) if v not in (None, "—", "") else default
        except (TypeError, ValueError):
            return default

    duration_mins = None
    try:
        if sig.get("timestamp") and sig.get("close_time"):
            t_open  = datetime.fromisoformat(sig["timestamp"].replace("Z", "+00:00"))
            t_close = datetime.fromisoformat(sig["close_time"].replace("Z", "+00:00"))
            duration_mins = int((t_close - t_open).total_seconds() / 60)
    except Exception:
        pass

    if duration_mins is not None:
        if duration_mins <= 5:
            reasons.append(
                f"SL hit in {duration_mins} min — short entry was at a local price trough. "
                f"Consider adding a 1–2 candle confirmation delay before entry.")
            improve.append("Add 1-candle confirmation delay (wait for close below entry signal)")
        elif duration_mins <= 15:
            reasons.append(
                f"SL hit in {duration_mins} min — very fast reversal upward. "
                f"Momentum faded immediately after short entry.")
            improve.append("Tighten entry timing — require EMA alignment on 3m before entry")
        elif duration_mins >= 120:
            reasons.append(
                f"Trade held {duration_mins} min before SL — sustained bullish reversal. "
                f"Macro / higher-TF bias likely shifted upward after entry.")
            improve.append("Add 4h or daily trend filter to avoid counter-trend short entries")

    if not reasons:
        reasons.append(
            "Reversal was caused by an external market event "
            "(macro news, short squeeze, or broader market pump).")
        improve.append(
            "Consider adding a market-wide sentiment check (e.g. BTC dominance "
            "or funding rate) to avoid entries during high-volatility macro windows")

    out_lines = [f"• {r}" for r in reasons]
    if improve:
        out_lines.append("")
        out_lines.append("💡 Improvement suggestions:")
        out_lines.extend(f"  → {i}" for i in improve)

    return "\n".join(out_lines)

# ─────────────────────────────────────────────────────────────────────────────
# Config persistence (unchanged)
# ─────────────────────────────────────────────────────────────────────────────
def _resolve_data_dir() -> pathlib.Path:
    candidates = []
    try:
        candidates.append(pathlib.Path(__file__).parent.absolute())
    except Exception:
        pass
    candidates.append(pathlib.Path.home() / "Documents" / "CryptoDemoTrades")

    for path in candidates:
        try:
            path.mkdir(parents=True, exist_ok=True)
            probe = path / ".write_probe"
            probe.touch(); probe.unlink()
            return path
        except OSError:
            continue

    import tempfile
    fallback = pathlib.Path(tempfile.gettempdir()) / "CryptoDemoTrades"
    fallback.mkdir(parents=True, exist_ok=True)
    return fallback

_SCRIPT_DIR  = _resolve_data_dir()
CONFIG_FILE  = _SCRIPT_DIR / "scanner_config.json"
LOG_FILE     = _SCRIPT_DIR / "scanner_log.json"
_config_lock = threading.Lock()

def load_config() -> dict:
    cfg = dict(DEFAULT_CONFIG)
    if CONFIG_FILE.exists():
        try:
            saved = json.loads(CONFIG_FILE.read_text(encoding="utf-8"))
            for k in DEFAULT_CONFIG:
                if k in saved:
                    cfg[k] = saved[k]
        except (json.JSONDecodeError, OSError, UnicodeDecodeError) as _e:
            pass
    import os as _os
    for _cfg_k, _env_k in (("api_key",        "OKX_API_KEY"),
                           ("api_secret",     "OKX_API_SECRET"),
                           ("api_passphrase", "OKX_API_PASSPHRASE")):
        _v = _os.environ.get(_env_k, "").strip()
        if _v:
            cfg[_cfg_k] = _v
    return cfg

def save_config(cfg: dict):
    import os as _os
    _cfg_to_save = dict(cfg)
    for _cfg_k, _env_k in (("api_key",        "OKX_API_KEY"),
                           ("api_secret",     "OKX_API_SECRET"),
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
    except (OSError, TypeError, ValueError) as _e:
        pass

def load_log():
    if LOG_FILE.exists():
        try:
            return json.loads(LOG_FILE.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError, UnicodeDecodeError):
            pass
    return {"health": {"total_cycles": 0, "last_scan_at": None,
                        "last_scan_duration_s": 0.0, "total_api_errors": 0,
                        "watchlist_size": 0, "pre_filtered_out": 0,
                        "deep_scanned": 0},
            "signals": []}

def save_log(log):
    try:
        LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
        LOG_FILE.write_text(json.dumps(log, indent=2), encoding="utf-8")
    except (OSError, TypeError, ValueError):
        pass

# ─────────────────────────────────────────────────────────────────────────────
# Module-level shared state
# ─────────────────────────────────────────────────────────────────────────────
if "_scanner_initialised" not in st.session_state:
    import builtins
    if not getattr(builtins, "_binance_scanner_globals_set", False):
        import builtins as _b
        _b._binance_scanner_globals_set = True
        _b._bsc_cfg           = load_config()
        _b._bsc_log           = load_log()
        _b._bsc_log_lock      = threading.Lock()
        _b._bsc_running       = threading.Event()
        _b._bsc_running.set()
        _b._bsc_thread        = None
        _b._bsc_filter_counts = {}
        _b._bsc_filter_lock   = threading.Lock()
        _b._bsc_last_error    = ""
        _b._bsc_rescan_event  = threading.Event()
        _b._bsc_sl_paused     = False
        _b._bsc_api_conn_status = {
            "status":      "untested",
            "message":     "",
            "tested_at":   None,
            "demo_mode":   None,
            "uid":         "",
            "pos_mode":    "net_mode",
            "acct_lv":     "2",
        }
        _b._bsc_last_trade_raw  = {}
        _b._bsc_error_log       = []
        _b._bsc_error_log_lock  = threading.Lock()
        _b._bsc_symbol_cache  = {"symbols": [], "fetched_at": 0, "wl_key": "", "ct_val": {}}
    st.session_state["_scanner_initialised"] = True

import builtins as _b

_cfg             = _b._bsc_cfg
_log             = _b._bsc_log
_log_lock        = _b._bsc_log_lock
_scanner_running = _b._bsc_running
_filter_lock     = _b._bsc_filter_lock
_filter_counts   = _b._bsc_filter_counts
_rescan_event    = _b._bsc_rescan_event

# ─────────────────────────────────────────────────────────────────────────────
# HTTP helpers
# ─────────────────────────────────────────────────────────────────────────────
HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/124.0.0.0 Safari/537.36"),
    "Accept":          "application/json",
    "Accept-Language": "en-US,en;q=0.9",
}
_local = threading.local()

def get_session():
    if not hasattr(_local, "session"):
        s = requests.Session()
        s.headers.update(HEADERS)
        from requests.adapters import HTTPAdapter
        adapter = HTTPAdapter(pool_connections=4, pool_maxsize=25, max_retries=0)
        s.mount("https://", adapter)
        s.mount("http://",  adapter)
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
                time.sleep(wait); continue
            if r.status_code in (418, 403, 451):
                raise RuntimeError(
                    f"HTTP {r.status_code}: OKX is blocking this server's IP.")
            r.raise_for_status()
            data = r.json()
            if isinstance(data, dict) and "code" in data and data["code"] != "0":
                raise RuntimeError(f"OKX API error {data['code']}: {data.get('msg','')}")
            return data
        except requests.exceptions.ConnectionError:
            if attempt < _retries - 1: time.sleep(1); continue
            raise
    raise RuntimeError(f"Failed after {_retries} retries: {url}")

def _append_error(err_type: str, message: str,
                  symbol: str = "", endpoint: str = "") -> None:
    entry = {
        "ts":       dubai_now().isoformat(),
        "type":     err_type,
        "symbol":   symbol,
        "endpoint": endpoint,
        "message":  message[:400],
    }
    try:
        lock = getattr(_b, "_bsc_error_log_lock", None)
        log  = getattr(_b, "_bsc_error_log", None)
        if lock is None or log is None:
            return
        with lock:
            log.append(entry)
            if len(log) > 500:
                del log[:len(log) - 500]
    except Exception:
        pass

# ─────────────────────────────────────────────────────────────────────────────
# OKX symbol helpers
# ─────────────────────────────────────────────────────────────────────────────
def _to_okx(sym: str) -> str:
    return f"{sym[:-4]}-USDT-SWAP" if sym.endswith("USDT") else sym

def _from_okx(inst_id: str) -> str:
    return inst_id.replace("-USDT-SWAP", "USDT") if inst_id.endswith("-USDT-SWAP") else inst_id

def get_symbols(watchlist: list) -> tuple:
    active  = set()
    ct_vals = {}
    data    = safe_get(f"{BASE}/api/v5/public/instruments", {"instType": "SWAP"})
    for s in data.get("data", []):
        inst_id = s.get("instId", "")
        if inst_id.endswith("-USDT-SWAP") and s.get("state") == "live":
            sym = _from_okx(inst_id)
            active.add(sym)
            try:
                _cv   = float(s.get("ctVal")  or 0)
                ct_vals[sym] = _cv if _cv > 0 else 0.0
                _b._bsc_symbol_cache.setdefault("ct_raw", {})[sym] = (_cv, 1.0, _cv)
            except (TypeError, ValueError):
                ct_vals[sym] = 0.0
    skipped = [s for s in watchlist if s not in active]
    if skipped:
        print(f"  [Symbol cache] {len(skipped)} watchlist coins not on OKX: {skipped[:5]}...")
    return [s for s in watchlist if s in active], ct_vals

def get_symbols_cached(watchlist: list) -> list:
    now    = time.time()
    cache  = _b._bsc_symbol_cache
    wl_key = ",".join(sorted(watchlist))
    if (not cache["symbols"] or
            now - cache["fetched_at"] > _SYMBOL_CACHE_TTL or
            cache["wl_key"] != wl_key):
        print("[Scanner] Refreshing OKX symbol cache…")
        syms, ct_vals       = get_symbols(watchlist)
        cache["symbols"]    = syms
        cache["ct_val"]     = ct_vals
        cache["fetched_at"] = now
        cache["wl_key"]     = wl_key
    return list(cache["symbols"])

# ─────────────────────────────────────────────────────────────────────────────
# OKX Auto-Trading — SHORT version
# ─────────────────────────────────────────────────────────────────────────────

def _okx_sign(timestamp: str, method: str, request_path: str, body: str, secret: str) -> str:
    msg = timestamp + method + request_path + body
    return base64.b64encode(
        hmac.new(secret.encode("utf-8"), msg.encode("utf-8"), hashlib.sha256).digest()
    ).decode()

def _trade_post(path: str, body: dict, cfg: dict) -> dict:
    ts       = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    body_str = json.dumps(body)
    sign     = _okx_sign(ts, "POST", path, body_str, cfg["api_secret"])
    headers  = {
        "OK-ACCESS-KEY":        cfg["api_key"],
        "OK-ACCESS-SIGN":       sign,
        "OK-ACCESS-TIMESTAMP":  ts,
        "OK-ACCESS-PASSPHRASE": cfg["api_passphrase"],
        "Content-Type":         "application/json",
    }
    if cfg.get("demo_mode", True):
        headers["x-simulated-trading"] = "1"
    sess = get_session()
    merged = {**dict(sess.headers), **headers}
    with _api_sem:
        r = sess.post(f"{BASE}{path}", headers=merged,
                      data=body_str, timeout=20)
    r.raise_for_status()
    try:
        return r.json()
    except (json.JSONDecodeError, ValueError) as _je:
        _snippet = r.text[:200].replace("\n", " ") if r.text else ""
        raise RuntimeError(
            f"OKX POST {path} returned non-JSON response "
            f"(status={r.status_code}): {_snippet!r}"
        ) from _je

def _trade_get(path: str, params: dict, cfg: dict) -> dict:
    qs  = ("?" + "&".join(f"{k}={v}" for k, v in params.items())) if params else ""
    ts  = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    sign = _okx_sign(ts, "GET", path + qs, "", cfg["api_secret"])
    headers = {
        "OK-ACCESS-KEY":        cfg["api_key"],
        "OK-ACCESS-SIGN":       sign,
        "OK-ACCESS-TIMESTAMP":  ts,
        "OK-ACCESS-PASSPHRASE": cfg["api_passphrase"],
        "Content-Type":         "application/json",
    }
    if cfg.get("demo_mode", True):
        headers["x-simulated-trading"] = "1"
    sess = get_session()
    merged = {**dict(sess.headers), **headers}
    with _api_sem:
        r = sess.get(f"{BASE}{path}", params=params,
                     headers=merged, timeout=20)
    r.raise_for_status()
    try:
        return r.json()
    except (json.JSONDecodeError, ValueError) as _je:
        _snippet = r.text[:200].replace("\n", " ") if r.text else ""
        raise RuntimeError(
            f"OKX GET {path} returned non-JSON response "
            f"(status={r.status_code}): {_snippet!r}"
        ) from _je

def test_api_connection(cfg: dict) -> dict:
    if not cfg.get("api_key") or not cfg.get("api_secret") or not cfg.get("api_passphrase"):
        return {"status": "error", "message": "API credentials are incomplete.",
                "uid": "", "pos_mode": "net_mode"}
    try:
        resp = _trade_get("/api/v5/account/balance", {}, cfg)
        if resp.get("code") != "0":
            msg = f"OKX error {resp.get('code')}: {resp.get('msg', 'unknown')}"
            return {"status": "error", "message": msg, "uid": "", "pos_mode": "net_mode"}

        details  = resp.get("data", [{}])[0]
        total_eq = details.get("totalEq", "0")

        cfg_resp = _trade_get("/api/v5/account/config", {}, cfg)
        uid      = ""
        pos_mode = "net_mode"
        acct_lv  = "2"
        if cfg_resp.get("code") == "0":
            acct     = cfg_resp.get("data", [{}])[0]
            uid      = acct.get("uid", "")
            pos_mode = acct.get("posMode", "net_mode")
            acct_lv  = str(acct.get("acctLv", "2"))

        _ACCT_MODE_LABELS = {
            "1": "Simple (⚠️ SWAP disabled)",
            "2": "Single-currency margin",
            "3": "Multi-currency margin",
            "4": "Portfolio margin",
        }
        acct_lv_label = _ACCT_MODE_LABELS.get(acct_lv, f"Level {acct_lv}")

        env      = "Demo" if cfg.get("demo_mode", True) else "Live"
        pm_label = "Hedge (Long/Short)" if pos_mode == "long_short_mode" else "Net"
        try:
            eq_str = f"{float(total_eq):,.2f}"
        except (ValueError, TypeError):
            eq_str = total_eq

        if acct_lv == "1":
            msg = (
                f"Connected ({env}) but account is in Simple mode — "
                "SWAP trading is disabled.\n\n"
                "Fix: OKX Demo → avatar (top-right) → Account mode → "
                "switch to 'Single-currency margin' or higher."
            )
            return {"status": "error", "message": msg, "uid": uid,
                    "pos_mode": pos_mode, "acct_lv": acct_lv}

        msg = (
            f"Connected ({env}) · {acct_lv_label} · "
            f"Equity: {eq_str} USDT · Pos mode: {pm_label}"
        )
        return {"status": "ok", "message": msg, "uid": uid,
                "pos_mode": pos_mode, "acct_lv": acct_lv}

    except Exception as exc:
        return {"status": "error", "message": str(exc), "uid": "", "pos_mode": "net_mode"}

def _get_ct_val(sym: str) -> float:
    ct = _b._bsc_symbol_cache.get("ct_val", {}).get(sym)
    if ct and ct > 0:
        return ct
    try:
        data = safe_get(f"{BASE}/api/v5/public/instruments",
                        {"instType": "SWAP", "instId": _to_okx(sym)})
        for s in data.get("data", []):
            raw   = s.get("ctVal", "")
            val   = float(raw)  if raw  not in ("", None) else 0.0
            eff = val if val > 0 else 0.0
            if eff > 0:
                _b._bsc_symbol_cache.setdefault("ct_val", {})[sym] = eff
                return eff
    except Exception as _e:
        raise ValueError(
            f"ctVal for {_to_okx(sym)} could not be fetched from OKX "
            f"({_e}) — order refused to prevent wrong position sizing"
        ) from _e
    raise ValueError(
        f"ctVal for {_to_okx(sym)} not found in OKX instruments response"
    )

def _set_leverage_okx(sym: str, cfg: dict) -> dict:
    lev = str(min(int(cfg.get("trade_leverage", 10)), get_max_leverage(sym)))
    resp = _trade_post("/api/v5/account/set-leverage", {
        "instId":  _to_okx(sym),
        "lever":   lev,
        "mgnMode": cfg.get("trade_margin_mode", "isolated"),
    }, cfg)
    if resp.get("code") != "0":
        okx_msg = resp.get("msg", "") or str(resp)
        raise RuntimeError(
            f"OKX rejected set-leverage {lev}x for {_to_okx(sym)}: {okx_msg}"
        )
    return resp

_OKX_KNOWN_ERRORS = {
    "51010": (
        "Account is in Simple mode — SWAP trading is disabled. "
        "Fix: OKX Demo → top-right avatar → Account mode → switch to "
        "'Single-currency margin' (or higher), then re-test."
    ),
    "51000": "Parameter error — check instId, tdMode, or sz.",
    "51001": ("Instrument doesn't exist on OKX — coin may be delisted. "
              "Remove it from your watchlist."),
    "51006": "Order price out of limit.",
    "51008": "Insufficient balance.",
    "51020": "Order quantity below minimum.",
    "58001": "Invalid API key — check credentials.",
    "50111": "Invalid API key.",
    "50100": "API frozen — IP or permissions issue.",
}

def _okx_err(resp: dict) -> str:
    top   = f"OKX {resp.get('code','?')}: {resp.get('msg','unknown')}"
    items = resp.get("data") or []
    if items:
        d = items[0]
        s_code = d.get("sCode", "")
        s_msg  = d.get("sMsg", "").strip()
        if s_code and s_code not in ("", "0"):
            friendly = _OKX_KNOWN_ERRORS.get(s_code)
            detail   = friendly if friendly else (s_msg or s_code)
            return f"[{s_code}] {detail}"
    return top

def _okx_log_entry(sig: dict, label: str, **fields) -> None:
    ts    = dubai_now().strftime("%m/%d %H:%M:%S")
    parts = [f"[{ts}] {label}"]
    for k, v in fields.items():
        parts.append(f"{k}: {v}")
    entry = " | ".join(parts)
    if not isinstance(sig.get("okx_log"), list):
        sig["okx_log"] = []
    sig["okx_log"].append(entry)

def _place_tp_only_order(sig: dict, cfg: dict,
                         tp_price: float, total_contracts: int,
                         sl_price: float = 0.0) -> str:
    """
    Place a conditional TP+SL algo order on OKX (cross margin mode) for SHORT.
    For shorts: TP is below entry, SL is above entry.
    """
    try:
        sym      = sig.get("symbol", "")
        mode     = (sig.get("order_margin_mode") or
                    cfg.get("trade_margin_mode", "isolated")).strip().lower()
        is_hedge = bool(sig.get("order_is_hedge", False))
        algo_body: dict = {
            "instId":          _to_okx(sym),
            "tdMode":          mode,
            "side":            "buy",        # BUY to close short
            "ordType":         "conditional",
            "sz":              str(int(max(1, total_contracts))),
            "tpTriggerPx":     str(_pround(tp_price)),
            "tpTriggerPxType": "mark",
            "tpOrdPx":         "-1",
        }
        if sl_price > 0:
            algo_body["slTriggerPx"]     = str(_pround(sl_price))
            algo_body["slTriggerPxType"] = "mark"
            algo_body["slOrdPx"]         = "-1"
        if is_hedge:
            algo_body["posSide"] = "short"   # closing a short in hedge mode
        else:
            algo_body["reduceOnly"] = "true"
        resp = _trade_post("/api/v5/trade/order-algo", algo_body, cfg)
        ad   = (resp.get("data") or [{}])[0]
        if resp.get("code") != "0" or (ad.get("sCode", "0") not in ("0", "") and ad.get("sCode")):
            err = (_okx_err(resp) if resp.get("code") != "0"
                   else f"{ad.get('sCode')}: {ad.get('sMsg', '')}")
            _append_error("trade", f"TP+SL algo failed: {err}",
                          symbol=sym, endpoint="/api/v5/trade/order-algo")
            return ""
        return ad.get("algoId", "")
    except Exception as exc:
        _append_error("trade", f"TP+SL algo exception: {exc}",
                      symbol=sig.get("symbol", ""),
                      endpoint="/api/v5/trade/order-algo")
        return ""

def place_okx_short_order(sig: dict, cfg: dict) -> dict:
    """
    Place a market SHORT order on OKX (demo or live), then immediately place a
    separate TP+SL algo order.
    
    For shorts:
      - Open: side="sell"
      - Close: side="buy"
      - TP: below entry
      - SL: above entry
    """
    try:
        sym = sig.get("symbol", "")
        if not sym:
            return {"ordId": "", "algoId": "", "sz": 0, "status": "error",
                    "error": "Signal missing 'symbol' field — refusing to place."}
        try:
            entry = float(sig["entry"])
            tp    = float(sig["tp"])      # TP below entry for shorts
            sl    = float(sig["sl"])      # SL above entry for shorts
            usdt  = float(cfg.get("trade_usdt_amount", 10))
            _lev_cfg = int(cfg.get("trade_leverage", 10))
        except (TypeError, ValueError, KeyError) as _valexc:
            _err = (f"Malformed signal — cannot parse entry/tp/sl/size/lev: "
                    f"{type(_valexc).__name__}: {_valexc}")
            _append_error("trade", _err, symbol=sym)
            return {"ordId": "", "algoId": "", "sz": 0,
                    "status": "error", "error": _err}
        if entry <= 0 or tp <= 0 or sl <= 0 or usdt <= 0 or _lev_cfg <= 0:
            _err = (f"Invalid numeric input — entry={entry}, tp={tp}, sl={sl}, "
                    f"usdt={usdt}, lev={_lev_cfg}. All must be > 0.")
            _append_error("trade", _err, symbol=sym)
            return {"ordId": "", "algoId": "", "sz": 0,
                    "status": "error", "error": _err}
        
        # SHORT validation: SL must be ABOVE entry; TP must be BELOW entry
        if sl <= entry:
            _err = f"SHORT SL ({sl}) must be above entry ({entry})."
            _append_error("trade", _err, symbol=sym)
            return {"ordId": "", "algoId": "", "sz": 0,
                    "status": "error", "error": _err}
        if tp >= entry:
            _err = f"SHORT TP ({tp}) must be below entry ({entry})."
            _append_error("trade", _err, symbol=sym)
            return {"ordId": "", "algoId": "", "sz": 0,
                    "status": "error", "error": _err}

        lev    = min(_lev_cfg, get_max_leverage(sym))
        mode   = cfg.get("trade_margin_mode", "isolated")

        ct_cache = _b._bsc_symbol_cache.get("ct_val", {})
        if ct_cache and sym not in ct_cache:
            err = (f"Instrument {_to_okx(sym)} not found in OKX live SWAP list — "
                   f"may be delisted. Remove {sym} from watchlist.")
            _append_error("trade", err, symbol=sym)
            return {"ordId": "", "algoId": "", "sz": 0, "status": "error", "error": err}

        try:
            ct_val = _get_ct_val(sym)
        except ValueError as _ctv_err:
            _err = str(_ctv_err)
            _append_error("trade", _err, symbol=sym)
            return {"ordId": "", "algoId": "", "sz": 0, "status": "error",
                    "error": _err}
        notional  = usdt * lev
        if ct_val <= 0 or entry <= 0:
            return {"ordId": "", "algoId": "", "sz": 0, "status": "error",
                    "error": f"Bad ct_val ({ct_val}) or entry ({entry}) — cannot size position"}
        raw_contracts = notional / (ct_val * entry)
        contracts     = max(1, int(raw_contracts))
        if contracts > 100_000:
            return {"ordId": "", "algoId": "", "sz": contracts, "status": "error",
                    "error": (f"Contract count too large ({contracts}) — "
                              f"ct_val={ct_val}, entry={entry}, notional={notional}")}

        # Fetch position mode
        is_hedge = False
        try:
            _pm_resp = _trade_get("/api/v5/account/config", {}, cfg)
            if _pm_resp.get("code") == "0":
                _pm = _pm_resp.get("data", [{}])[0].get("posMode", "net_mode")
                is_hedge = (_pm == "long_short_mode")
                _cs = getattr(_b, "_bsc_api_conn_status", None)
                if _cs is not None:
                    _cs["pos_mode"] = _pm
        except Exception as _pm_exc:
            is_hedge = (getattr(_b, "_bsc_api_conn_status", {})
                        .get("pos_mode", "net_mode") == "long_short_mode")

        # Set leverage
        try:
            _set_leverage_okx(sym, cfg)
        except Exception as lev_exc:
            _err = f"set-leverage failed — order aborted: {lev_exc}"
            _append_error("trade", _err, symbol=sym)
            return {"ordId": "", "algoId": "", "sz": 0,
                    "status": "error", "error": _err}

        # ── Step 1: Place market SHORT (SELL to open) ─────────────────────────
        order_body: dict = {
            "instId":  _to_okx(sym),
            "tdMode":  mode,
            "side":    "sell",       # SHORT: sell to open
            "ordType": "market",
            "sz":      str(contracts),
        }
        if is_hedge:
            order_body["posSide"] = "short"   # short position in hedge mode

        resp   = _trade_post("/api/v5/trade/order", order_body, cfg)
        _b._bsc_last_trade_raw = {
            "endpoint":    "/api/v5/trade/order",
            "body_sent":   order_body,
            "response":    resp,
            "is_hedge":    is_hedge,
            "contracts":   contracts,
            "ct_val":      ct_val,
        }
        d0     = (resp.get("data") or [{}])[0]
        ord_id = d0.get("ordId", "")

        if resp.get("code") != "0":
            err = _okx_err(resp)
            _append_error("trade",
                          f"{err} | body={json.dumps(order_body)} | resp={json.dumps(resp)[:300]}",
                          symbol=sym, endpoint="/api/v5/trade/order")
            return {"ordId": "", "algoId": "", "sz": contracts,
                    "status": "error", "error": err}
        if d0.get("sCode", "0") != "0":
            err = f"Entry order: {d0.get('sCode')}: {d0.get('sMsg','')}"
            _append_error("trade",
                          f"{err} | body={json.dumps(order_body)}",
                          symbol=sym, endpoint="/api/v5/trade/order")
            return {"ordId": ord_id, "algoId": "", "sz": contracts,
                    "status": "error", "error": err}

        # ── Step 2: Fetch actual fill price (avgPx) ───────────────────────────
        actual_entry = entry
        time.sleep(0.3)
        try:
            fill_resp = _trade_get(
                "/api/v5/trade/order",
                {"instId": _to_okx(sym), "ordId": ord_id},
                cfg)
            if fill_resp.get("code") == "0":
                fill_d = fill_resp.get("data", [{}])[0]
                avg_px = float(fill_d.get("avgPx", 0) or 0)
                if avg_px > 0:
                    actual_entry = avg_px
        except Exception as fill_exc:
            _append_error("trade",
                          f"Could not fetch fill price, using signal entry: {fill_exc}",
                          symbol=sym, endpoint="/api/v5/trade/order[GET]")

        # Recalculate TP and SL from actual fill price
        tp_pct    = float(cfg.get("tp_pct", 1.5)) / 100
        actual_tp = _pround(actual_entry * (1 - tp_pct))   # TP below entry
        sl_pct    = float(cfg.get("sl_pct", 3.0)) / 100
        actual_sl = _pround(actual_entry * (1 + sl_pct))   # SL above entry

        # ── Step 3: Place TP+SL algo ──────────────────────────────────────────
        if mode == "cross":
            _tp_sig = _place_tp_only_order(
                {"symbol": sym, "order_margin_mode": mode,
                 "order_is_hedge": is_hedge},
                cfg, actual_tp, contracts,
                sl_price=actual_sl,
            )
            if not _tp_sig:
                return {"ordId": ord_id, "algoId": "", "sz": contracts,
                        "status": "partial",
                        "error": "Entry ✅ · TP+SL algo ❌ (cross mode)",
                        "actual_entry": actual_entry,
                        "actual_tp": actual_tp, "actual_sl": actual_sl}

            return {"ordId": ord_id, "algoId": _tp_sig, "sz": contracts,
                    "status": "placed", "error": "",
                    "actual_entry": actual_entry,
                    "actual_tp": actual_tp, "actual_sl": actual_sl,
                    "tp_algo_id":    _tp_sig}

        # ── Isolated: OCO (TP + SL) ───────────────────────────────────────────
        algo_body: dict = {
            "instId":          _to_okx(sym),
            "tdMode":          mode,
            "side":            "buy",        # BUY to close short
            "ordType":         "oco",
            "sz":              str(contracts),
            "tpTriggerPx":     str(actual_tp),
            "tpTriggerPxType": "mark",
            "tpOrdPx":         "-1",
            "slTriggerPx":     str(actual_sl),
            "slTriggerPxType": "mark",
            "slOrdPx":         "-1",
        }
        if is_hedge:
            algo_body["posSide"] = "short"    # closing a short in hedge mode
        else:
            algo_body["reduceOnly"] = "true"
        algo_resp = _trade_post("/api/v5/trade/order-algo", algo_body, cfg)
        ad        = (algo_resp.get("data") or [{}])[0]
        algo_id   = ad.get("algoId", "")

        if algo_resp.get("code") != "0" or (ad.get("sCode","0") != "0" and ad.get("sCode","")):
            algo_err = _okx_err(algo_resp) if algo_resp.get("code") != "0" \
                       else f"OCO algo: {ad.get('sCode')}: {ad.get('sMsg','')}"
            _append_error("trade", f"OCO algo failed (entry placed): {algo_err}",
                          symbol=sym, endpoint="/api/v5/trade/order-algo")
            return {"ordId": ord_id, "algoId": "", "sz": contracts,
                    "status": "partial", "error": f"Entry ✅ · TP/SL ❌ {algo_err}",
                    "actual_entry": actual_entry,
                    "actual_tp": actual_tp, "actual_sl": actual_sl}

        return {"ordId": ord_id, "algoId": algo_id, "sz": contracts,
                "status": "placed", "error": "",
                "actual_entry": actual_entry,
                "actual_tp": actual_tp, "actual_sl": actual_sl}

    except Exception as exc:
        _append_error("trade", str(exc), symbol=sig.get("symbol", ""))
        return {"ordId": "", "algoId": "", "sz": 0,
                "status": "error", "error": str(exc)}

def _place_market_close_order(sig: dict, cfg: dict, reason: str = "trend_exit") -> bool:
    """
    Place a market BUY order on OKX to close an open short position.
    """
    sym = sig.get("symbol", "")
    try:
        mode     = (sig.get("order_margin_mode") or
                    cfg.get("trade_margin_mode", "isolated")).strip().lower()
        is_hedge = bool(sig.get("order_is_hedge", False))

        sz = int(sig.get("order_sz", 0) or 0)
        if sz <= 0:
            ct_val = _get_ct_val(sym)
            entry  = float(sig.get("entry", 0) or 0)
            usdt   = float(sig.get("trade_usdt", cfg.get("trade_usdt_amount", 5)) or 5)
            lev    = int(sig.get("trade_lev",  cfg.get("trade_leverage", 10))  or 10)
            if ct_val > 0 and entry > 0:
                sz = max(1, int(usdt * lev / (entry * ct_val)))

        if sz <= 0:
            _append_error("trade", f"{reason}: cannot compute contract size",
                          symbol=sym, endpoint="/api/v5/trade/order")
            return False

        body: dict = {
            "instId":  _to_okx(sym),
            "tdMode":  mode,
            "side":    "buy",        # BUY to close short
            "ordType": "market",
            "sz":      str(sz),
        }
        if is_hedge:
            body["posSide"] = "short"
        else:
            body["reduceOnly"] = "true"

        resp = _trade_post("/api/v5/trade/order", body, cfg)
        if resp.get("code") != "0":
            _append_error("trade",
                          f"{reason} close failed: {_okx_err(resp)}",
                          symbol=sym, endpoint="/api/v5/trade/order")
            return False
        _okx_log_entry(sig, f"MARKET CLOSE ({reason.upper()})",
                       sz=sz, mode=mode, side="buy")
        return True
    except Exception as exc:
        _append_error("trade", f"{reason} close exception: {exc}",
                      symbol=sym, endpoint="/api/v5/trade/order")
        return False

# ─────────────────────────────────────────────────────────────────────────────
# Bulk ticker fetch + pre-filter
# ─────────────────────────────────────────────────────────────────────────────
def get_bulk_tickers() -> dict:
    data   = safe_get(f"{BASE}/api/v5/market/tickers", {"instType": "SWAP"})
    result = {}
    for t in data.get("data", []):
        inst_id = t.get("instId", "")
        if not inst_id.endswith("-USDT-SWAP"):
            continue
        sym = _from_okx(inst_id)
        try:
            result[sym] = {
                "last":      float(t.get("last",      0) or 0),
                "open24h":   float(t.get("open24h",   0) or 0),
                "high24h":   float(t.get("high24h",   0) or 0),
                "low24h":    float(t.get("low24h",    0) or 0),
                "volCcy24h": float(t.get("volCcy24h", 0) or 0),
            }
        except Exception:
            pass
    return result

def pre_filter_by_ticker(symbols: list, tickers: dict) -> list:
    """
    SHORT version: keeps coins that are near 24h HIGH (momentum exhaustion).
    Keeps a coin when:
      1. 24 h USDT volume ≥ PRE_FILTER_MIN_VOL_USDT
      2. Last price ≤ 24 h high × PRE_FILTER_HIGH_BUFFER (near the top)
    """
    kept = []
    for sym in symbols:
        t = tickers.get(sym)
        if not t:
            continue
        last     = t["last"]
        high24h  = t["high24h"]
        vol_usdt = t["volCcy24h"]

        if vol_usdt < PRE_FILTER_MIN_VOL_USDT:
            continue

        if high24h > 0 and last > high24h * PRE_FILTER_HIGH_BUFFER:
            continue

        kept.append(sym)
    return kept

# ─────────────────────────────────────────────────────────────────────────────
# Candle fetch
# ─────────────────────────────────────────────────────────────────────────────
def get_klines(sym: str, interval: str, limit: int) -> list:
    okx_iv  = OKX_INTERVALS.get(interval, interval)
    inst_id = _to_okx(sym)
    all_bars: list = []
    after = None
    while len(all_bars) < limit:
        batch  = min(300, limit - len(all_bars))
        params = {"instId": inst_id, "bar": okx_iv, "limit": batch}
        if after:
            params["after"] = after
        data = safe_get(f"{BASE}/api/v5/market/candles", params)
        bars = data.get("data", [])
        if not bars: break
        all_bars.extend(bars)
        after = bars[-1][0]
        if len(bars) < batch: break
    all_bars.reverse()
    return [{"time": int(b[0]), "open": float(b[1]), "high": float(b[2]),
             "low":  float(b[3]), "close": float(b[4]), "volume": float(b[5])}
            for b in all_bars]

# ─────────────────────────────────────────────────────────────────────────────
# Technical indicators (unchanged)
# ─────────────────────────────────────────────────────────────────────────────

def _wilder_atr(candles: list, period: int) -> list:
    n = len(candles)
    if n == 0:
        return []
    trs = []
    for i, c in enumerate(candles):
        if i == 0:
            trs.append(c["high"] - c["low"])
        else:
            p = candles[i - 1]
            trs.append(max(c["high"] - c["low"],
                           abs(c["high"] - p["close"]),
                           abs(c["low"]  - p["close"])))
    atrs = [0.0] * n
    if n < period:
        return atrs
    atrs[period - 1] = sum(trs[:period]) / period
    for i in range(period, n):
        atrs[i] = (atrs[i - 1] * (period - 1) + trs[i]) / period
    return atrs

def _calc_supertrend(candles: list, period: int = 10, mult: float = 3.0) -> list:
    n = len(candles)
    if n < period + 1:
        return [{"trend": 1, "buy": False}] * n
    atrs   = _wilder_atr(candles, period)
    up     = [0.0] * n
    dn     = [0.0] * n
    trend  = [1]   * n
    for i in range(period, n):
        src   = (candles[i]["high"] + candles[i]["low"]) / 2
        u     = src - mult * atrs[i]
        d     = src + mult * atrs[i]
        if i > period:
            u = max(u, up[i - 1]) if candles[i - 1]["close"] > up[i - 1] else u
            d = min(d, dn[i - 1]) if candles[i - 1]["close"] < dn[i - 1] else d
        up[i] = u
        dn[i] = d
        if   trend[i - 1] == -1 and candles[i]["close"] > dn[i - 1]:
            trend[i] = 1
        elif trend[i - 1] ==  1 and candles[i]["close"] < up[i - 1]:
            trend[i] = -1
        else:
            trend[i] = trend[i - 1]
    return [{"trend": trend[i],
             "buy":   trend[i] == 1 and trend[i - 1] == -1}
            for i in range(n)]

def _calc_chandelier_exit(candles: list, period: int = 22, mult: float = 3.0) -> list:
    n      = len(candles)
    if n < period + 1:
        return [{"dir": 1, "buy": False}] * n
    atrs   = _wilder_atr(candles, period)
    closes = [c["close"] for c in candles]
    ls     = [0.0] * n
    ss     = [0.0] * n
    d      = [1]   * n
    for i in range(period, n):
        atr_val  = atrs[i] * mult
        hi_close = max(closes[i - period + 1: i + 1])
        lo_close = min(closes[i - period + 1: i + 1])
        l_stop   = hi_close - atr_val
        s_stop   = lo_close + atr_val
        if i > period:
            l_stop = max(l_stop, ls[i - 1]) if closes[i - 1] > ls[i - 1] else l_stop
            s_stop = min(s_stop, ss[i - 1]) if closes[i - 1] < ss[i - 1] else s_stop
        ls[i] = l_stop
        ss[i] = s_stop
        if   closes[i] > ss[i - 1]:
            d[i] =  1
        elif closes[i] < ls[i - 1]:
            d[i] = -1
        else:
            d[i] = d[i - 1]
    return [{"dir": d[i],
             "buy": d[i] == 1 and d[i - 1] == -1}
            for i in range(n)]

def _calc_lux_trend(candles: list, period: int = 14, mult: float = 2.0) -> list:
    return _calc_supertrend(candles, period=period, mult=mult)

def _check_trend_confirmation(candles_15m: list,
                               use_st:  bool = True,
                               use_ce:  bool = True,
                               use_lux: bool = True) -> tuple:
    """
    SHORT version: Entry qualification for BEARISH signals.
    
    A coin qualifies for SHORT when ALL of the following hold:
      1. At least 2 of the enabled indicators have an ACTIVE sell signal —
         meaning their most recent directional flip was bearish (+1 → -1)
         and no bullish flip has occurred since.
      2. No indicator (F2, F3, or F4) fired a BUY signal (bullish flip)
         at any candle AFTER the earlier of the two qualifying sell flips.
    
    Returns (True, ["F2","F4"]) if qualified, (False, []) otherwise.
    """
    _LABEL = {"st": "F2", "ce": "F3", "lux": "F4"}
    _enabled = sum([use_st, use_ce, use_lux])
    if _enabled < 2 or len(candles_15m) < 50:
        return False, []

    st_res  = _calc_supertrend(candles_15m)      if use_st  else None
    ce_res  = _calc_chandelier_exit(candles_15m) if use_ce  else None
    lux_res = _calc_lux_trend(candles_15m)       if use_lux else None

    def _last_flip_idx(res, tkey, target_val):
        if not res:
            return None
        for i in range(len(res) - 1, 0, -1):
            if res[i][tkey] == target_val and res[i - 1][tkey] != target_val:
                return i
        return None

    indicators = []
    if use_st and st_res:
        indicators.append(("st",
                           _last_flip_idx(st_res,  "trend",  1),   # bullish flip idx
                           _last_flip_idx(st_res,  "trend", -1)))  # bearish flip idx
    if use_ce and ce_res:
        indicators.append(("ce",
                           _last_flip_idx(ce_res,  "dir",    1),
                           _last_flip_idx(ce_res,  "dir",   -1)))
    if use_lux and lux_res:
        indicators.append(("lux",
                           _last_flip_idx(lux_res, "trend",  1),
                           _last_flip_idx(lux_res, "trend", -1)))

    def _active_short(buy_idx, sell_idx):
        """For short: sell flip (bearish) must be most recent."""
        if sell_idx is None:
            return False
        if buy_idx is None:
            return True
        return sell_idx > buy_idx

    active = [(name, b, s) for name, b, s in indicators if _active_short(b, s)]

    if len(active) < 2:
        return False, []

    active.sort(key=lambda x: x[2])  # sort by sell flip idx
    _, _, idx1 = active[0]
    _, _, idx2 = active[1]
    window_start = min(idx1, idx2)

    # Check no buy flips after window_start
    for _, b_idx, _ in indicators:
        if b_idx is not None and b_idx > window_start:
            return False, []

    active_labels = [_LABEL[name] for name, _, _ in active]
    return True, active_labels

def _check_trend_exit(candles_15m: list,
                      use_st:  bool = True,
                      use_ce:  bool = True,
                      use_lux: bool = True):
    """
    SHORT version: Return (True, ["F2","F4"]) if ANY ONE enabled indicator fired a
    BUY (trend flipped -1 → +1) on the last completed 15m candle.
    Returns (False, []) otherwise.
    """
    _enabled = sum([use_st, use_ce, use_lux])
    if _enabled < 1 or len(candles_15m) < 50:
        return False, []
    st_res  = _calc_supertrend(candles_15m)      if use_st  else None
    ce_res  = _calc_chandelier_exit(candles_15m) if use_ce  else None
    lux_res = _calc_lux_trend(candles_15m)       if use_lux else None

    def _buy_on_last(res, tkey):
        if not res or len(res) < 2:
            return False
        return res[-1][tkey] == 1 and res[-2][tkey] == -1

    fired = []
    if _buy_on_last(st_res,  "trend"): fired.append("F2")
    if _buy_on_last(ce_res,  "dir"):   fired.append("F3")
    if _buy_on_last(lux_res, "trend"): fired.append("F4")

    return (len(fired) > 0, fired)

def _pround(x, sig=6):
    try:
        x = float(x)
        if x == 0:
            return 0.0
        magnitude = math.floor(math.log10(abs(x)))
        decimals  = max(2, sig - int(magnitude))
        return round(x, decimals)
    except Exception:
        return x

# ─────────────────────────────────────────────────────────────────────────────
# Filter funnel counter
# ─────────────────────────────────────────────────────────────────────────────

def _reset_filter_counts():
    counts = {
        "total_watchlist":           0,
        "pre_filtered_out":          0,
        "checked":                   0,
        "f_trend_filter":            0,
        "f_empty_data":              0,
        "passed":                    0,
        "f_sl_cooldown":             0,
        "errors":                    0,
        "scan_cfg":                  {},
        "pre_filter_passed_syms":    [],
        "checked_syms":              [],
        "f_trend_filter_syms":       [],
        "f_empty_data_syms":         [],
        "passed_syms":               [],
        "f_sl_cooldown_syms":        [],
        "blocked_by_sl_cooldown_syms": [],
        "flushed_at":                0.0,
        "scan_completed_at":         0.0,
    }
    with _filter_lock:
        _filter_counts.clear()
        _filter_counts.update(counts)
        _b._bsc_filter_counts = _filter_counts

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

# ─────────────────────────────────────────────────────────────────────────────
def process(sym, cfg: dict, **_kwargs):
    """
    Evaluate a single symbol for SHORT signals only.
    """
    _tl_counts.delta = {}
    try:
        _incr_filter("checked")
        _filter_counts["checked_syms"].append(sym)

        m5 = get_klines(sym, "5m", 50)[:-1]
        if not m5:
            _incr_filter("f_empty_data")
            _filter_counts["f_empty_data_syms"].append(sym)
            return None

        entry   = _pround(m5[-1]["close"])
        sec     = SECTORS.get(sym, "Other")
        max_lev = get_max_leverage(sym)

        # SHORT: TP below entry, SL above entry
        tp = _pround(entry * (1 - cfg["tp_pct"] / 100))
        sl = _pround(entry * (1 + cfg["sl_pct"] / 100))

        _use_st  = bool(cfg.get("f2_supertrend", True))
        _use_ce  = bool(cfg.get("f3_chandelier", True))
        _use_lux = bool(cfg.get("f4_lux",        True))
        _entry_indicators = []
        if _use_st or _use_ce or _use_lux:
            _c15 = get_klines(sym, "15m", 100)[:-1]
            _confirmed, _entry_indicators = _check_trend_confirmation(
                _c15, _use_st, _use_ce, _use_lux)
            if not _confirmed:
                _record_elim("f_trend_filter", "f_trend_filter_syms", sym)
                return None

        _incr_filter("passed")
        _filter_counts["passed_syms"].append(sym)

        return {
            "id":               str(uuid.uuid4())[:8],
            "timestamp":        dubai_now().isoformat(),
            "symbol":           sym,
            "direction":        "short",
            "entry":            entry,
            "tp":               tp,
            "sl":               sl,
            "sector":           sec,
            "status":           "open",
            "close_price":      None,
            "close_time":       None,
            "max_lev":          max_lev,
            "is_super_setup":   False,
            "criteria":         {},
            "entry_indicators": "+".join(_entry_indicators) if _entry_indicators else "—",
        }

    except Exception as _proc_exc:
        _incr_filter("errors")
        _append_error("scan", str(_proc_exc), symbol=sym)
        return "error"
    finally:
        _flush_tl_counts()

def scan(cfg: dict, skip_symbols: set = None):
    """Full watchlist scan. Returns (sorted_results, error_count)."""
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

    results = []
    with ThreadPoolExecutor(max_workers=10) as exe:
        futs = [exe.submit(process, s, cfg) for s in pre_filtered]
        for f in as_completed(futs):
            r = f.result()
            if r and r != "error":
                results.append(r)

    with _filter_lock:
        _filter_counts["scan_completed_at"] = time.time()
    return sorted(results, key=lambda x: x["symbol"]), _filter_counts.get("errors", 0)

# ─────────────────────────────────────────────────────────────────────────────
# Open signal tracker
# ─────────────────────────────────────────────────────────────────────────────
_PRICE_ALERT_PCT = 3.0

def _parse_iso_safe(ts_str: str):
    s = str(ts_str).strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt

def _update_one_signal(sig: dict) -> None:
    """Fetch candles for a single open SHORT signal and update its status in-place."""
    _sym = sig.get("symbol", "?")
    try:
        sig_dt    = _parse_iso_safe(sig["timestamp"])
        sig_ts_ms = int(sig_dt.timestamp() * 1000)
        candles   = get_klines(sig["symbol"], "5m", 200)
        post      = [c for c in candles if c["time"] >= sig_ts_ms]

        tp_time = sl_time = None
        for c in post:
            # For SHORT: TP is BELOW entry, SL is ABOVE entry
            if tp_time is None and c["low"] <= sig["tp"]:   # TP hit when price goes down
                tp_time = c["time"]
            if sl_time is None and c["high"] >= sig["sl"]:  # SL hit when price goes up
                sl_time = c["time"]
        if tp_time is not None or sl_time is not None:
            if tp_time is not None and (sl_time is None or tp_time <= sl_time):
                sig.update(status="tp_hit", close_price=sig["tp"],
                           close_time=to_dubai(datetime.fromtimestamp(
                               tp_time/1000, tz=timezone.utc)).isoformat(),
                           exit_indicators="TP Hit")
                sig.pop("price_alert", None)
            else:
                sig.update(status="sl_hit", close_price=sig["sl"],
                           close_time=to_dubai(datetime.fromtimestamp(
                               sl_time/1000, tz=timezone.utc)).isoformat(),
                           exit_indicators="SL Hit")
                sig.pop("price_alert", None)
        else:
            if candles:
                latest_price = candles[-1]["close"]
                _ref = float(sig.get("entry", 0) or 0)
                sig["latest_price"] = float(latest_price)
                if _ref > 0:
                    # For SHORT: price_alert when price goes UP (against the short)
                    rise_pct = (latest_price - _ref) / _ref * 100
                    sig["price_alert"]     = rise_pct >= _PRICE_ALERT_PCT
                    sig["price_alert_pct"] = round(rise_pct, 2)
                else:
                    sig["price_alert"]     = False
                    sig["price_alert_pct"] = 0.0

                with _config_lock:
                    _te_cfg = dict(_b._bsc_cfg)
                _use_st  = bool(_te_cfg.get("f2_supertrend", True))
                _use_ce  = bool(_te_cfg.get("f3_chandelier", True))
                _use_lux = bool(_te_cfg.get("f4_lux",        True))
                if _use_st or _use_ce or _use_lux:
                    _c15 = get_klines(sig["symbol"], "15m", 100)[:-1]

                    if not sig.get("entry_indicators") or sig.get("entry_indicators") == "—":
                        _, _cur_inds = _check_trend_confirmation(
                            _c15, _use_st, _use_ce, _use_lux)
                        sig["entry_indicators"] = (
                            "+".join(_cur_inds) if _cur_inds else "F2/F3/F4 unclear"
                        )

                if _te_cfg.get("use_trend_exit", True):
                    if _use_st or _use_ce or _use_lux:
                        _exited, _exit_inds = _check_trend_exit(
                            _c15, _use_st, _use_ce, _use_lux)
                        if _exited:
                            sig.update(
                                status          = "trend_exit",
                                close_price     = float(latest_price),
                                close_time      = dubai_now().isoformat(),
                                exit_indicators = "+".join(_exit_inds) if _exit_inds else "Trend Exit",
                            )
                            sig.pop("price_alert", None)
                            _is_live = (bool(_te_cfg.get("trade_enabled")) and
                                        not bool(_te_cfg.get("demo_mode", True)) and
                                        sig.get("order_id"))
                            if _is_live:
                                _place_market_close_order(sig, _te_cfg, "trend_exit")
    except Exception as _upd_exc:
        _append_error("signal_update",
                      f"{_sym}: {type(_upd_exc).__name__}: {_upd_exc}",
                      symbol=_sym)

def update_open_signals(signals):
    open_sigs = [s for s in signals if s["status"] == "open"]
    if not open_sigs:
        return signals
    with ThreadPoolExecutor(max_workers=min(len(open_sigs), 15)) as pool:
        futs = [pool.submit(_update_one_signal, s) for s in open_sigs]
        for f in as_completed(futs):
            f.result()
    return signals

def _check_sl_circuit_breaker():
    with _log_lock:
        _closed = sorted(
            [s for s in _b._bsc_log["signals"]
             if s.get("status") in ("tp_hit", "sl_hit") and s.get("close_time")],
            key=lambda x: x.get("close_time", ""),
        )
    return len(_closed) >= 3 and all(s["status"] == "sl_hit" for s in _closed[-3:])

def _bg_loop():
    while True:
        if not _scanner_running.is_set() or getattr(_b, "_bsc_sl_paused", False):
            time.sleep(2); continue
        with _config_lock:
            cfg = dict(_b._bsc_cfg)
        t0 = time.time()
        try:
            with _log_lock:
                _open_snapshot = [s for s in _b._bsc_log["signals"]
                                  if s.get("status") == "open"]
            if _open_snapshot:
                update_open_signals(_open_snapshot)

            if _check_sl_circuit_breaker():
                _b._bsc_sl_paused = True
                continue

            now_dubai   = dubai_now()
            tp_cutoff   = now_dubai - timedelta(minutes=int(cfg["cooldown_minutes"]))
            sl_cd_hours = max(1, int(cfg.get("sl_cooldown_hours", 24)))
            sl_cutoff   = now_dubai - timedelta(hours=sl_cd_hours)

            with _log_lock:
                cooled_tp = {s["symbol"] for s in _b._bsc_log["signals"]
                             if s.get("close_time")
                             and s.get("status") == "tp_hit"
                             and datetime.fromisoformat(
                                 s["close_time"].replace("Z","+00:00")) >= tp_cutoff}
                cooled_sl = {s["symbol"] for s in _b._bsc_log["signals"]
                             if s.get("close_time")
                             and s.get("status") == "sl_hit"
                             and datetime.fromisoformat(
                                 s["close_time"].replace("Z","+00:00")) >= sl_cutoff}
                cooled = cooled_tp | cooled_sl
                active = {s["symbol"] for s in _b._bsc_log["signals"] if s["status"]=="open"}

            if cfg.get("scan_hour_enabled", False):
                _now_h   = dubai_now().hour
                _h_start = int(cfg.get("scan_hour_start", 0))
                _h_end   = int(cfg.get("scan_hour_end", 23))
                if _h_start <= _h_end:
                    _in_window = _h_start <= _now_h <= _h_end
                else:
                    _in_window = _now_h >= _h_start or _now_h <= _h_end
                if not _in_window:
                    _rescan_event.wait(timeout=60)
                    _rescan_event.clear()
                    continue

            _pre_scan_skip = cooled | active
            new_sigs, errors = scan(cfg, skip_symbols=_pre_scan_skip)

            with _log_lock:
                open_trade_count = len(active)
                MAX_OPEN_TRADES  = max(1, int(cfg.get("max_open_trades", 15)))
                _new_sig_syms      = [s["symbol"] for s in new_sigs]
                _blocked_active    = sorted(active    & set(_new_sig_syms))
                _blocked_cooldown  = sorted(cooled_tp & set(_new_sig_syms))
                _blocked_sl_cooldown = sorted(cooled_sl & set(_new_sig_syms))
                _filter_counts["f_sl_cooldown"] = len(_blocked_sl_cooldown)
                _filter_counts["f_sl_cooldown_syms"] = list(_blocked_sl_cooldown)
                skip         = cooled | active
                _added_syms  = []
                _queued_syms = []
                sigs_to_trade = []
                for sig in new_sigs:
                    if sig["symbol"] not in skip:
                        if open_trade_count >= MAX_OPEN_TRADES:
                            queued_sig = dict(sig)
                            queued_sig["status"] = "queue_limit"
                            _b._bsc_log["signals"].append(queued_sig)
                            _queued_syms.append(sig["symbol"])
                            skip.add(sig["symbol"])
                        else:
                            _b._bsc_log["signals"].append(sig)
                            skip.add(sig["symbol"])
                            _added_syms.append(sig["symbol"])
                            open_trade_count += 1
                            if (cfg.get("trade_enabled") and
                                    cfg.get("api_key") and cfg.get("api_secret") and
                                    cfg.get("api_passphrase")):
                                sigs_to_trade.append(sig)
                _filter_counts["new_signal_syms"]         = _added_syms
                _filter_counts["queued_syms"]             = _queued_syms
                _filter_counts["blocked_by_active_syms"]  = _blocked_active
                _filter_counts["blocked_by_cooldown_syms"]= _blocked_cooldown
                _filter_counts["blocked_by_sl_cooldown_syms"] = _blocked_sl_cooldown
                elapsed = time.time() - t0
                _b._bsc_log["health"].update(
                    total_cycles         = _b._bsc_log["health"].get("total_cycles",0)+1,
                    last_scan_at         = dubai_now().isoformat(),
                    last_scan_duration_s = round(elapsed, 1),
                    total_api_errors     = _b._bsc_log["health"].get("total_api_errors",0)+errors,
                    watchlist_size       = len(cfg["watchlist"]),
                    pre_filtered_out     = _filter_counts.get("pre_filtered_out",0),
                    deep_scanned         = _filter_counts.get("checked",0),
                )
                save_log(_b._bsc_log)

            if sigs_to_trade:
                for sig in sigs_to_trade:
                    result = place_okx_short_order(sig, cfg)
                    sig["order_id"]          = result.get("ordId", "")
                    sig["algo_id"]           = result.get("algoId", "")
                    sig["order_sz"]          = result.get("sz", 0)
                    sig["order_status"]      = result.get("status", "")
                    sig["order_error"]       = result.get("error", "")
                    if result.get("status") == "error":
                        sig["status"] = "entry_failed"
                    sig["trade_usdt"]        = float(cfg.get("trade_usdt_amount", 0))
                    sig["trade_lev"]         = int(cfg.get("trade_leverage", 10))
                    sig["demo_mode"]         = bool(cfg.get("demo_mode", True))
                    sig["order_ct_val"]      = float(result.get("ct_val", 0) or 0)
                    sig["order_notional"]    = float(result.get("notional", 0) or 0)
                    sig["order_margin_mode"] = (
                        result.get("tdMode")
                        or cfg.get("trade_margin_mode", "isolated")
                    )
                    sig["order_is_hedge"]    = bool(result.get("is_hedge", False))
                    sig["tp_algo_id"]        = result.get("tp_algo_id", "")
                    if result.get("actual_entry"):
                        sig["entry"]        = result["actual_entry"]
                        sig["tp"]           = result["actual_tp"]
                        sig["sl"]           = result["actual_sl"]
                        sig["signal_entry"] = sig.get("entry")
                    if result.get("tp_algo_id"):
                        sig["tp_algo_id"] = result["tp_algo_id"]
                    _env_tag = "DEMO" if sig.get("demo_mode") else "LIVE"
                    if result.get("status") in ("placed", "partial", "error"):
                        _okx_log_entry(
                            sig, f"SHORT ENTRY [{_env_tag}]",
                            instId   = _to_okx(sig.get("symbol", "")),
                            ordType  = "market",
                            side     = "sell",
                            tdMode   = sig.get("order_margin_mode", ""),
                            sz       = f"{sig.get('order_sz', 0)} contracts",
                            ordId    = result.get("ordId", "—"),
                            entry_px = result.get("actual_entry", sig.get("entry", "")),
                            status   = result.get("status", ""),
                        )
                    _algo_placed = result.get("algoId", "") or result.get("tp_algo_id", "")
                    if _algo_placed:
                        _is_cross_log = (sig.get("order_margin_mode", "").strip().lower() == "cross")
                        if _is_cross_log:
                            _okx_log_entry(
                                sig, f"SHORT TP ALGO [{_env_tag}]",
                                ordType  = "conditional",
                                side     = "buy",
                                tpTriggerPx = result.get("actual_tp", sig.get("tp", "")),
                                slTriggerPx = result.get("actual_sl", sig.get("sl", "")),
                                tpTriggerPxType = "mark",
                                algoId   = _algo_placed,
                            )
                        else:
                            _okx_log_entry(
                                sig, f"SHORT OCO ALGO [{_env_tag}]",
                                ordType  = "oco",
                                side     = "buy",
                                tpTriggerPx = result.get("actual_tp", sig.get("tp", "")),
                                slTriggerPx = result.get("actual_sl", sig.get("sl", "")),
                                tpTriggerPxType = "mark",
                                slTriggerPxType = "mark",
                                algoId   = _algo_placed,
                            )
                with _log_lock:
                    save_log(_b._bsc_log)
            _b._bsc_last_error = ""
        except Exception as e:
            _b._bsc_last_error = str(e)
            _append_error("loop", str(e))
        elapsed   = time.time() - t0
        sleep_sec = max(0, cfg["loop_minutes"] * 60 - elapsed)
        _rescan_event.wait(timeout=sleep_sec)
        _rescan_event.clear()

def _ensure_scanner():
    if _b._bsc_thread is None or not _b._bsc_thread.is_alive():
        t = threading.Thread(target=_bg_loop, daemon=True, name="okx-scanner-short")
        t.start(); _b._bsc_thread = t

# ─────────────────────────────────────────────────────────────────────────────
# STREAMLIT UI
# ─────────────────────────────────────────────────────────────────────────────
st.set_page_config(page_title="S&R Crypto Intelligent Portal — SHORT", page_icon="📉",
                   layout="wide", initial_sidebar_state="expanded")

st.markdown("""
<style>
/* ── Google Fonts ─────────────────────────────────────────────────────────── */
@import url('https://fonts.googleapis.com/css2?family=Sora:wght@300;400;600;700&family=JetBrains+Mono:wght@400;500&display=swap');

html, body, [class*="css"] {
    font-family: 'Sora', sans-serif !important;
    color: #1A1A2E !important;
}

.stApp {
    background-color: #F0F0FA !important;
}

[data-testid="stSidebar"] {
    background-color: #E0E0F0 !important;
    border-right: 1px solid #1A1A2E44 !important;
}

h1 {
    font-family: 'Sora', sans-serif !important;
    font-weight: 700 !important;
    font-size: 1.9rem !important;
    color: #C0392B !important;
    border-bottom: 2px solid #C0392B66 !important;
    padding-bottom: 8px !important;
}

[data-testid="stMetric"] {
    background-color: #E8E8F5 !important;
    border: 1px solid #1A1A2E66 !important;
    border-radius: 8px !important;
}
</style>
""", unsafe_allow_html=True)

_ensure_scanner()

with _log_lock:    _snap_log = json.loads(json.dumps(_b._bsc_log))
with _config_lock: _snap_cfg = dict(_b._bsc_cfg)

health  = _snap_log.get("health", {})
signals = _snap_log.get("signals", [])

# ─────────────────────────────────────────────────────────────────────────────
# SIDEBAR (SHORT version - simplified)
# ─────────────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## ⚙️ Configuration")
    st.markdown("### 📉 SHORT-ONLY MODE")
    st.caption("Signals trigger on bearish flips (F2/F3/F4 → -1)")
    
    running   = _scanner_running.is_set()
    btn_label = "⏹ Stop Scanner" if running else "▶️ Start Scanner"
    if st.button(btn_label, use_container_width=True, type="primary" if running else "secondary"):
        if running: _scanner_running.clear()
        else:       _scanner_running.set()
        st.rerun()
    st.caption(f"{'🟢' if running else '🔴'}  Scanner {'running' if running else 'stopped'}")

    _sl_paused = getattr(_b, "_bsc_sl_paused", False)
    if _sl_paused:
        st.warning("🔴 **Paused — 3 consecutive SL hits**")
        if st.button("▶️ Resume Scanning", key="resume_sl_circuit", type="primary", use_container_width=True):
            _b._bsc_sl_paused = False
            _rescan_event.set()
            st.rerun()
    else:
        st.success("✅ Circuit Breaker: OK")
    st.divider()

    # ── Auto-Trading ───────────────────────────────────────────────────────────
    st.markdown("**🤖 Auto-Trading (SHORT)**")
    new_trade_enabled = st.checkbox(
        "Enable Auto-Trading",
        value=bool(_snap_cfg.get("trade_enabled", False)), key="cfg_trade_enabled_short")

    new_demo_mode = st.radio(
        "Environment", ["Demo", "Live"],
        index=0 if _snap_cfg.get("demo_mode", True) else 1,
        horizontal=True, key="cfg_demo_mode_short",
        disabled=not new_trade_enabled)
    
    # API credentials (same as original)
    new_api_key = st.text_input("API Key", value=_snap_cfg.get("api_key", ""),
                                key="cfg_api_key_short", type="password",
                                disabled=not new_trade_enabled)
    new_api_secret = st.text_input("API Secret", value=_snap_cfg.get("api_secret", ""),
                                   key="cfg_api_secret_short", type="password",
                                   disabled=not new_trade_enabled)
    new_api_passphrase = st.text_input("API Passphrase", value=_snap_cfg.get("api_passphrase", ""),
                                       key="cfg_api_passphrase_short", type="password",
                                       disabled=not new_trade_enabled)

    # ── Size / Leverage ────────────────────────────────────────────────────────
    ta1, ta2 = st.columns(2)
    new_trade_usdt = ta1.number_input("Size (USDT)", min_value=1.0, max_value=100000.0, step=1.0,
                                      value=float(_snap_cfg.get("trade_usdt_amount", 10.0)),
                                      key="cfg_trade_usdt_short")
    new_trade_lev = ta2.number_input("Leverage ×", min_value=1, max_value=125, step=1,
                                     value=int(_snap_cfg.get("trade_leverage", 10)),
                                     key="cfg_trade_lev_short")
    
    notional_usdt = new_trade_usdt * new_trade_lev
    st.caption(f"📐 Notional per SHORT: ~${notional_usdt:,.0f} USDT")

    st.divider()

    # ── Trade Settings ─────────────────────────────────────────────────────────
    st.markdown("**📊 Trade Settings (SHORT)**")
    c1, c2 = st.columns(2)
    new_tp = c1.number_input("TP %", min_value=0.1, max_value=20.0, step=0.1,
                             value=float(_snap_cfg.get("tp_pct", 1.5)), key="cfg_tp_short",
                             help="SHORT: TP is BELOW entry price")
    new_sl = c2.number_input("SL %", min_value=0.1, max_value=20.0, step=0.1,
                             value=float(_snap_cfg.get("sl_pct", 3.0)), key="cfg_sl_short",
                             help="SHORT: SL is ABOVE entry price")
    st.caption("📉 TP = entry × (1 − TP%)  |  📈 SL = entry × (1 + SL%)")
    st.divider()

    # ── Trend Indicators ───────────────────────────────────────────────────────
    st.markdown("**📈 F2/F3/F4 — Bearish Confirmation (15m)**")
    new_f2_st = st.checkbox("F2 — SuperTrend", value=bool(_snap_cfg.get("f2_supertrend", True)), key="cfg_f2_st_short")
    new_f3_ce = st.checkbox("F3 — Chandelier Exit", value=bool(_snap_cfg.get("f3_chandelier", True)), key="cfg_f3_ce_short")
    new_f4_lux = st.checkbox("F4 — Lux Trend", value=bool(_snap_cfg.get("f4_lux", True)), key="cfg_f4_lux_short")
    st.caption("Entry requires ≥2 indicators with ACTIVE bearish signals")
    st.divider()

    # ── Trend Exit ─────────────────────────────────────────────────────────────
    new_use_trend_exit = st.checkbox("Enable Trend Exit", value=bool(_snap_cfg.get("use_trend_exit", True)), key="cfg_trend_exit_short",
                                     help="Auto-close SHORT when ANY indicator flips bullish")
    st.divider()

    # ── Cooldowns ──────────────────────────────────────────────────────────────
    st.markdown("**⏱ Cooldowns**")
    c5, c6 = st.columns(2)
    new_loop = c5.number_input("Loop (min)", min_value=1, max_value=60, step=1,
                               value=int(_snap_cfg.get("loop_minutes", 4)), key="cfg_loop_short")
    new_cool = c6.number_input("TP Cooldown (min)", min_value=1, max_value=120, step=1,
                               value=int(_snap_cfg.get("cooldown_minutes", 2)), key="cfg_cool_short")
    new_sl_cooldown_hours = st.number_input("SL Cooldown (hrs)", min_value=1, max_value=720, step=1,
                                            value=int(_snap_cfg.get("sl_cooldown_hours", 24)), key="cfg_sl_cooldown_short")
    new_max_open_trades = st.number_input("Max Open Trades", min_value=1, max_value=50, step=1,
                                          value=int(_snap_cfg.get("max_open_trades", 7)), key="cfg_max_open_short")
    st.divider()

    # ── Watchlist ──────────────────────────────────────────────────────────────
    st.markdown("**📋 Watchlist**")
    wl_text = st.text_area("wl", value="\n".join(_snap_cfg.get("watchlist", [])),
                            height=180, label_visibility="collapsed", key="cfg_wl_short")
    st.divider()

    # ── Save Button ────────────────────────────────────────────────────────────
    if st.button("💾 Save & Apply", use_container_width=True, type="primary"):
        new_wl = [s.strip().upper() for s in wl_text.splitlines() if s.strip()]
        new_cfg = {
            "tp_pct": new_tp, "sl_pct": new_sl,
            "use_pre_filter":      bool(_snap_cfg.get("use_pre_filter", True)),
            "use_trend_exit":      bool(new_use_trend_exit),
            "f2_supertrend":       bool(new_f2_st),
            "f3_chandelier":       bool(new_f3_ce),
            "f4_lux":              bool(new_f4_lux),
            "loop_minutes": int(new_loop), "cooldown_minutes": int(new_cool),
            "max_open_trades":    max(1, int(new_max_open_trades)),
            "sl_cooldown_hours":  max(1, int(new_sl_cooldown_hours)),
            "scan_hour_enabled":  bool(_snap_cfg.get("scan_hour_enabled", False)),
            "scan_hour_start":    int(_snap_cfg.get("scan_hour_start", 0)),
            "scan_hour_end":      int(_snap_cfg.get("scan_hour_end", 23)),
            "watchlist": new_wl,
            "trade_enabled":     bool(new_trade_enabled),
            "demo_mode":         (new_demo_mode == "Demo"),
            "api_key":           new_api_key.strip(),
            "api_secret":        new_api_secret.strip(),
            "api_passphrase":    new_api_passphrase.strip(),
            "trade_usdt_amount": float(new_trade_usdt),
            "trade_leverage":    int(new_trade_lev),
            "trade_margin_mode": "cross",
        }
        with _config_lock: _b._bsc_cfg.clear(); _b._bsc_cfg.update(new_cfg)
        save_config(new_cfg)
        if new_wl != _snap_cfg.get("watchlist", []):
            _b._bsc_symbol_cache["fetched_at"] = 0
        _b._bsc_rescan_event.set()
        st.success(f"✅ Saved — {len(new_wl)} coins — SHORT-ONLY mode")
        st.rerun()

# ─────────────────────────────────────────────────────────────────────────────
# MAIN AREA
# ─────────────────────────────────────────────────────────────────────────────
_NOW_GST = dubai_now().strftime("%d %b %Y  %H:%M GST")
st.title(f"📉 S&R — Crypto Intelligent Portal (SHORT-ONLY)  ·  🕐 {_NOW_GST}")

# ── Total Realized PnL computation (SHORT formula) ─────────────────────────────
def _pnl_topline_short(sig: dict, usdt_fb: float, lev_fb: int):
    """PnL for SHORT: (1 - close/entry) × notional"""
    try:
        _close = float(sig.get("close_price") or 0)
    except (TypeError, ValueError):
        return None
    if _close <= 0:
        return None
    try:
        _ent = float(sig.get("entry", 0) or 0)
        _usd = float(sig.get("trade_usdt", usdt_fb) or 0)
        _lev = int(sig.get("trade_lev", lev_fb) or 0)
    except (TypeError, ValueError):
        return None
    if _ent <= 0 or _usd <= 0 or _lev <= 0:
        return None
    # SHORT PnL: profit when price goes down
    return (1.0 - _close / _ent) * (_usd * _lev)

_total_pnl = 0.0
_total_tp_ct = 0
_total_sl_ct = 0
_cfg_usdt_fb_top = float(_snap_cfg.get("trade_usdt_amount", 0) or 0)
_cfg_lev_fb_top = int(_snap_cfg.get("trade_leverage", 10) or 0)
for _s in signals:
    if _s.get("status") not in ("tp_hit", "sl_hit", "trend_exit"):
        continue
    _v = _pnl_topline_short(_s, _cfg_usdt_fb_top, _cfg_lev_fb_top)
    if _v is None:
        continue
    _total_pnl += _v
    if _s["status"] == "tp_hit":
        _total_tp_ct += 1
    else:
        _total_sl_ct += 1

_closed_total = _total_tp_ct + _total_sl_ct
st.markdown(
    f"<div style='padding:10px 14px; margin:4px 0 10px 0; "
    f"border:1px solid #C0392B33; border-radius:10px; "
    f"background:linear-gradient(90deg, #C0392B14, transparent);'>"
    f"<span style='font-size:0.95em; opacity:0.75;'>📉 Total Realized PnL (SHORTS)</span><br>"
    f"<span style='font-size:2.2em; font-weight:700; color:#C0392B;'>"
    f"{_total_pnl:+,.2f} $</span>  "
    f"<span style='opacity:0.75; font-size:0.9em; margin-left:8px;'>"
    f"{_closed_total} closed · {_total_tp_ct} TP | {_total_sl_ct} SL</span>"
    f"</div>",
    unsafe_allow_html=True,
)

# ── Health Metrics ─────────────────────────────────────────────────────────────
open_count = sum(1 for s in signals if s["status"] == "open")
tp_count = sum(1 for s in signals if s["status"] == "tp_hit")
sl_count = sum(1 for s in signals if s["status"] == "sl_hit")
trend_exit_count = sum(1 for s in signals if s["status"] == "trend_exit")
queue_count = sum(1 for s in signals if s["status"] == "queue_limit")

m1, m2, m3, m4, m5 = st.columns(5)
m1.metric("Open SHORTS", open_count)
m2.metric("TP Hit ✅", tp_count)
m3.metric("SL Hit ❌", sl_count)
m4.metric("Trend Exit 🚨", trend_exit_count)
m5.metric("Queued ⏳", queue_count)

_max_open_cap = max(1, int(_snap_cfg.get("max_open_trades", 7)))
if open_count >= _max_open_cap:
    st.warning(f"🔒 Queue full ({open_count}/{_max_open_cap}) — new signals will be queued")

st.divider()

# ── Signal Tables (simplified for SHORT version) ───────────────────────────────
def _build_short_signal_row(s: dict) -> dict:
    status = s.get("status", "open")
    status_icon = {
        "open": "📉 Open Short",
        "tp_hit": "✅ TP Hit (Profit)",
        "sl_hit": "❌ SL Hit (Loss)",
        "trend_exit": "🚨 Trend Exit",
        "queue_limit": "⏳ Queue Limit",
    }.get(status, status)
    
    ts_str = fmt_dubai(s.get("timestamp", ""))
    close_str = fmt_dubai(s["close_time"]) if s.get("close_time") else "—"
    
    return {
        "Time (GST)": ts_str,
        "Symbol": s.get("symbol", ""),
        "Entry": s.get("entry", ""),
        "TP": s.get("tp", ""),
        "SL": s.get("sl", ""),
        "Entry Signals": s.get("entry_indicators", "—"),
        "Status": status_icon,
        "Close Time": close_str,
        "Close $": s.get("close_price") or "—",
    }

def _render_short_table(sig_list: list, header: str, empty_msg: str):
    rows = [_build_short_signal_row(s) for s in sig_list]
    st.markdown(f"### {header} ({len(rows)})")
    if rows:
        st.dataframe(rows, use_container_width=True, hide_index=True)
    else:
        st.info(empty_msg)

_open_sigs = [s for s in signals if s.get("status") == "open"]
_tp_sigs = [s for s in signals if s.get("status") == "tp_hit"]
_sl_sigs = [s for s in signals if s.get("status") == "sl_hit"]
_trend_exit_sigs = [s for s in signals if s.get("status") == "trend_exit"]
_queue_sigs = [s for s in signals if s.get("status") == "queue_limit"]

_render_short_table(_open_sigs, "📉 Open Shorts", "No open shorts right now.")
st.divider()
_render_short_table(_tp_sigs, "✅ TP Hit (Profitable)", "No TP hits yet.")
st.divider()
_render_short_table(_sl_sigs, "❌ SL Hit (Losses)", "No SL hits yet.")
st.divider()
_render_short_table(_trend_exit_sigs, "🚨 Trend Exit (Closed Early)", "No trend exits yet.")
st.divider()

if _queue_sigs:
    with st.expander(f"⏳ Queue Limit ({len(_queue_sigs)})", expanded=False):
        _render_short_table(_queue_sigs, "Queued Signals", "")
        if st.button("🗑️ Clear Queue Records", key="clear_queue_short"):
            with _log_lock:
                _b._bsc_log["signals"] = [s for s in _b._bsc_log["signals"] if s.get("status") != "queue_limit"]
                save_log(_b._bsc_log)
            st.rerun()

# ─────────────────────────────────────────────────────────────────────────────
# Run the app
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    pass  # Streamlit handles execution
