#!/usr/bin/env python3
"""
OKX Futures Scanner — Streamlit Dashboard  v2
Performance improvements:
  A. Bulk ticker pre-filter  — 1 API call eliminates ~70% of coins before any candle fetch
  B. Parallel timeframe fetch — 4 timeframes fetched simultaneously per coin
  C. Staged candle fetch      — quick 50-candle RSI/resistance check before full fetch
  D. Symbol cache             — OKX instrument list cached for 6 h, not re-fetched every cycle
  + API semaphore             — hard cap of 8 concurrent HTTP requests (no exchange blocking)
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
# API rate-limiter  — hard cap: 8 concurrent HTTP requests at any time
# OKX public-market limit is 20 req / 2 s.  Staying well below keeps us safe.
# ─────────────────────────────────────────────────────────────────────────────
_api_sem = threading.Semaphore(8)

# ─────────────────────────────────────────────────────────────────────────────
# Symbol-cache TTL  (D)
# ─────────────────────────────────────────────────────────────────────────────
_SYMBOL_CACHE_TTL = 6 * 3600   # refresh OKX instrument list every 6 hours

# ─────────────────────────────────────────────────────────────────────────────
# Bulk pre-filter thresholds  (A)
# ─────────────────────────────────────────────────────────────────────────────
PRE_FILTER_MIN_VOL_USDT  =  100_000   # minimum 24 h USDT volume
PRE_FILTER_LOW_BUFFER    =    1.005   # price must be ≥ 0.5 % above 24 h low

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
# Default configuration
# ─────────────────────────────────────────────────────────────────────────────
DEFAULT_CONFIG: dict = {
    "tp_pct":               1.5,
    "sl_pct":               3.0,
    # ── per-filter enable/disable ──────────────────────────────────────────────
    "use_pre_filter":       True,   # Bulk ticker pre-filter (volume / change / low)
    "use_rsi_5m":           True,   # F4 — 5m RSI
    "rsi_5m_min":           30,
    "use_rsi_1h":           True,   # F5 — 1h RSI
    "rsi_1h_min":           30,
    "rsi_1h_max":           95,
    "loop_minutes":         5,
    "cooldown_minutes":     5,
    "use_ema_3m":           False,
    "ema_period_3m":      12,
    "use_ema_5m":         True,
    "ema_period_5m":      12,
    "use_ema_15m":        True,
    "ema_period_15m":     12,
    "use_macd":           True,
    "use_sar":            True,
    "use_vol_spike":      False,
    "vol_spike_mult":     2.0,
    "vol_spike_lookback": 20,
    "use_pdz_5m":            True,   # F3 — PDZ (5m)
    "use_pdz_15m":           True,   # F2 — PDZ (15m)
    # ── Auto-trading (OKX Demo / Live) ────────────────────────────────────────
    "trade_enabled":         False,
    "demo_mode":             True,   # True = Demo API, False = Live API
    "api_key":               "",
    "api_secret":            "",
    "api_passphrase":        "",
    "trade_usdt_amount":     10.0,   # USDT collateral per trade (before leverage)
    "trade_leverage":        10,     # leverage applied (capped by MAX_LEVERAGE)
    "trade_margin_mode":     "cross",# "cross" or "isolated"
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
# Max leverage tiers
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
    criteria = sig.get("criteria", {})
    if not criteria:
        return "No criteria data captured"
    reasons = []
    # Safely convert — Super Setup signals store "—" for unused criteria
    try:
        rsi_5m = float(criteria.get("rsi_5m", 50))
    except (TypeError, ValueError):
        rsi_5m = 50.0
    try:
        rsi_1h = float(criteria.get("rsi_1h", 50))
    except (TypeError, ValueError):
        rsi_1h = 50.0
    if rsi_5m < 35:
        reasons.append(f"RSI 5m very low at entry ({rsi_5m}) — momentum already fading")
    elif rsi_5m < 42:
        reasons.append(f"RSI 5m borderline ({rsi_5m}) — weak short-term momentum")
    if rsi_1h > 82:
        reasons.append(f"RSI 1h near overbought ({rsi_1h}) — higher-TF exhaustion risk")
    elif rsi_1h < 40:
        reasons.append(f"RSI 1h weak ({rsi_1h}) — no hourly uptrend support")
    if not reasons:
        reasons.append("Unexpected market reversal / macro event — filters were healthy at entry")
    return "\n".join(f"• {r}" for r in reasons)

# ─────────────────────────────────────────────────────────────────────────────
# Config persistence
# ─────────────────────────────────────────────────────────────────────────────
# Data is always saved to ~/Documents/CryptoDemoTrades/ — a fixed, reliable
# location on the user's computer that survives app restarts, script moves,
# and working-directory changes.  A fallback to the script's own directory is
# tried first so that running multiple instances from different folders stays
# independent; the home-Documents path is the final, always-writable fallback.

def _resolve_data_dir() -> pathlib.Path:
    candidates = []
    # 1. Same directory as the script (keeps data next to code when writable)
    try:
        candidates.append(pathlib.Path(__file__).parent.absolute())
    except Exception:
        pass
    # 2. Stable home-directory location — survives script moves & restarts
    candidates.append(pathlib.Path.home() / "Documents" / "CryptoDemoTrades")

    for path in candidates:
        try:
            path.mkdir(parents=True, exist_ok=True)
            probe = path / ".write_probe"
            probe.touch(); probe.unlink()
            return path          # first writable candidate wins
        except OSError:
            continue

    # Last resort: temp dir (data won't survive OS restart — shown in UI)
    import tempfile
    fallback = pathlib.Path(tempfile.gettempdir()) / "CryptoDemoTrades"
    fallback.mkdir(parents=True, exist_ok=True)
    return fallback

_SCRIPT_DIR  = _resolve_data_dir()
CONFIG_FILE  = _SCRIPT_DIR / "scanner_config.json"
LOG_FILE     = _SCRIPT_DIR / "scanner_log.json"
_config_lock = threading.Lock()

def load_config() -> dict:
    if CONFIG_FILE.exists():
        try:
            saved = json.loads(CONFIG_FILE.read_text(encoding="utf-8"))
            cfg   = dict(DEFAULT_CONFIG)
            for k in DEFAULT_CONFIG:
                if k in saved:
                    cfg[k] = saved[k]
            return cfg
        except Exception:
            pass
    return dict(DEFAULT_CONFIG)

def save_config(cfg: dict):
    with _config_lock:
        CONFIG_FILE.write_text(json.dumps(cfg, indent=2), encoding="utf-8")

def _migrate_criteria(crit: dict) -> dict:
    """
    Convert old-format criteria that stored "✅"/"—" into the new per-timeframe
    numeric keys.  Values that were "✅" become None (no data available) so the
    UI shows "—" instead of a stale tick mark.
    """
    if not crit:
        return crit
    # Migrate flat MACD/SAR/Vol keys → per-timeframe keys
    if "macd" in crit and "macd_3m" not in crit:
        v = crit.pop("macd")
        crit["macd_3m"]  = None if v == "✅" else v
        crit["macd_5m"]  = None
        crit["macd_15m"] = None
    if "sar" in crit and "sar_3m" not in crit:
        v = crit.pop("sar")
        crit["sar_3m"]  = None if v == "✅" else v
        crit["sar_5m"]  = None
        crit["sar_15m"] = None
    if "vol" in crit and "vol_ratio" not in crit:
        v = crit.pop("vol")
        crit["vol_ratio"] = None if v == "✅" else v
    # EMA: if stored as "✅" we have no actual value — set to None
    for key in ("ema_3m", "ema_5m", "ema_15m"):
        if crit.get(key) == "✅":
            crit[key] = None
    return crit

def load_log():
    if LOG_FILE.exists():
        try:
            data = json.loads(LOG_FILE.read_text(encoding="utf-8"))
            # Migrate any pre-update signals transparently
            for sig in data.get("signals", []):
                if "criteria" in sig:
                    sig["criteria"] = _migrate_criteria(sig["criteria"])
            return data
        except Exception:
            pass
    return {"health": {"total_cycles": 0, "last_scan_at": None,
                        "last_scan_duration_s": 0.0, "total_api_errors": 0,
                        "watchlist_size": 0, "pre_filtered_out": 0,
                        "deep_scanned": 0},
            "signals": []}

def save_log(log):
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    LOG_FILE.write_text(json.dumps(log, indent=2), encoding="utf-8")

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
        _b._bsc_rescan_event  = threading.Event()   # set to skip sleep & rescan immediately
        _b._bsc_api_conn_status = {          # result of last "Test Connection" call
            "status":      "untested",       # "untested" | "ok" | "error"
            "message":     "",
            "tested_at":   None,             # Dubai ISO timestamp
            "demo_mode":   None,
            "uid":         "",               # OKX account UID on success
            "pos_mode":    "net_mode",       # "net_mode" | "long_short_mode" (hedge)
            "acct_lv":     "2",              # OKX account level (1=Simple, 2=Single-margin…)
        }
        _b._bsc_last_trade_raw  = {}         # full raw OKX response from last order attempt
        _b._bsc_error_log       = []         # structured API error log (max 500 entries)
        _b._bsc_error_log_lock  = threading.Lock()
        # D — symbol cache (also stores ctVal per symbol for position sizing)
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
# HTTP helpers  (+semaphore rate limiter)
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
        _local.session = s
    return _local.session

def safe_get(url, params=None, _retries=4):
    for attempt in range(_retries):
        try:
            with _api_sem:                           # ← rate-limiter (B/D)
                r = get_session().get(url, params=params, timeout=20)
            if r.status_code == 429:
                wait = int(r.headers.get("Retry-After", 30))
                time.sleep(wait); continue
            if r.status_code in (418, 403, 451):
                raise RuntimeError(
                    f"HTTP {r.status_code}: OKX is blocking this server's IP. "
                    "Try Railway (railway.app) — uses residential IPs.")
            r.raise_for_status()
            data = r.json()
            if isinstance(data, dict) and "code" in data and data["code"] != "0":
                raise RuntimeError(f"OKX API error {data['code']}: {data.get('msg','')}")
            return data
        except requests.exceptions.ConnectionError:
            if attempt < _retries - 1: time.sleep(3); continue
            raise
    msg = f"Failed after {_retries} retries: {url}"
    _append_error("http", msg, endpoint=url)
    raise RuntimeError(msg)

# ─────────────────────────────────────────────────────────────────────────────
# Structured API error logger
# ─────────────────────────────────────────────────────────────────────────────
_ERROR_LOG_MAX = 500   # rolling cap — oldest entries are dropped beyond this

def _append_error(err_type: str, message: str,
                  symbol: str = "", endpoint: str = "") -> None:
    """
    Append one structured entry to the shared error log.

    err_type : "scan" | "trade" | "loop" | "http"
    message  : human-readable error string
    symbol   : coin that triggered the error (empty for non-coin errors)
    endpoint : OKX API path, if known
    """
    entry = {
        "ts":       dubai_now().isoformat(),
        "type":     err_type,
        "symbol":   symbol,
        "endpoint": endpoint,
        "message":  message[:400],   # cap length for display
    }
    try:
        lock = getattr(_b, "_bsc_error_log_lock", None)
        log  = getattr(_b, "_bsc_error_log", None)
        if lock is None or log is None:
            return
        with lock:
            log.append(entry)
            if len(log) > _ERROR_LOG_MAX:
                del log[:len(log) - _ERROR_LOG_MAX]
    except Exception:
        pass   # never let the logger itself crash the scanner

# ─────────────────────────────────────────────────────────────────────────────
# OKX symbol helpers
# ─────────────────────────────────────────────────────────────────────────────
def _to_okx(sym: str) -> str:
    return f"{sym[:-4]}-USDT-SWAP" if sym.endswith("USDT") else sym

def _from_okx(inst_id: str) -> str:
    return inst_id.replace("-USDT-SWAP", "USDT") if inst_id.endswith("-USDT-SWAP") else inst_id

def get_symbols(watchlist: list) -> tuple:
    """Return (watchlist_symbols_live_on_OKX, ct_val_dict).
    ct_val_dict maps e.g. 'BTCUSDT' → contract size in base currency (float).
    """
    active  = set()
    ct_vals = {}
    data    = safe_get(f"{BASE}/api/v5/public/instruments", {"instType": "SWAP"})
    for s in data.get("data", []):
        inst_id = s.get("instId", "")
        if inst_id.endswith("-USDT-SWAP") and s.get("state") == "live":
            sym = _from_okx(inst_id)
            active.add(sym)
            try:
                ct_vals[sym] = float(s.get("ctVal", 1) or 1)
            except (TypeError, ValueError):
                ct_vals[sym] = 1.0
    skipped = [s for s in watchlist if s not in active]
    if skipped:
        print(f"  [Symbol cache] {len(skipped)} watchlist coins not on OKX: {skipped[:5]}...")
    return [s for s in watchlist if s in active], ct_vals

# ─────────────────────────────────────────────────────────────────────────────
# D — Cached symbol lookup (refresh every 6 h or on watchlist change)
# ─────────────────────────────────────────────────────────────────────────────
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
# OKX Auto-Trading — authentication + order placement
# ─────────────────────────────────────────────────────────────────────────────

def _okx_sign(timestamp: str, method: str, request_path: str, body: str, secret: str) -> str:
    """HMAC-SHA256 signature required by OKX private endpoints."""
    msg = timestamp + method + request_path + body
    return base64.b64encode(
        hmac.new(secret.encode("utf-8"), msg.encode("utf-8"), hashlib.sha256).digest()
    ).decode()

def _trade_post(path: str, body: dict, cfg: dict) -> dict:
    """
    Authenticated POST to an OKX private endpoint.
    Adds x-simulated-trading: 1 header when demo_mode is True.
    Uses the same _api_sem semaphore as public calls to respect rate limits.
    """
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
    # Merge with session-level headers (User-Agent, Accept, etc.) so auth
    # headers supplement rather than replace the defaults.
    sess = get_session()
    merged = {**dict(sess.headers), **headers}
    with _api_sem:
        r = sess.post(f"{BASE}{path}", headers=merged,
                      data=body_str, timeout=20)
    r.raise_for_status()
    return r.json()

def _trade_get(path: str, params: dict, cfg: dict) -> dict:
    """Authenticated GET to an OKX private endpoint."""
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
    # Merge with session-level headers (User-Agent, Accept, etc.) so auth
    # headers supplement rather than replace the defaults.
    sess = get_session()
    merged = {**dict(sess.headers), **headers}
    with _api_sem:
        r = sess.get(f"{BASE}{path}", params=params,
                     headers=merged, timeout=20)
    r.raise_for_status()
    return r.json()

def test_api_connection(cfg: dict) -> dict:
    """
    Verify OKX API credentials by calling GET /api/v5/account/balance and
    GET /api/v5/account/config.

    Also detects the account's position mode (net_mode vs long_short_mode) which
    controls whether posSide must be included in trade orders.

    Returns {"status":"ok"|"error", "message":str, "uid":str, "pos_mode":str}.
    """
    if not cfg.get("api_key") or not cfg.get("api_secret") or not cfg.get("api_passphrase"):
        return {"status": "error", "message": "API credentials are incomplete.",
                "uid": "", "pos_mode": "net_mode"}
    try:
        # 1 — balance check
        resp = _trade_get("/api/v5/account/balance", {}, cfg)
        if resp.get("code") != "0":
            msg = f"OKX error {resp.get('code')}: {resp.get('msg', 'unknown')}"
            return {"status": "error", "message": msg, "uid": "", "pos_mode": "net_mode"}

        details  = resp.get("data", [{}])[0]
        total_eq = details.get("totalEq", "0")

        # 2 — account config: UID + position mode
        cfg_resp = _trade_get("/api/v5/account/config", {}, cfg)
        uid      = ""
        pos_mode = "net_mode"
        acct_lv  = "2"   # assume single-currency margin if unknown
        if cfg_resp.get("code") == "0":
            acct     = cfg_resp.get("data", [{}])[0]
            uid      = acct.get("uid", "")
            pos_mode = acct.get("posMode", "net_mode")
            acct_lv  = str(acct.get("acctLv", "2"))

        # acctLv "1" = Simple mode — SWAP trading not allowed
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
            # Connected but cannot trade — return as error with clear instructions
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
    """
    Return contract size (ctVal) for a symbol.
    Uses the symbol cache populated at each scan cycle.
    Falls back to a live API call if the cache is cold.
    """
    ct = _b._bsc_symbol_cache.get("ct_val", {}).get(sym)
    if ct:
        return ct
    # Cache miss — fetch on-demand (rare, e.g. first run before any scan)
    try:
        data = safe_get(f"{BASE}/api/v5/public/instruments",
                        {"instType": "SWAP", "instId": _to_okx(sym)})
        for s in data.get("data", []):
            val = float(s.get("ctVal", 1) or 1)
            _b._bsc_symbol_cache.setdefault("ct_val", {})[sym] = val
            return val
    except Exception:
        pass
    return 1.0

def _set_leverage_okx(sym: str, cfg: dict) -> None:
    """Set leverage for a symbol before placing an order."""
    lev = str(min(int(cfg.get("trade_leverage", 10)), get_max_leverage(sym)))
    _trade_post("/api/v5/account/set-leverage", {
        "instId":  _to_okx(sym),
        "lever":   lev,
        "mgnMode": cfg.get("trade_margin_mode", "cross"),
    }, cfg)

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
    """Extract the most specific error string from an OKX response dict."""
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

def place_okx_order(sig: dict, cfg: dict) -> dict:
    """
    Place a market LONG order on OKX (demo or live), then immediately place a
    separate OCO algo order (TP + SL in one call) via /api/v5/trade/order-algo.

    Two-step design avoids the 'All operations failed' error that occurs when
    attachAlgoOrds in the entry order fails validation on the exchange side.

    Position sizing:
        notional   = trade_usdt_amount × leverage
        contracts  = floor(notional / (ctVal × entry_price))   ← minimum 1

    Returns a result dict:
        {"ordId": str, "algoId": str, "sz": int,
         "status": "placed"|"partial"|"error", "error": str}
        status="partial" means the entry order placed but the OCO algo failed.
    """
    try:
        sym    = sig["symbol"]
        entry  = float(sig["entry"])
        tp     = float(sig["tp"])
        sl     = float(sig["sl"])
        usdt   = float(cfg.get("trade_usdt_amount", 10))
        lev    = min(int(cfg.get("trade_leverage", 10)), get_max_leverage(sym))
        mode   = cfg.get("trade_margin_mode", "cross")

        # ── Pre-trade instrument validation ───────────────────────────────────
        # A coin can appear in signals even after being delisted from OKX SWAP
        # markets because historical candle data stays accessible. The ct_val
        # cache is populated from the live instruments endpoint (state=live), so
        # if a symbol is absent from that cache the SWAP instrument doesn't exist
        # and OKX will return sCode 51001 (instrument not found).
        ct_cache = _b._bsc_symbol_cache.get("ct_val", {})
        if ct_cache and sym not in ct_cache:
            err = (f"Instrument {_to_okx(sym)} not found in OKX live SWAP list — "
                   f"may be delisted. Remove {sym} from watchlist.")
            _append_error("trade", err, symbol=sym)
            return {"ordId": "", "algoId": "", "sz": 0, "status": "error", "error": err}

        ct_val = _get_ct_val(sym)
        notional  = usdt * lev
        if ct_val <= 0 or entry <= 0:
            return {"ordId": "", "algoId": "", "sz": 0, "status": "error",
                    "error": f"Bad ct_val ({ct_val}) or entry ({entry}) — cannot size position"}
        raw_contracts = notional / (ct_val * entry)
        contracts     = max(1, int(raw_contracts))
        # Guard against absurdly large values (e.g. micro-priced tokens with wrong ct_val)
        if contracts > 100_000:
            return {"ordId": "", "algoId": "", "sz": contracts, "status": "error",
                    "error": (f"Contract count too large ({contracts}) — "
                              f"ct_val={ct_val}, entry={entry}, notional={notional}. "
                              f"Check ctVal for {sym}.")}

        # ── Fetch position mode LIVE from OKX (not from cached conn status) ──
        # Caching pos_mode is unreliable: builtins reset on process restart and
        # the user may not click Test Connection before the first signal fires.
        is_hedge = False
        try:
            _pm_resp = _trade_get("/api/v5/account/config", {}, cfg)
            if _pm_resp.get("code") == "0":
                _pm = _pm_resp.get("data", [{}])[0].get("posMode", "net_mode")
                is_hedge = (_pm == "long_short_mode")
                # Keep the UI conn status in sync too
                _cs = getattr(_b, "_bsc_api_conn_status", None)
                if _cs is not None:
                    _cs["pos_mode"] = _pm
        except Exception as _pm_exc:
            # Fall back to whatever was last cached
            is_hedge = (getattr(_b, "_bsc_api_conn_status", {})
                        .get("pos_mode", "net_mode") == "long_short_mode")
            _append_error("trade", f"posMode fetch failed: {_pm_exc}",
                          symbol=sym, endpoint="/api/v5/account/config")

        # ── Step 1: Set leverage ──────────────────────────────────────────────
        try:
            _set_leverage_okx(sym, cfg)
        except Exception as lev_exc:
            _append_error("trade", f"set-leverage warning: {lev_exc}",
                          symbol=sym, endpoint="/api/v5/account/set-leverage")

        # ── Step 2: Place clean market buy ───────────────────────────────────
        order_body: dict = {
            "instId":  _to_okx(sym),
            "tdMode":  mode,
            "side":    "buy",
            "ordType": "market",
            "sz":      str(contracts),
        }
        if is_hedge:
            order_body["posSide"] = "long"   # required in Long/Short (hedge) mode

        resp   = _trade_post("/api/v5/trade/order", order_body, cfg)
        # Store full raw response for debugging (visible in UI)
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

        # ── Step 3: Fetch actual fill price (avgPx) ───────────────────────────
        # Market orders fill at the live market price, which differs from the
        # signal's entry price (captured up to loop_minutes ago).
        # We must recalculate TP and SL from the real fill price so the
        # percentage targets match what the user configured.
        actual_entry = entry   # fallback to signal entry if fetch fails
        time.sleep(0.3)        # brief pause — give OKX time to record the fill
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

        # Recalculate TP and SL from the actual fill price
        tp_pct   = float(cfg.get("tp_pct", 1.5)) / 100
        sl_pct   = float(cfg.get("sl_pct", 3.0)) / 100
        actual_tp = _pround(actual_entry * (1 + tp_pct))
        actual_sl = _pround(actual_entry * (1 - sl_pct))

        # ── Step 4: Place OCO algo (TP + SL) using actual fill price ─────────
        # In hedge mode, closing a long requires posSide="long" on the sell too.
        algo_body: dict = {
            "instId":       _to_okx(sym),
            "tdMode":       mode,
            "side":         "sell",
            "ordType":      "oco",
            "sz":           str(contracts),
            "tpTriggerPx":  str(actual_tp),
            "tpOrdPx":      "-1",    # market fill when TP triggers
            "slTriggerPx":  str(actual_sl),
            "slOrdPx":      "-1",    # market fill when SL triggers
        }
        if is_hedge:
            algo_body["posSide"] = "long"    # closing a long in hedge mode
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
        _append_error("trade", str(exc), symbol=sig.get("symbol", ""),
                      endpoint="/api/v5/trade/order")
        return {"ordId": "", "algoId": "", "sz": 0,
                "status": "error", "error": str(exc)}

def place_okx_manual_order(sym: str, entry: float, tp: float, sl: float,
                           cfg: dict) -> dict:
    """
    Place a manually specified order:
      • entry > 0  → LIMIT buy at exactly that price, TP/SL used as-is
      • entry == 0 → MARKET buy at live price, TP/SL used as-is (no recalc)

    Unlike place_okx_order (auto-trading), this function never recalculates
    TP or SL — the user's values are sent directly to OKX.

    Returns {"ordId", "algoId", "sz", "status", "error",
             "actual_entry", "actual_tp", "actual_sl"}
    """
    try:
        usdt   = float(cfg.get("trade_usdt_amount", 10))
        lev    = min(int(cfg.get("trade_leverage", 10)), get_max_leverage(sym))
        mode   = cfg.get("trade_margin_mode", "cross")

        # Pre-trade instrument check
        ct_cache = _b._bsc_symbol_cache.get("ct_val", {})
        if ct_cache and sym not in ct_cache:
            err = f"Instrument {_to_okx(sym)} not found in OKX live SWAP list."
            _append_error("trade", err, symbol=sym)
            return {"ordId": "", "algoId": "", "sz": 0,
                    "status": "error", "error": err}

        ct_val = _get_ct_val(sym)
        ref_price = entry if entry > 0 else None

        # If no entry price given, fetch live market price for sizing
        if ref_price is None:
            try:
                tick = safe_get(f"{BASE}/api/v5/market/ticker",
                                {"instId": _to_okx(sym)})
                ref_price = float(tick.get("data", [{}])[0].get("last", 0) or 0)
            except Exception:
                pass
        if not ref_price or ref_price <= 0:
            return {"ordId": "", "algoId": "", "sz": 0, "status": "error",
                    "error": "Could not determine price for contract sizing."}

        contracts = max(1, int((usdt * lev) / (ct_val * ref_price)))

        # Position mode
        is_hedge = False
        try:
            _pm_resp = _trade_get("/api/v5/account/config", {}, cfg)
            if _pm_resp.get("code") == "0":
                _pm      = _pm_resp.get("data", [{}])[0].get("posMode", "net_mode")
                is_hedge = (_pm == "long_short_mode")
                _cs = getattr(_b, "_bsc_api_conn_status", None)
                if _cs is not None:
                    _cs["pos_mode"] = _pm
        except Exception:
            is_hedge = (getattr(_b, "_bsc_api_conn_status", {})
                        .get("pos_mode", "net_mode") == "long_short_mode")

        # Set leverage
        try:
            _set_leverage_okx(sym, cfg)
        except Exception as lev_exc:
            _append_error("trade", f"set-leverage warning: {lev_exc}", symbol=sym)

        # ── Build order body ──────────────────────────────────────────────────
        # LIMIT: embed TP/SL via attachAlgoOrds in the same request.
        #   OKX activates the algo when the limit order fills — no race condition.
        # MARKET: two-step (entry → OCO), same as auto-trading.
        if entry > 0:
            # LIMIT order with TP/SL attached inline
            order_body: dict = {
                "instId":  _to_okx(sym),
                "tdMode":  mode,
                "side":    "buy",
                "ordType": "limit",
                "px":      str(_pround(entry)),
                "sz":      str(contracts),
                "attachAlgoOrds": [{
                    "attachAlgoClOrdId": str(uuid.uuid4()).replace("-","")[:24],
                    "tpTriggerPx":       str(_pround(tp)),
                    "tpOrdPx":           "-1",
                    "slTriggerPx":       str(_pround(sl)),
                    "slOrdPx":           "-1",
                }],
            }
            if is_hedge:
                order_body["posSide"] = "long"

            resp   = _trade_post("/api/v5/trade/order", order_body, cfg)
            _b._bsc_last_trade_raw = {
                "endpoint":  "/api/v5/trade/order (LIMIT + attachAlgoOrds)",
                "body_sent": order_body,
                "response":  resp,
                "is_hedge":  is_hedge,
                "contracts": contracts,
                "ct_val":    ct_val,
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
                err = f"Limit order: {d0.get('sCode')}: {d0.get('sMsg','')}"
                _append_error("trade", f"{err} | body={json.dumps(order_body)}",
                              symbol=sym, endpoint="/api/v5/trade/order")
                return {"ordId": ord_id, "algoId": "", "sz": contracts,
                        "status": "error", "error": err}

            return {"ordId": ord_id, "algoId": "", "sz": contracts,
                    "status": "placed", "error": "",
                    "actual_entry": entry, "actual_tp": tp, "actual_sl": sl}

        else:
            # MARKET order — two-step: entry then OCO
            order_body = {
                "instId":  _to_okx(sym),
                "tdMode":  mode,
                "side":    "buy",
                "ordType": "market",
                "sz":      str(contracts),
            }
            if is_hedge:
                order_body["posSide"] = "long"

            resp   = _trade_post("/api/v5/trade/order", order_body, cfg)
            _b._bsc_last_trade_raw = {
                "endpoint":  "/api/v5/trade/order (MARKET)",
                "body_sent": order_body,
                "response":  resp,
                "is_hedge":  is_hedge,
                "contracts": contracts,
                "ct_val":    ct_val,
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
                err = f"Market order: {d0.get('sCode')}: {d0.get('sMsg','')}"
                _append_error("trade", f"{err} | body={json.dumps(order_body)}",
                              symbol=sym, endpoint="/api/v5/trade/order")
                return {"ordId": ord_id, "algoId": "", "sz": contracts,
                        "status": "error", "error": err}

            # OCO with user-specified TP/SL (no recalculation for manual orders)
            algo_body: dict = {
                "instId":      _to_okx(sym),
                "tdMode":      mode,
                "side":        "sell",
                "ordType":     "oco",
                "sz":          str(contracts),
                "tpTriggerPx": str(_pround(tp)),
                "tpOrdPx":     "-1",
                "slTriggerPx": str(_pround(sl)),
                "slOrdPx":     "-1",
            }
            if is_hedge:
                algo_body["posSide"] = "long"

            algo_resp = _trade_post("/api/v5/trade/order-algo", algo_body, cfg)
            _b._bsc_last_trade_raw["algo_body_sent"] = algo_body
            _b._bsc_last_trade_raw["algo_response"]  = algo_resp
            ad      = (algo_resp.get("data") or [{}])[0]
            algo_id = ad.get("algoId", "")

            if algo_resp.get("code") != "0" or (ad.get("sCode","0") != "0" and ad.get("sCode","")):
                algo_err = _okx_err(algo_resp) if algo_resp.get("code") != "0" \
                           else f"OCO: {ad.get('sCode')}: {ad.get('sMsg','')}"
                _append_error("trade", f"OCO algo failed: {algo_err}",
                              symbol=sym, endpoint="/api/v5/trade/order-algo")
                return {"ordId": ord_id, "algoId": "", "sz": contracts,
                        "status": "partial",
                        "error": f"Entry ✅ · TP/SL ❌ {algo_err}",
                        "actual_entry": 0, "actual_tp": tp, "actual_sl": sl}

            return {"ordId": ord_id, "algoId": algo_id, "sz": contracts,
                    "status": "placed", "error": "",
                    "actual_entry": 0, "actual_tp": tp, "actual_sl": sl}

    except Exception as exc:
        _append_error("trade", str(exc), symbol=sym, endpoint="/api/v5/trade/order")
        return {"ordId": "", "algoId": "", "sz": 0,
                "status": "error", "error": str(exc)}

# ─────────────────────────────────────────────────────────────────────────────
# A — Bulk ticker fetch + pre-filter (single API call)
# ─────────────────────────────────────────────────────────────────────────────
def get_bulk_tickers() -> dict:
    """
    ONE API call → dict {BTCUSDT: {last, open24h, high24h, low24h, volCcy24h}}
    for every USDT-SWAP pair on OKX.
    """
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
    Zero extra API calls — uses data already in the bulk ticker snapshot.

    Keeps a coin only when:
      1. 24 h USDT volume ≥ PRE_FILTER_MIN_VOL_USDT  (liquid market)
      2. Last price ≥ 24 h low × PRE_FILTER_LOW_BUFFER (off the lows)
    """
    kept = []
    for sym in symbols:
        t = tickers.get(sym)
        if not t:
            continue
        last     = t["last"]
        low24h   = t["low24h"]
        vol_usdt = t["volCcy24h"]

        if vol_usdt < PRE_FILTER_MIN_VOL_USDT:
            continue

        if low24h > 0 and last < low24h * PRE_FILTER_LOW_BUFFER:
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
# Technical indicators
# ─────────────────────────────────────────────────────────────────────────────

def _pround(x, sig=6):
    """Round price to `sig` significant figures — handles micro-priced tokens.
    Prevents ultra-low prices (e.g. SATS at 0.000000003) from rounding to 0."""
    try:
        x = float(x)
        if x == 0:
            return 0.0
        magnitude = math.floor(math.log10(abs(x)))
        decimals  = max(2, sig - int(magnitude))
        return round(x, decimals)
    except Exception:
        return x

def calc_rsi_series(closes, period=14):
    if len(closes) < period + 2: return []
    deltas = [closes[i]-closes[i-1] for i in range(1, len(closes))]
    gains  = [max(d, 0.) for d in deltas]
    losses = [max(-d, 0.) for d in deltas]
    ag = sum(gains[:period]) / period
    al = sum(losses[:period]) / period
    rsi = [100. if al == 0 else 100 - 100 / (1 + ag / al)]
    for i in range(period, len(deltas)):
        ag = (ag*(period-1)+gains[i])/period
        al = (al*(period-1)+losses[i])/period
        rsi.append(100. if al == 0 else 100 - 100/(1+ag/al))
    return rsi

def calc_ema(values: list, period: int) -> list:
    if len(values) < period: return []
    k      = 2.0 / (period + 1)
    result = [sum(values[:period]) / period]
    for v in values[period:]:
        result.append(v * k + result[-1] * (1 - k))
    return result

def calc_macd(closes: list, fast=12, slow=26, signal_period=9):
    if len(closes) < slow + signal_period: return [], [], []
    ema_f = calc_ema(closes, fast)
    ema_s = calc_ema(closes, slow)
    trim  = len(ema_f) - len(ema_s)
    ema_f = ema_f[trim:]
    macd_line = [f-s for f,s in zip(ema_f, ema_s)]
    if len(macd_line) < signal_period: return [], [], []
    sig_line     = calc_ema(macd_line, signal_period)
    trim2        = len(macd_line) - len(sig_line)
    macd_aligned = macd_line[trim2:]
    histogram    = [m-s for m,s in zip(macd_aligned, sig_line)]
    return macd_aligned, sig_line, histogram

def macd_bullish(closes: list, crossover_lookback: int = 12) -> bool:
    """
    True when (all on given closes):
      1. MACD line > 0
      2. Signal line > 0
      3. Histogram > 0 AND increasing  ← dark green only
      4. Bullish crossover within last crossover_lookback candles
    """
    macd_line, sig_line, histogram = calc_macd(closes)
    if not histogram or len(histogram) < 2: return False
    if macd_line[-1] <= 0 or sig_line[-1] <= 0: return False
    if histogram[-1] <= 0 or histogram[-1] <= histogram[-2]: return False
    n = min(crossover_lookback + 1, len(macd_line))
    for i in range(1, n):
        prev, curr = -(i+1), -i
        if (len(macd_line)+prev >= 0 and
                macd_line[prev] <= sig_line[prev] and
                macd_line[curr] >  sig_line[curr]):
            return True
    return False

def calc_parabolic_sar(candles: list, af_start=0.02, af_step=0.02, af_max=0.20):
    if not candles: return []
    if len(candles) < 2: return [(candles[0]["close"], True)]
    highs  = [c["high"]  for c in candles]
    lows   = [c["low"]   for c in candles]
    closes = [c["close"] for c in candles]
    bullish = closes[1] >= closes[0]
    ep, sar, af = (highs[0], lows[0], af_start) if bullish else (lows[0], highs[0], af_start)
    result = [(sar, bullish)]
    for i in range(1, len(candles)):
        new_sar = sar + af*(ep-sar)
        if bullish:
            new_sar = min(new_sar, lows[i-1])
            if i >= 2: new_sar = min(new_sar, lows[i-2])
            if lows[i] < new_sar:
                bullish, new_sar, ep, af = False, ep, lows[i], af_start
            else:
                if highs[i] > ep: ep = highs[i]; af = min(af+af_step, af_max)
        else:
            new_sar = max(new_sar, highs[i-1])
            if i >= 2: new_sar = max(new_sar, highs[i-2])
            if highs[i] > new_sar:
                bullish, new_sar, ep, af = True, ep, highs[i], af_start
            else:
                if lows[i] < ep: ep = lows[i]; af = min(af+af_step, af_max)
        sar = new_sar
        result.append((sar, bullish))
    return result

# ─────────────────────────────────────────────────────────────────────────────
# F12 — Premium / Discount / Equilibrium zone  (DZSAFM Pine Script logic)
# ─────────────────────────────────────────────────────────────────────────────
def calc_pdz_zone(candles: list, price: float) -> tuple:
    """
    Compute Smart Money Premium/Discount/Equilibrium zones from up to the last
    50 candles on a given timeframe (mirrors the DZSAFM TradingView indicator exactly).

    Zone boundaries  (H = swing high, L = swing low over last 50 candles):
      Premium zone    : price >= 0.95·H + 0.05·L     ← top 5% of range
      Equilibrium zone: 0.475·H+0.525·L ≤ price ≤ 0.525·H+0.475·L
      Discount zone   : price ≤ 0.05·H + 0.95·L      ← bottom 5% of range

    Qualification logic (LONG trades only):
      • Discount zone                         → QUALIFIES  (greatest room to pump)
      • Equilibrium zone                      → REJECTED   (indecision, risky)
      • Premium zone                          → REJECTED   (no upward room)
      • Band A (equil_top < price < prem_bot) → QUALIFIES if room ≥ 1.5 %
      • Band B (disc_top  < price < equil_bot)→ QUALIFIES if room ≥ 1.5 %

    Returns (qualifies: bool, zone_label: str)
    """
    if not candles or len(candles) < 2:
        return True, "unknown"

    lookback = candles[-290:]  # 290 candles — 1 OKX API call (~72 hrs on 15m, ~24 hrs on 5m)
    H = max(c["high"] for c in lookback)
    L = min(c["low"]  for c in lookback)

    if H <= L:
        return True, "unknown"

    # Boundary levels  (exact Pine Script maths from DZSAFM)
    premium_bottom = 0.95 * H + 0.05 * L    # bottom edge of Premium zone
    equil_top      = 0.525 * H + 0.475 * L  # top edge of Equilibrium zone
    equil_bottom   = 0.475 * H + 0.525 * L  # bottom edge of Equilibrium zone
    discount_top   = 0.05  * H + 0.95  * L  # top edge of Discount zone

    band_a_ceil = premium_bottom * (1 - 0.015)   # 1.5% below Premium boundary
    band_b_ceil = equil_bottom   * (1 - 0.015)   # 1.5% below Equilibrium boundary

    if price <= discount_top:
        # Fully inside Discount zone — best long setup
        return True, "Discount"
    elif price >= premium_bottom:
        # Fully inside Premium zone — no upward room
        return False, "Premium"
    elif equil_bottom <= price <= equil_top:
        # Equilibrium band — can go either way, skip
        return False, "Equilibrium"
    elif equil_top < price < premium_bottom:
        # Band A: qualifies only if price is at least 1.5% below the Premium boundary
        dist_pct = (premium_bottom - price) / premium_bottom * 100
        label    = f"BandA({dist_pct:.1f}%\u2193Prem)"
        return (price <= band_a_ceil), label
    else:
        # Band B: qualifies only if price is at least 1.5% below the Equilibrium boundary
        dist_pct = (equil_bottom - price) / equil_bottom * 100
        label    = f"BandB({dist_pct:.1f}%\u2193Equil)"
        return (price <= band_b_ceil), label


# ─────────────────────────────────────────────────────────────────────────────
# Filter funnel counter
# ─────────────────────────────────────────────────────────────────────────────

# ─────────────────────────────────────────────────────────────────────────────
# Filter funnel counter
# ─────────────────────────────────────────────────────────────────────────────
def _reset_filter_counts():
    counts = {
        "total_watchlist":  0,
        "pre_filtered_out": 0,
        "checked":          0,
        # ── elimination counters (new F-order) ────────────────────────────────
        "f2_pdz15m":        0,   # F2 — PDZ 15m
        "f3_pdz5m":         0,   # F3 — PDZ 5m
        "f4_rsi5m":         0,   # F4 — 5m RSI
        "f5_rsi1h":         0,   # F5 — 1h RSI
        "f6_ema":           0,   # F6 — EMA
        "f7_macd":          0,   # F7 — MACD
        "f8_sar":           0,   # F8 — SAR
        "f9_vol":           0,   # F9 — Vol Spike
        "f_empty_data":     0,   # Stage 2 candle fetch returned empty for a timeframe
        "passed":           0,
        "super_setup":      0,   # subset of passed — 15m Discount instant buys
        "errors":           0,
        "scan_cfg":         {},
        # ── per-stage symbol lists ─────────────────────────────────────────────
        "pre_filter_passed_syms": [],
        "checked_syms":           [],
        "f2_elim_syms":           [],
        "f3_elim_syms":           [],
        "f4_elim_syms":           [],
        "f5_elim_syms":           [],
        "f6_elim_syms":           [],
        "f7_elim_syms":           [],
        "f8_elim_syms":           [],
        "f9_elim_syms":           [],
        "f_empty_data_syms":      [],
        "passed_syms":            [],
        "super_setup_syms":       [],
    }
    with _filter_lock:
        _filter_counts.clear()
        _filter_counts.update(counts)
        _b._bsc_filter_counts = _filter_counts   # keep reference in sync under lock

# ─────────────────────────────────────────────────────────────────────────────
# Per-coin processing
# ─────────────────────────────────────────────────────────────────────────────
def process(sym, cfg: dict):
    try:
        with _filter_lock:
            _filter_counts["checked"] = _filter_counts.get("checked", 0) + 1
            _filter_counts["checked_syms"].append(sym)

        # ── Stage 1: Quick parallel fetch — 5m (entry/RSI) + 15m (PDZ) ───────
        with ThreadPoolExecutor(max_workers=2) as pool:
            f_5m_q  = pool.submit(get_klines, sym, "5m",  300)
            f_15m_q = pool.submit(get_klines, sym, "15m", 300)
            m5_quick  = f_5m_q.result()[:-1]
            m15_quick = f_15m_q.result()[:-1]

        closes_5m_q = [c["close"] for c in m5_quick]
        entry_q     = _pround(m5_quick[-1]["close"])

        # ── F2: PDZ 15m — checked FIRST (Super Setup possible) ───────────────
        pdz_zone_15m   = "—"
        is_super_setup = False
        if cfg.get("use_pdz_15m", True):
            pdz_pass_15m, pdz_zone_15m = calc_pdz_zone(m15_quick, entry_q)
            if pdz_zone_15m == "Discount":
                is_super_setup = True          # ⭐ skip all remaining filters
            elif not pdz_pass_15m:
                with _filter_lock:
                    _filter_counts["f2_pdz15m"] = _filter_counts.get("f2_pdz15m", 0) + 1
                    _filter_counts["f2_elim_syms"].append(sym)
                return None
            # else Band A/B passes — continue to F3

        # ── SUPER SETUP — 15m Discount → instant trade, no further checks ─────
        if is_super_setup:
            tp      = _pround(entry_q * (1 + cfg["tp_pct"] / 100))
            sl      = _pround(entry_q * (1 - cfg["sl_pct"] / 100))
            sec     = SECTORS.get(sym, "Other")
            max_lev = get_max_leverage(sym)
            with _filter_lock:
                _filter_counts["passed"]           = _filter_counts.get("passed", 0) + 1
                _filter_counts["super_setup"]      = _filter_counts.get("super_setup", 0) + 1
                _filter_counts["passed_syms"].append(sym)
                _filter_counts["super_setup_syms"].append(sym)
            return {
                "id":             str(uuid.uuid4())[:8],
                "timestamp":      dubai_now().isoformat(),
                "symbol":         sym,
                "entry":          entry_q,
                "tp":             tp,
                "sl":             sl,
                "sector":         sec,
                "status":         "open",
                "close_price":    None,
                "close_time":     None,
                "max_lev":        max_lev,
                "is_super_setup": True,
                "criteria": {
                    "rsi_5m": "—", "rsi_1h": "—",
                    "ema_3m": "—", "ema_5m": "—", "ema_15m": "—",
                    "macd_3m": "—", "macd_5m": "—", "macd_15m": "—",
                    "sar_3m": "—", "sar_5m": "—", "sar_15m": "—",
                    "vol_ratio": "—",
                    "pdz_zone_5m":  "—",
                    "pdz_zone_15m": pdz_zone_15m,
                },
            }

        # ── F3: PDZ 5m ────────────────────────────────────────────────────────
        pdz_zone_5m = "—"
        if cfg.get("use_pdz_5m", True):
            pdz_pass_5m, pdz_zone_5m = calc_pdz_zone(m5_quick, entry_q)
            if not pdz_pass_5m:
                with _filter_lock:
                    _filter_counts["f3_pdz5m"] = _filter_counts.get("f3_pdz5m", 0) + 1
                    _filter_counts["f3_elim_syms"].append(sym)
                return None

        # ── F4: Quick 5m RSI (still early — before full fetch) ────────────────
        rsi5_q = (calc_rsi_series(closes_5m_q) or [0])[-1]
        if cfg.get("use_rsi_5m", True) and rsi5_q < cfg["rsi_5m_min"]:
            with _filter_lock:
                _filter_counts["f4_rsi5m"] = _filter_counts.get("f4_rsi5m", 0) + 1
                _filter_counts["f4_elim_syms"].append(sym)
            return None

        # ── Stage 2: Full parallel fetch (EMA/MACD/SAR/RSI1h need more data) ─
        candle_limit_5m  = 210 if (cfg.get("use_ema_5m")  or cfg.get("use_macd") or cfg.get("use_sar")) else 51
        candle_limit_15m = 210 if (cfg.get("use_ema_15m") or cfg.get("use_macd") or cfg.get("use_sar")) else 51
        candle_limit_3m  =  80 if (cfg.get("use_ema_3m")  or cfg.get("use_macd") or cfg.get("use_sar")) else 30

        with ThreadPoolExecutor(max_workers=4) as pool:
            f_5m  = pool.submit(get_klines, sym, "5m",  candle_limit_5m)
            f_15m = pool.submit(get_klines, sym, "15m", candle_limit_15m)
            f_1h  = pool.submit(get_klines, sym, "1h",  19)
            f_3m  = pool.submit(get_klines, sym, "3m",  candle_limit_3m)
            m5          = f_5m.result()[:-1]
            m15         = f_15m.result()[:-1]
            m1h_candles = f_1h.result()[:-1]
            m3_candles  = f_3m.result()[:-1]

        # ── Guard: Stage 2 returned empty candles for one or more timeframes ─────
        # OKX can return {"code":"0","data":[]} for newly listed or illiquid coins.
        # Without this check the IndexError on m5[-1] is silently swallowed by the
        # outer except, the coin disappears from all _fXe buckets, and the filter
        # funnel falsely shows it as "passed all filters" while no trade is opened.
        _missing_tf = [tf for tf, bars in (("5m", m5), ("15m", m15),
                                            ("1h", m1h_candles), ("3m", m3_candles))
                       if not bars]
        if _missing_tf:
            with _filter_lock:
                _filter_counts["f_empty_data"] = _filter_counts.get("f_empty_data", 0) + 1
                _filter_counts["f_empty_data_syms"].append(
                    f"{sym}(no {','.join(_missing_tf)})")
            return None

        closes_5m  = [c["close"] for c in m5]
        closes_15m = [c["close"] for c in m15]
        closes_3m  = [c["close"] for c in m3_candles]
        closes_1h  = [c["close"] for c in m1h_candles]
        entry      = _pround(m5[-1]["close"])

        # ── F5: 1h RSI ────────────────────────────────────────────────────────
        rsi1h = (calc_rsi_series(closes_1h) or [0])[-1]
        if cfg.get("use_rsi_1h", True) and \
                not (cfg["rsi_1h_min"] <= rsi1h <= cfg["rsi_1h_max"]):
            with _filter_lock:
                _filter_counts["f5_rsi1h"] = _filter_counts.get("f5_rsi1h", 0) + 1
                _filter_counts["f5_elim_syms"].append(sym)
            return None

        # ── F6: EMA ───────────────────────────────────────────────────────────
        ema_3m_val = ema_5m_val = ema_15m_val = None
        if cfg.get("use_ema_3m"):
            ema = calc_ema(closes_3m, max(2, int(cfg.get("ema_period_3m", 12))))
            if not ema or entry < ema[-1]:
                with _filter_lock:
                    _filter_counts["f6_ema"] = _filter_counts.get("f6_ema", 0) + 1
                    _filter_counts["f6_elim_syms"].append(sym)
                return None
            ema_3m_val = _pround(ema[-1])
        if cfg.get("use_ema_5m"):
            ema = calc_ema(closes_5m, max(2, int(cfg.get("ema_period_5m", 12))))
            if not ema or entry < ema[-1]:
                with _filter_lock:
                    _filter_counts["f6_ema"] = _filter_counts.get("f6_ema", 0) + 1
                    _filter_counts["f6_elim_syms"].append(sym)
                return None
            ema_5m_val = _pround(ema[-1])
        if cfg.get("use_ema_15m"):
            ema = calc_ema(closes_15m, max(2, int(cfg.get("ema_period_15m", 12))))
            if not ema or entry < ema[-1]:
                with _filter_lock:
                    _filter_counts["f6_ema"] = _filter_counts.get("f6_ema", 0) + 1
                    _filter_counts["f6_elim_syms"].append(sym)
                return None
            ema_15m_val = _pround(ema[-1])

        # ── F7: MACD dark-green ───────────────────────────────────────────────
        macd_3m_val = macd_5m_val = macd_15m_val = None
        if cfg.get("use_macd"):
            if not (macd_bullish(closes_3m) and
                    macd_bullish(closes_5m)  and
                    macd_bullish(closes_15m)):
                with _filter_lock:
                    _filter_counts["f7_macd"] = _filter_counts.get("f7_macd", 0) + 1
                    _filter_counts["f7_elim_syms"].append(sym)
                return None
            ml3,  _, _ = calc_macd(closes_3m)
            ml5,  _, _ = calc_macd(closes_5m)
            ml15, _, _ = calc_macd(closes_15m)
            if ml3:  macd_3m_val  = round(ml3[-1],  8)
            if ml5:  macd_5m_val  = round(ml5[-1],  8)
            if ml15: macd_15m_val = round(ml15[-1], 8)

        # ── F8: Parabolic SAR ─────────────────────────────────────────────────
        sar_3m_val = sar_5m_val = sar_15m_val = None
        if cfg.get("use_sar"):
            sar_3m  = calc_parabolic_sar(m3_candles)
            sar_5m  = calc_parabolic_sar(m5)
            sar_15m = calc_parabolic_sar(m15)
            if not (sar_3m  and sar_3m[-1][1]  and
                    sar_5m  and sar_5m[-1][1]  and
                    sar_15m and sar_15m[-1][1]):
                with _filter_lock:
                    _filter_counts["f8_sar"] = _filter_counts.get("f8_sar", 0) + 1
                    _filter_counts["f8_elim_syms"].append(sym)
                return None
            sar_3m_val  = _pround(sar_3m[-1][0])
            sar_5m_val  = _pround(sar_5m[-1][0])
            sar_15m_val = _pround(sar_15m[-1][0])

        # ── F9: Volume Spike ──────────────────────────────────────────────────
        vol_ratio = None
        if cfg.get("use_vol_spike"):
            lookback = max(2, int(cfg.get("vol_spike_lookback", 20)))
            mult     = float(cfg.get("vol_spike_mult", 2.0))
            vols_15m = [c["volume"] for c in m15]
            if len(vols_15m) >= lookback + 1:
                window   = vols_15m[-(lookback+1):-1]
                avg_vol  = sum(window) / len(window)
                if avg_vol <= 0 or vols_15m[-1] < mult * avg_vol:
                    with _filter_lock:
                        _filter_counts["f9_vol"] = _filter_counts.get("f9_vol", 0) + 1
                        _filter_counts["f9_elim_syms"].append(sym)
                    return None
                vol_ratio = round(vols_15m[-1] / avg_vol, 2) if avg_vol > 0 else None

        # ── All filters passed ────────────────────────────────────────────────
        tp      = _pround(entry * (1 + cfg["tp_pct"] / 100))
        sl      = _pround(entry * (1 - cfg["sl_pct"] / 100))
        sec     = SECTORS.get(sym, "Other")
        max_lev = get_max_leverage(sym)

        with _filter_lock:
            _filter_counts["passed"] = _filter_counts.get("passed", 0) + 1
            _filter_counts["passed_syms"].append(sym)

        rsi5 = (calc_rsi_series(closes_5m) or [rsi5_q])[-1]
        criteria = {
            "rsi_5m":       round(rsi5,  1),
            "rsi_1h":       round(rsi1h, 1),
            "ema_3m":       ema_3m_val  if cfg.get("use_ema_3m")    else "—",
            "ema_5m":       ema_5m_val  if cfg.get("use_ema_5m")    else "—",
            "ema_15m":      ema_15m_val if cfg.get("use_ema_15m")   else "—",
            "macd_3m":      macd_3m_val  if cfg.get("use_macd")     else "—",
            "macd_5m":      macd_5m_val  if cfg.get("use_macd")     else "—",
            "macd_15m":     macd_15m_val if cfg.get("use_macd")     else "—",
            "sar_3m":       sar_3m_val   if cfg.get("use_sar")      else "—",
            "sar_5m":       sar_5m_val   if cfg.get("use_sar")      else "—",
            "sar_15m":      sar_15m_val  if cfg.get("use_sar")      else "—",
            "vol_ratio":    vol_ratio    if cfg.get("use_vol_spike") else "—",
            "pdz_zone_5m":  pdz_zone_5m  if cfg.get("use_pdz_5m",  True) else "—",
            "pdz_zone_15m": pdz_zone_15m if cfg.get("use_pdz_15m", True) else "—",
        }

        return {
            "id":             str(uuid.uuid4())[:8],
            "timestamp":      dubai_now().isoformat(),
            "symbol":         sym,
            "entry":          entry,
            "tp":             tp,
            "sl":             sl,
            "sector":         sec,
            "status":         "open",
            "close_price":    None,
            "close_time":     None,
            "max_lev":        max_lev,
            "is_super_setup": False,
            "criteria":       criteria,
        }

    except Exception as _proc_exc:
        with _filter_lock:
            _filter_counts["errors"] = _filter_counts.get("errors", 0) + 1
        _append_error("scan", str(_proc_exc), symbol=sym)
        return "error"

def scan(cfg: dict):
    _reset_filter_counts()
    with _filter_lock:                          # store config used for this scan
        _filter_counts["scan_cfg"] = dict(cfg)

    # D — cached symbol list (one instrument API call max every 6 h)
    symbols = get_symbols_cached(cfg["watchlist"])
    with _filter_lock:
        _filter_counts["total_watchlist"] = len(symbols)

    # A — bulk ticker pre-filter (1 API call → eliminates ~70 % of coins)
    tickers = get_bulk_tickers()
    if cfg.get("use_pre_filter", True):
        pre_filtered = pre_filter_by_ticker(symbols, tickers)
    else:
        pre_filtered = list(symbols)   # pre-filter disabled — deep-scan all
    with _filter_lock:
        _filter_counts["pre_filtered_out"] = len(symbols) - len(pre_filtered)
        _filter_counts["pre_filter_passed_syms"] = list(pre_filtered)

    print(f"[Scan] {len(symbols)} valid symbols → "
          f"{len(pre_filtered)} after bulk pre-filter "
          f"({'disabled' if not cfg.get('use_pre_filter', True) else _filter_counts['pre_filtered_out']} removed)")

    results = []
    # 6 outer workers (reduced from 8 to stay safely under rate limit with inner parallel fetches)
    with ThreadPoolExecutor(max_workers=6) as exe:
        futs = [exe.submit(process, s, cfg) for s in pre_filtered]
        for f in as_completed(futs):
            r = f.result()
            if r and r != "error":
                results.append(r)

    return sorted(results, key=lambda x: x["symbol"]), _filter_counts.get("errors", 0)

# ─────────────────────────────────────────────────────────────────────────────
# Open signal tracker
# ─────────────────────────────────────────────────────────────────────────────
def update_open_signals(signals):
    for sig in signals:
        if sig["status"] != "open": continue
        try:
            sig_ts_ms = int(datetime.fromisoformat(sig["timestamp"]).timestamp() * 1000)
            candles   = get_klines(sig["symbol"], "5m", 200)
            post      = [c for c in candles if c["time"] >= sig_ts_ms]
            tp_time = sl_time = None
            for c in post:
                if tp_time is None and c["high"] >= sig["tp"]: tp_time = c["time"]
                if sl_time is None and c["low"]  <= sig["sl"]: sl_time = c["time"]
            if tp_time is not None or sl_time is not None:
                if tp_time is not None and (sl_time is None or tp_time <= sl_time):
                    sig.update(status="tp_hit", close_price=sig["tp"],
                               close_time=to_dubai(datetime.fromtimestamp(
                                   tp_time/1000, tz=timezone.utc)).isoformat())
                else:
                    sig.update(status="sl_hit", close_price=sig["sl"],
                               close_time=to_dubai(datetime.fromtimestamp(
                                   sl_time/1000, tz=timezone.utc)).isoformat())
        except Exception:
            pass
    return signals

# ─────────────────────────────────────────────────────────────────────────────
# Background scanner thread
# ─────────────────────────────────────────────────────────────────────────────
def _bg_loop():
    while True:
        if not _scanner_running.is_set():
            time.sleep(2); continue
        with _config_lock:
            cfg = dict(_b._bsc_cfg)
        t0 = time.time()
        try:
            with _log_lock:
                _b._bsc_log["signals"] = update_open_signals(_b._bsc_log["signals"])
            new_sigs, errors = scan(cfg)
            cutoff = dubai_now() - timedelta(minutes=cfg["cooldown_minutes"])
            with _log_lock:
                # Cooldown measured from close_time of last closed trade (TP/SL hit)
                cooled = {s["symbol"] for s in _b._bsc_log["signals"]
                          if s.get("close_time")
                          and s.get("status") in ("tp_hit", "sl_hit")
                          and datetime.fromisoformat(s["close_time"].replace("Z","+00:00")) >= cutoff}
                active = {s["symbol"] for s in _b._bsc_log["signals"] if s["status"]=="open"}
                _new_sig_syms      = [s["symbol"] for s in new_sigs]
                _blocked_active    = sorted(active  & set(_new_sig_syms))
                _blocked_cooldown  = sorted(cooled  & set(_new_sig_syms))
                skip         = cooled | active
                _added_syms  = []
                sigs_to_trade = []          # signals that need an order placed
                for sig in new_sigs:
                    if sig["symbol"] not in skip:
                        _b._bsc_log["signals"].append(sig)
                        skip.add(sig["symbol"])
                        _added_syms.append(sig["symbol"])
                        if (cfg.get("trade_enabled") and
                                cfg.get("api_key") and cfg.get("api_secret") and
                                cfg.get("api_passphrase")):
                            sigs_to_trade.append(sig)
                _filter_counts["new_signal_syms"]         = _added_syms
                _filter_counts["blocked_by_active_syms"]  = _blocked_active
                _filter_counts["blocked_by_cooldown_syms"]= _blocked_cooldown
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

            # ── Place orders outside the log lock ────────────────────────────
            # sigs_to_trade are already appended to _b._bsc_log["signals"],
            # so in-place dict updates here are reflected in the stored log.
            if sigs_to_trade:
                for sig in sigs_to_trade:
                    result = place_okx_order(sig, cfg)
                    sig["order_id"]     = result.get("ordId", "")
                    sig["algo_id"]      = result.get("algoId", "")
                    sig["order_sz"]     = result.get("sz", 0)
                    sig["order_status"] = result.get("status", "")
                    sig["order_error"]  = result.get("error", "")
                    sig["trade_usdt"]   = float(cfg.get("trade_usdt_amount", 0))
                    sig["trade_lev"]    = int(cfg.get("trade_leverage", 10))
                    sig["demo_mode"]    = bool(cfg.get("demo_mode", True))
                    # Overwrite entry/TP/SL with actual fill values if available.
                    # Market orders fill at the live price, not the signal price —
                    # the OCO algo order was placed using these corrected levels.
                    if result.get("actual_entry"):
                        sig["entry"]        = result["actual_entry"]
                        sig["tp"]           = result["actual_tp"]
                        sig["sl"]           = result["actual_sl"]
                        sig["signal_entry"] = sig.get("entry")  # keep original for reference
                with _log_lock:
                    save_log(_b._bsc_log)
            _b._bsc_last_error = ""
        except Exception as e:
            _b._bsc_last_error = str(e)
            _append_error("loop", str(e))
        elapsed   = time.time() - t0
        sleep_sec = max(0, cfg["loop_minutes"] * 60 - elapsed)
        # Wait for sleep_sec, but wake immediately if Save & Apply triggers rescan
        _rescan_event.wait(timeout=sleep_sec)
        _rescan_event.clear()

def _ensure_scanner():
    if _b._bsc_thread is None or not _b._bsc_thread.is_alive():
        t = threading.Thread(target=_bg_loop, daemon=True, name="okx-scanner")
        t.start(); _b._bsc_thread = t

# ─────────────────────────────────────────────────────────────────────────────
# STREAMLIT UI
# ─────────────────────────────────────────────────────────────────────────────
st.set_page_config(page_title="Crypto Demo Trades", page_icon="🔍",
                   layout="wide", initial_sidebar_state="expanded")
_ensure_scanner()

with _log_lock:    _snap_log = json.loads(json.dumps(_b._bsc_log))
with _config_lock: _snap_cfg = dict(_b._bsc_cfg)

health  = _snap_log.get("health", {})
signals = _snap_log.get("signals", [])

# ─────────────────────────────────────────────────────────────────────────────
# SIDEBAR
# ─────────────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## ⚙️ Configuration")
    running   = _scanner_running.is_set()
    btn_label = "⏹ Stop Scanner" if running else "▶️ Start Scanner"
    if st.button(btn_label, use_container_width=True, type="primary" if running else "secondary"):
        if running: _scanner_running.clear()
        else:       _scanner_running.set()
        st.rerun()
    st.caption(f"{'🟢' if running else '🔴'}  Scanner {'running' if running else 'stopped'}")
    st.divider()

    # ── Auto-Trading ───────────────────────────────────────────────────────────
    st.markdown("**🤖 Auto-Trading**")
    new_trade_enabled = st.checkbox(
        "Enable Auto-Trading",
        value=bool(_snap_cfg.get("trade_enabled", False)), key="cfg_trade_enabled",
        help="Automatically place a market LONG order on OKX when a signal fires.")

    new_demo_mode = st.radio(
        "Environment", ["Demo", "Live"],
        index=0 if _snap_cfg.get("demo_mode", True) else 1,
        horizontal=True, key="cfg_demo_mode",
        help="Demo uses x-simulated-trading header. Live places real orders.",
        disabled=not new_trade_enabled)
    if new_trade_enabled and new_demo_mode == "Live":
        st.warning("⚠️ LIVE mode — real funds at risk!")

    new_api_key = st.text_input(
        "API Key", value=_snap_cfg.get("api_key", ""),
        key="cfg_api_key", disabled=not new_trade_enabled,
        placeholder="Your OKX API key")
    new_api_secret = st.text_input(
        "API Secret", value=_snap_cfg.get("api_secret", ""),
        key="cfg_api_secret", type="password", disabled=not new_trade_enabled,
        placeholder="Your OKX API secret")
    new_api_passphrase = st.text_input(
        "API Passphrase", value=_snap_cfg.get("api_passphrase", ""),
        key="cfg_api_passphrase", type="password", disabled=not new_trade_enabled,
        placeholder="Your OKX API passphrase")

    ta1, ta2 = st.columns(2)
    new_trade_usdt = ta1.number_input(
        "Size (USDT)", min_value=1.0, max_value=100000.0, step=1.0,
        value=float(_snap_cfg.get("trade_usdt_amount", 10.0)),
        key="cfg_trade_usdt", disabled=not new_trade_enabled,
        help="Collateral per trade in USDT (before leverage)")
    new_trade_lev = ta2.number_input(
        "Leverage ×", min_value=1, max_value=125, step=1,
        value=int(_snap_cfg.get("trade_leverage", 10)),
        key="cfg_trade_lev", disabled=not new_trade_enabled,
        help="Leverage applied (capped by OKX max for each coin)")
    new_margin_mode = st.selectbox(
        "Margin Mode", ["cross", "isolated"],
        index=0 if _snap_cfg.get("trade_margin_mode", "cross") == "cross" else 1,
        key="cfg_margin_mode", disabled=not new_trade_enabled)

    if new_trade_enabled:
        notional_usdt = new_trade_usdt * new_trade_lev
        st.caption(f"📐 Notional per trade: ~${notional_usdt:,.0f} USDT   "
                   f"| TP +${notional_usdt * _snap_cfg.get('tp_pct',1.5)/100:.2f}   "
                   f"| SL -${notional_usdt * _snap_cfg.get('sl_pct',3.0)/100:.2f}")
        st.caption("🔒 Credentials stored in scanner_config.json. "
                   "Use a trade-only API key — never withdrawal permissions.")

    # ── Connection test ────────────────────────────────────────────────────────
    _conn = getattr(_b, "_bsc_api_conn_status",
                    {"status": "untested", "message": "", "tested_at": None,
                     "demo_mode": None, "uid": ""})
    _has_creds = bool(_snap_cfg.get("api_key") and _snap_cfg.get("api_secret")
                      and _snap_cfg.get("api_passphrase"))
    if st.button("🔌 Test Connection", use_container_width=True,
                 disabled=not _has_creds,
                 help="Verify your API credentials against OKX right now."):
        with st.spinner("Testing…"):
            _result = test_api_connection(dict(_snap_cfg))
        _b._bsc_api_conn_status = {
            "status":    _result["status"],
            "message":   _result["message"],
            "tested_at": dubai_now().isoformat(),
            "demo_mode": _snap_cfg.get("demo_mode", True),
            "uid":       _result.get("uid", ""),
            "pos_mode":  _result.get("pos_mode", "net_mode"),
            "acct_lv":   _result.get("acct_lv", "2"),
        }
        st.rerun()

    # Show last test result
    _conn_now = getattr(_b, "_bsc_api_conn_status",
                        {"status": "untested", "message": "", "tested_at": None})
    _conn_status = _conn_now.get("status", "untested")
    if _conn_status == "ok":
        st.success(f"🟢 {_conn_now['message']}")
        if _conn_now.get("uid"):
            st.caption(f"UID: {_conn_now['uid']}  ·  tested {fmt_dubai(_conn_now['tested_at'])}")
    elif _conn_status == "error":
        st.error(f"🔴 {_conn_now['message']}")
        st.caption(f"Last tested: {fmt_dubai(_conn_now['tested_at'])}")
    else:
        if _has_creds:
            st.caption("⚫ Not tested yet — click Test Connection")
        else:
            st.caption("⚫ Enter API credentials above to enable")

    # Show detected position mode (critical for order placement)
    if _conn_status == "ok":
        _pm = _conn_now.get("pos_mode", "net_mode")
        if _pm == "long_short_mode":
            st.info("📐 Position mode: **Hedge (Long/Short)** — `posSide: long` added to orders automatically.")
        else:
            st.info("📐 Position mode: **Net** — standard order placement.")

    # ── Test Trade button (places + cancels a minimal BTCUSDT order) ──────────
    _conn_ok = _conn_status == "ok" and _snap_cfg.get("trade_enabled", False)
    if st.button("🧪 Test Trade (BTC · 1 contract)", use_container_width=True,
                 disabled=not _conn_ok,
                 help="Places a 1-contract market buy on BTCUSDT, then immediately cancels it. "
                      "Use this to confirm order placement works before signals fire."):
        with st.spinner("Placing test order…"):
            _tc = dict(_snap_cfg)
            _cs = getattr(_b, "_bsc_api_conn_status", {})
            _is_h = _cs.get("pos_mode", "net_mode") == "long_short_mode"
            _mode = _tc.get("trade_margin_mode", "cross")

            # Set leverage
            try:
                _set_leverage_okx("BTCUSDT", _tc)
            except Exception:
                pass

            _tbody: dict = {
                "instId":  "BTC-USDT-SWAP",
                "tdMode":  _mode,
                "side":    "buy",
                "ordType": "market",
                "sz":      "1",
            }
            if _is_h:
                _tbody["posSide"] = "long"

            _tresp = _trade_post("/api/v5/trade/order", _tbody, _tc)
            _b._bsc_last_trade_raw = {
                "endpoint":  "/api/v5/trade/order",
                "body_sent": _tbody,
                "response":  _tresp,
                "is_hedge":  _is_h,
            }

            # Immediately cancel if placed
            _td0    = (_tresp.get("data") or [{}])[0]
            _tordid = _td0.get("ordId", "")
            if _tresp.get("code") == "0" and _tordid:
                try:
                    _trade_post("/api/v5/trade/cancel-order",
                                {"instId": "BTC-USDT-SWAP", "ordId": _tordid}, _tc)
                except Exception:
                    pass
                st.session_state["_test_trade_result"] = ("ok", f"✅ Test order placed & cancelled · ordId: {_tordid}")
            else:
                _terr = _okx_err(_tresp)
                _append_error("trade", f"Test trade failed: {_terr} | body={json.dumps(_tbody)} | resp={json.dumps(_tresp)[:300]}", endpoint="/api/v5/trade/order")
                st.session_state["_test_trade_result"] = ("err", f"❌ {_terr}")

    _ttr = st.session_state.get("_test_trade_result")
    if _ttr:
        if _ttr[0] == "ok":
            st.success(_ttr[1])
        else:
            st.error(_ttr[1])

    # ── Raw response debug expander ───────────────────────────────────────────
    _raw = getattr(_b, "_bsc_last_trade_raw", {})
    if _raw:
        with st.expander("🔬 Last order raw response (debug)"):
            st.caption("Body sent to OKX:")
            st.json(_raw.get("body_sent", {}))
            st.caption("OKX response:")
            st.json(_raw.get("response", {}))
            st.caption(f"is_hedge={_raw.get('is_hedge')}  "
                       f"contracts={_raw.get('contracts','?')}  "
                       f"ct_val={_raw.get('ct_val','?')}")
    st.divider()

    st.markdown("**📊 Trade Settings**")
    c1, c2 = st.columns(2)
    new_tp = c1.number_input("TP %", min_value=0.1, max_value=20.0, step=0.1, value=float(_snap_cfg["tp_pct"]),    key="cfg_tp")
    new_sl = c2.number_input("SL %", min_value=0.1, max_value=20.0, step=0.1, value=float(_snap_cfg["sl_pct"]),    key="cfg_sl")
    st.divider()

    # ── F1: Bulk Pre-filter ────────────────────────────────────────────────────
    st.markdown("**⚡ F1 — Bulk Pre-filter**")
    new_use_pre_filter = st.checkbox(
        "Enable F1 — Bulk Pre-filter",
        value=bool(_snap_cfg.get("use_pre_filter", True)), key="cfg_use_pre_filter",
        help=(
            "F1 — Bulk Pre-filter\n\n"
            "One API call screens the entire watchlist before any candle fetching.\n"
            "✅ Keeps: 24h Vol ≥ 100,000 USDT  ·  Price ≥ 24h Low × 1.005\n"
            "❌ Drops: Low-volume coins · Coins hugging the 24h low\n\n"
            "Disabling forces a full deep-scan of every coin (much slower)."
        ))
    if new_use_pre_filter:
        st.caption("✅ Vol ≥ 100k USDT · Price ≥ 24h Low × 1.005")
    st.divider()

    # ── F2: PDZ 15m ────────────────────────────────────────────────────────────
    st.markdown("**🎯 F2 — PDZ Zones** (15m)")
    new_use_pdz_15m = st.checkbox(
        "Enable F2 — PDZ Filter (15m)",
        value=bool(_snap_cfg.get("use_pdz_15m", True)), key="cfg_use_pdz_15m",
        help=(
            "F2 — Premium / Discount / Equilibrium Zone Filter (15m)\n\n"
            "Same DZSAFM zone logic as F3, applied to last 50 × 15m candles.\n\n"
            "Passing both F3 (5m) and F2 (15m) confirms the coin is in a\n"
            "favourable Smart Money zone on both timeframes simultaneously.\n\n"
            "✅ Qualifies: Discount zone · Band A/B with ≥1.5% room to next zone\n"
            "❌ Rejected:  Equilibrium (indecision) · Premium (no upward room)"
        ))
    if new_use_pdz_15m:
        st.caption("✅ Discount · BandA(≥1.5%↓Premium) · BandB(≥1.5%↓Equilibrium) | ❌ Premium · Equilibrium")
    st.divider()

    # ── F3: PDZ 5m ─────────────────────────────────────────────────────────────
    st.markdown("**🎯 F3 — PDZ Zones** (5m)")
    new_use_pdz_5m = st.checkbox(
        "Enable F3 — PDZ Filter (5m)",
        value=bool(_snap_cfg.get("use_pdz_5m", True)), key="cfg_use_pdz_5m",
        help=(
            "F3 — Premium / Discount / Equilibrium Zone Filter (5m)\n\n"
            "DZSAFM Smart Money zones on last 50 × 5m candles.\n"
            "Zones from swing High (H) and Low (L):\n"
            "  Premium bottom = 0.95×H + 0.05×L\n"
            "  Equilibrium    = middle band around midpoint\n"
            "  Discount top   = 0.05×H + 0.95×L\n\n"
            "✅ Qualifies: Discount zone · Band A/B with ≥1.5% room to next zone\n"
            "❌ Rejected:  Equilibrium (indecision) · Premium (no upward room)"
        ))
    if new_use_pdz_5m:
        st.caption("✅ Discount · BandA(≥1.5%↓Premium) · BandB(≥1.5%↓Equilibrium) | ❌ Premium · Equilibrium")
    st.divider()

    # ── F4: 5m RSI ─────────────────────────────────────────────────────────────
    st.markdown("**📈 F4 — 5m RSI**")
    new_use_rsi_5m = st.checkbox(
        "Enable F4 — 5m RSI",
        value=bool(_snap_cfg.get("use_rsi_5m", True)), key="cfg_use_rsi_5m",
        help=(
            "F4 — 5-Minute RSI Filter\n\n"
            "RSI(14) on the last 50 × 5m candles must be ≥ the minimum threshold.\n"
            "This is a fast Stage-1 exit — coins failing here skip all further fetches.\n\n"
            "A low RSI signals weak short-term momentum — not suitable for a long entry."
        ))
    new_rsi5_min = st.number_input("5m RSI min", min_value=0, max_value=100, step=1,
                                    value=int(_snap_cfg["rsi_5m_min"]), key="cfg_rsi5",
                                    disabled=not new_use_rsi_5m)
    st.divider()

    # ── F5: 1h RSI ─────────────────────────────────────────────────────────────
    st.markdown("**📈 F5 — 1h RSI**")
    new_use_rsi_1h = st.checkbox(
        "Enable F5 — 1h RSI",
        value=bool(_snap_cfg.get("use_rsi_1h", True)), key="cfg_use_rsi_1h",
        help=(
            "F5 — 1-Hour RSI Filter\n\n"
            "RSI(14) on the 1h timeframe must fall within the Min–Max band.\n\n"
            "Min: ensures real hourly momentum exists (coin is not dead).\n"
            "Max: avoids entries when the coin is already overbought on the higher timeframe."
        ))
    c3, c4 = st.columns(2)
    new_rsi1h_min = c3.number_input("1h min", min_value=0, max_value=100, step=1,
                                     value=int(_snap_cfg["rsi_1h_min"]), key="cfg_rsi1h_min",
                                     disabled=not new_use_rsi_1h)
    new_rsi1h_max = c4.number_input("1h max", min_value=0, max_value=100, step=1,
                                     value=int(_snap_cfg["rsi_1h_max"]), key="cfg_rsi1h_max",
                                     disabled=not new_use_rsi_1h)
    st.divider()

    # ── F6: EMA Selection ──────────────────────────────────────────────────────
    st.markdown("**📉 F6 — EMA Selection** (price must be above EMA)",
                help=(
                    "F6 — EMA Selection Filter\n\n"
                    "Entry price must be ABOVE the chosen EMA on each enabled timeframe.\n"
                    "Being above the EMA confirms the short-term trend is bullish.\n\n"
                    "Each timeframe (3m, 5m, 15m) is independently toggleable.\n"
                    "Default EMA period: 12. Adjust per timeframe using the number input."
                ))
    ea1, ea2 = st.columns([1,2])
    new_use_ema_3m    = ea1.checkbox("3m EMA", value=bool(_snap_cfg.get("use_ema_3m",False)), key="cfg_use_ema_3m")
    new_ema_period_3m = ea2.number_input("P##3m", min_value=2, max_value=500, step=1,
                                         value=int(_snap_cfg.get("ema_period_3m",12)),
                                         key="cfg_ema_period_3m", disabled=not new_use_ema_3m,
                                         label_visibility="collapsed")
    eb1, eb2 = st.columns([1,2])
    new_use_ema_5m    = eb1.checkbox("5m EMA", value=bool(_snap_cfg.get("use_ema_5m",True)), key="cfg_use_ema_5m")
    new_ema_period_5m = eb2.number_input("P##5m", min_value=2, max_value=500, step=1,
                                         value=int(_snap_cfg.get("ema_period_5m",12)),
                                         key="cfg_ema_period_5m", disabled=not new_use_ema_5m,
                                         label_visibility="collapsed")
    ec1, ec2 = st.columns([1,2])
    new_use_ema_15m    = ec1.checkbox("15m EMA", value=bool(_snap_cfg.get("use_ema_15m",True)), key="cfg_use_ema_15m")
    new_ema_period_15m = ec2.number_input("P##15m", min_value=2, max_value=500, step=1,
                                          value=int(_snap_cfg.get("ema_period_15m",12)),
                                          key="cfg_ema_period_15m", disabled=not new_use_ema_15m,
                                          label_visibility="collapsed")
    st.divider()

    # ── F7: MACD ───────────────────────────────────────────────────────────────
    st.markdown("**📊 F7 — MACD** (dark 🟢 histogram, 3m·5m·15m)")
    new_use_macd = st.checkbox(
        "Enable F7 — MACD filter",
        value=bool(_snap_cfg.get("use_macd",True)), key="cfg_use_macd",
        help=(
            "F7 — MACD Dark Green Histogram Filter\n\n"
            "All of the following must be true on 3m, 5m, AND 15m simultaneously:\n"
            "  • MACD line > 0\n"
            "  • Signal line > 0\n"
            "  • Histogram > 0 AND increasing (dark green — not fading)\n"
            "  • Bullish crossover within the last 12 candles\n\n"
            "Ensures strong, fresh bullish momentum across all timeframes."
        ))
    if new_use_macd: st.caption("✅ MACD>0 · Signal>0 · Histogram 🟢↑ · Crossover within 12 candles")
    st.divider()

    # ── F8: Parabolic SAR ──────────────────────────────────────────────────────
    st.markdown("**🪂 F8 — Parabolic SAR** (3m·5m·15m)")
    new_use_sar = st.checkbox(
        "Enable F8 — SAR filter",
        value=bool(_snap_cfg.get("use_sar",True)), key="cfg_use_sar",
        help=(
            "F8 — Parabolic SAR Filter\n\n"
            "SAR must be positioned BELOW current price on 3m, 5m, AND 15m.\n\n"
            "SAR below price = bullish mode.\n"
            "If any timeframe has SAR above price, the trend is bearish → coin rejected."
        ))
    if new_use_sar: st.caption("✅ SAR below price on 3m · 5m · 15m")
    st.divider()

    # ── F9: Volume Spike ───────────────────────────────────────────────────────
    st.markdown("**📦 F9 — Volume Spike** (15m)")
    new_use_vol_spike = st.checkbox(
        "Enable F9 — Volume Spike",
        value=bool(_snap_cfg.get("use_vol_spike",False)), key="cfg_use_vol_spike",
        help=(
            "F9 — Volume Spike Filter (disabled by default)\n\n"
            "Most recent 15m candle volume must be ≥ Mult × average of prior Lookback candles.\n\n"
            "Confirms strong buying pressure behind the move.\n"
            "Mult: spike multiplier (e.g. 2.0 = must be 2× average).\n"
            "Lookback: number of prior candles used to compute the average."
        ))
    vx1, vx2 = st.columns(2)
    new_vol_mult     = vx1.number_input("Mult (X×)", min_value=1.0, max_value=20.0, step=0.5,
                                         value=float(_snap_cfg.get("vol_spike_mult",2.0)), key="cfg_vol_mult",
                                         disabled=not new_use_vol_spike)
    new_vol_lookback = vx2.number_input("Lookback (N)", min_value=2, max_value=100, step=1,
                                         value=int(_snap_cfg.get("vol_spike_lookback",20)), key="cfg_vol_lookback",
                                         disabled=not new_use_vol_spike)
    st.divider()

    st.markdown("**⏱ Execution**")
    c5, c6 = st.columns(2)
    new_loop = c5.number_input("Loop (min)", min_value=1, max_value=60, step=1, value=int(_snap_cfg["loop_minutes"]),    key="cfg_loop")
    new_cool = c6.number_input("Cooldown (min)", min_value=1, max_value=120, step=1, value=int(_snap_cfg["cooldown_minutes"]), key="cfg_cool")
    st.divider()

    st.markdown("**📋 Watchlist** (one symbol per line)")
    wl_text = st.text_area("wl", value="\n".join(_snap_cfg["watchlist"]),
                            height=180, label_visibility="collapsed", key="cfg_wl")

    # Highlight any watchlist coins absent from the OKX live SWAP instrument list
    _ct_cache = _b._bsc_symbol_cache.get("ct_val", {})
    if _ct_cache:
        _wl_delisted = [s for s in _snap_cfg.get("watchlist", [])
                        if s not in _ct_cache]
        if _wl_delisted:
            st.warning(
                f"⚠️ {len(_wl_delisted)} coin(s) not found as live OKX SWAP instruments "
                f"(delisted or wrong ticker) — trades will be skipped for these:\n\n"
                + ", ".join(_wl_delisted)
            )
    st.divider()

    st.markdown("**💾 Data Storage**")
    _is_temp = str(_SCRIPT_DIR).startswith(str(pathlib.Path(__import__("tempfile").gettempdir())))
    if _is_temp:
        st.warning(
            f"⚠️ Saving to **temp directory** — data will be lost on OS restart!\n\n"
            f"`{_SCRIPT_DIR}`\n\n"
            "Move the script to a writable folder (e.g. Documents) to fix this.")
    else:
        st.caption(f"📁 Data folder: `{_SCRIPT_DIR}`")
        st.caption(f"  • `scanner_log.json` — all signals (loaded on restart)")
        st.caption(f"  • `scanner_config.json` — filters, API keys, settings")
    st.divider()

    st.markdown("**🗑 Clear History**")
    if st.button("⚡ Flush All", use_container_width=True, type="secondary"):
        # 1 — signals + health log
        with _log_lock:
            _b._bsc_log["signals"] = []
            _b._bsc_log["health"]  = {
                "total_cycles": 0, "last_scan_at": None,
                "last_scan_duration_s": 0.0, "total_api_errors": 0,
                "watchlist_size": 0, "pre_filtered_out": 0, "deep_scanned": 0,
            }
            save_log(_b._bsc_log)
        # 2 — filter funnel (use _reset_filter_counts so the in-place dict
        #     is reinitialised to zero-state, keeping the same object reference
        #     that the scanner thread uses — avoids the split-reference bug)
        _reset_filter_counts()
        # 3 — API error log
        with getattr(_b, "_bsc_error_log_lock", threading.Lock()):
            if hasattr(_b, "_bsc_error_log"):
                _b._bsc_error_log.clear()
        # 4 — last trade debug panel + manual/test trade session results
        _b._bsc_last_trade_raw = {}
        _b._bsc_last_error     = ""
        for _ss_key in ("_mt_last_result", "_test_trade_result"):
            st.session_state.pop(_ss_key, None)
        st.success("✅ Flushed"); st.rerun()

    cd1, cd2 = st.columns(2)
    if cd1.button("📅 Clear 24h", use_container_width=True):
        cutoff = dubai_now() - timedelta(hours=24)
        with _log_lock:
            before = len(_b._bsc_log["signals"])
            _b._bsc_log["signals"] = [s for s in _b._bsc_log["signals"]
                if datetime.fromisoformat(s["timestamp"].replace("Z","+00:00")) < cutoff]
            save_log(_b._bsc_log)
        st.success(f"✅ Removed {before-len(_b._bsc_log['signals'])}"); st.rerun()
    if cd2.button("📆 Clear 7d", use_container_width=True):
        cutoff = dubai_now() - timedelta(days=7)
        with _log_lock:
            before = len(_b._bsc_log["signals"])
            _b._bsc_log["signals"] = [s for s in _b._bsc_log["signals"]
                if datetime.fromisoformat(s["timestamp"].replace("Z","+00:00")) < cutoff]
            save_log(_b._bsc_log)
        st.success(f"✅ Removed {before-len(_b._bsc_log['signals'])}"); st.rerun()
    st.divider()

    if st.button("↩️ Reset Defaults", use_container_width=True, type="secondary"):
        d = dict(DEFAULT_CONFIG)
        with _config_lock: _b._bsc_cfg.clear(); _b._bsc_cfg.update(d)
        save_config(d)
        # Also invalidate symbol cache so it re-verifies on next scan
        _b._bsc_symbol_cache["fetched_at"] = 0
        st.success("✅ Reset"); st.rerun()

    if st.button("💾 Save & Apply", use_container_width=True, type="primary"):
        new_wl = [s.strip().upper() for s in wl_text.splitlines() if s.strip()]
        new_cfg = {
            "tp_pct": new_tp, "sl_pct": new_sl,
            # enable/disable flags
            "use_pre_filter":      bool(new_use_pre_filter),
            "use_rsi_5m":          bool(new_use_rsi_5m),
            "use_rsi_1h":          bool(new_use_rsi_1h),
            # filter parameters
            "rsi_5m_min": int(new_rsi5_min),
            "rsi_1h_min": int(new_rsi1h_min), "rsi_1h_max": int(new_rsi1h_max),
            "loop_minutes": int(new_loop), "cooldown_minutes": int(new_cool),
            "use_ema_3m": bool(new_use_ema_3m), "ema_period_3m": int(new_ema_period_3m),
            "use_ema_5m": bool(new_use_ema_5m), "ema_period_5m": int(new_ema_period_5m),
            "use_ema_15m": bool(new_use_ema_15m), "ema_period_15m": int(new_ema_period_15m),
            "use_macd": bool(new_use_macd), "use_sar": bool(new_use_sar),
            "use_vol_spike": bool(new_use_vol_spike),
            "vol_spike_mult": float(new_vol_mult), "vol_spike_lookback": int(new_vol_lookback),
            "use_pdz_5m": bool(new_use_pdz_5m),
            "use_pdz_15m": bool(new_use_pdz_15m),
            "watchlist": new_wl,
            # ── Auto-trading ─────────────────────────────────────────────────
            "trade_enabled":     bool(new_trade_enabled),
            "demo_mode":         (new_demo_mode == "Demo"),
            "api_key":           new_api_key.strip(),
            "api_secret":        new_api_secret.strip(),
            "api_passphrase":    new_api_passphrase.strip(),
            "trade_usdt_amount": float(new_trade_usdt),
            "trade_leverage":    int(new_trade_lev),
            "trade_margin_mode": new_margin_mode,
        }
        with _config_lock: _b._bsc_cfg.clear(); _b._bsc_cfg.update(new_cfg)
        save_config(new_cfg)
        # Invalidate symbol cache if watchlist changed
        if new_wl != _snap_cfg.get("watchlist", []):
            _b._bsc_symbol_cache["fetched_at"] = 0
        _b._bsc_rescan_event.set()   # wake bg thread immediately — no waiting for next cycle
        st.success(f"✅ Saved — {len(new_wl)} coins — rescanning now…"); st.rerun()

# ─────────────────────────────────────────────────────────────────────────────
# MAIN AREA
# ─────────────────────────────────────────────────────────────────────────────
st.title("🔍 Crypto Demo Trades")

last_scan = health.get("last_scan_at", "never")
if last_scan and last_scan != "never":
    try:
        ts       = datetime.fromisoformat(last_scan.replace("Z","+00:00"))
        ts_dubai = to_dubai(ts)
        ago      = int((dubai_now()-ts_dubai).total_seconds()/60)
        last_scan = f"{ago}m ago  ({ts_dubai.strftime('%H:%M')} GST)"
    except Exception: pass

col_h1, col_h2, col_h3 = st.columns([3, 1, 1])
col_h1.caption(f"Last scan: {last_scan}   |   Auto-refresh every 30 s   |   🕐 Dubai / GST (UTC+4)")
if col_h2.button("🔄 Refresh", key="manual_refresh"): st.rerun()

# ── API connection status badge ───────────────────────────────────────────────
_api_cs   = getattr(_b, "_bsc_api_conn_status",
                    {"status": "untested", "message": "", "tested_at": None,
                     "demo_mode": None, "uid": ""})
_api_stat = _api_cs.get("status", "untested")
_trade_on = _snap_cfg.get("trade_enabled", False)
if not _trade_on:
    _badge = "⚫ Auto-Trade: Off"
    col_h3.caption(_badge)
elif _api_stat == "ok":
    _env_lbl = "Demo" if _api_cs.get("demo_mode") else "Live"
    _badge   = f"🟢 API: {_env_lbl}"
    col_h3.caption(_badge)
elif _api_stat == "error":
    col_h3.caption("🔴 API: Error")
else:
    col_h3.caption("🟡 API: Untested")

if _trade_on and _api_stat == "ok":
    _tested_str = fmt_dubai(_api_cs.get("tested_at", "")) if _api_cs.get("tested_at") else "—"
    st.caption(f"🤖 Auto-Trading active · {_api_cs.get('message','')} · tested {_tested_str}")

# ── Health metrics ─────────────────────────────────────────────────────────────
open_count = sum(1 for s in signals if s["status"]=="open")
tp_count   = sum(1 for s in signals if s["status"]=="tp_hit")
sl_count   = sum(1 for s in signals if s["status"]=="sl_hit")
pre_out    = health.get("pre_filtered_out", 0)
deep_sc    = health.get("deep_scanned",     0)

m1,m2,m3,m4,m5c,m6,m7,m8 = st.columns(8)
m1.metric("Cycles",        health.get("total_cycles",0))
m2.metric("Scan Time",     f"{health.get('last_scan_duration_s',0)}s")
m3.metric("API Errors",    health.get("total_api_errors",0))
m4.metric("Pre-filtered ⚡", pre_out, help="Coins removed by bulk ticker pre-filter (saves API calls)")
m5c.metric("Deep Scanned", deep_sc, help="Coins that passed pre-filter and received full candle analysis")
m6.metric("Open",          open_count)
m7.metric("TP Hit ✅",     tp_count)
m8.metric("SL Hit ❌",     sl_count)

if getattr(_b, "_bsc_last_error", ""):
    st.warning(f"⚠️ {_b._bsc_last_error}")

# ─────────────────────────────────────────────────────────────────────────────
# API Error Log
# ─────────────────────────────────────────────────────────────────────────────
with getattr(_b, "_bsc_error_log_lock", threading.Lock()):
    _err_snap = list(getattr(_b, "_bsc_error_log", []))

_TYPE_ICON = {"scan": "🔴", "trade": "🟠", "loop": "🟣", "http": "🔵"}
_TYPE_LABEL = {"scan": "Scan/Candle", "trade": "Trade/Order",
               "loop": "Scanner loop", "http": "HTTP/Network"}

_err_count = len(_err_snap)
_err_label = f"⚠️ API Error Log — {_err_count} entr{'y' if _err_count == 1 else 'ies'}"

with st.expander(_err_label, expanded=(_err_count > 0)):
    if not _err_snap:
        st.success("✅ No errors recorded yet.")
    else:
        ecol1, ecol2 = st.columns([1, 1])

        # ── Summary counts by type ─────────────────────────────────────────
        _type_counts = {}
        for _e in _err_snap:
            _type_counts[_e["type"]] = _type_counts.get(_e["type"], 0) + 1
        _summary = "  ·  ".join(
            f"{_TYPE_ICON.get(t,'⚪')} {_TYPE_LABEL.get(t,t)}: **{n}**"
            for t, n in sorted(_type_counts.items())
        )
        ecol1.markdown(_summary)

        # ── Clear button ──────────────────────────────────────────────────
        if ecol2.button("🗑 Clear error log", key="clear_err_log"):
            with _b._bsc_error_log_lock:
                _b._bsc_error_log.clear()
            st.rerun()

        # ── Type filter ───────────────────────────────────────────────────
        _all_types = sorted({e["type"] for e in _err_snap})
        _filt_cols = st.columns(len(_all_types) + 1)
        _sel_type  = st.session_state.get("err_type_filter", "All")
        if _filt_cols[0].button("All", key="err_f_all",
                                type="primary" if _sel_type == "All" else "secondary"):
            st.session_state["err_type_filter"] = "All"; st.rerun()
        for _fi, _ft in enumerate(_all_types):
            if _filt_cols[_fi + 1].button(
                    f"{_TYPE_ICON.get(_ft,'')} {_TYPE_LABEL.get(_ft,_ft)}",
                    key=f"err_f_{_ft}",
                    type="primary" if _sel_type == _ft else "secondary"):
                st.session_state["err_type_filter"] = _ft; st.rerun()

        # ── Error table ───────────────────────────────────────────────────
        _sel_type = st.session_state.get("err_type_filter", "All")
        _shown = [e for e in reversed(_err_snap)
                  if _sel_type == "All" or e["type"] == _sel_type]
        _err_rows = []
        for _e in _shown:
            _err_rows.append({
                "Time (GST)": fmt_dubai(_e["ts"]),
                "Type":       f"{_TYPE_ICON.get(_e['type'],'')} {_TYPE_LABEL.get(_e['type'],_e['type'])}",
                "Symbol":     _e.get("symbol", "—") or "—",
                "Endpoint":   _e.get("endpoint", "—") or "—",
                "Error":      _e.get("message", ""),
            })
        st.dataframe(
            _err_rows,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Time (GST)": st.column_config.TextColumn(width="small"),
                "Type":       st.column_config.TextColumn(width="medium"),
                "Symbol":     st.column_config.TextColumn(width="small"),
                "Endpoint":   st.column_config.TextColumn(width="medium"),
                "Error":      st.column_config.TextColumn(width="large"),
            },
        )
        st.caption(f"Showing {len(_err_rows)} of {_err_count} entries (newest first) · max {_ERROR_LOG_MAX} kept")
st.divider()

# ── Active filter badges ───────────────────────────────────────────────────────
badges = []
if _snap_cfg.get("use_pdz_15m", True): badges.append(f"🎯 F2 — PDZ 15m")
if _snap_cfg.get("use_pdz_5m",  True): badges.append(f"🎯 F3 — PDZ 5m")
if _snap_cfg.get("use_rsi_5m",  True): badges.append(f"📈 F4 — 5m RSI ≥{_snap_cfg.get('rsi_5m_min',30)}")
if _snap_cfg.get("use_rsi_1h",  True): badges.append(f"📈 F5 — 1h RSI {_snap_cfg.get('rsi_1h_min',30)}–{_snap_cfg.get('rsi_1h_max',95)}")
if _snap_cfg.get("use_ema_3m"):    badges.append(f"📉 F6 — EMA{_snap_cfg.get('ema_period_3m',12)} 3m")
if _snap_cfg.get("use_ema_5m"):    badges.append(f"📉 F6 — EMA{_snap_cfg.get('ema_period_5m',12)} 5m")
if _snap_cfg.get("use_ema_15m"):   badges.append(f"📉 F6 — EMA{_snap_cfg.get('ema_period_15m',12)} 15m")
if _snap_cfg.get("use_macd"):      badges.append("📊 F7 — MACD 🟢↑ 3m·5m·15m")
if _snap_cfg.get("use_sar"):       badges.append("🪂 F8 — SAR 3m·5m·15m")
if _snap_cfg.get("use_vol_spike"): badges.append(
    f"📦 F9 — Vol ≥{_snap_cfg.get('vol_spike_mult',2.0)}× / {_snap_cfg.get('vol_spike_lookback',20)} 15m")
st.markdown("**Active Filters:**")
st.caption("  |  ".join(badges) if badges else "No advanced filters enabled")
st.divider()

# ── Sector filter ──────────────────────────────────────────────────────────────
all_sectors = ["All","BTC","L1","L2","DeFi","AI","Privacy","Meme","Gaming","Other"]
if "sector_filter" not in st.session_state: st.session_state["sector_filter"] = "All"
scols = st.columns(len(all_sectors))
for i, sec in enumerate(all_sectors):
    active = st.session_state["sector_filter"] == sec
    if scols[i].button(sec, key=f"sec_{sec}", type="primary" if active else "secondary",
                        use_container_width=True):
        st.session_state["sector_filter"] = sec; st.rerun()
selected_sector = st.session_state["sector_filter"]

# ── Signals table ──────────────────────────────────────────────────────────────
filtered        = signals if selected_sector=="All" else \
                  [s for s in signals if s.get("sector")==selected_sector]
filtered_sorted = sorted(filtered, key=lambda x: x.get("timestamp",""), reverse=True)
st.markdown(f"### Signals ({len(filtered_sorted)} shown)")

if filtered_sorted:
    rows = []
    for s in filtered_sorted:
        status      = s.get("status","open")
        status_icon = {"open":"🔵 Open","tp_hit":"✅ TP Hit","sl_hit":"❌ SL Hit"}.get(status,status)
        ts_str      = fmt_dubai(s.get("timestamp",""))
        close_str   = fmt_dubai(s["close_time"]) if s.get("close_time") else "—"
        crit        = s.get("criteria",{})
        def _cv(v):
            """Format a criteria value: show number if numeric, else show as-is."""
            if v is None or v == "—" or v == "✅": return "—"
            try: return f"{float(v):.6f}".rstrip("0").rstrip(".")
            except (TypeError, ValueError): return str(v)
        pdz_5m_val  = crit.get("pdz_zone_5m",  "—") or "—"
        pdz_15m_val = crit.get("pdz_zone_15m", "—") or "—"
        crit_str    = (
            f"• RSI 5m    : {crit.get('rsi_5m','—')}\n"
            f"• RSI 1h    : {crit.get('rsi_1h','—')}\n"
            f"• EMA 3m    : {_cv(crit.get('ema_3m','—'))}\n"
            f"• EMA 5m    : {_cv(crit.get('ema_5m','—'))}\n"
            f"• EMA 15m   : {_cv(crit.get('ema_15m','—'))}\n"
            f"• MACD 3m   : {_cv(crit.get('macd_3m'))}\n"
            f"• MACD 5m   : {_cv(crit.get('macd_5m'))}\n"
            f"• MACD 15m  : {_cv(crit.get('macd_15m'))}\n"
            f"• SAR 3m    : {_cv(crit.get('sar_3m'))}\n"
            f"• SAR 5m    : {_cv(crit.get('sar_5m'))}\n"
            f"• SAR 15m   : {_cv(crit.get('sar_15m'))}\n"
            f"• Vol ×avg  : {_cv(crit.get('vol_ratio'))}\n"
            f"• PDZ 5m    : {pdz_5m_val}\n"
            f"• PDZ 15m   : {pdz_15m_val}"
        ) if crit else "—"
        max_lev    = s.get("max_lev", get_max_leverage(s.get("symbol","")))
        sl_reason  = analyze_sl_reason(s) if status=="sl_hit" else "—"
        setup_type = "⭐ Super" if s.get("is_super_setup") else "Normal"
        # ── $TP / $SL USDT value ─────────────────────────────────────────────
        # Use trade params stored in the signal (auto-traded) or fall back to
        # current config values for indicative display on manual/older signals.
        _usdt = float(s.get("trade_usdt", _snap_cfg.get("trade_usdt_amount", 0)))
        _lev  = int(s.get("trade_lev",   _snap_cfg.get("trade_leverage", 10)))
        _entry_p = float(s.get("entry", 0) or 0)
        _tp_p    = float(s.get("tp",    0) or 0)
        _sl_p    = float(s.get("sl",    0) or 0)
        if _usdt > 0 and _lev > 0 and _entry_p > 0:
            _pos = _usdt * _lev
            tp_usd_str = f"+${_pos * (_tp_p - _entry_p) / _entry_p:.2f}"
            sl_usd_str = f"-${_pos * (_entry_p - _sl_p) / _entry_p:.2f}"
        else:
            tp_usd_str = sl_usd_str = "—"
        # ── Order info (auto-trading) ─────────────────────────────────────────
        ord_id_str  = s.get("order_id", "") or "—"
        algo_id_str = s.get("algo_id",  "") or "—"
        ord_env     = "🟡 Demo" if s.get("demo_mode") else "🔴 Live"
        ord_status  = s.get("order_status", "")
        ord_err     = s.get("order_error", "")
        if ord_status == "placed":
            ord_status_str = f"✅ Entry+OCO {ord_env}"
        elif ord_status == "partial":
            ord_status_str = f"⚠️ Entry only {ord_env} · {ord_err[:80]}"
        elif ord_status == "error":
            ord_status_str = f"❌ {ord_err[:80]}" if ord_err else "❌ Error"
        else:
            ord_status_str = "—"
        # ── Duration: time from signal entry → TP/SL close ───────────────────
        duration_str = "—"
        if s.get("close_time") and s.get("timestamp"):
            try:
                t_open  = datetime.fromisoformat(s["timestamp"].replace("Z", "+00:00"))
                t_close = datetime.fromisoformat(s["close_time"].replace("Z", "+00:00"))
                secs    = int((t_close - t_open).total_seconds())
                if secs < 0:
                    duration_str = "—"
                elif secs < 3600:
                    duration_str = f"{secs // 60}m {secs % 60}s"
                elif secs < 86400:
                    h, rem = divmod(secs, 3600)
                    duration_str = f"{h}h {rem // 60}m"
                else:
                    d, rem = divmod(secs, 86400)
                    duration_str = f"{d}d {rem // 3600}h"
            except Exception:
                pass
        rows.append({
            "Time (GST)":     ts_str,
            "Symbol":         s.get("symbol",""),
            "Setup":          setup_type,
            "Sector":         s.get("sector","Other"),
            "Signal Entry":   s.get("signal_entry", s.get("entry","")),
            "Fill $":         s.get("entry","") if s.get("signal_entry") else "—",
            "TP":             s.get("tp",""),
            "TP $":           tp_usd_str,
            "SL":             s.get("sl",""),
            "SL $":           sl_usd_str,
            "Status":         status_icon,
            "Duration":       duration_str,
            "Close Time":     close_str,
            "Close $":        s.get("close_price") or "—",
            "Max Lev":        f"{max_lev}×",
            "Order":          ord_status_str,
            "Order ID":       ord_id_str,
            "Algo ID":        algo_id_str,
            "Entry Criteria": crit_str,
            "⚠️ SL Reason":  sl_reason,
        })
    st.dataframe(rows, use_container_width=True, hide_index=True,
                 column_config={
                     "Setup":          st.column_config.TextColumn(width="small"),
                     "Signal Entry":   st.column_config.NumberColumn(format="%.8f",
                                           help="Price when the scanner signal fired"),
                     "Fill $":         st.column_config.NumberColumn(format="%.8f",
                                           help="Actual market fill price (may differ from signal entry)"),
                     "TP":             st.column_config.NumberColumn(format="%.8f"),
                     "TP $":           st.column_config.TextColumn(width="small"),
                     "SL":             st.column_config.NumberColumn(format="%.8f"),
                     "SL $":           st.column_config.TextColumn(width="small"),
                     "Duration":       st.column_config.TextColumn(width="small"),
                     "Close Time":     st.column_config.TextColumn(width="small"),
                     "Max Lev":        st.column_config.TextColumn(width="small"),
                     "Order":          st.column_config.TextColumn(width="medium"),
                     "Order ID":       st.column_config.TextColumn(width="medium"),
                     "Algo ID":        st.column_config.TextColumn(width="medium"),
                     "Entry Criteria": st.column_config.TextColumn(width="medium"),
                     "⚠️ SL Reason":  st.column_config.TextColumn(width="medium"),
                 })
else:
    st.info("No signals yet — scanner runs every few minutes.")

# ─────────────────────────────────────────────────────────────────────────────
# Manual Trade Panel
# ─────────────────────────────────────────────────────────────────────────────
_mt_has_creds = bool(
    _snap_cfg.get("api_key") and _snap_cfg.get("api_secret")
    and _snap_cfg.get("api_passphrase") and _snap_cfg.get("trade_enabled")
)

with st.expander("🤖 Manual Trade", expanded=False):
    if not _mt_has_creds:
        st.warning("Enable Auto-Trading and enter API credentials in the sidebar first.")
    else:
        st.caption("Place a trade manually on any coin. Use this to debug order errors — "
                   "the full OKX request and response are shown immediately below.")

        # ── Coin selector ─────────────────────────────────────────────────────
        # Pre-populate with open signals that have no order yet (or any signal)
        _sig_syms  = [s["symbol"] for s in signals if s.get("status") == "open"]
        _wl_syms   = _snap_cfg.get("watchlist", [])
        _all_syms  = sorted(set(_sig_syms + _wl_syms))
        _mt_default = _sig_syms[0] if _sig_syms else (_all_syms[0] if _all_syms else "BTCUSDT")

        mt_col1, mt_col2 = st.columns([2, 1])
        mt_sym = mt_col1.selectbox(
            "Symbol", _all_syms,
            index=_all_syms.index(_mt_default) if _mt_default in _all_syms else 0,
            key="mt_sym")
        mt_mode = mt_col2.selectbox(
            "Margin mode", ["cross", "isolated"],
            index=0 if _snap_cfg.get("trade_margin_mode","cross") == "cross" else 1,
            key="mt_mode")

        # ── Auto-fill from existing open signal if available ──────────────────
        _mt_sig = next((s for s in signals
                        if s["symbol"] == mt_sym and s["status"] == "open"), None)
        _mt_entry_def = float(_mt_sig["entry"]) if _mt_sig else 0.0
        _mt_tp_def    = float(_mt_sig["tp"])    if _mt_sig else 0.0
        _mt_sl_def    = float(_mt_sig["sl"])    if _mt_sig else 0.0

        mc1, mc2, mc3 = st.columns(3)
        mt_entry = mc1.number_input("Entry price (0 = live market)",
                                    min_value=0.0, value=_mt_entry_def,
                                    format="%.8f", key="mt_entry")
        mt_tp    = mc2.number_input("TP price",
                                    min_value=0.0, value=_mt_tp_def,
                                    format="%.8f", key="mt_tp")
        mt_sl    = mc3.number_input("SL price",
                                    min_value=0.0, value=_mt_sl_def,
                                    format="%.8f", key="mt_sl")

        md1, md2 = st.columns(2)
        mt_usdt = md1.number_input("Size (USDT collateral)",
                                   min_value=1.0, value=float(_snap_cfg.get("trade_usdt_amount", 10)),
                                   key="mt_usdt")
        mt_lev  = md2.number_input("Leverage ×",
                                   min_value=1, max_value=125,
                                   value=int(_snap_cfg.get("trade_leverage", 10)),
                                   key="mt_lev")

        if _mt_sig:
            st.caption(f"ℹ️ Pre-filled from open signal for {mt_sym} "
                       f"(entry {_mt_entry_def}, TP {_mt_tp_def}, SL {_mt_sl_def})")

        # Hint: if TP/SL are 0, show what will be calculated
        if mt_entry > 0:
            _mt_tp_hint = mt_tp if mt_tp > 0 else mt_entry * (1 + _snap_cfg.get("tp_pct",1.5)/100)
            _mt_sl_hint = mt_sl if mt_sl > 0 else mt_entry * (1 - _snap_cfg.get("sl_pct",3.0)/100)
            st.caption(f"📋 Will place **LIMIT** buy at {mt_entry} "
                       f"· TP: {_pround(_mt_tp_hint)} · SL: {_pround(_mt_sl_hint)}")
        else:
            st.caption("📋 Entry = 0 → **MARKET** buy at live price · "
                       "TP/SL from configured % if left at 0")

        if st.button("🚀 Place Manual Trade", type="primary", key="mt_place"):
            _mt_cfg = dict(_snap_cfg)
            _mt_cfg["trade_usdt_amount"] = mt_usdt
            _mt_cfg["trade_leverage"]    = mt_lev
            _mt_cfg["trade_margin_mode"] = mt_mode

            # TP/SL: use user values if provided, else fall back to config %
            # (only relevant when entry == 0, i.e. market order)
            _ref = mt_entry if mt_entry > 0 else 0
            _mt_tp_use = mt_tp if mt_tp > 0 else (_ref * (1 + _snap_cfg.get("tp_pct",1.5)/100) if _ref else 0)
            _mt_sl_use = mt_sl if mt_sl > 0 else (_ref * (1 - _snap_cfg.get("sl_pct",3.0)/100) if _ref else 0)

            with st.spinner(f"Placing {'LIMIT' if mt_entry > 0 else 'MARKET'} order for {mt_sym}…"):
                _mt_result = place_okx_manual_order(
                    mt_sym, mt_entry, _mt_tp_use, _mt_sl_use, _mt_cfg)

            st.session_state["_mt_last_result"] = _mt_result

        # ── Show last manual trade result ─────────────────────────────────────
        _mt_res = st.session_state.get("_mt_last_result")
        if _mt_res:
            _mt_status = _mt_res.get("status", "")
            _mt_err    = _mt_res.get("error", "")
            if _mt_status == "placed":
                st.success(f"✅ Placed · Order ID: {_mt_res.get('ordId')} "
                           f"· Algo ID: {_mt_res.get('algoId')} "
                           f"· Contracts: {_mt_res.get('sz')}")
            elif _mt_status == "partial":
                st.warning(f"⚠️ {_mt_err}")
            else:
                st.error(f"❌ {_mt_err}")

            # Show full raw OKX request/response for debugging
            _mt_raw = getattr(_b, "_bsc_last_trade_raw", {})
            if _mt_raw:
                st.markdown(f"**Entry order — {_mt_raw.get('endpoint','')}**")
                st.json(_mt_raw.get("body_sent", {}))
                st.markdown("**OKX entry response:**")
                st.json(_mt_raw.get("response", {}))
                if _mt_raw.get("algo_body_sent"):
                    st.markdown("**OCO algo order sent:**")
                    st.json(_mt_raw["algo_body_sent"])
                    st.markdown("**OKX OCO response:**")
                    st.json(_mt_raw.get("algo_response", {}))
                st.caption(
                    f"is_hedge={_mt_raw.get('is_hedge')}  "
                    f"contracts={_mt_raw.get('contracts','?')}  "
                    f"ct_val={_mt_raw.get('ct_val','?')}")

# ── Charts ─────────────────────────────────────────────────────────────────────
if signals:
    st.divider()
    ch1, ch2 = st.columns(2)
    sec_counts: dict = {}
    for s in signals:
        k = s.get("sector","Other"); sec_counts[k] = sec_counts.get(k,0)+1
    ch1.plotly_chart(go.Figure(go.Pie(
        labels=list(sec_counts.keys()), values=list(sec_counts.values()),
        hole=0.4, marker=dict(colors=px.colors.qualitative.Dark24)
    )).update_layout(title="Signals by Sector", paper_bgcolor="rgba(0,0,0,0)",
                     plot_bgcolor="rgba(0,0,0,0)", font=dict(color="#e6edf3"),
                     margin=dict(t=40,b=10,l=10,r=10)),
                     use_container_width=True)
    outcome = {"Open":open_count,"TP Hit":tp_count,"SL Hit":sl_count}
    ch2.plotly_chart(go.Figure(go.Bar(
        x=list(outcome.keys()), y=list(outcome.values()),
        marker_color=["#58a6ff","#3fb950","#f85149"],
        text=list(outcome.values()), textposition="outside"
    )).update_layout(title="Signal Outcomes", paper_bgcolor="rgba(0,0,0,0)",
                     plot_bgcolor="rgba(0,0,0,0)", font=dict(color="#e6edf3"),
                     yaxis=dict(gridcolor="#21262d"), margin=dict(t=40,b=10,l=10,r=10)),
                     use_container_width=True)

    if len(signals) > 1:
        from collections import Counter
        dc: Counter = Counter()
        for s in signals:
            try:
                d = to_dubai(datetime.fromisoformat(s["timestamp"].replace("Z","+00:00"))).strftime("%m/%d")
                dc[d] += 1
            except Exception: pass
        if dc:
            days = sorted(dc.keys())
            st.plotly_chart(go.Figure(go.Bar(
                x=days, y=[dc[d] for d in days],
                marker_color="#d29922", text=[dc[d] for d in days], textposition="outside"
            )).update_layout(title="Signals Per Day (Dubai/GST)", paper_bgcolor="rgba(0,0,0,0)",
                             plot_bgcolor="rgba(0,0,0,0)", font=dict(color="#e6edf3"),
                             yaxis=dict(gridcolor="#21262d"), margin=dict(t=40,b=10,l=10,r=10)),
                             use_container_width=True)

# ── Filter funnel ──────────────────────────────────────────────────────────────
# Deep-copy under lock so background thread can't mutate lists mid-render
with _filter_lock:
    fc = {k: (list(v) if isinstance(v, list) else v) for k, v in _filter_counts.items()}
if fc.get("total_watchlist", 0) > 0:
    with st.expander("🔬 Last scan filter funnel"):
        total     = fc.get("total_watchlist", 0)
        pre_out_n = fc.get("pre_filtered_out", 0)
        after_pre = total - pre_out_n
        checked   = fc.get("checked", after_pre)
        after_f2  = checked   - fc.get("f2_pdz15m", 0)
        after_f3  = after_f2  - fc.get("f3_pdz5m",  0)
        after_f4  = after_f3  - fc.get("f4_rsi5m",  0)
        after_f5  = after_f4  - fc.get("f5_rsi1h",  0)
        after_f6  = after_f5  - fc.get("f6_ema",    0)
        after_f7  = after_f6  - fc.get("f7_macd",   0)
        after_f8       = after_f7  - fc.get("f8_sar",        0)
        after_f9       = after_f8  - fc.get("f9_vol",        0)
        after_empty    = after_f9  - fc.get("f_empty_data",  0)

        # Use the config that was ACTIVE during the last scan for labels
        # This ensures labels always match the counts (no mismatch after config changes)
        sc = fc.get("scan_cfg") or _snap_cfg   # fallback to current cfg on first run

        if sc is not _snap_cfg and sc != _snap_cfg:
            st.caption("⚠️ Config changed since last scan — funnel reflects the previous settings. "
                       "Rescan is in progress with the new config.")

        def _lbl(enabled, on_str, off_str="off"):
            return on_str if enabled else f"{on_str.split('—')[0].strip()} ({off_str})"

        pre_lbl    = "⚡ After Bulk Pre-filter" if sc.get("use_pre_filter", True) else "⚡ Pre-filter (disabled)"
        pdz15m_lbl = "F2 — PDZ Zones (15m)" if sc.get("use_pdz_15m", True) else "F2 — PDZ 15m (off)"
        pdz5m_lbl  = "F3 — PDZ Zones (5m)"  if sc.get("use_pdz_5m",  True) else "F3 — PDZ 5m (off)"
        f4_lbl     = f"F4 — 5m RSI ≥{sc.get('rsi_5m_min',30)}" if sc.get("use_rsi_5m", True) else "F4 — 5m RSI (off)"
        f5_lbl     = (f"F5 — 1h RSI {sc.get('rsi_1h_min',30)}–{sc.get('rsi_1h_max',95)}"
                      if sc.get("use_rsi_1h", True) else "F5 — 1h RSI (off)")
        ema_parts = []
        if sc.get("use_ema_3m"):  ema_parts.append(f"3m EMA{sc.get('ema_period_3m',12)}")
        if sc.get("use_ema_5m"):  ema_parts.append(f"5m EMA{sc.get('ema_period_5m',12)}")
        if sc.get("use_ema_15m"): ema_parts.append(f"15m EMA{sc.get('ema_period_15m',12)}")
        ema_lbl    = ("F6 — EMA (" + (" · ".join(ema_parts)) + ")") if ema_parts else "F6 — EMA (off)"
        macd_lbl   = "F7 — MACD \U0001f7e2\u2191 3m\xb75m\xb715m" if sc.get("use_macd")      else "F7 — MACD (off)"
        sar_lbl    = "F8 — SAR 3m\xb75m\xb715m"                    if sc.get("use_sar")       else "F8 — SAR (off)"
        vol_lbl    = (f"F9 — Vol \u2265{sc.get('vol_spike_mult',2.0)}\xd7 / {sc.get('vol_spike_lookback',20)} 15m"
                      if sc.get("use_vol_spike") else "F9 — Vol (off)")

        funnel_data = [
            (f"Watchlist ({total})",      total),
            (pre_lbl,                     after_pre),
            ("Entered Deep Scan",         checked),
            (f"After {pdz15m_lbl}",       after_f2),
            (f"After {pdz5m_lbl}",        after_f3),
            (f"After {f4_lbl}",           after_f4),
            (f"After {f5_lbl}",           after_f5),
            (f"After {ema_lbl}",          after_f6),
            (f"After {macd_lbl}",         after_f7),
            (f"After {sar_lbl}",          after_f8),
            (f"After {vol_lbl}",          after_f9),
            ("After ⚠️ Empty Candle Drop", after_empty),
        ]
        fig_funnel = go.Figure(go.Funnel(
            y=[d[0] for d in funnel_data],
            x=[d[1] for d in funnel_data],
            marker=dict(color=["#1f6feb","#388bfd","#58a6ff","#79c0ff","#a5d6ff",
                                "#3fb950","#56d364","#7ee787","#d29922","#e3b341","#f85149"]),
            textinfo="value+percent initial",
        ))
        fig_funnel.update_layout(
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            font=dict(color="#e6edf3"), margin=dict(t=10,b=10,l=10,r=10), height=500)
        st.plotly_chart(fig_funnel, use_container_width=True)
        col_a, col_b, col_c = st.columns(3)
        col_a.metric("Pre-filtered out ⚡", pre_out_n, help="Removed cheaply — no candle API calls used")
        col_b.metric("Deep scanned 🔬",     checked,   help="Received full multi-timeframe candle analysis")
        col_c.metric("Errors",              fc.get("errors",0))
        super_n = fc.get("super_setup", 0)
        if super_n:
            st.success(f"⭐ {super_n} Super Setup(s) this cycle — 15m Discount zone instant buy!")

        # ── Qualified coins table — one row per filter stage ────────────────────
        st.markdown("#### 🪙 Qualified Coins at Each Stage")
        st.caption("Shows which coins survived after each filter was applied in the last scan cycle.")

        # Build remaining sets by subtracting eliminated coins stage-by-stage
        _pre  = set(fc.get("pre_filter_passed_syms", []))
        _chk  = set(fc.get("checked_syms", []))
        _f2e  = set(fc.get("f2_elim_syms",  []))
        _f3e  = set(fc.get("f3_elim_syms",  []))
        _f4e  = set(fc.get("f4_elim_syms",  []))
        _f5e  = set(fc.get("f5_elim_syms",  []))
        _f6e  = set(fc.get("f6_elim_syms",  []))
        _f7e  = set(fc.get("f7_elim_syms",  []))
        _f8e  = set(fc.get("f8_elim_syms",  []))
        _f9e    = set(fc.get("f9_elim_syms",         []))
        # Note: f_empty_data_syms stores "SYM(no 3m,1h)" strings for readability
        _fempty = set(fc.get("f_empty_data_syms",    []))
        _pass   = set(fc.get("passed_syms",          []))

        _after_f2 = _chk      - _f2e
        _after_f3 = _after_f2 - _f3e
        _after_f4 = _after_f3 - _f4e
        _after_f5 = _after_f4 - _f5e
        _after_f6 = _after_f5 - _f6e
        _after_f7 = _after_f6 - _f7e
        _after_f8    = _after_f7 - _f8e
        _after_f9    = _after_f8 - _f9e
        # _fempty uses "SYM(no ...)" strings — extract plain symbol for set subtraction
        _fempty_syms = {s.split("(")[0] for s in _fempty}
        _after_empty = _after_f9 - _fempty_syms

        # Process errors = coins that survived all filter stages but threw an
        # exception inside process() before returning a signal dict.
        # Computed as: survived all eliminations - actually passed - empty data drops.
        # Note: _pass may lag by one cycle vs _after_empty due to snapshot timing,
        # so we cap at 0 to avoid negative display values.
        _process_err_count = max(0, fc.get("errors", 0))

        def _coin_str(s: set) -> str:
            return ", ".join(sorted(s)) if s else "—"

        _new_sig_s    = set(fc.get("new_signal_syms",          []))
        _blk_active_s = set(fc.get("blocked_by_active_syms",  []))
        _blk_cool_s   = set(fc.get("blocked_by_cooldown_syms",[]))

        # "Returned Signal" is computed from the _bg_loop side (new + blocked) —
        # this is always internally consistent because all three lists are written
        # in the same lock acquisition, unlike passed_syms which is written
        # during the scan and may be from a different cycle when the UI renders.
        _returned_syms = _new_sig_s | _blk_active_s | _blk_cool_s

        stage_rows = [
            ("⚡ After Bulk Pre-filter",            len(_pre),              _coin_str(_pre)),
            ("🔬 Entered Deep Scan",                len(_chk),              _coin_str(_chk)),
            (f"After {pdz15m_lbl}",                 len(_after_f2),         _coin_str(_after_f2)),
            (f"After {pdz5m_lbl}",                  len(_after_f3),         _coin_str(_after_f3)),
            (f"After {f4_lbl}",                     len(_after_f4),         _coin_str(_after_f4)),
            (f"After {f5_lbl}",                     len(_after_f5),         _coin_str(_after_f5)),
            (f"After {ema_lbl}",                    len(_after_f6),         _coin_str(_after_f6)),
            (f"After {macd_lbl}",                   len(_after_f7),         _coin_str(_after_f7)),
            (f"After {sar_lbl}",                    len(_after_f8),         _coin_str(_after_f8)),
            (f"After {vol_lbl}",                    len(_after_f9),         _coin_str(_after_f9)),
            ("⚠️ Dropped — Empty Candle Data",      len(_fempty),           ", ".join(sorted(_fempty)) if _fempty else "—"),
            ("💥 Dropped — Process Error",          _process_err_count,     "See API Error Log below ↓"),
            ("✅ Returned Signal",                  len(_returned_syms),    _coin_str(_returned_syms)),
            ("🔵 Blocked — Open trade exists",      len(_blk_active_s),     _coin_str(_blk_active_s)),
            ("🟡 Blocked — Cooldown active",         len(_blk_cool_s),       _coin_str(_blk_cool_s)),
            ("🟢 New Signals Fired",                len(_new_sig_s),        _coin_str(_new_sig_s)),
        ]

        st.dataframe(
            [{"Filter Stage": r[0], "Count": r[1], "Qualified Coins": r[2]} for r in stage_rows],
            use_container_width=True,
            hide_index=True,
            column_config={
                "Filter Stage":    st.column_config.TextColumn(width="medium"),
                "Count":           st.column_config.NumberColumn(width="small"),
                "Qualified Coins": st.column_config.TextColumn(width="large"),
            }
        )

# ─────────────────────────────────────────────────────────────────────────────
# Auto-refresh
# ─────────────────────────────────────────────────────────────────────────────
time.sleep(30)
st.rerun()
