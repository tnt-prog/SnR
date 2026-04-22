#!/usr/bin/env python3
"""
MEXC Futures Scanner — Streamlit Dashboard  v2
Performance improvements:
  A. Bulk ticker pre-filter  — 1 API call eliminates ~70% of coins before any candle fetch
  B. Parallel timeframe fetch — 4 timeframes fetched simultaneously per coin
  C. Staged candle fetch      — quick 50-candle RSI/resistance check before full fetch
  D. Symbol cache             — MEXC instrument list cached for 6 h, not re-fetched every cycle
  + API semaphore             — hard cap of 8 concurrent HTTP requests (no exchange blocking)

v2 additions:
  E. N-trade Queue Limit      — when the max open-trade cap (cfg "max_open_trades",
                                 default 15, adjustable from sidebar) is reached, new
                                 signals are logged as status="queue_limit" (no order
                                 placed). The coin is NOT blocked by active or cooldown
                                 filters — it is freely re-scanned every cycle until a
                                 slot opens. Lowering the cap does NOT close existing
                                 trades; overflow simply routes to queue_limit.
  F. Alert column (col 2)     — signals table column 2 shows a quick visual status flag;
                                 turns 🔴 red when an open trade's latest price has dropped
                                 ≥5% below its entry price (early warning, before SL is hit).
                                 Threshold constant: _PRICE_ALERT_PCT = 5.0
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
BASE           = "https://contract.mexc.com"
# MEXC Futures has no 3-minute candles — 3m filters use Min5 data instead.
MEXC_INTERVALS = {"30m": "Min30", "3m": "Min5", "5m": "Min5", "15m": "Min15", "1h": "Hour1"}
MEXC_INTERVALS  = MEXC_INTERVALS   # alias so rest of code works unchanged

# ─────────────────────────────────────────────────────────────────────────────
# API rate-limiter  — hard cap: 8 concurrent HTTP requests at any time
# MEXC public-market limit is 20 req / 2 s.  Staying well below keeps us safe.
# ─────────────────────────────────────────────────────────────────────────────
_api_sem = threading.Semaphore(20)  # max concurrent in-flight requests
# Rate limiter (_rate_wait) already caps at 15 req/s; semaphore just needs to
# allow enough concurrent in-flight so the pipeline stays full at any latency.
# Formula: slots = target_rps × round_trip_latency → 15 × 1.3s ≈ 20 slots.

# ── Token-bucket rate limiter ─────────────────────────────────────────────────
# MEXC public candle endpoint: 40 req / 2 s = 20 req/s hard limit.
# We target 15 req/s (75 % of limit) to stay safe regardless of network latency.
# Semaphore alone is not enough — if MEXC responds in <67 ms we still exceed 20/s.
_RATE_INTERVAL  = 1.0 / 15          # 66.7 ms minimum between token grants
_rate_lock       = threading.Lock()
_rate_last_grant = 0.0              # timestamp of the last granted token

def _rate_wait():
    """Block the calling thread until its turn within the 15 req/s budget."""
    global _rate_last_grant
    while True:
        with _rate_lock:
            now  = time.time()
            wait = _rate_last_grant + _RATE_INTERVAL - now
            if wait <= 0:
                _rate_last_grant = now
                return                  # token granted — proceed immediately
        time.sleep(max(0.001, wait))    # sleep outside lock so others can check

# ─────────────────────────────────────────────────────────────────────────────
# Symbol-cache TTL  (D)
# ─────────────────────────────────────────────────────────────────────────────
_SYMBOL_CACHE_TTL = 6 * 3600   # refresh MEXC instrument list every 6 hours

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
    "use_macd_3m":        True,   # F7 — MACD dark-green (3m)
    "use_macd_5m":        True,   # F7 — MACD dark-green (5m)
    "use_macd_15m":       True,   # F7 — MACD dark-green (15m)
    "use_sar_3m":         True,   # F8 — Parabolic SAR (3m)
    "use_sar_5m":         True,   # F8 — Parabolic SAR (5m)
    "use_sar_15m":        True,   # F8 — Parabolic SAR (15m)
    "use_vol_spike":      False,
    "vol_spike_mult":     2.0,
    "vol_spike_lookback": 20,
    "use_pdz_5m":            True,   # F3 — PDZ (5m)
    "use_pdz_15m":           True,   # F2 — PDZ (15m)
    "use_ema_cross_15m":     True,   # F10 — 15m EMA crossover (fast > slow)
    "ema_cross_fast_15m":    12,
    "ema_cross_slow_15m":    21,
    # ── Queue limit (max concurrent open trades) ──────────────────────────────
    "max_open_trades":       15,     # Hard cap on concurrent open trades.
                                     # Lowering this does NOT close existing
                                     # trades — new signals during overflow are
                                     # logged as queue_limit until natural TP/SL
                                     # closures bring the count down.
    # ── Super Setup cap (max concurrent Super trades) ─────────────────────────
    "max_super_trades":      5,      # Hard cap on concurrent open Super trades.
                                     # When cap is reached, Super-eligible coins
                                     # fall through to the normal F3–F10 pipeline
                                     # and open as regular trades if they pass.
    # ── SL cooldown (per-coin blackout after SL hit) ──────────────────────────
    "sl_cooldown_hours":     24,     # Hours to skip a coin after an SL hit.
                                     # Applies UNIVERSALLY — even to Super Setups.
                                     # Separate from TP cooldown (cooldown_minutes).
    # ── Auto-trading (MEXC / Live) ────────────────────────────────────────
    "trade_enabled":         False,
    "demo_mode":             True,   # True = Demo API, False = Live API
    "api_key":               "",
    "api_secret":            "",
    "api_passphrase":        "",  # Not used by MEXC (kept for config compat)
    "trade_usdt_amount":     10.0,   # USDT collateral per trade (before leverage)
    "trade_leverage":        10,     # leverage applied (capped by MAX_LEVERAGE)
    "trade_margin_mode":     "isolated",  # "cross" or "isolated"
    # ── DCA (Dollar-Cost Averaging) ──────────────────────────────────────────
    # 0 = DCA OFF (legacy behavior — sidebar TP/SL apply normally).
    # 1-6 = Allow up to N automated DCA adds per trade.
    # Each DCA doubles the previous add's margin (10 → 10 → 20 → 40 → 80 …).
    # Isolated: DCA triggers when price reaches 70% of the distance from the
    #           current blended average to the current liquidation price.
    # Cross:    DCA triggers when price drops 7% below the current blended avg.
    # After N DCAs are consumed, a final -3% SL (below blended avg) closes the
    # position into a dedicated DCA SL Hit table. When DCA > 0, the sidebar SL %
    # is ignored for that trade — only TP and the final -3% rule can close it.
    "trade_max_dca":         1,
        "watchlist": [
        "XPDUSDT","WIFUSDT","PIUSDT","EDGEUSDT","RECALLUSDT","SUSHIUSDT","RAVEUSDT","XLMUSDT","DASHUSDT","TRUSTUSDT",
        "GPSUSDT","CROUSDT","ACUUSDT","UNIUSDT","STRKUSDT","ZKPUSDT","APEUSDT","ENJUSDT",
        "INJUSDT","RAYUSDT","OLUSDT","HUSDT","OKBUSDT","APTUSDT","WCTUSDT","NEOUSDT","SNXUSDT","LITUSDT",
        "WUSDT","SYRUPUSDT","AVAXUSDT","LPTUSDT","ACTUSDT","FLOKIUSDT","MEMEUSDT","DOGEUSDT","KGENUSDT",
        "MOODENGUSDT","NOTUSDT","AEROUSDT","STXUSDT","GIGGLEUSDT","AUCTIONUSDT","WALUSDT","ETHUSDT","SHIBUSDT",
        "ZROUSDT","GMXUSDT","LAYERUSDT","ARBUSDT","MINAUSDT","IMXUSDT","LINEAUSDT","VANAUSDT","FOGOUSDT",
        "BASEDUSDT","ZBTUSDT","KITEUSDT","XTZUSDT","SUIUSDT","ATHUSDT","AIXBTUSDT","TRIAUSDT","PIPPINUSDT","ANIMEUSDT",
        "PEPEUSDT","LRCUSDT","DYDXUSDT","LAUSDT","GLMUSDT","CHZUSDT","ACHUSDT","INITUSDT","PLUMEUSDT","BCHUSDT",
        "BLURUSDT","SENTUSDT","ALLOUSDT","XPTUSDT","YGGUSDT","AAVEUSDT","METISUSDT","ZAMAUSDT",
        "MERLUSDT","EGLDUSDT","AVNTUSDT","HMSTRUSDT","AGLDUSDT","ONTUSDT","ALGOUSDT","ADAUSDT","MEUSDT",
        "GALAUSDT","TRXUSDT","BEATUSDT","BABYUSDT","EDENUSDT","PNUTUSDT",
        "BICOUSDT","ICPUSDT","BANDUSDT","LDOUSDT","OFCUSDT","SATSUSDT","ZRXUSDT","ZECUSDT",
        "MORPHOUSDT","QTUMUSDT","SPACEUSDT","SIGNUSDT","TRUTHUSDT","YFIUSDT","1INCHUSDT","BRETTUSDT","SOLUSDT",
        "RIVERUSDT","FARTCOINUSDT","API3USDT","DOTUSDT","JELLYJELLYUSDT","STABLEUSDT","FUSDT","DOODUSDT","COAIUSDT",
        "WLFIUSDT","USDCUSDT","IPUSDT","ATUSDT","WLDUSDT","LQTYUSDT","IOTAUSDT","TRBUSDT","RVNUSDT",
        "KSMUSDT","CFXUSDT","SOPHUSDT","BARDUSDT","UMAUSDT","ZENUSDT","2ZUSDT","YBUSDT","RENDERUSDT",
        "JUPUSDT","MAGICUSDT","TURBOUSDT","ORDIUSDT","PYTHUSDT","ETCUSDT","MEWUSDT","CRVUSDT","MUBARAKUSDT","BIGTIMEUSDT",
        "ORDERUSDT","VIRTUALUSDT","THETAUSDT","ONDOUSDT","LTCUSDT","SPKUSDT","AUSDT","ROBOUSDT","EWJUSDT",
        "ASTERUSDT","BREVUSDT","IOSTUSDT","BTCUSDT","EWYUSDT","BNBUSDT","SAHARAUSDT","RSRUSDT",
        "KAITOUSDT","LINKUSDT","NMRUSDT","XPLUSDT","COMPUSDT","ENAUSDT","CCUSDT",
        "USELESSUSDT","RLSUSDT","HOMEUSDT","GRTUSDT","LIGHTUSDT","KATUSDT","LABUSDT","JTOUSDT",
        "TIAUSDT","SKYUSDT","ENSUSDT","BOMEUSDT","PIEVERSEUSDT","0GUSDT","GASUSDT","SEIUSDT",
        "OPUSDT","BIOUSDT","COREUSDT","MOVEUSDT","GRASSUSDT","KMNOUSDT","SAPIENUSDT","OPNUSDT",
        "TONUSDT","ATOMUSDT","ETHWUSDT","ONEUSDT","ESPUSDT","NIGHTUSDT","BSBUSDT","PENGUUSDT",
        "ETHFIUSDT","SSVUSDT","CVXUSDT","RESOLVUSDT","UPUSDT","METUSDT","SANDUSDT","CELOUSDT","MANAUSDT",
        "POPCATUSDT","TAOUSDT","ARUSDT","FLOWUSDT","SUSDT","AZTECUSDT","ARKMUSDT","WETUSDT","HUMAUSDT","APRUSDT",
        "AEVOUSDT","BATUSDT","ZORAUSDT","BERAUSDT","HYPEUSDT","WOOUSDT","PEOPLEUSDT","PENDLEUSDT",
        "MMTUSDT","EIGENUSDT","POLUSDT","PROVEUSDT","GMTUSDT","ZILUSDT","PARTIUSDT","MASKUSDT","ENSOUSDT",
        "NEARUSDT","SHELLUSDT","ZETAUSDT","XRPUSDT","HBARUSDT","ICXUSDT","SPXUSDT","AXSUSDT",
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
    # MEXC Futures leverage tiers (default 20 for unlisted coins)
    "BTCUSDT":125,"ETHUSDT":100,
    "SOLUSDT":75,"XRPUSDT":75,"DOGEUSDT":75,"BNBUSDT":50,
    "ADAUSDT":50,"LTCUSDT":50,"BCHUSDT":50,"TRXUSDT":50,
    "DOTUSDT":50,"AVAXUSDT":50,"LINKUSDT":50,"UNIUSDT":50,
    "NEARUSDT":50,"APTUSDT":50,"SUIUSDT":50,"ARBUSDT":50,
    "OPUSDT":50,"INJUSDT":50,"TONUSDT":50,"AAVEUSDT":50,
    "XLMUSDT":50,"ATOMUSDT":50,"ETCUSDT":50,"FILUSDT":50,
    "LDOUSDT":50,"STXUSDT":50,"ORDIUSDT":50,"WLDUSDT":50,
    "JUPUSDT":50,"PENDLEUSDT":50,"CRVUSDT":50,"SEIUSDT":50,
    "RENDERUSDT":50,"HBARUSDT":50,"SANDUSDT":50,"GALAUSDT":50,
    "AXSUSDT":50,"APEUSDT":50,"GRTUSDT":50,"ENAUSDT":50,
}

def get_max_leverage(sym: str) -> int:
    return MAX_LEVERAGE.get(sym, 20)

# ─────────────────────────────────────────────────────────────────────────────
# SL reason analyzer
# ─────────────────────────────────────────────────────────────────────────────
def analyze_sl_reason(sig: dict) -> str:
    """
    Post-mortem analysis of a stopped-out trade using data captured at entry.
    Pure computation — no API calls, no I/O. Safe to call during UI render.
    Produces specific, actionable observations and improvement suggestions.
    """
    criteria = sig.get("criteria", {})
    reasons  = []
    improve  = []   # improvement suggestions for next release

    # ── Helper: safe float conversion (criteria may store "—" strings) ────────
    def _f(key, default=None):
        try:
            v = criteria.get(key, default)
            return float(v) if v not in (None, "—", "") else default
        except (TypeError, ValueError):
            return default

    # ── 1. Speed of loss ─────────────────────────────────────────────────────
    # Fast SL = bad entry timing.  Slow SL = sustained trend reversal.
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
                f"SL hit in {duration_mins} min — entry was at a local price peak. "
                f"Consider adding a 1–2 candle confirmation delay before entry.")
            improve.append("Add 1-candle confirmation delay (wait for close above entry signal)")
        elif duration_mins <= 15:
            reasons.append(
                f"SL hit in {duration_mins} min — very fast reversal. "
                f"Momentum faded immediately after entry.")
            improve.append("Tighten entry timing — require EMA alignment on 3m before entry")
        elif duration_mins >= 120:
            reasons.append(
                f"Trade held {duration_mins} min before SL — sustained trend reversal. "
                f"Macro / higher-TF bias likely shifted after entry.")
            improve.append("Add 4h or daily trend filter to avoid counter-trend entries")

    # ── 2. Super Setup bypass ─────────────────────────────────────────────────
    if sig.get("is_super_setup"):
        reasons.append(
            "Super Setup — 15m Discount zone bypassed all filters. "
            "No RSI / MACD / SAR confirmation was required.")
        improve.append(
            "Consider requiring at least RSI 5m ≥ 40 even for Super Setups "
            "to avoid entering during momentum exhaustion")

    # ── 3. PDZ zone risk ──────────────────────────────────────────────────────
    pdz_5m  = str(criteria.get("pdz_zone_5m",  "") or "")
    pdz_15m = str(criteria.get("pdz_zone_15m", "") or "")
    for zone_label, zone_val in [("5m", pdz_5m), ("15m", pdz_15m)]:
        if "BandA" in zone_val:
            reasons.append(
                f"PDZ {zone_label} zone was {zone_val} — price was near the Premium "
                f"boundary. BandA entries carry higher reversal risk than Discount entries.")
            improve.append(
                f"Consider restricting {zone_label} PDZ to Discount + Equilibrium only "
                f"(disable BandA/BandB) for lower-risk entries")
        elif "BandB" in zone_val:
            reasons.append(
                f"PDZ {zone_label} zone was {zone_val} — borderline zone with elevated "
                f"Premium exposure. Higher false-signal rate than Discount.")
            improve.append(
                f"Tighten {zone_label} PDZ to Discount / Equilibrium to reduce BandB SL rate")
        elif "Premium" in zone_val:
            reasons.append(
                f"PDZ {zone_label} zone was Premium — price was in overbought territory "
                f"at entry. This zone has the highest reversal probability.")

    # ── 4. RSI analysis ───────────────────────────────────────────────────────
    rsi_5m = _f("rsi_5m")
    rsi_1h = _f("rsi_1h")

    if rsi_5m is not None:
        if rsi_5m < 35:
            reasons.append(
                f"RSI 5m was very low at entry ({rsi_5m:.1f}) — short-term momentum "
                f"was already fading before trade opened.")
            improve.append("Raise RSI 5m minimum threshold (e.g. 42 → 48) to avoid fading momentum")
        elif rsi_5m < 42:
            reasons.append(
                f"RSI 5m borderline ({rsi_5m:.1f}) — weak short-term momentum at entry.")
        elif rsi_5m > 75:
            reasons.append(
                f"RSI 5m was high at entry ({rsi_5m:.1f}) — near overbought on 5m, "
                f"limited upside headroom.")
            improve.append("Add RSI 5m upper cap (e.g. max 72) to avoid overbought entries")

    if rsi_1h is not None:
        if rsi_1h > 82:
            reasons.append(
                f"RSI 1h near overbought ({rsi_1h:.1f}) — hourly timeframe showing "
                f"exhaustion. Higher-TF reversal likely.")
            improve.append("Lower RSI 1h maximum threshold (e.g. 95 → 80)")
        elif rsi_1h < 40:
            reasons.append(
                f"RSI 1h weak ({rsi_1h:.1f}) — no bullish higher-TF support at entry.")
            improve.append("Raise RSI 1h minimum threshold (e.g. 30 → 45)")

    # ── 5. RSI divergence (5m strong but 1h weak) ────────────────────────────
    if rsi_5m is not None and rsi_1h is not None:
        if rsi_5m >= 55 and rsi_1h < 45:
            reasons.append(
                f"RSI divergence — 5m ({rsi_5m:.1f}) was bullish but 1h ({rsi_1h:.1f}) "
                f"was bearish. Short-term bounce against the hourly trend.")
            improve.append(
                "Enforce RSI 1h ≥ RSI 5m × 0.75 to reduce timeframe divergence entries")

    # ── 6. MACD status at entry ───────────────────────────────────────────────
    macd_flags = {
        "3m":  criteria.get("macd_3m"),
        "5m":  criteria.get("macd_5m"),
        "15m": criteria.get("macd_15m"),
    }
    disabled_macd = [tf for tf, v in macd_flags.items() if v in (None, "—", "")]
    false_macd    = [tf for tf, v in macd_flags.items()
                     if str(v).strip().lower() in ("false", "0", "no")]
    if disabled_macd:
        reasons.append(
            f"MACD was disabled for {', '.join(disabled_macd)} timeframe(s) at entry — "
            f"no histogram confirmation on those timeframes.")
        improve.append(
            f"Enable MACD on {', '.join(disabled_macd)} for additional confirmation")
    if false_macd:
        reasons.append(
            f"MACD was bearish on {', '.join(false_macd)} timeframe(s) at entry — "
            f"filter was active but histogram was dark-red / declining.")

    # ── 7. SAR status at entry ────────────────────────────────────────────────
    sar_flags = {
        "3m":  criteria.get("sar_3m"),
        "5m":  criteria.get("sar_5m"),
        "15m": criteria.get("sar_15m"),
    }
    disabled_sar = [tf for tf, v in sar_flags.items() if v in (None, "—", "")]
    if disabled_sar:
        reasons.append(
            f"Parabolic SAR was disabled for {', '.join(disabled_sar)} timeframe(s) — "
            f"no trend-direction confirmation on those timeframes.")
        improve.append(
            f"Enable SAR on {', '.join(disabled_sar)} as additional reversal guard")

    # ── 8. Volume conviction ──────────────────────────────────────────────────
    vol_ratio = _f("vol_ratio")
    if vol_ratio is not None:
        if vol_ratio < 1.5:
            reasons.append(
                f"Volume spike was only {vol_ratio:.1f}× average — low conviction. "
                f"Weak volume entries have higher false-signal rate.")
            improve.append("Raise volume spike minimum to 2.0× average for stronger conviction")
        elif vol_ratio < 2.0:
            reasons.append(
                f"Volume spike was {vol_ratio:.1f}× average — moderate conviction. "
                f"A 2×+ spike would indicate stronger participation.")

    # ── Fallback if nothing specific was found ────────────────────────────────
    if not reasons:
        reasons.append(
            "All entry filters were healthy — reversal was caused by an external "
            "market event (macro news, liquidation cascade, or broader market dump) "
            "that no technical filter can predict.")
        improve.append(
            "Consider adding a market-wide sentiment check (e.g. BTC dominance or "
            "funding rate) to avoid entries during high-volatility macro windows")

    # ── Build output ──────────────────────────────────────────────────────────
    lines = [f"• {r}" for r in reasons]
    if improve:
        lines.append("")
        lines.append("💡 Improvement suggestions:")
        lines.extend(f"  → {i}" for i in improve)

    return "\n".join(lines)

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
CONFIG_FILE  = _SCRIPT_DIR / "mexc_scanner_config.json"
LOG_FILE     = _SCRIPT_DIR / "mexc_scanner_log.json"
_config_lock = threading.Lock()

def load_config() -> dict:
    """Load persisted config, applying env-var overrides for API credentials.

    Environment variables (preferred — never written back to disk):
      • MEXC_API_KEY
      • MEXC_API_SECRET
      • MEXC_API_PASSPHRASE
    These take precedence over the plaintext values in scanner_config.json.
    If the user has set them, the on-disk credentials can stay blank.
    """
    cfg = dict(DEFAULT_CONFIG)
    if CONFIG_FILE.exists():
        try:
            saved = json.loads(CONFIG_FILE.read_text(encoding="utf-8"))
            for k in DEFAULT_CONFIG:
                if k in saved:
                    cfg[k] = saved[k]
        except (json.JSONDecodeError, OSError, UnicodeDecodeError) as _e:
            # Corrupt/unreadable config — log if error-logger is live, fall back to defaults.
            _log_fn = globals().get("_append_error")
            if callable(_log_fn):
                try:
                    _log_fn("io", f"load_config: {type(_e).__name__}: {_e}")
                except Exception:
                    pass
            else:
                print(f"[Config] WARNING — failed to parse {CONFIG_FILE}: {type(_e).__name__}: {_e}")

    # Env-var overrides (credentials only — never persisted)
    import os as _os
    for _cfg_k, _env_k in (("api_key",        "MEXC_API_KEY"),
                           ("api_secret",     "MEXC_API_SECRET"),
                           ("api_passphrase", "MEXC_API_PASSPHRASE")):
        _v = _os.environ.get(_env_k, "").strip()
        if _v:
            cfg[_cfg_k] = _v
    return cfg

def save_config(cfg: dict):
    """Persist config to disk. Strips env-var-sourced credentials so they are
    never written in plaintext. Sets 0o600 permissions on POSIX (best effort).
    Errors are logged, not raised, so a failed save never crashes the loop.
    """
    import os as _os
    # If the user supplied credentials via env vars, do NOT write them back
    # to the JSON file — keep the stored values blank so the secrets never
    # land on disk from the env-var path.
    _cfg_to_save = dict(cfg)
    for _cfg_k, _env_k in (("api_key",        "MEXC_API_KEY"),
                           ("api_secret",     "MEXC_API_SECRET"),
                           ("api_passphrase", "MEXC_API_PASSPHRASE")):
        if _os.environ.get(_env_k, "").strip():
            # Preserve whatever the user previously typed into the UI by leaving
            # the key blank if env is the active source.
            _cfg_to_save[_cfg_k] = ""
    try:
        with _config_lock:
            CONFIG_FILE.write_text(json.dumps(_cfg_to_save, indent=2), encoding="utf-8")
            # Best-effort: restrict to owner-only read/write (POSIX).
            # On Windows, os.chmod only toggles the read-only bit — harmless.
            try:
                _os.chmod(CONFIG_FILE, 0o600)
            except (OSError, NotImplementedError):
                pass
    except (OSError, TypeError, ValueError) as _e:
        _log_fn = globals().get("_append_error")
        if callable(_log_fn):
            try:
                _log_fn("io", f"save_config: {type(_e).__name__}: {_e}")
            except Exception:
                pass
        else:
            print(f"[Config] WARNING — failed to save {CONFIG_FILE}: {type(_e).__name__}: {_e}")

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
        except (json.JSONDecodeError, OSError, UnicodeDecodeError) as _e:
            # Corrupt log — don't crash the app; log and fall back to empty.
            _log_fn = globals().get("_append_error")
            if callable(_log_fn):
                try:
                    _log_fn("io", f"load_log: {type(_e).__name__}: {_e}")
                except Exception:
                    pass
            else:
                print(f"[Log] WARNING — failed to parse {LOG_FILE}: {type(_e).__name__}: {_e}")
    return {"health": {"total_cycles": 0, "last_scan_at": None,
                        "last_scan_duration_s": 0.0, "total_api_errors": 0,
                        "watchlist_size": 0, "pre_filtered_out": 0,
                        "deep_scanned": 0},
            "signals": []}

def save_log(log):
    """Atomic-ish write of the log JSON. All errors are logged, never raised —
    a failed save must not abort the scanner loop.
    """
    try:
        LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
        LOG_FILE.write_text(json.dumps(log, indent=2), encoding="utf-8")
    except (OSError, TypeError, ValueError) as _e:
        _log_fn = globals().get("_append_error")
        if callable(_log_fn):
            try:
                _log_fn("io", f"save_log: {type(_e).__name__}: {_e}")
            except Exception:
                pass
        else:
            print(f"[Log] WARNING — failed to save {LOG_FILE}: {type(_e).__name__}: {_e}")

# ─────────────────────────────────────────────────────────────────────────────
# Module-level shared state
# ─────────────────────────────────────────────────────────────────────────────
_MEXC_CODE_VERSION = "2.3"   # bump this on every deploy to force full re-init

import builtins as _b_check
_needs_init = (
    not getattr(_b_check, "_mexc_scanner_globals_set", False) or
    getattr(_b_check, "_mexc_code_version", "") != _MEXC_CODE_VERSION
)

if _needs_init:
    import builtins as _b
    # Stop the old background thread if it exists (version mismatch = stale thread)
    _old_thread = getattr(_b, "_msc_thread", None)
    _old_running = getattr(_b, "_msc_running", None)
    if _old_running is not None:
        _old_running.clear()   # signal old loop to exit at next sleep
    _b._mexc_scanner_globals_set = True
    _b._mexc_code_version = _MEXC_CODE_VERSION
    _b._msc_cfg           = load_config()
    _b._msc_log           = load_log()
    _b._msc_log_lock      = threading.Lock()
    _b._msc_running       = threading.Event()
    _b._msc_running.set()
    _b._msc_thread        = None
    _b._msc_filter_counts = {}
    _b._msc_filter_lock   = threading.Lock()
    _b._msc_last_error    = ""
    _b._msc_rescan_event  = threading.Event()
    _b._msc_sl_paused     = False
    _b._msc_api_conn_status = {
        "status":      "untested",
        "message":     "",
        "tested_at":   None,
        "demo_mode":   None,
        "uid":         "",
        "pos_mode":    "net_mode",
        "acct_lv":     "2",
    }
    _b._msc_last_trade_raw  = {}
    _b._msc_error_log       = []
    _b._msc_error_log_lock  = threading.Lock()
    _b._msc_symbol_cache  = {"symbols": [], "fetched_at": 0, "wl_key": "", "ct_val": {}}

if "_mexc_scanner_initialised" not in st.session_state:
    st.session_state["_mexc_scanner_initialised"] = True

import builtins as _b
_cfg             = _b._msc_cfg
_log             = _b._msc_log
_log_lock        = _b._msc_log_lock
_scanner_running = _b._msc_running
_filter_lock     = _b._msc_filter_lock
_filter_counts   = _b._msc_filter_counts
_rescan_event    = _b._msc_rescan_event

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
        # Increase connection pool to match semaphore concurrency (20 slots).
        # Default pool_maxsize=10 would cause urllib3 to queue connections,
        # adding extra latency on top of the network round-trip.
        from requests.adapters import HTTPAdapter
        adapter = HTTPAdapter(pool_connections=4, pool_maxsize=25, max_retries=0)
        s.mount("https://", adapter)
        s.mount("http://",  adapter)
        _local.session = s
    return _local.session

def safe_get(url, params=None, _retries=4):
    for attempt in range(_retries):
        try:
            _rate_wait()                            # ← token-bucket: max 15 req/s
            with _api_sem:                          # ← concurrency cap: max 20 in-flight
                r = get_session().get(url, params=params, timeout=10)
            if r.status_code == 429:
                wait = min(int(r.headers.get("Retry-After", 5)), 5)
                time.sleep(wait); continue
            # Log raw snippet for debugging before anything else
            _raw_snippet = r.text[:400] if r.text else ""
            # ── Only raise on real HTTP errors — NEVER on application-level codes ──
            # MEXC returns HTTP 200 even for error responses (code != 0 / success=false).
            # Application-level error checking must be done by each caller, not here.
            # Exception: hard HTTP blocks (403/418/451/429 already handled above).
            if r.status_code >= 400:
                _append_error("http",
                    f"HTTP {r.status_code} from {url} | raw={_raw_snippet!r}",
                    endpoint=url)
                raise RuntimeError(
                    f"HTTP {r.status_code} from {url} — "
                    f"{'MEXC is blocking this IP. Try Railway (railway.app).' if r.status_code in (403,418,451) else _raw_snippet[:120]}")
            try:
                data = r.json()
            except Exception as _je:
                raise RuntimeError(
                    f"MEXC non-JSON response from {url}: {_je} | raw={_raw_snippet!r}")
            # Log any application-level error for visibility — but DO NOT raise.
            # Callers receive the raw dict and decide what to do with it.
            if isinstance(data, dict):
                _sc = data.get("success")
                _cd = data.get("code")
                if _sc is False or (_sc is None and _cd is not None and str(_cd) not in ("0","200")):
                    _msg = data.get("message", data.get("msg", ""))
                    _append_error("http",
                        f"[MEXC app-error] code={_cd} success={_sc} msg={_msg!r} "
                        f"url={url} raw={_raw_snippet[:200]!r}",
                        endpoint=url)
            return data
        except requests.exceptions.ConnectionError as _ce:
            _append_error("http", f"ConnectionError attempt {attempt+1}: {_ce}", endpoint=url)
            if attempt < _retries - 1: time.sleep(1); continue
            raise
        except RuntimeError:
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
    endpoint : MEXC API path, if known
    """
    entry = {
        "ts":       dubai_now().isoformat(),
        "type":     err_type,
        "symbol":   symbol,
        "endpoint": endpoint,
        "message":  message[:400],   # cap length for display
    }
    try:
        lock = getattr(_b, "_msc_error_log_lock", None)
        log  = getattr(_b, "_msc_error_log", None)
        if lock is None or log is None:
            return
        with lock:
            log.append(entry)
            if len(log) > _ERROR_LOG_MAX:
                del log[:len(log) - _ERROR_LOG_MAX]
    except Exception:
        pass   # never let the logger itself crash the scanner

# ─────────────────────────────────────────────────────────────────────────────
# MEXC symbol helpers
# ─────────────────────────────────────────────────────────────────────────────
def _to_mexc(sym: str) -> str:
    return f"{sym[:-4]}_USDT" if sym.endswith("USDT") else sym

def _from_mexc(inst_id: str) -> str:
    return inst_id.replace("_USDT", "USDT") if inst_id.endswith("_USDT") else inst_id

# Aliases so unchanged code that calls _to_okx / _from_okx still works
_to_okx   = _to_mexc
_from_okx = _from_mexc

def get_symbols(watchlist: list) -> tuple:
    """Return (watchlist_symbols_live_on_MEXC, ct_val_dict).
    ct_val_dict maps e.g. 'BTCUSDT' → contract size in base currency (float).
    """
    active  = set()
    ct_vals = {}
    data    = safe_get(f"{BASE}/api/v1/contract/detail")
    # ── Always log the raw response shape so we can diagnose any issues ──────
    _d_type = type(data).__name__
    _d_keys = list(data.keys()) if isinstance(data, dict) else "not-a-dict"
    _items  = data.get("data") if isinstance(data, dict) else None
    _items  = _items if isinstance(_items, list) else []
    _append_error("http",
        f"[diag] /api/v1/contract/detail → type={_d_type} keys={_d_keys} "
        f"data_len={len(_items)} success={data.get('success') if isinstance(data,dict) else 'N/A'} "
        f"code={data.get('code') if isinstance(data,dict) else 'N/A'}",
        endpoint="/api/v1/contract/detail")
    for s in _items:
        inst_id = s.get("symbol", "")
        if inst_id.endswith("_USDT"):
            sym = _from_mexc(inst_id)
            active.add(sym)
            try:
                _cv = float(s.get("contractSize") or 0)
                ct_vals[sym] = _cv if _cv > 0 else 0.0
            except (TypeError, ValueError):
                ct_vals[sym] = 0.0
    skipped = [s for s in watchlist if s not in active]
    if skipped:
        print(f"  [Symbol cache] {len(skipped)} watchlist coins not on MEXC: {skipped[:5]}...")
    return [s for s in watchlist if s in active], ct_vals

# ─────────────────────────────────────────────────────────────────────────────
# D — Cached symbol lookup (refresh every 6 h or on watchlist change)
# ─────────────────────────────────────────────────────────────────────────────
def get_symbols_cached(watchlist: list) -> list:
    now    = time.time()
    cache  = _b._msc_symbol_cache
    wl_key = ",".join(sorted(watchlist))
    if (not cache["symbols"] or
            now - cache["fetched_at"] > _SYMBOL_CACHE_TTL or
            cache["wl_key"] != wl_key):
        print("[Scanner] Refreshing MEXC symbol cache…")
        syms, ct_vals       = get_symbols(watchlist)
        cache["symbols"]    = syms
        cache["ct_val"]     = ct_vals
        cache["fetched_at"] = now
        cache["wl_key"]     = wl_key
    return list(cache["symbols"])

# ─────────────────────────────────────────────────────────────────────────────
# MEXC Auto-Trading — authentication + order placement
# ─────────────────────────────────────────────────────────────────────────────

def _mexc_sign(api_key: str, timestamp: str, body_str: str, secret: str) -> str:
    """HMAC-SHA256 signature for MEXC Futures private endpoints.
    Signature = HMAC-SHA256(api_key + timestamp + body_str, api_secret).hexdigest()
    """
    msg = api_key + timestamp + body_str
    return hmac.new(secret.encode("utf-8"), msg.encode("utf-8"), hashlib.sha256).hexdigest()

def _trade_post(path: str, body: dict, cfg: dict) -> dict:
    """Authenticated POST to a MEXC Futures private endpoint.
    Note: MEXC does not support simulated/demo trading via headers.
    All orders are live — use a test account for paper trading.
    """
    ts       = str(int(time.time() * 1000))
    body_str = json.dumps(body)
    sign     = _mexc_sign(cfg["api_key"], ts, body_str, cfg["api_secret"])
    headers  = {
        "ApiKey":       cfg["api_key"],
        "Request-Time": ts,
        "Signature":    sign,
        "Content-Type": "application/json",
    }
    sess   = get_session()
    merged = {**dict(sess.headers), **headers}
    with _api_sem:
        r = sess.post(f"{BASE}{path}", headers=merged, data=body_str, timeout=20)
    r.raise_for_status()
    try:
        return r.json()
    except (json.JSONDecodeError, ValueError) as _je:
        _snippet = r.text[:200].replace("\n", " ") if r.text else ""
        raise RuntimeError(
            f"MEXC POST {path} returned non-JSON response "
            f"(status={r.status_code}): {_snippet!r}"
        ) from _je

def _trade_get(path: str, params: dict, cfg: dict) -> dict:
    """Authenticated GET to a MEXC Futures private endpoint."""
    qs     = "&".join(f"{k}={v}" for k, v in sorted(params.items())) if params else ""
    ts     = str(int(time.time() * 1000))
    sign   = _mexc_sign(cfg["api_key"], ts, qs, cfg["api_secret"])
    headers = {
        "ApiKey":       cfg["api_key"],
        "Request-Time": ts,
        "Signature":    sign,
        "Content-Type": "application/json",
    }
    sess   = get_session()
    merged = {**dict(sess.headers), **headers}
    with _api_sem:
        r = sess.get(f"{BASE}{path}", params=params, headers=merged, timeout=20)
    r.raise_for_status()
    try:
        return r.json()
    except (json.JSONDecodeError, ValueError) as _je:
        _snippet = r.text[:200].replace("\n", " ") if r.text else ""
        raise RuntimeError(
            f"MEXC GET {path} returned non-JSON response "
            f"(status={r.status_code}): {_snippet!r}"
        ) from _je

def test_api_connection(cfg: dict) -> dict:
    """
    Verify MEXC Futures API credentials via GET /api/v1/private/account/assets.
    Note: MEXC does not support demo/simulated trading via API headers.
    Returns {"status":"ok"|"error", "message":str, "uid":str, "pos_mode":str}.
    """
    if not cfg.get("api_key") or not cfg.get("api_secret"):
        return {"status": "error", "message": "API Key and Secret are required for MEXC.",
                "uid": "", "pos_mode": "net_mode"}
    try:
        resp = _trade_get("/api/v1/private/account/assets", {}, cfg)
        if resp.get("success") is False or (resp.get("success") is None and resp.get("code", 200) not in (0, 200)):
            msg = f"MEXC error {resp.get('code','?')}: {resp.get('message', 'unknown')}"
            return {"status": "error", "message": msg, "uid": "", "pos_mode": "net_mode"}

        assets   = resp.get("data", [])
        usdt_ast = next((a for a in assets if a.get("currency") == "USDT"), {})
        total_eq = float(usdt_ast.get("equity", 0) or 0)
        try:
            eq_str = f"{total_eq:,.2f}"
        except (ValueError, TypeError):
            eq_str = str(total_eq)

        msg = f"Connected (Live) · USDT Equity: {eq_str} · MEXC Futures"
        return {"status": "ok", "message": msg, "uid": "", "pos_mode": "net_mode",
                "acct_lv": "2"}

    except Exception as exc:
        return {"status": "error", "message": str(exc), "uid": "", "pos_mode": "net_mode"}

def _get_ct_val(sym: str) -> float:
    """
    Return contract size (ctVal) for a symbol.
    Uses the symbol cache populated at each scan cycle.
    Falls back to a live API call if the cache is cold.

    IMPORTANT: Never falls back to 1.0.  If ctVal cannot be confirmed from
    MEXC, raises ValueError so the caller can refuse the order.  Using 1.0 as
    a fallback for a token whose real ctVal is e.g. 10 would send 10× the
    intended position size.
    """
    ct = _b._msc_symbol_cache.get("ct_val", {}).get(sym)
    if ct and ct > 0:
        return ct
    # Cache miss — fetch on-demand (rare, e.g. first run before any scan)
    try:
        data = safe_get(f"{BASE}/api/v1/contract/detail")
        for s in data.get("data", []):
            if s.get("symbol") == _to_mexc(sym):
                val = float(s.get("contractSize", 0) or 0)
                if val > 0:
                    _b._msc_symbol_cache.setdefault("ct_val", {})[sym] = val
                    _append_error("info",
                        f"contractSize for {_to_mexc(sym)} fetched on-demand: {val} "
                        f"(cache was cold — populated now)")
                    return val
    except Exception as _e:
        raise ValueError(
            f"contractSize for {_to_mexc(sym)} could not be fetched from MEXC "
            f"({_e}) — order refused to prevent wrong position sizing"
        ) from _e
    raise ValueError(
        f"contractSize for {_to_mexc(sym)} not found in MEXC contract detail "
        f"— order refused to prevent wrong position sizing"
    )

def _set_leverage_okx(sym: str, cfg: dict) -> None:
    """No-op for MEXC — leverage is passed directly in each order body."""
    pass

_MEXC_KNOWN_ERRORS = {
    "1001": "Request parameter error — check symbol, vol, or leverage.",
    "1002": "Insufficient balance.",
    "1003": "Invalid API key — check credentials.",
    "1004": "Signature mismatch — check API secret.",
    "1005": "Permission denied — API key may lack trading permissions.",
    "1006": "Order quantity below minimum.",
    "1007": "Order price out of limit.",
    "1008": "Instrument not found — coin may be delisted, remove from watchlist.",
    "1009": "Leverage exceeds maximum allowed for this symbol.",
    "1010": "Position not found.",
    "1100": "IP not whitelisted.",
}

def _okx_err(resp: dict) -> str:
    """Extract the most specific error string from a MEXC response dict."""
    code = str(resp.get("code", "?"))
    msg  = resp.get("message", resp.get("msg", "unknown"))
    friendly = _MEXC_KNOWN_ERRORS.get(code)
    detail   = friendly if friendly else msg
    return f"[{code}] {detail}"

def place_okx_order(sig: dict, cfg: dict) -> dict:
    """
    Place a market LONG order on MEXC (demo or live), then immediately place a
    separate OCO algo order (TP + SL in one call) via /api/v1/private/order/submit.

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
        # ── Input validation — fail fast with a clear error, BEFORE touching
        # any MEXC endpoint. A malformed signal (missing field, None, unparseable
        # string, or nonsensical values like tp≤entry / sl≥entry for a long)
        # would otherwise cascade into opaque exchange rejections or silently
        # size a position incorrectly. Catch all of that here.
        sym = sig.get("symbol", "")
        if not sym:
            return {"ordId": "", "algoId": "", "sz": 0, "status": "error",
                    "error": "Signal missing 'symbol' field — refusing to place."}
        try:
            entry = float(sig["entry"])
            tp    = float(sig["tp"])
            sl    = float(sig["sl"])
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
        # LONG sanity: SL must be below entry; TP must be above entry.
        if sl >= entry:
            _err = f"LONG SL ({sl}) must be below entry ({entry})."
            _append_error("trade", _err, symbol=sym)
            return {"ordId": "", "algoId": "", "sz": 0,
                    "status": "error", "error": _err}
        if tp <= entry:
            _err = f"LONG TP ({tp}) must be above entry ({entry})."
            _append_error("trade", _err, symbol=sym)
            return {"ordId": "", "algoId": "", "sz": 0,
                    "status": "error", "error": _err}

        lev    = min(_lev_cfg, get_max_leverage(sym))
        mode   = cfg.get("trade_margin_mode", "isolated")

        # ── Pre-trade instrument validation ──────────────────────────────────
        ct_cache = _b._msc_symbol_cache.get("ct_val", {})
        if ct_cache and sym not in ct_cache:
            err = (f"Instrument {_to_mexc(sym)} not found in MEXC Futures — "
                   f"may be delisted. Remove {sym} from watchlist.")
            _append_error("trade", err, symbol=sym)
            return {"ordId": "", "algoId": "", "sz": 0, "status": "error", "error": err}

        try:
            ct_val = _get_ct_val(sym)
        except ValueError as _ctv_err:
            _err = str(_ctv_err)
            _append_error("trade", _err, symbol=sym)
            return {"ordId": "", "algoId": "", "sz": 0, "status": "error",
                    "error": _err,
                    "ct_val": 0, "notional": 0, "tdMode": mode, "is_hedge": False}
        notional  = usdt * lev
        _base_info = {"ct_val": ct_val, "notional": notional,
                      "tdMode": mode, "is_hedge": False}
        if ct_val <= 0 or entry <= 0:
            return {"ordId": "", "algoId": "", "sz": 0, "status": "error",
                    "error": f"Bad ct_val ({ct_val}) or entry ({entry}) — cannot size position",
                    **_base_info}
        contracts = max(1, int(notional / (ct_val * entry)))
        if contracts > 100_000:
            return {"ordId": "", "algoId": "", "sz": contracts, "status": "error",
                    "error": (f"Contract count too large ({contracts}) — "
                              f"ct_val={ct_val}, entry={entry}, notional={notional}. "
                              f"Check contractSize for {sym}."),
                    **_base_info}

        # ── Compute TP/SL prices from signal values ───────────────────────────
        tp_pct    = float(cfg.get("tp_pct", 1.5)) / 100
        actual_tp = _pround(entry * (1 + tp_pct))
        if mode == "isolated":
            actual_sl = _pround(entry * (1 - 1 / lev))
        else:
            sl_pct    = float(cfg.get("sl_pct", 3.0)) / 100
            actual_sl = _pround(entry * (1 - sl_pct))

        # ── Place market long order with inline TP/SL ─────────────────────────
        # MEXC side: 1=open long | openType: 1=isolated, 2=cross | type: 2=market
        open_type  = 1 if mode == "isolated" else 2
        order_body: dict = {
            "symbol":           _to_mexc(sym),
            "side":             1,
            "openType":         open_type,
            "type":             2,
            "vol":              contracts,
            "leverage":         lev,
            "takeProfitPrice":  actual_tp,
            "stopLossPrice":    actual_sl,
        }
        resp   = _trade_post("/api/v1/private/order/submit", order_body, cfg)
        _b._msc_last_trade_raw = {
            "endpoint":  "/api/v1/private/order/submit",
            "body_sent": order_body,
            "response":  resp,
            "contracts": contracts,
            "ct_val":    ct_val,
        }
        if resp.get("success") is False or (resp.get("success") is None and resp.get("code", 200) not in (0, 200)):
            err = _okx_err(resp)
            _append_error("trade",
                          f"{err} | body={json.dumps(order_body)} | resp={json.dumps(resp)[:300]}",
                          symbol=sym, endpoint="/api/v1/private/order/submit")
            return {"ordId": "", "algoId": "", "sz": contracts,
                    "status": "error", "error": err, **_base_info}

        ord_id = str(resp.get("data", "") or "")
        return {"ordId": ord_id, "algoId": "", "sz": contracts,
                "status": "placed", "error": "",
                "actual_entry": entry,
                "actual_tp": actual_tp, "actual_sl": actual_sl,
                **_base_info}

    except Exception as exc:
        _append_error("trade", str(exc), symbol=sig.get("symbol", ""),
                      endpoint="/api/v1/private/order/submit")
        return {"ordId": "", "algoId": "", "sz": 0,
                "status": "error", "error": str(exc)}

def place_okx_manual_order(sym: str, entry: float, tp: float, sl: float,
                           cfg: dict) -> dict:
    """
    Place a manually specified order:
      • entry > 0  → LIMIT buy at exactly that price, TP/SL used as-is
      • entry == 0 → MARKET buy at live price, TP/SL used as-is (no recalc)

    Unlike place_okx_order (auto-trading), this function never recalculates
    TP or SL — the user's values are sent directly to MEXC.

    Returns {"ordId", "algoId", "sz", "status", "error",
             "actual_entry", "actual_tp", "actual_sl"}
    """
    try:
        usdt   = float(cfg.get("trade_usdt_amount", 10))
        lev    = min(int(cfg.get("trade_leverage", 10)), get_max_leverage(sym))
        mode   = cfg.get("trade_margin_mode", "isolated")

        # Pre-trade instrument check
        ct_cache = _b._msc_symbol_cache.get("ct_val", {})
        if ct_cache and sym not in ct_cache:
            err = f"Instrument {_to_mexc(sym)} not found in MEXC Futures list."
            _append_error("trade", err, symbol=sym)
            return {"ordId": "", "algoId": "", "sz": 0,
                    "status": "error", "error": err}

        try:
            ct_val = _get_ct_val(sym)
        except ValueError as _ctv_err:
            _err = str(_ctv_err)
            _append_error("trade", _err, symbol=sym)
            return {"ordId": "", "algoId": "", "sz": 0, "status": "error", "error": _err}
        ref_price = entry if entry > 0 else None

        # If no entry price given, fetch live market price for sizing
        if ref_price is None:
            try:
                tick = safe_get(f"{BASE}/api/v1/contract/ticker",
                                {"symbol": _to_mexc(sym)})
                _tick_data = (tick.get("data") if isinstance(tick, dict) else None) or [{}]
                ref_price = float((_tick_data[0] if _tick_data else {}).get("lastPrice", 0) or 0)
            except Exception:
                pass
        if not ref_price or ref_price <= 0:
            return {"ordId": "", "algoId": "", "sz": 0, "status": "error",
                    "error": "Could not determine price for contract sizing."}

        contracts = max(1, int((usdt * lev) / (ct_val * ref_price)))

        # Position mode
        is_hedge = False
        try:
            _pm_resp = _trade_get("/api/v1/private/account/assets", {}, cfg)
            if _pm_resp.get("code") == "0":
                _pm      = _pm_resp.get("data", [{}])[0].get("posMode", "net_mode")
                is_hedge = (_pm == "long_short_mode")
                _cs = getattr(_b, "_msc_api_conn_status", None)
                if _cs is not None:
                    _cs["pos_mode"] = _pm
        except Exception:
            is_hedge = (getattr(_b, "_msc_api_conn_status", {})
                        .get("pos_mode", "net_mode") == "long_short_mode")

        # Set leverage
        try:
            _set_leverage_okx(sym, cfg)
        except Exception as lev_exc:
            _append_error("trade", f"set-leverage warning: {lev_exc}", symbol=sym)

        # ── Build order body ──────────────────────────────────────────────────
        # LIMIT: embed TP/SL via attachAlgoOrds in the same request.
        #   MEXC activates the order when the limit order fills — no race condition.
        # MARKET: two-step (entry → OCO), same as auto-trading.
        if entry > 0:
            # LIMIT order with TP/SL attached inline
            order_body: dict = {
                "instId":  _to_mexc(sym),
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

            resp   = _trade_post("/api/v1/private/order/submit", order_body, cfg)
            _b._msc_last_trade_raw = {
                "endpoint":  "/api/v1/private/order/submit (LIMIT + attachAlgoOrds)",
                "body_sent": order_body,
                "response":  resp,
                "is_hedge":  is_hedge,
                "contracts": contracts,
                "ct_val":    ct_val,
            }
            d0     = (resp.get("data") or [{}])[0]
            ord_id = d0.get("ordId", "")

            if resp.get("success") is False or (resp.get("success") is None and resp.get("code", 200) not in (0, 200)):
                err = _okx_err(resp)
                _append_error("trade",
                              f"{err} | body={json.dumps(order_body)} | resp={json.dumps(resp)[:300]}",
                              symbol=sym, endpoint="/api/v1/private/order/submit")
                return {"ordId": "", "algoId": "", "sz": contracts,
                        "status": "error", "error": err}
            if False:  # MEXC does not use sCode sub-errors
                err = f"Limit order: {d0.get('sCode')}: {d0.get('sMsg','')}"
                _append_error("trade", f"{err} | body={json.dumps(order_body)}",
                              symbol=sym, endpoint="/api/v1/private/order/submit")
                return {"ordId": ord_id, "algoId": "", "sz": contracts,
                        "status": "error", "error": err}

            return {"ordId": ord_id, "algoId": "", "sz": contracts,
                    "status": "placed", "error": "",
                    "actual_entry": entry, "actual_tp": tp, "actual_sl": sl}

        else:
            # MARKET order — two-step: entry then OCO
            order_body = {
                "instId":  _to_mexc(sym),
                "tdMode":  mode,
                "side":    "buy",
                "ordType": "market",
                "sz":      str(contracts),
            }
            if is_hedge:
                order_body["posSide"] = "long"

            resp   = _trade_post("/api/v1/private/order/submit", order_body, cfg)
            _b._msc_last_trade_raw = {
                "endpoint":  "/api/v1/private/order/submit (MARKET)",
                "body_sent": order_body,
                "response":  resp,
                "is_hedge":  is_hedge,
                "contracts": contracts,
                "ct_val":    ct_val,
            }
            d0     = (resp.get("data") or [{}])[0]
            ord_id = d0.get("ordId", "")

            if resp.get("success") is False or (resp.get("success") is None and resp.get("code", 200) not in (0, 200)):
                err = _okx_err(resp)
                _append_error("trade",
                              f"{err} | body={json.dumps(order_body)} | resp={json.dumps(resp)[:300]}",
                              symbol=sym, endpoint="/api/v1/private/order/submit")
                return {"ordId": "", "algoId": "", "sz": contracts,
                        "status": "error", "error": err}
            if False:  # MEXC does not use sCode sub-errors
                err = f"Market order: {d0.get('sCode')}: {d0.get('sMsg','')}"
                _append_error("trade", f"{err} | body={json.dumps(order_body)}",
                              symbol=sym, endpoint="/api/v1/private/order/submit")
                return {"ordId": ord_id, "algoId": "", "sz": contracts,
                        "status": "error", "error": err}

            # OCO with user-specified TP/SL (no recalculation for manual orders)
            algo_body: dict = {
                "instId":      _to_mexc(sym),
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

            algo_resp = _trade_post("/api/v1/private/order/submit", algo_body, cfg)
            _b._msc_last_trade_raw["algo_body_sent"] = algo_body
            _b._msc_last_trade_raw["algo_response"]  = algo_resp
            ad      = (algo_resp.get("data") or [{}])[0]
            algo_id = ad.get("algoId", "")

            if algo_resp.get("success") is False or (algo_resp.get("success") is None and algo_resp.get("code", 200) not in (0, 200)):
                algo_err = _okx_err(algo_resp)
                _append_error("trade", f"OCO algo failed: {algo_err}",
                              symbol=sym, endpoint="/api/v1/private/order/submit")
                return {"ordId": ord_id, "algoId": "", "sz": contracts,
                        "status": "partial",
                        "error": f"Entry ✅ · TP/SL ❌ {algo_err}",
                        "actual_entry": 0, "actual_tp": tp, "actual_sl": sl}

            return {"ordId": ord_id, "algoId": algo_id, "sz": contracts,
                    "status": "placed", "error": "",
                    "actual_entry": 0, "actual_tp": tp, "actual_sl": sl}

    except Exception as exc:
        _append_error("trade", str(exc), symbol=sym, endpoint="/api/v1/private/order/submit")
        return {"ordId": "", "algoId": "", "sz": 0,
                "status": "error", "error": str(exc)}

# ─────────────────────────────────────────────────────────────────────────────
# A — Bulk ticker fetch + pre-filter (single API call)
# ─────────────────────────────────────────────────────────────────────────────
def get_bulk_tickers() -> dict:
    """
    ONE API call → dict {BTCUSDT: {last, open24h, high24h, low24h, volCcy24h}}
    for every USDT pair on MEXC Futures.
    """
    data   = safe_get(f"{BASE}/api/v1/contract/ticker")
    result = {}
    _ticker_items = (data.get("data") if isinstance(data, dict) else None) or []
    for t in _ticker_items:
        inst_id = t.get("symbol", "")
        if not inst_id.endswith("_USDT"):
            continue
        sym = _from_mexc(inst_id)
        try:
            result[sym] = {
                "last":      float(t.get("lastPrice",   0) or 0),
                "open24h":   float(t.get("lastPrice",   0) or 0),   # MEXC has no open24h
                "high24h":   float(t.get("high24Price", 0) or 0),
                "low24h":    float(t.get("low24Price",  0) or 0),
                "volCcy24h": float(t.get("amount24",    0) or 0),   # 24h turnover USDT
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
    """Fetch candles from MEXC Futures.
    MEXC returns up to 2000 candles per call in a columnar format (arrays per field).
    Time values are in seconds — converted to milliseconds for compatibility.
    Note: MEXC has no 3m interval; MEXC_INTERVALS maps '3m' → 'Min5'.
    """
    mexc_iv = MEXC_INTERVALS.get(interval, interval)
    inst_id = _to_mexc(sym)
    params  = {"interval": mexc_iv, "limit": min(limit, 2000)}
    data    = safe_get(f"{BASE}/api/v1/contract/kline/{inst_id}", params)
    d       = (data.get("data") if isinstance(data, dict) else None) or {}
    if not d:
        return []
    times  = d.get("time",  [])
    opens  = d.get("open",  [])
    highs  = d.get("high",  [])
    lows   = d.get("low",   [])
    closes = d.get("close", [])
    vols   = d.get("vol",   [])
    result = []
    for i in range(len(times)):
        try:
            result.append({
                "time":   int(times[i]) * 1000,   # seconds → milliseconds
                "open":   float(opens[i]),
                "high":   float(highs[i]),
                "low":    float(lows[i]),
                "close":  float(closes[i]),
                "volume": float(vols[i]),
            })
        except (IndexError, TypeError, ValueError):
            break
    return result  # MEXC returns oldest→newest already

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

def macd_bullish_and_value(closes: list, crossover_lookback: int = 12):
    """
    Single-pass MACD evaluation — computes MACD once and returns BOTH the
    bullish verdict and the last MACD-line value, so callers don't need to
    call calc_macd() a second time just to get the line value for display.

    Returns (is_bullish: bool, macd_line_value: float | None).
    """
    macd_line, sig_line, histogram = calc_macd(closes)
    _last_val = macd_line[-1] if macd_line else None
    if not histogram or len(histogram) < 2:
        return False, _last_val
    if macd_line[-1] <= 0 or sig_line[-1] <= 0:
        return False, _last_val
    if histogram[-1] <= 0 or histogram[-1] <= histogram[-2]:
        return False, _last_val
    n = min(crossover_lookback + 1, len(macd_line))
    for i in range(1, n):
        prev, curr = -(i+1), -i
        if (len(macd_line)+prev >= 0 and
                macd_line[prev] <= sig_line[prev] and
                macd_line[curr] >  sig_line[curr]):
            return True, _last_val
    return False, _last_val


def macd_bullish(closes: list, crossover_lookback: int = 12) -> bool:
    """
    True when (all on given closes):
      1. MACD line > 0
      2. Signal line > 0
      3. Histogram > 0 AND increasing  ← dark green only
      4. Bullish crossover within last crossover_lookback candles

    Thin wrapper around macd_bullish_and_value — kept for any external caller
    that only wants the bool.
    """
    ok, _ = macd_bullish_and_value(closes, crossover_lookback)
    return ok

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
    # ── Empty / insufficient data → FAIL-CLOSED ──────────────────────────────
    # Previously returned (True, "unknown") which silently qualified coins
    # with no candle data — a serious fail-open bug. Now we fail-closed so
    # missing data cannot accidentally pass the PDZ filter (or trigger a
    # Super Setup on empty 1h data, for example).
    #
    # The DZSAFM indicator needs enough history for a meaningful swing
    # high / swing low — use 50 as the functional minimum (matches the
    # Pine Script's default lookback length).
    if not candles or len(candles) < 50:
        return False, "insufficient_data"

    lookback = candles[-290:]  # 290 candles — 1 MEXC API call (~72 hrs on 15m, ~24 hrs on 5m)
    H = max(c["high"] for c in lookback)
    L = min(c["low"]  for c in lookback)

    if H <= L:
        # Degenerate range (flat or inverted) — cannot compute zones.
        return False, "flat_range"

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
        # F7 MACD — per timeframe
        "f7_macd_3m":       0,
        "f7_macd_5m":       0,
        "f7_macd_15m":      0,
        # F8 SAR — per timeframe
        "f8_sar_3m":        0,
        "f8_sar_5m":        0,
        "f8_sar_15m":       0,
        "f9_vol":           0,   # F9 — Vol Spike
        "f10_ema_cross":    0,   # F10 — 15m EMA crossover (fast > slow)
        "f_empty_data":     0,   # Stage 2 candle fetch returned empty for a timeframe
        "passed":           0,
        "super_setup":      0,   # subset of passed — 15m+1h Discount instant buys
        "super_cap_demoted":0,   # Super-eligible coins demoted to F3-F10 pipeline
        "f_sl_cooldown":    0,   # coins blocked by 24h SL blackout this cycle
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
        "f7_macd_3m_elim_syms":   [],
        "f7_macd_5m_elim_syms":   [],
        "f7_macd_15m_elim_syms":  [],
        "f8_sar_3m_elim_syms":    [],
        "f8_sar_5m_elim_syms":    [],
        "f8_sar_15m_elim_syms":   [],
        "f9_elim_syms":           [],
        "f10_elim_syms":          [],
        "f_empty_data_syms":      [],
        "passed_syms":            [],
        "super_setup_syms":       [],
        "super_cap_demoted_syms": [],
        "f_sl_cooldown_syms":     [],
        "blocked_by_sl_cooldown_syms": [],
    }
    with _filter_lock:
        _filter_counts.clear()
        _filter_counts.update(counts)
        _b._msc_filter_counts = _filter_counts   # keep reference in sync under lock

# ─── Thread-local counter batching ──────────────────────────────────────────
# Each process() call accumulates integer counter increments in a thread-local
# delta dict instead of taking `_filter_lock` on every elimination. List
# appends (e.g. *_elim_syms) are GIL-atomic on CPython so they bypass the
# lock entirely. The delta is flushed into `_filter_counts` exactly once per
# process() call (in the outer finally block) — collapsing 20+ lock
# acquisitions per coin into a single brief merge, which dramatically cuts
# contention under the 10-worker ThreadPoolExecutor.
_tl_counts = threading.local()

def _tl_delta() -> dict:
    """Return (and lazily create) the current thread's counter delta dict."""
    _d = getattr(_tl_counts, "delta", None)
    if _d is None:
        _d = {}
        _tl_counts.delta = _d
    return _d

def _incr_filter(key: str, n: int = 1) -> None:
    """Increment a counter in the thread-local delta (lock-free)."""
    _d = _tl_delta()
    _d[key] = _d.get(key, 0) + n

def _record_elim(count_key: str, sym_list_key: str, sym: str) -> None:
    """Record a filter elimination: bump counter locally + append sym to the
    shared list (list.append is GIL-atomic — no lock required).
    """
    _incr_filter(count_key)
    # list.append is atomic in CPython; safe without _filter_lock as long as
    # the list itself exists (it's pre-initialised in _reset_filter_counts).
    _lst = _filter_counts.get(sym_list_key)
    if _lst is not None:
        _lst.append(sym)

def _flush_tl_counts() -> None:
    """Merge the thread-local delta into `_filter_counts` under one lock acquisition, then reset it for the next process() call."""
    _d = getattr(_tl_counts, "delta", None)
    if not _d:
        _tl_counts.delta = {}
        return
    with _filter_lock:
        for _k, _v in _d.items():
            _filter_counts[_k] = _filter_counts.get(_k, 0) + _v
    _tl_counts.delta = {}   # reset for next call on this worker thread

# ─────────────────────────────────────────────────────────────────────────────
# Per-coin processing
# ─────────────────────────────────────────────────────────────────────────────
def process(sym, cfg: dict, super_counter: dict = None, super_lock=None):
    """Per-coin pipeline.

    super_counter / super_lock — shared atomic slot tracker for Super-cap
    coordination across concurrent scan threads. When the Super slot count
    reaches zero, any Super-eligible coin falls through to the F3–F10
    pipeline and opens as a normal (non-super) trade if it passes.
    If these args are None (e.g. unit test, standalone call), behave as if
    unlimited Super slots are available.
    """
    # Reset thread-local delta at entry so the previous process() call on the
    # same worker thread doesn't bleed into this one. Flush happens in the
    # outer finally below.
    _tl_counts.delta = {}
    try:
        _incr_filter("checked")
        # list.append is GIL-atomic — no lock needed.
        _filter_counts["checked_syms"].append(sym)

        # ── Stage 1: Quick parallel fetch — 5m + 15m (PDZ) + 1h (PDZ+RSI) ────
        # 1h is fetched here (not Stage 2) because Super Setup requires 1h
        # to be in Discount zone — without 1h we can't decide Super vs normal.
        # The same 1h candles are reused later for F5 (1h RSI) so no extra
        # API call is introduced by moving the fetch earlier.
        #
        # Candle counts (all single API call — MEXC cap is 2000/call):
        #   • 5m  = 300 → 25 hours of 5m structure
        #   • 15m = 300 → 75 hours of 15m structure (PDZ uses last 290)
        #   • 1h  = 300 → 12.5 days of 1h structure (PDZ uses last 290,
        #                 giving ~12 days of 1h swing pivots for Discount-zone
        #                 detection — matches the 290-candle internal cap of
        #                 calc_pdz_zone which mirrors the DZSAFM indicator).
        with ThreadPoolExecutor(max_workers=3) as pool:
            f_5m_q  = pool.submit(get_klines, sym, "5m",  300)
            f_15m_q = pool.submit(get_klines, sym, "15m", 300)
            f_1h_q  = pool.submit(get_klines, sym, "1h",  300)
            m5_quick   = f_5m_q.result()[:-1]
            m15_quick  = f_15m_q.result()[:-1]
            m1h_quick  = f_1h_q.result()[:-1]

        if not m5_quick:
            return None   # insufficient candle data — skip coin silently
        closes_5m_q = [c["close"] for c in m5_quick]
        entry_q     = _pround(m5_quick[-1]["close"])

        # ── F2: PDZ 15m — Super Setup requires 15m Discount ──────────────────
        pdz_zone_15m       = "—"
        pdz_zone_1h        = "—"
        is_super_eligible  = False
        if cfg.get("use_pdz_15m", True):
            pdz_pass_15m, pdz_zone_15m = calc_pdz_zone(m15_quick, entry_q)
            if pdz_zone_15m == "Discount":
                # 15m is Discount — now we need 1h also in Discount for Super
                # (if 1h fetch failed, can't verify → treat as not-super, fall
                # through to F3-F10 rather than blocking entry).
                if m1h_quick:
                    _, pdz_zone_1h = calc_pdz_zone(m1h_quick, entry_q)
                if pdz_zone_1h == "Discount":
                    is_super_eligible = True      # ⭐ BOTH 15m and 1h Discount
                # else: only 15m Discount (not 1h) → fall through to F3-F10
            elif not pdz_pass_15m:
                _record_elim("f2_pdz15m", "f2_elim_syms", sym)
                return None
            # else Band A/B passes — continue to F3

        # ── SUPER SETUP — BOTH 15m AND 1h Discount → instant trade ───────────
        # Atomic slot check: only take the Super shortcut if slots remain.
        # If the cap is exhausted, fall through to F3-F10 and open as normal.
        if is_super_eligible:
            _take_super_slot = True
            if super_counter is not None and super_lock is not None:
                with super_lock:
                    if super_counter.get("slots", 0) > 0:
                        super_counter["slots"] -= 1
                    else:
                        _take_super_slot = False
            if _take_super_slot:
                tp      = _pround(entry_q * (1 + cfg["tp_pct"] / 100))
                _ss_lev = max(1, int(cfg.get("trade_leverage", 10)))
                sl      = (_pround(entry_q * (1 - 1 / _ss_lev))
                           if cfg.get("trade_margin_mode", "isolated") == "isolated"
                           else _pround(entry_q * (1 - cfg["sl_pct"] / 100)))
                sec     = SECTORS.get(sym, "Other")
                max_lev = get_max_leverage(sym)
                _incr_filter("passed")
                _incr_filter("super_setup")
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
                        "pdz_zone_1h":  pdz_zone_1h,
                        "ema_cross_12_15m": "—",
                        "ema_cross_21_15m": "—",
                    },
                }
            # else: super cap exhausted — demote and fall through to F3-F10
            _record_elim("super_cap_demoted", "super_cap_demoted_syms", sym)

        # ── F3: PDZ 5m ────────────────────────────────────────────────────────
        pdz_zone_5m = "—"
        if cfg.get("use_pdz_5m", True):
            pdz_pass_5m, pdz_zone_5m = calc_pdz_zone(m5_quick, entry_q)
            if not pdz_pass_5m:
                _record_elim("f3_pdz5m", "f3_elim_syms", sym)
                return None

        # ── F4: Quick 5m RSI (still early — before full fetch) ────────────────
        rsi5_q = (calc_rsi_series(closes_5m_q) or [0])[-1]
        if cfg.get("use_rsi_5m", True) and rsi5_q < cfg["rsi_5m_min"]:
            _record_elim("f4_rsi5m", "f4_elim_syms", sym)
            return None

        # ── Stage 2: Fetch ONLY new timeframes — 3m ──────────────────────────
        # 5m, 15m, and 1h are already fetched in Stage 1 (m5_quick, m15_quick,
        # m1h_quick). 1h was moved to Stage 1 so Super Setup can verify the
        # 1h Discount requirement before committing. Here we reuse that 1h
        # data for F5 (1h RSI) — no re-fetch needed.
        #
        # Only 3m (EMA/MACD/SAR) is genuinely new at this stage.
        # This cuts Stage 2 from 2 API calls per coin → 1 API call per coin.
        _need_3m_detail = (cfg.get("use_ema_3m") or cfg.get("use_macd_3m")
                           or cfg.get("use_sar_3m"))
        candle_limit_3m = 80 if _need_3m_detail else 30

        m3_candles = get_klines(sym, "3m", candle_limit_3m)[:-1]

        # Reuse Stage 1 data for 5m / 15m / 1h — no re-fetch needed
        m5          = m5_quick
        m15         = m15_quick
        m1h_candles = m1h_quick

        # ── Guard: check all required timeframes have data ────────────────────
        _missing_tf = [tf for tf, bars in (("5m", m5), ("15m", m15),
                                            ("1h", m1h_candles), ("3m", m3_candles))
                       if not bars]
        if _missing_tf:
            _incr_filter("f_empty_data")
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
            _record_elim("f5_rsi1h", "f5_elim_syms", sym)
            return None

        # ── F6: EMA ───────────────────────────────────────────────────────────
        ema_3m_val = ema_5m_val = ema_15m_val = None
        if cfg.get("use_ema_3m"):
            ema = calc_ema(closes_3m, max(2, int(cfg.get("ema_period_3m", 12))))
            if not ema or entry < ema[-1]:
                _record_elim("f6_ema", "f6_elim_syms", sym)
                return None
            ema_3m_val = _pround(ema[-1])
        if cfg.get("use_ema_5m"):
            ema = calc_ema(closes_5m, max(2, int(cfg.get("ema_period_5m", 12))))
            if not ema or entry < ema[-1]:
                _record_elim("f6_ema", "f6_elim_syms", sym)
                return None
            ema_5m_val = _pround(ema[-1])
        if cfg.get("use_ema_15m"):
            ema = calc_ema(closes_15m, max(2, int(cfg.get("ema_period_15m", 12))))
            if not ema or entry < ema[-1]:
                _record_elim("f6_ema", "f6_elim_syms", sym)
                return None
            ema_15m_val = _pround(ema[-1])

        # ── F7: MACD dark-green — per-timeframe independent checks ──────────
        # Each enabled timeframe is checked in order (3m → 5m → 15m).
        # The first failure increments that timeframe's own counter and eliminates
        # the coin — giving the funnel an accurate per-timeframe breakdown.
        # Uses macd_bullish_and_value so MACD is computed ONCE per timeframe
        # — the line value for display comes back from the same call, avoiding
        # a second calc_macd() pass that the old code did after each check.
        macd_3m_val = macd_5m_val = macd_15m_val = None
        _macd_3m_on  = cfg.get("use_macd_3m",  True)
        _macd_5m_on  = cfg.get("use_macd_5m",  True)
        _macd_15m_on = cfg.get("use_macd_15m", True)
        if _macd_3m_on:
            _ok_3m, _ml3 = macd_bullish_and_value(closes_3m)
            if _ml3 is not None:
                macd_3m_val = round(_ml3, 8)
            if not _ok_3m:
                _record_elim("f7_macd_3m", "f7_macd_3m_elim_syms", sym)
                return None
        if _macd_5m_on:
            _ok_5m, _ml5 = macd_bullish_and_value(closes_5m)
            if _ml5 is not None:
                macd_5m_val = round(_ml5, 8)
            if not _ok_5m:
                _record_elim("f7_macd_5m", "f7_macd_5m_elim_syms", sym)
                return None
        if _macd_15m_on:
            _ok_15m, _ml15 = macd_bullish_and_value(closes_15m)
            if _ml15 is not None:
                macd_15m_val = round(_ml15, 8)
            if not _ok_15m:
                _record_elim("f7_macd_15m", "f7_macd_15m_elim_syms", sym)
                return None

        # ── F8: Parabolic SAR — per-timeframe independent checks ─────────────
        # Each enabled timeframe checked in order (3m → 5m → 15m).
        # First failure increments that timeframe's counter and eliminates the coin.
        sar_3m_val = sar_5m_val = sar_15m_val = None
        _sar_3m_on  = cfg.get("use_sar_3m",  True)
        _sar_5m_on  = cfg.get("use_sar_5m",  True)
        _sar_15m_on = cfg.get("use_sar_15m", True)
        if _sar_3m_on:
            sar_3m = calc_parabolic_sar(m3_candles)
            if not (sar_3m and sar_3m[-1][1]):
                _record_elim("f8_sar_3m", "f8_sar_3m_elim_syms", sym)
                return None
            sar_3m_val = _pround(sar_3m[-1][0])
        if _sar_5m_on:
            sar_5m = calc_parabolic_sar(m5)
            if not (sar_5m and sar_5m[-1][1]):
                _record_elim("f8_sar_5m", "f8_sar_5m_elim_syms", sym)
                return None
            sar_5m_val = _pround(sar_5m[-1][0])
        if _sar_15m_on:
            sar_15m = calc_parabolic_sar(m15)
            if not (sar_15m and sar_15m[-1][1]):
                _record_elim("f8_sar_15m", "f8_sar_15m_elim_syms", sym)
                return None
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
                    _record_elim("f9_vol", "f9_elim_syms", sym)
                    return None
                vol_ratio = round(vols_15m[-1] / avg_vol, 2) if avg_vol > 0 else None

        # ── F10: 15m EMA Crossover (fast > slow) ─────────────────────────────
        # Uses 15m candles already fetched in Stage 1 (m15/closes_15m).
        # Coin passes only when EMA(fast) is strictly greater than EMA(slow).
        ema_cross_12_15m_val = ema_cross_21_15m_val = None
        if cfg.get("use_ema_cross_15m", True):
            _fast_p = max(2, int(cfg.get("ema_cross_fast_15m", 12)))
            _slow_p = max(_fast_p + 1, int(cfg.get("ema_cross_slow_15m", 21)))
            ema_fast_15m = calc_ema(closes_15m, _fast_p)
            ema_slow_15m = calc_ema(closes_15m, _slow_p)
            if not ema_fast_15m or not ema_slow_15m or \
                    ema_fast_15m[-1] <= ema_slow_15m[-1]:
                _record_elim("f10_ema_cross", "f10_elim_syms", sym)
                return None
            ema_cross_12_15m_val = _pround(ema_fast_15m[-1])
            ema_cross_21_15m_val = _pround(ema_slow_15m[-1])

        # ── All filters passed ────────────────────────────────────────────────
        tp      = _pround(entry * (1 + cfg["tp_pct"] / 100))
        _lev_sl = max(1, int(cfg.get("trade_leverage", 10)))
        sl      = (_pround(entry * (1 - 1 / _lev_sl))
                   if cfg.get("trade_margin_mode", "isolated") == "isolated"
                   else _pround(entry * (1 - cfg["sl_pct"] / 100)))
        sec     = SECTORS.get(sym, "Other")
        max_lev = get_max_leverage(sym)

        _incr_filter("passed")
        _filter_counts["passed_syms"].append(sym)

        rsi5 = (calc_rsi_series(closes_5m) or [rsi5_q])[-1]
        criteria = {
            "rsi_5m":       round(rsi5,  1),
            "rsi_1h":       round(rsi1h, 1),
            "ema_3m":       ema_3m_val  if cfg.get("use_ema_3m")    else "—",
            "ema_5m":       ema_5m_val  if cfg.get("use_ema_5m")    else "—",
            "ema_15m":      ema_15m_val if cfg.get("use_ema_15m")   else "—",
            "macd_3m":      macd_3m_val  if cfg.get("use_macd_3m",  True) else "—",
            "macd_5m":      macd_5m_val  if cfg.get("use_macd_5m",  True) else "—",
            "macd_15m":     macd_15m_val if cfg.get("use_macd_15m", True) else "—",
            "sar_3m":       sar_3m_val   if cfg.get("use_sar_3m",   True) else "—",
            "sar_5m":       sar_5m_val   if cfg.get("use_sar_5m",   True) else "—",
            "sar_15m":      sar_15m_val  if cfg.get("use_sar_15m",  True) else "—",
            "vol_ratio":    vol_ratio    if cfg.get("use_vol_spike") else "—",
            "pdz_zone_5m":  pdz_zone_5m  if cfg.get("use_pdz_5m",  True) else "—",
            "pdz_zone_15m": pdz_zone_15m if cfg.get("use_pdz_15m", True) else "—",
            "pdz_zone_1h":  pdz_zone_1h  if cfg.get("use_pdz_15m", True) else "—",
            "ema_cross_12_15m": ema_cross_12_15m_val if cfg.get("use_ema_cross_15m", True) else "—",
            "ema_cross_21_15m": ema_cross_21_15m_val if cfg.get("use_ema_cross_15m", True) else "—",
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
        _incr_filter("errors")
        _append_error("scan", str(_proc_exc), symbol=sym)
        return "error"
    finally:
        # Single lock acquisition per process() call — merges all thread-local
        # counter deltas into the shared `_filter_counts`. Runs on every
        # return path (None, signal dict, "error").
        _flush_tl_counts()

def scan(cfg: dict, super_slots_remaining: int = None, skip_symbols: set = None):
    """Full watchlist scan. Returns (sorted_results, error_count).

    super_slots_remaining — number of additional Super Setup trades this cycle
    may open before the cap (cfg['max_super_trades']) is reached. Computed by
    the caller (bg loop) by subtracting currently-open Super trades from the
    configured cap. If None, defaults to the full cap (fresh state).

    skip_symbols — pre-computed blacklist (SL-cooldown ∪ TP-cooldown ∪ currently
    active). Removed from the deep-scan pool BEFORE any candle fetches so that
    (a) no API calls are wasted on coins that will be rejected at placement
    time, and (b) SL-cooldown-blocked coins can never consume Super slots.
    """
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

    # ── Cooldown / active blacklist ─────────────────────────────────────────
    # Drop these BEFORE deep-scan so:
    #   • SL-cooldown blackout is respected at the earliest possible stage
    #   • no Super slot is consumed by a coin that would be rejected anyway
    #   • no candle API calls are wasted fetching data for skipped coins
    skip_set = set(skip_symbols) if skip_symbols else set()
    _skipped_count = 0
    if skip_set:
        before_skip = len(pre_filtered)
        pre_filtered = [s for s in pre_filtered if s not in skip_set]
        _skipped_count = before_skip - len(pre_filtered)
        with _filter_lock:
            _filter_counts["skipped_pre_scan"] = _skipped_count

    _removed_pre = (0 if not cfg.get('use_pre_filter', True)
                    else _filter_counts['pre_filtered_out'])
    _blk_tag = f", {_skipped_count} blacklisted" if skip_set else ""
    print(f"[Scan] {len(symbols)} valid symbols → "
          f"{len(pre_filtered)} after bulk pre-filter "
          f"({_removed_pre} removed{_blk_tag})")

    # ── Super Setup cap coordination ─────────────────────────────────────────
    # Shared atomic counter + lock — used by process() to decide whether a
    # Super-eligible coin takes the shortcut or falls through to F3-F10.
    # When slots hit 0, any further Super-eligible coin is demoted and must
    # qualify through the normal pipeline to open as a regular trade.
    if super_slots_remaining is None:
        super_slots_remaining = max(0, int(cfg.get("max_super_trades", 5)))
    super_counter = {"slots": max(0, int(super_slots_remaining))}
    super_lock    = threading.Lock()

    results = []
    # 10 outer workers — semaphore(5) caps actual concurrent HTTP requests to 5
    # so more workers just means less idle time between coin batches, not more API pressure.
    with ThreadPoolExecutor(max_workers=10) as exe:
        futs = [exe.submit(process, s, cfg, super_counter, super_lock)
                for s in pre_filtered]
        for f in as_completed(futs):
            r = f.result()
            if r and r != "error":
                results.append(r)

    return sorted(results, key=lambda x: x["symbol"]), _filter_counts.get("errors", 0)

# ─────────────────────────────────────────────────────────────────────────────
# Open signal tracker
# ─────────────────────────────────────────────────────────────────────────────
_PRICE_ALERT_PCT = 3.0   # alert when current price is this % or more below entry

def _parse_iso_safe(ts_str: str):
    """Parse an ISO-8601 timestamp tolerating Z / naive formats.

    Accepts:
      • "2026-04-19T12:34:56.789+04:00"  (current dubai_now() format)
      • "2026-04-19T12:34:56Z"           (UTC-suffixed)
      • "2026-04-19T12:34:56"            (naive — assumed UTC)
    Raises ValueError on un-parseable input (caller must handle).
    """
    s = str(ts_str).strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:                    # naive → assume UTC
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


# ─────────────────────────────────────────────────────────────────────────────
# DCA helpers (Dollar-Cost Averaging ladder — Isolated 70% / Cross fixed 7%)
# ─────────────────────────────────────────────────────────────────────────────
def _dca_compute_trigger(sig: dict) -> float:
    """Compute the next-DCA trigger price for an open signal.

    Isolated: 70% of the distance from blended average to liquidation
              (liq ≈ avg × (1 − 1/lev) → dca_drop = 0.70 × 1/lev).
    Cross:    fixed 7% below blended average per DCA.

    Returns 0.0 if the trigger cannot be computed.
    """
    avg_entry = float(sig.get("avg_entry", sig.get("entry", 0)) or 0)
    if avg_entry <= 0:
        return 0.0
    mode = (sig.get("order_margin_mode") or "isolated").strip().lower()
    lev  = int(sig.get("trade_lev", 0) or 0)
    if lev <= 0:
        lev = 10
    if mode == "isolated":
        dca_drop_pct = 0.70 * (1.0 / lev)
    else:
        dca_drop_pct = 0.07
    return _pround(avg_entry * (1.0 - dca_drop_pct))


def _dca_next_usdt(sig: dict) -> float:
    """Return the USDT margin for the NEXT DCA add (doubles previous add).

    Fill 0 is the original entry. DCA 1 matches the original size. Each
    subsequent DCA doubles the prior DCA's margin:
        DCA 1 → base
        DCA 2 → 2× base
        DCA 3 → 4× base
        DCA 4 → 8× base …
    """
    fills = sig.get("dca_fills") or []
    base  = float(sig.get("trade_usdt", 0) or 0)
    if base <= 0:
        return 0.0
    # dca_count = number of DCAs already done (fills beyond index 0).
    dca_count = max(0, len(fills) - 1)
    # Next DCA index is dca_count + 1. DCA 1 = base; DCA N = base × 2^(N-1).
    next_idx = dca_count + 1
    return base * (2 ** (next_idx - 1))


def place_okx_dca_order(sig: dict, cfg: dict, dca_usdt: float) -> dict:
    """Place a DCA market-buy add on MEXC using the sig's existing leverage/mode.

    Does NOT touch the OCO algo order — caller is responsible for cancelling
    the old algo and placing a new one with updated TP/SL after this returns.

    Returns a dict compatible with the main order-result shape:
      {"ordId", "sz", "status": "placed"|"error", "error",
       "actual_entry", "ct_val", "notional", "tdMode", "is_hedge"}
    """
    try:
        sym = sig.get("symbol", "")
        if not sym:
            return {"ordId": "", "sz": 0, "status": "error",
                    "error": "Missing symbol"}
        if dca_usdt <= 0:
            return {"ordId": "", "sz": 0, "status": "error",
                    "error": f"Invalid DCA size: {dca_usdt}"}

        lev  = int(sig.get("trade_lev", 0) or 0)
        if lev <= 0:
            lev = int(cfg.get("trade_leverage", 10) or 10)
        lev  = min(lev, get_max_leverage(sym))
        mode = (sig.get("order_margin_mode") or
                cfg.get("trade_margin_mode", "isolated")).strip().lower()

        # Current market price reference (use latest close as sizing reference)
        ref_price = float(sig.get("latest_price",
                                  sig.get("avg_entry",
                                          sig.get("entry", 0))) or 0)
        if ref_price <= 0:
            return {"ordId": "", "sz": 0, "status": "error",
                    "error": "No reference price for DCA sizing"}

        try:
            ct_val = _get_ct_val(sym)
        except ValueError as _cv:
            return {"ordId": "", "sz": 0, "status": "error",
                    "error": f"ct_val: {_cv}"}
        if ct_val <= 0:
            return {"ordId": "", "sz": 0, "status": "error",
                    "error": f"Invalid ct_val: {ct_val}"}

        notional  = dca_usdt * lev
        contracts = max(1, int(notional / (ct_val * ref_price)))
        if contracts > 100_000:
            return {"ordId": "", "sz": contracts, "status": "error",
                    "error": f"DCA contract count too large ({contracts})"}

        # Fetch posMode (hedge vs net) — best-effort
        is_hedge = bool(sig.get("order_is_hedge", False))
        try:
            _pm_resp = _trade_get("/api/v1/private/account/assets", {}, cfg)
            if _pm_resp.get("code") == "0":
                _pm = _pm_resp.get("data", [{}])[0].get("posMode", "net_mode")
                is_hedge = (_pm == "long_short_mode")
        except Exception:
            pass

        # Re-apply leverage (idempotent)
        try:
            _set_leverage_okx(sym, cfg)
        except Exception as lev_exc:
            _append_error("trade", f"DCA set-leverage warning: {lev_exc}",
                          symbol=sym, endpoint="/api/v1/private/account/change_leverage")

        order_body: dict = {
            "instId":  _to_mexc(sym),
            "tdMode":  mode,
            "side":    "buy",
            "ordType": "market",
            "sz":      str(contracts),
        }
        if is_hedge:
            order_body["posSide"] = "long"

        resp   = _trade_post("/api/v1/private/order/submit", order_body, cfg)
        _b._msc_last_trade_raw = {
            "endpoint":  "/api/v1/private/order/submit (DCA add)",
            "body_sent": order_body,
            "response":  resp,
            "is_hedge":  is_hedge,
            "contracts": contracts,
            "ct_val":    ct_val,
            "dca_usdt":  dca_usdt,
        }
        d0     = (resp.get("data") or [{}])[0]
        ord_id = d0.get("ordId", "")

        if resp.get("success") is False or (resp.get("success") is None and resp.get("code", 200) not in (0, 200)):
            err = _okx_err(resp)
            _append_error("trade",
                          f"DCA {err} | body={json.dumps(order_body)} | "
                          f"resp={json.dumps(resp)[:300]}",
                          symbol=sym, endpoint="/api/v1/private/order/submit")
            return {"ordId": "", "sz": contracts, "status": "error",
                    "error": err, "ct_val": ct_val, "notional": notional,
                    "tdMode": mode, "is_hedge": is_hedge}
        if False:  # MEXC does not use sCode sub-errors
            err = f"DCA order: {d0.get('sCode')}: {d0.get('sMsg','')}"
            _append_error("trade",
                          f"{err} | body={json.dumps(order_body)}",
                          symbol=sym, endpoint="/api/v1/private/order/submit")
            return {"ordId": ord_id, "sz": contracts, "status": "error",
                    "error": err, "ct_val": ct_val, "notional": notional,
                    "tdMode": mode, "is_hedge": is_hedge}

        # Fetch actual fill price
        actual_entry = ref_price
        time.sleep(0.3)
        try:
            fill_resp = _trade_get("/api/v1/private/order/submit",
                                   {"instId": _to_mexc(sym), "ordId": ord_id},
                                   cfg)
            if fill_resp.get("success", False):
                fill_d = fill_resp.get("data", [{}])[0]
                avg_px = float(fill_d.get("avgPx", 0) or 0)
                if avg_px > 0:
                    actual_entry = avg_px
        except Exception as fill_exc:
            _append_error("trade",
                          f"DCA fill fetch failed: {fill_exc}",
                          symbol=sym, endpoint="/api/v1/private/order/get_order_id")

        return {"ordId": ord_id, "sz": contracts,
                "status": "placed", "error": "",
                "actual_entry": actual_entry,
                "ct_val": ct_val, "notional": notional,
                "tdMode": mode, "is_hedge": is_hedge}

    except Exception as exc:
        _append_error("trade", f"DCA exception: {exc}",
                      symbol=sig.get("symbol", ""),
                      endpoint="/api/v1/private/order/submit")
        return {"ordId": "", "sz": 0, "status": "error", "error": str(exc)}


def _cancel_algo_best_effort(algo_id: str, sym: str, cfg: dict) -> None:
    """Cancel an OCO algo order on MEXC — log failure, never raise."""
    if not algo_id or not sym:
        return
    try:
        _trade_post("/api/v1/private/order/cancel_orders",
                    [{"algoId": algo_id, "instId": _to_mexc(sym)}],
                    cfg)
    except Exception as exc:
        _append_error("trade",
                      f"cancel-algo failed for {sym} algoId={algo_id}: {exc}",
                      symbol=sym, endpoint="/api/v1/private/order/cancel_orders")


def _place_dca_oco_algo(sig: dict, cfg: dict, new_tp: float,
                         new_sl: float, total_contracts: int) -> str:
    """Place a fresh OCO (TP + SL) algo on MEXC after a DCA add.

    Returns the new algoId, or "" on failure (non-fatal — DCA already placed).
    """
    try:
        sym  = sig.get("symbol", "")
        mode = (sig.get("order_margin_mode") or
                cfg.get("trade_margin_mode", "isolated")).strip().lower()
        is_hedge = bool(sig.get("order_is_hedge", False))
        algo_body: dict = {
            "instId":      _to_mexc(sym),
            "tdMode":      mode,
            "side":        "sell",
            "ordType":     "oco",
            "sz":          str(int(total_contracts)),
            "tpTriggerPx": str(_pround(new_tp)),
            "tpOrdPx":     "-1",
            "slTriggerPx": str(_pround(new_sl)),
            "slOrdPx":     "-1",
        }
        if is_hedge:
            algo_body["posSide"] = "long"
        algo_resp = _trade_post("/api/v1/private/order/submit", algo_body, cfg)
        ad        = (algo_resp.get("data") or [{}])[0]
        if algo_resp.get("success") is False or (algo_resp.get("success") is None and algo_resp.get("code", 200) not in (0, 200)):
            err = _okx_err(algo_resp)
            _append_error("trade", f"DCA OCO algo failed: {err}",
                          symbol=sym, endpoint="/api/v1/private/order/submit")
            return ""
        return ad.get("algoId", "")
    except Exception as exc:
        _append_error("trade", f"DCA OCO exception: {exc}",
                      symbol=sig.get("symbol", ""),
                      endpoint="/api/v1/private/order/submit")
        return ""


def _execute_dca_fill(sig: dict, cfg: dict) -> bool:
    """Execute one DCA add for a signal flagged with `_dca_pending=True`.

    Steps:
      1. Compute next DCA margin (doubles previous).
      2. Place the market-buy add via place_okx_dca_order().
      3. On success: append to dca_fills, recompute blended avg / total_usdt /
         total_notional, increment dca_count, recompute tp & (if ladder now
         exhausted) final_sl_price.
      4. Cancel the prior OCO algo and place a fresh one at the new TP/SL.

    Returns True on successful add, False otherwise. Clears _dca_pending.
    """
    sym = sig.get("symbol", "?")
    try:
        dca_usdt = _dca_next_usdt(sig)
        if dca_usdt <= 0:
            sig.pop("_dca_pending", None)
            return False

        result = place_okx_dca_order(sig, cfg, dca_usdt)
        if result.get("status") != "placed":
            # Leave _dca_pending=True so the next cycle retries? For safety,
            # clear it to avoid hammering on a structural failure. User can
            # see the error in the error log. Add it back as True only on
            # retryable paths; for now clear unconditionally.
            sig.pop("_dca_pending", None)
            sig["_dca_last_error"] = result.get("error", "")
            return False

        fill_px   = float(result.get("actual_entry", 0) or 0)
        fill_sz   = int(result.get("sz", 0) or 0)
        fill_not  = float(result.get("notional", 0) or 0) or (dca_usdt * int(sig.get("trade_lev", 10) or 10))
        fill_ordid = result.get("ordId", "")

        # Append fill record
        fills = sig.get("dca_fills") or []
        next_idx = len(fills)  # 0-based; new fill is at index next_idx
        fills.append({
            "price":    fill_px,
            "usdt":     dca_usdt,
            "leverage": int(sig.get("trade_lev", 0) or 0),
            "notional": fill_not,
            "ts":       dubai_now().isoformat(),
            "order_id": fill_ordid,
            "dca_idx":  next_idx,
        })
        sig["dca_fills"] = fills

        # Recompute blended average as notional-weighted mean of fills.
        # If any fill is missing notional, fall back to equal weighting.
        total_notional = 0.0
        total_units    = 0.0
        total_usdt     = 0.0
        for f in fills:
            px = float(f.get("price", 0) or 0)
            nt = float(f.get("notional", 0) or 0)
            ud = float(f.get("usdt", 0) or 0)
            if px > 0 and nt > 0:
                total_notional += nt
                total_units    += nt / px
                total_usdt     += ud
        if total_units > 0:
            avg_entry = total_notional / total_units
        else:
            # Fallback: straight arithmetic mean of prices
            prices = [float(f.get("price", 0) or 0) for f in fills
                      if float(f.get("price", 0) or 0) > 0]
            avg_entry = sum(prices) / len(prices) if prices else float(sig.get("avg_entry", 0) or 0)

        sig["avg_entry"]      = _pround(avg_entry)
        sig["total_usdt"]     = total_usdt
        sig["total_notional"] = total_notional
        sig["dca_count"]      = next_idx  # fill 0 is entry; DCA count = fills-1

        # Recompute TP from sidebar tp_pct and new blended avg
        tp_pct = float(cfg.get("tp_pct", 1.5)) / 100.0
        new_tp = _pround(sig["avg_entry"] * (1.0 + tp_pct))
        sig["tp"] = new_tp

        # If ladder now exhausted, set final SL at -3% below blended avg.
        # Otherwise, keep sig["sl"] at a sentinel (use 0 so candle-SL check
        # can't accidentally fire — _update_one_signal's DCA path already
        # skips SL checking while ladder has adds remaining).
        dca_max = int(sig.get("dca_max", 0) or 0)
        if sig["dca_count"] >= dca_max:
            final_sl = _pround(sig["avg_entry"] * 0.97)
            sig["final_sl_price"] = final_sl
            sig["sl"] = final_sl
        else:
            sig["final_sl_price"] = None
            # Keep a reasonable-looking SL for display purposes — we'll use
            # the next-DCA trigger price so the table's SL column has meaning
            # while the ladder is still active. The actual SL check is
            # bypassed for DCA trades with remaining adds.
            sig["sl"] = _dca_compute_trigger(sig)

        # Update last-order/display fields so MEXC Command / Order ID reflect
        # the newest add.
        sig["order_id"]       = fill_ordid or sig.get("order_id", "")
        sig["order_sz"]       = fill_sz
        sig["order_notional"] = fill_not
        sig["order_status"]   = "placed"

        # Cancel old OCO and place fresh one at new levels, sized to the
        # TOTAL contract count across all fills.
        total_contracts = sum(int(f.get("sz", 0) or 0) for f in fills)
        # fill 0 (original entry) stored its sz in sig["order_sz"] historically
        # — reconstruct total: contracts from each DCA fill plus the initial
        # entry (stored separately before we started appending fills).
        _entry_contracts = 0
        try:
            if fills:
                # First fill record may lack 'sz' (we didn't capture it at entry).
                # Derive contract count from the initial sig order_sz set after
                # the first place_okx_order.
                _entry_contracts = int(fills[0].get("sz", 0) or 0)
        except Exception:
            pass
        # If the entry fill didn't store sz, reconstruct from initial sig fields.
        # dca_fills[0] was populated BEFORE we tracked contract counts; use
        # sig["order_sz"] snapshot that existed at entry (but we've now
        # overwritten it). As a simple rule: reconstruct total from notional / (ct_val * price).
        try:
            _ct_val = float(sig.get("order_ct_val", 0) or 0)
            if _ct_val > 0:
                total_contracts = 0
                for f in fills:
                    _fp = float(f.get("price", 0) or 0)
                    _fn = float(f.get("notional", 0) or 0)
                    if _fp > 0 and _fn > 0:
                        total_contracts += max(1, int(_fn / (_ct_val * _fp)))
        except Exception:
            pass
        if total_contracts <= 0:
            total_contracts = fill_sz

        _old_algo = sig.get("algo_id", "")
        if _old_algo:
            _cancel_algo_best_effort(_old_algo, sym, cfg)
        new_algo = _place_dca_oco_algo(sig, cfg, sig["tp"], sig["sl"],
                                       total_contracts)
        if new_algo:
            sig["algo_id"] = new_algo

        sig.pop("_dca_pending", None)
        sig.pop("_dca_trigger_px", None)
        sig.pop("_dca_trigger_time", None)
        return True

    except Exception as exc:
        _append_error("trade", f"DCA execute exception: {exc}",
                      symbol=sym, endpoint="_execute_dca_fill")
        sig.pop("_dca_pending", None)
        return False


def _update_one_signal(sig: dict) -> None:
    """Fetch candles for a single open signal and update its status in-place.

    Runs inside a ThreadPoolExecutor — must not hold any locks.
    All writes go directly into the signal dict (dicts are shared references).

    Any exception is logged (not silently swallowed) so TP/SL detection
    failures become visible in the API Error Log panel.
    """
    _sym = sig.get("symbol", "?")
    try:
        # Robust timestamp parsing — tolerates Z-suffixed or naive strings
        # that may exist in older log entries.
        sig_dt    = _parse_iso_safe(sig["timestamp"])
        sig_ts_ms = int(sig_dt.timestamp() * 1000)
        candles   = get_klines(sig["symbol"], "5m", 200)
        post      = [c for c in candles if c["time"] >= sig_ts_ms]

        # ── DCA branch: trade has DCA ladder + unused slots ─────────────────
        _dca_enabled = bool(sig.get("dca_enabled", False))
        _dca_max     = int(sig.get("dca_max", 0) or 0)
        _dca_count   = int(sig.get("dca_count", 0) or 0)
        _ladder_slots_left = _dca_enabled and _dca_count < _dca_max

        if _ladder_slots_left:
            # While the ladder still has unused slots the sidebar SL is
            # IGNORED — only TP exit or a DCA trigger can act on this trade.
            _dca_trigger_price = _dca_compute_trigger(sig)
            # Scan candles AFTER the last fill timestamp (entry or last DCA).
            _last_fill_ts_ms = sig_ts_ms
            _fills = sig.get("dca_fills") or []
            if _fills:
                _last_fill = _fills[-1]
                try:
                    _last_fill_ts_ms = int(_parse_iso_safe(
                        _last_fill.get("ts") or sig["timestamp"]
                    ).timestamp() * 1000)
                except Exception:
                    _last_fill_ts_ms = sig_ts_ms
            _post_dca = [c for c in post if c["time"] >= _last_fill_ts_ms]
            tp_time  = None
            dca_time = None
            for c in _post_dca:
                if tp_time is None and c["high"] >= sig["tp"]:
                    tp_time = c["time"]
                if (dca_time is None and _dca_trigger_price > 0
                        and c["low"] <= _dca_trigger_price):
                    dca_time = c["time"]
            if tp_time is not None and (dca_time is None or tp_time <= dca_time):
                # TP wins — close (DCA trade closes via TP Hit table, which
                # is shared; table layer recognises DCA from dca_count).
                sig.update(status="tp_hit", close_price=sig["tp"],
                           close_time=to_dubai(datetime.fromtimestamp(
                               tp_time/1000, tz=timezone.utc)).isoformat())
                sig.pop("price_alert", None)
                sig.pop("_dca_pending", None)
            elif dca_time is not None:
                # DCA trigger fired — main loop will place the order.
                sig["_dca_pending"]      = True
                sig["_dca_trigger_px"]   = _dca_trigger_price
                sig["_dca_trigger_time"] = to_dubai(datetime.fromtimestamp(
                    dca_time/1000, tz=timezone.utc)).isoformat()
                # Still update latest_price / Current Status for the table.
                if candles:
                    latest_price = candles[-1]["close"]
                    sig["latest_price"] = float(latest_price)
                    _avg = float(sig.get("avg_entry",
                                         sig.get("entry", 0)) or 0)
                    if _avg > 0:
                        drop_pct = (_avg - latest_price) / _avg * 100
                        sig["price_alert"]     = drop_pct >= _PRICE_ALERT_PCT
                        sig["price_alert_pct"] = round(drop_pct, 2)
            else:
                # No trigger — display-only refresh using blended avg.
                if candles:
                    latest_price = candles[-1]["close"]
                    sig["latest_price"] = float(latest_price)
                    _avg = float(sig.get("avg_entry",
                                         sig.get("entry", 0)) or 0)
                    if _avg > 0:
                        drop_pct = (_avg - latest_price) / _avg * 100
                        sig["price_alert"]     = drop_pct >= _PRICE_ALERT_PCT
                        sig["price_alert_pct"] = round(drop_pct, 2)
                    else:
                        sig["price_alert"]     = False
                        sig["price_alert_pct"] = 0.0
            return  # DCA branch done — do not fall through to legacy logic

        # ── Legacy branch (no DCA) OR DCA ladder fully exhausted ────────────
        # When ladder is exhausted, sig["sl"] == final_sl_price (blended × 0.97)
        # and a hit routes to "dca_sl_hit" instead of "sl_hit".
        _ladder_full = _dca_enabled and _dca_max > 0 and _dca_count >= _dca_max
        tp_time = sl_time = None
        for c in post:
            if tp_time is None and c["high"] >= sig["tp"]: tp_time = c["time"]
            if sl_time is None and c["low"]  <= sig["sl"]: sl_time = c["time"]
        if tp_time is not None or sl_time is not None:
            if tp_time is not None and (sl_time is None or tp_time <= sl_time):
                sig.update(status="tp_hit", close_price=sig["tp"],
                           close_time=to_dubai(datetime.fromtimestamp(
                               tp_time/1000, tz=timezone.utc)).isoformat())
                sig.pop("price_alert", None)
            else:
                _close_status = "dca_sl_hit" if _ladder_full else "sl_hit"
                sig.update(status=_close_status, close_price=sig["sl"],
                           close_time=to_dubai(datetime.fromtimestamp(
                               sl_time/1000, tz=timezone.utc)).isoformat())
                sig.pop("price_alert", None)
        else:
            # Trade still open — update price-drop alert flag
            if candles:
                latest_price = candles[-1]["close"]
                # Use blended avg if present (post-DCA), else original entry.
                _ref = float(sig.get("avg_entry",
                                     sig.get("entry", 0)) or 0)
                # Store raw latest close — used by PnL $ column (avoids the
                # precision loss of deriving from rounded price_alert_pct).
                sig["latest_price"] = float(latest_price)
                if _ref > 0:
                    drop_pct = (_ref - latest_price) / _ref * 100
                    sig["price_alert"]     = drop_pct >= _PRICE_ALERT_PCT
                    sig["price_alert_pct"] = round(drop_pct, 2)
                else:
                    sig["price_alert"]     = False
                    sig["price_alert_pct"] = 0.0
    except Exception as _upd_exc:
        # Previously swallowed silently — surfaced now so malformed signals
        # (bad timestamp, missing tp/sl, MEXC fetch failure) become visible
        # in the API Error Log instead of leaving trades "open" forever.
        _append_error("signal_update",
                      f"{_sym}: {type(_upd_exc).__name__}: {_upd_exc}",
                      symbol=_sym)


def update_open_signals(signals):
    """Update all open signals in parallel (one candle fetch per open trade).

    Previously sequential — with 15 open trades this blocked for ~4-5 s.
    Now all fetches run concurrently, cutting Phase 1 to ~0.3-0.8 s.
    """
    open_sigs = [s for s in signals if s["status"] == "open"]
    if not open_sigs:
        return signals   # nothing to do — skip thread overhead entirely

    # Each worker fetches 200×5m candles for one trade.  Cap at 15 workers
    # (matches MAX_OPEN_TRADES) — well within the _api_sem(20) concurrency window.
    with ThreadPoolExecutor(max_workers=min(len(open_sigs), 15)) as pool:
        futs = [pool.submit(_update_one_signal, s) for s in open_sigs]
        for f in as_completed(futs):
            f.result()   # re-raise any unexpected exception for logging

    return signals

# ─────────────────────────────────────────────────────────────────────────────
# Background scanner thread
# ─────────────────────────────────────────────────────────────────────────────
def _check_sl_circuit_breaker():
    """Return True if the last 3 closed trades are all sl_hit (triggers auto-pause)."""
    with _log_lock:
        _closed = sorted(
            [s for s in _b._msc_log["signals"]
             if s.get("status") in ("tp_hit", "sl_hit") and s.get("close_time")],
            key=lambda x: x.get("close_time", ""),
        )
    return len(_closed) >= 3 and all(s["status"] == "sl_hit" for s in _closed[-3:])


def _bg_loop():
    while True:
        # Pause if user manually stopped OR circuit breaker fired (3 consec SL).
        # NOTE: When _msc_sl_paused is True, the loop is fully idle — no
        # open-signal updates, no scans, no cycle logic. The flag is cleared
        # ONLY by the manual "▶️ Resume Scanning" button (see sidebar UI). TP
        # hits do NOT auto-resume the scanner.
        if not _scanner_running.is_set() or getattr(_b, "_msc_sl_paused", False):
            time.sleep(2); continue
        with _config_lock:
            cfg = dict(_b._msc_cfg)
        t0 = time.time()
        try:
            # ── Phase 1: Update open signals (candle fetch per open trade) ──
            # This is network-bound (0.3–0.8 s typical, up to several seconds
            # on slow connections). We intentionally run it OUTSIDE `_log_lock`
            # so the UI snapshot in the main render thread is never blocked
            # waiting for MEXC. Signal dicts are shared references, so in-place
            # mutations performed by _update_one_signal are visible to the
            # main log automatically — no write-back required.
            # Safety: the only path that REPLACES the signals list wholesale
            # (Flush All / Clear 24h) holds `_log_lock` while doing so, so
            # taking a shallow copy under the lock here gives us a stable
            # snapshot even if the user flushes mid-cycle. Any dicts that
            # got removed from the live list won't be re-added — we only
            # mutate dict fields, never reassign the list.
            with _log_lock:
                _open_snapshot = [s for s in _b._msc_log["signals"]
                                  if s.get("status") == "open"]
            if _open_snapshot:
                update_open_signals(_open_snapshot)   # in-place dict mutations

                # ── DCA execution pass ───────────────────────────────────────
                # _update_one_signal flags any signal whose price has crossed
                # its DCA trigger with sig["_dca_pending"] = True. We now place
                # those DCA market orders sequentially (network I/O) under the
                # same auto-trading gating as normal entries. Skipped entirely
                # when auto-trading is disabled.
                _dca_pending_sigs = [s for s in _open_snapshot
                                     if s.get("_dca_pending")]
                if _dca_pending_sigs:
                    if cfg.get("trade_enabled"):
                        for _dsig in _dca_pending_sigs:
                            _ok = _execute_dca_fill(_dsig, cfg)
                            # _execute_dca_fill clears _dca_pending regardless
                            # of outcome. Errors surface in the Error Log.
                            _ = _ok
                        with _log_lock:
                            save_log(_b._msc_log)
                    else:
                        # Auto-trading off — skip real orders but clear the
                        # flag so it doesn't accumulate. Surface as error.
                        for _dsig in _dca_pending_sigs:
                            _append_error(
                                "trade",
                                f"DCA trigger hit but auto-trading disabled; "
                                f"skipping add for {_dsig.get('symbol', '?')}",
                                symbol=_dsig.get("symbol", ""),
                            )
                            _dsig.pop("_dca_pending", None)

            # ── Circuit breaker: check AFTER updating signals so newly-closed
            # sl_hit trades are counted immediately ─────────────────────────
            if _check_sl_circuit_breaker():
                _b._msc_sl_paused = True
                continue   # halt this cycle; UI will show the resume banner

            # ── Super Setup cap: count currently-open Super trades ───────────
            # and compute how many additional Super slots are available this cycle.
            with _log_lock:
                _current_supers = sum(
                    1 for s in _b._msc_log["signals"]
                    if s.get("status") == "open" and s.get("is_super_setup")
                )
            _super_cap        = max(0, int(cfg.get("max_super_trades", 5)))
            _super_slots_left = max(0, _super_cap - _current_supers)

            # ── Cooldown windows (TP — minutes; SL — hours, universal blackout) ─
            # Compute BEFORE scan() so the skip set can be passed in and SL-cooldown
            # coins never consume Super slots or trigger any candle fetches.
            now_dubai   = dubai_now()
            tp_cutoff   = now_dubai - timedelta(minutes=int(cfg["cooldown_minutes"]))
            sl_cd_hours = max(1, int(cfg.get("sl_cooldown_hours", 24)))
            sl_cutoff   = now_dubai - timedelta(hours=sl_cd_hours)

            with _log_lock:
                # TP cooldown — short (minutes), blocks re-entry after a TP close
                cooled_tp = {s["symbol"] for s in _b._msc_log["signals"]
                             if s.get("close_time")
                             and s.get("status") == "tp_hit"
                             and datetime.fromisoformat(
                                 s["close_time"].replace("Z","+00:00")) >= tp_cutoff}
                # SL cooldown — long (hours, default 24), applies UNIVERSALLY
                # including to Super Setups. A stopped-out coin is blacklisted
                # for the full sl_cooldown_hours window regardless of what new
                # setup it might qualify for.
                cooled_sl = {s["symbol"] for s in _b._msc_log["signals"]
                             if s.get("close_time")
                             and s.get("status") == "sl_hit"
                             and datetime.fromisoformat(
                                 s["close_time"].replace("Z","+00:00")) >= sl_cutoff}
                cooled = cooled_tp | cooled_sl
                active = {s["symbol"] for s in _b._msc_log["signals"] if s["status"]=="open"}

            # Pass the full skip set (TP cooldown ∪ SL cooldown ∪ currently open)
            # into scan() so these coins are dropped pre-deep-scan.
            _pre_scan_skip = cooled | active
            new_sigs, errors = scan(
                cfg,
                super_slots_remaining=_super_slots_left,
                skip_symbols=_pre_scan_skip,
            )

            with _log_lock:
                # ── Queue-limit check ─────────────────────────────────────────
                # Count only genuinely OPEN trades (queue_limit entries are NOT
                # real open positions — no order was placed for them).
                # The cap is adjustable via sidebar (config key: max_open_trades).
                # Graceful reduction: if user lowers the limit below current
                # open count, existing trades are NOT force-closed. New signals
                # during the overflow period are logged as queue_limit until
                # natural TP/SL closures bring the count down to the new limit.
                open_trade_count = len(active)
                MAX_OPEN_TRADES  = max(1, int(cfg.get("max_open_trades", 15)))
                _new_sig_syms      = [s["symbol"] for s in new_sigs]
                _blocked_active    = sorted(active    & set(_new_sig_syms))
                _blocked_cooldown  = sorted(cooled_tp & set(_new_sig_syms))
                _blocked_sl_cooldown = sorted(cooled_sl & set(_new_sig_syms))
                # Record the per-cycle SL-cooldown block count for the funnel.
                # (process() counter `f_sl_cooldown` is populated here — the
                # scanner's filter chain doesn't see the trade history; the
                # blackout is enforced at placement time in _bg_loop.)
                _filter_counts["f_sl_cooldown"] = len(_blocked_sl_cooldown)
                _filter_counts["f_sl_cooldown_syms"] = list(_blocked_sl_cooldown)
                skip         = cooled | active
                _added_syms  = []
                _queued_syms = []           # symbols logged as queue_limit this cycle
                sigs_to_trade = []          # signals that need an order placed
                for sig in new_sigs:
                    if sig["symbol"] not in skip:
                        if open_trade_count >= MAX_OPEN_TRADES:
                            # ── Queue Limit: log the signal but do NOT place a trade
                            # and do NOT add to `active` / `skip` so this coin
                            # remains freely re-scannable on every future cycle.
                            # No cooldown applies because no trade was ever opened.
                            queued_sig = dict(sig)
                            queued_sig["status"] = "queue_limit"
                            _b._msc_log["signals"].append(queued_sig)
                            _queued_syms.append(sig["symbol"])
                            # Add to skip only for THIS cycle to prevent duplicate
                            # queue_limit entries within the same scan batch.
                            skip.add(sig["symbol"])
                        else:
                            _b._msc_log["signals"].append(sig)
                            skip.add(sig["symbol"])
                            _added_syms.append(sig["symbol"])
                            open_trade_count += 1   # keep running count accurate
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
                _b._msc_log["health"].update(
                    total_cycles         = _b._msc_log["health"].get("total_cycles",0)+1,
                    last_scan_at         = dubai_now().isoformat(),
                    last_scan_duration_s = round(elapsed, 1),
                    total_api_errors     = _b._msc_log["health"].get("total_api_errors",0)+errors,
                    watchlist_size       = len(cfg["watchlist"]),
                    pre_filtered_out     = _filter_counts.get("pre_filtered_out",0),
                    deep_scanned         = _filter_counts.get("checked",0),
                )
                save_log(_b._msc_log)

            # ── Place orders outside the log lock ────────────────────────────
            # sigs_to_trade are already appended to _b._msc_log["signals"],
            # so in-place dict updates here are reflected in the stored log.
            if sigs_to_trade:
                for sig in sigs_to_trade:
                    result = place_okx_order(sig, cfg)
                    sig["order_id"]          = result.get("ordId", "")
                    sig["algo_id"]           = result.get("algoId", "")
                    sig["order_sz"]          = result.get("sz", 0)
                    sig["order_status"]      = result.get("status", "")
                    sig["order_error"]       = result.get("error", "")
                    sig["trade_usdt"]        = float(cfg.get("trade_usdt_amount", 0))
                    sig["trade_lev"]         = int(cfg.get("trade_leverage", 10))
                    sig["demo_mode"]         = bool(cfg.get("demo_mode", True))
                    sig["order_ct_val"]      = float(result.get("ct_val", 0) or 0)
                    sig["order_notional"]    = float(result.get("notional", 0) or 0)
                    # Prefer the tdMode that `place_okx_order` actually used
                    # (passed back in its `_base_info`). If the call bailed on
                    # early validation before building _base_info, fall back to
                    # the cfg value — which matches what would have been sent.
                    sig["order_margin_mode"] = (
                        result.get("tdMode")
                        or cfg.get("trade_margin_mode", "isolated")
                    )
                    sig["order_is_hedge"]    = bool(result.get("is_hedge", False))
                    # Overwrite entry/TP/SL with actual fill values if available.
                    # Market orders fill at the live price, not the signal price —
                    # the OCO algo order was placed using these corrected levels.
                    if result.get("actual_entry"):
                        sig["entry"]        = result["actual_entry"]
                        sig["tp"]           = result["actual_tp"]
                        sig["sl"]           = result["actual_sl"]
                        sig["signal_entry"] = sig.get("entry")  # keep original for reference
                    # ── DCA state initialization ────────────────────────────
                    # Snapshot the DCA config at entry time so later sidebar
                    # changes don't retroactively alter already-open trades.
                    # Fill 0 IS the original entry (same price, margin, lev).
                    _dca_max_snap = int(cfg.get("trade_max_dca", 0) or 0)
                    _dca_max_snap = max(0, min(6, _dca_max_snap))
                    _entry_px     = float(sig.get("entry", 0) or 0)
                    _entry_usdt   = float(sig.get("trade_usdt", 0) or 0)
                    _entry_lev    = int(sig.get("trade_lev",   0) or 0)
                    _entry_notnl  = _entry_usdt * _entry_lev if _entry_lev > 0 else 0.0
                    sig["dca_enabled"]    = _dca_max_snap > 0
                    sig["dca_max"]        = _dca_max_snap
                    sig["dca_count"]      = 0
                    sig["dca_fills"]      = [{
                        "price":    _entry_px,
                        "usdt":     _entry_usdt,
                        "leverage": _entry_lev,
                        "notional": _entry_notnl,
                        "ts":       sig.get("timestamp", ""),
                        "order_id": sig.get("order_id", ""),
                        "dca_idx":  0,
                    }] if _entry_px > 0 else []
                    sig["avg_entry"]      = _entry_px
                    sig["total_usdt"]     = _entry_usdt
                    sig["total_notional"] = _entry_notnl
                    sig["original_entry"] = _entry_px
                    # final_sl_price is populated only after the DCA ladder is
                    # fully exhausted (dca_count == dca_max).
                    sig["final_sl_price"] = None
                with _log_lock:
                    save_log(_b._msc_log)
            _b._msc_last_error = ""
        except Exception as e:
            import traceback as _tb
            _full = _tb.format_exc()
            _b._msc_last_error = f"{e} | traceback: {_full[-600:]}"
            _append_error("loop", str(e))
        elapsed   = time.time() - t0
        sleep_sec = max(0, cfg["loop_minutes"] * 60 - elapsed)
        # Wait for sleep_sec, but wake immediately if Save & Apply triggers rescan
        _rescan_event.wait(timeout=sleep_sec)
        _rescan_event.clear()

def _ensure_scanner():
    if _b._msc_thread is None or not _b._msc_thread.is_alive():
        t = threading.Thread(target=_bg_loop, daemon=True, name="okx-scanner")
        t.start(); _b._msc_thread = t

# ─────────────────────────────────────────────────────────────────────────────
# STREAMLIT UI
# ─────────────────────────────────────────────────────────────────────────────
st.set_page_config(page_title="S&R — Crypto Intelligent Portal", page_icon="💎",
                   layout="wide", initial_sidebar_state="expanded")

# ─────────────────────────────────────────────────────────────────────────────
# Option 2 — Dark Navy + Gold  (S&R Crypto Intelligent Portal theme)
# ─────────────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
/* ── Google Fonts ─────────────────────────────────────────────────────────── */
@import url('https://fonts.googleapis.com/css2?family=Sora:wght@300;400;600;700&family=JetBrains+Mono:wght@400;500&display=swap');

/* ── Global base ──────────────────────────────────────────────────────────── */
html, body, [class*="css"] {
    font-family: 'Sora', sans-serif !important;
    color: #F9FAFB !important;
}

/* ── App background ───────────────────────────────────────────────────────── */
.stApp {
    background-color: #0A0F1E !important;
}

/* ── Sidebar ──────────────────────────────────────────────────────────────── */
[data-testid="stSidebar"] {
    background-color: #0D1424 !important;
    border-right: 1px solid #C9A84C33 !important;
}
[data-testid="stSidebar"] * {
    color: #F9FAFB !important;
}
[data-testid="stSidebar"] .stMarkdown h2 {
    color: #C9A84C !important;
    font-weight: 700 !important;
    letter-spacing: 0.04em !important;
    border-bottom: 1px solid #C9A84C55 !important;
    padding-bottom: 6px !important;
}

/* ── Main area headings ───────────────────────────────────────────────────── */
h1 {
    font-family: 'Sora', sans-serif !important;
    font-weight: 700 !important;
    font-size: 1.9rem !important;
    color: #C9A84C !important;
    letter-spacing: 0.05em !important;
    border-bottom: 2px solid #C9A84C55 !important;
    padding-bottom: 8px !important;
    margin-bottom: 12px !important;
}
h2, h3 {
    font-family: 'Sora', sans-serif !important;
    font-weight: 600 !important;
    color: #F9FAFB !important;
    letter-spacing: 0.03em !important;
}

/* ── Metrics ──────────────────────────────────────────────────────────────── */
[data-testid="stMetric"] {
    background-color: #111827 !important;
    border: 1px solid #C9A84C44 !important;
    border-radius: 8px !important;
    padding: 10px 14px !important;
}
[data-testid="stMetricLabel"] {
    color: #C9A84C !important;
    font-size: 0.72rem !important;
    font-weight: 600 !important;
    text-transform: uppercase !important;
    letter-spacing: 0.08em !important;
}
[data-testid="stMetricValue"] {
    color: #F9FAFB !important;
    font-family: 'JetBrains Mono', monospace !important;
    font-size: 1.4rem !important;
    font-weight: 500 !important;
}

/* ── Dataframes ───────────────────────────────────────────────────────────── */
[data-testid="stDataFrame"] {
    border: 1px solid #C9A84C33 !important;
    border-radius: 8px !important;
    overflow: hidden !important;
}
[data-testid="stDataFrame"] * {
    font-family: 'JetBrains Mono', monospace !important;
    font-size: 0.78rem !important;
}

/* ── Buttons ──────────────────────────────────────────────────────────────── */
.stButton > button {
    background-color: #111827 !important;
    color: #F9FAFB !important;
    border: 1px solid #C9A84C66 !important;
    border-radius: 6px !important;
    font-family: 'Sora', sans-serif !important;
    font-weight: 600 !important;
    font-size: 0.78rem !important;
    letter-spacing: 0.04em !important;
    transition: all 0.15s ease !important;
}
.stButton > button:hover {
    background-color: #C9A84C !important;
    color: #0A0F1E !important;
    border-color: #C9A84C !important;
}
.stButton > button[kind="primary"] {
    background-color: #C9A84C !important;
    color: #0A0F1E !important;
    border-color: #C9A84C !important;
    font-weight: 700 !important;
}
.stButton > button[kind="primary"]:hover {
    background-color: #E0BE6A !important;
}

/* ── Expanders ────────────────────────────────────────────────────────────── */
[data-testid="stExpander"] {
    background-color: #111827 !important;
    border: 1px solid #C9A84C33 !important;
    border-radius: 8px !important;
}
[data-testid="stExpander"] summary {
    color: #C9A84C !important;
    font-weight: 600 !important;
    font-size: 0.85rem !important;
    letter-spacing: 0.03em !important;
}

/* ── Dividers ─────────────────────────────────────────────────────────────── */
hr {
    border-color: #C9A84C33 !important;
}

/* ── Captions & info boxes ────────────────────────────────────────────────── */
[data-testid="stCaptionContainer"] {
    color: #9CA3AF !important;
    font-size: 0.74rem !important;
}
[data-testid="stInfo"] {
    background-color: #111827 !important;
    border-left: 3px solid #C9A84C !important;
    color: #F9FAFB !important;
    border-radius: 6px !important;
}
[data-testid="stSuccess"] {
    background-color: #052E16 !important;
    border-left: 3px solid #10B981 !important;
    color: #D1FAE5 !important;
}
[data-testid="stWarning"] {
    background-color: #1C1508 !important;
    border-left: 3px solid #FBBF24 !important;
    color: #FEF3C7 !important;
}
[data-testid="stError"] {
    background-color: #2D0B0B !important;
    border-left: 3px solid #F43F5E !important;
    color: #FFE4E6 !important;
}

/* ── Inputs / selects / checkboxes ───────────────────────────────────────── */
[data-testid="stTextInput"] input,
[data-testid="stNumberInput"] input,
[data-testid="stTextArea"] textarea,
[data-testid="stSelectbox"] select {
    background-color: #111827 !important;
    color: #F9FAFB !important;
    border: 1px solid #C9A84C44 !important;
    border-radius: 6px !important;
    font-family: 'JetBrains Mono', monospace !important;
}
[data-testid="stCheckbox"] label {
    color: #F9FAFB !important;
    font-size: 0.82rem !important;
}

/* ── Plotly chart backgrounds ─────────────────────────────────────────────── */
.js-plotly-plot .plotly .bg {
    fill: #111827 !important;
}

/* ── Scrollbar ────────────────────────────────────────────────────────────── */
::-webkit-scrollbar { width: 6px; height: 6px; }
::-webkit-scrollbar-track { background: #0A0F1E; }
::-webkit-scrollbar-thumb { background: #C9A84C55; border-radius: 3px; }
::-webkit-scrollbar-thumb:hover { background: #C9A84C; }
</style>
""", unsafe_allow_html=True)

_ensure_scanner()

with _log_lock:    _snap_log = json.loads(json.dumps(_b._msc_log))
with _config_lock: _snap_cfg = dict(_b._msc_cfg)

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

    # ── Circuit Breaker Status ─────────────────────────────────────────────────
    _sl_paused = getattr(_b, "_msc_sl_paused", False)
    if _sl_paused:
        st.warning(
            "🔴 **Paused — 3 consecutive SL hit**\n\n"
            "Review market conditions before resuming.",
            icon=None,
        )
        if st.button("▶️ Resume Scanning", key="resume_sl_circuit",
                     type="primary", use_container_width=True):
            # ONLY place in the codebase where _msc_sl_paused is cleared back
            # to False (apart from fresh process startup). Auto-resume on TP
            # hit is intentionally disabled — the halt sticks until a human
            # reviews market conditions and clicks this button.
            _b._msc_sl_paused = False
            _rescan_event.set()
            st.rerun()
    else:
        st.success("✅ **Circuit Breaker: OK**  —  No consecutive SL pause", icon=None)
    st.divider()

    # ── Auto-Trading ───────────────────────────────────────────────────────────
    st.markdown("**🤖 Auto-Trading**")
    new_trade_enabled = st.checkbox(
        "Enable Auto-Trading",
        value=bool(_snap_cfg.get("trade_enabled", False)), key="cfg_trade_enabled",
        help="Automatically place a market LONG order on MEXC when a signal fires.")

    new_demo_mode = st.radio(
        "Environment", ["Demo", "Live"],
        index=0 if _snap_cfg.get("demo_mode", True) else 1,
        horizontal=True, key="cfg_demo_mode",
        help="Demo uses x-simulated-trading header. Live places real orders.",
        disabled=not new_trade_enabled)
    if new_trade_enabled and new_demo_mode == "Live":
        st.warning("⚠️ LIVE mode — real funds at risk!")

    # ── Credential source indicator ────────────────────────────────────────
    # Env vars (MEXC_API_KEY / MEXC_API_SECRET / MEXC_API_PASSPHRASE) take
    # precedence over plaintext fields. Show the user which source is active
    # so they know whether their secrets are sitting in scanner_config.json.
    import os as _os_env
    _env_has = {
        "api_key":        bool(_os_env.environ.get("MEXC_API_KEY", "").strip()),
        "api_secret":     bool(_os_env.environ.get("MEXC_API_SECRET", "").strip()),
        "api_passphrase": bool(_os_env.environ.get("MEXC_API_PASSPHRASE", "").strip()),
    }
    _any_env = any(_env_has.values())
    _all_env = all(_env_has.values())
    _has_plaintext = any(
        str(_snap_cfg.get(k, "")).strip() for k in ("api_key", "api_secret", "api_passphrase")
    )
    if _all_env:
        st.success(
            "🔐 All 3 credentials loaded from environment variables "
            "(`MEXC_API_KEY`, `MEXC_API_SECRET`, `MEXC_API_PASSPHRASE`). "
            "No secrets are persisted to disk.",
            icon=None,
        )
    elif _any_env:
        _missing = [k for k, v in _env_has.items() if not v]
        st.info(
            "🔑 Partial env-var credentials detected. Missing: "
            f"`{', '.join(_missing)}`. Set all three env vars or fill the "
            "text fields below.",
            icon=None,
        )
    elif _has_plaintext:
        st.warning(
            "⚠️ API credentials are stored in **plaintext** in "
            "`scanner_config.json`. Consider exporting `MEXC_API_KEY`, "
            "`MEXC_API_SECRET`, and `MEXC_API_PASSPHRASE` as environment "
            "variables instead — they override the file and are never "
            "written back to disk.",
            icon=None,
        )

    # When env vars provide a credential, hide the saved value from the input
    # and show a neutral placeholder instead of letting the user overwrite
    # the empty-string placeholder with an actual key on save.
    _key_display  = ("(from env)" if _env_has["api_key"]
                     else _snap_cfg.get("api_key", ""))
    _sec_display  = ("" if _env_has["api_secret"]
                     else _snap_cfg.get("api_secret", ""))
    _pass_display = ("" if _env_has["api_passphrase"]
                     else _snap_cfg.get("api_passphrase", ""))

    new_api_key = st.text_input(
        "API Key", value=_key_display,
        key="cfg_api_key", disabled=not new_trade_enabled or _env_has["api_key"],
        placeholder=("Loaded from MEXC_API_KEY env var" if _env_has["api_key"]
                     else "Your MEXC API key"),
        help=(
            "MEXC API key used to place and manage orders.\n\n"
            "Env-var override: `MEXC_API_KEY` (preferred — not written to disk)."
        ))
    new_api_secret = st.text_input(
        "API Secret", value=_sec_display,
        key="cfg_api_secret", type="password",
        disabled=not new_trade_enabled or _env_has["api_secret"],
        placeholder=("Loaded from MEXC_API_SECRET env var" if _env_has["api_secret"]
                     else "Your MEXC API secret"),
        help=(
            "MEXC API secret used to sign requests.\n\n"
            "Env-var override: `MEXC_API_SECRET` (preferred — not written to disk)."
        ))
    new_api_passphrase = st.text_input(
        "API Passphrase", value=_pass_display,
        key="cfg_api_passphrase", type="password",
        disabled=not new_trade_enabled or _env_has["api_passphrase"],
        placeholder=("Loaded from MEXC_API_PASSPHRASE env var" if _env_has["api_passphrase"]
                     else "Your MEXC API passphrase"),
        help=(
            "MEXC API passphrase (not required for MEXC) (set when the key was created).\n\n"
            "Env-var override: `MEXC_API_PASSPHRASE` (preferred — not written to disk)."
        ))

    # ── Size / Leverage / Margin Mode ────────────────────────────────────────
    # These fields are EDITABLE even when Auto-Trading is OFF, because they
    # drive display-only calculations too:
    #   • Est Liquidity column    — uses leverage + margin mode
    #   • PnL $ column        — uses size × leverage as notional
    #   • Super / normal SL   — uses leverage to derive isolated SL (1 / lev)
    # The user needs to be able to tune these to preview what a live trade
    # would look like without having to enable live trading first.
    ta1, ta2 = st.columns(2)
    new_trade_usdt = ta1.number_input(
        "Size (USDT)", min_value=1.0, max_value=100000.0, step=1.0,
        value=float(_snap_cfg.get("trade_usdt_amount", 10.0)),
        key="cfg_trade_usdt",
        help=(
            "Collateral per trade in USDT (before leverage).\n\n"
            "Used by:\n"
            "  • Live order size (when Auto-Trading is enabled)\n"
            "  • PnL $ column — notional = Size × Leverage\n"
            "  • Notional preview below\n\n"
            "Editable whether or not Auto-Trading is enabled."
        ))
    new_trade_lev = ta2.number_input(
        "Leverage ×", min_value=1, max_value=125, step=1,
        value=int(_snap_cfg.get("trade_leverage", 10)),
        key="cfg_trade_lev",
        help=(
            "Leverage applied (capped by MEXC max for each coin).\n\n"
            "Used by:\n"
            "  • Live order leverage (when Auto-Trading is enabled)\n"
            "  • Est Liquidity column — isolated liq ≈ entry × (1 − 1/lev)\n"
            "  • PnL $ column — notional = Size × Leverage\n"
            "  • Isolated SL level — SL = entry × (1 − 1/lev)\n\n"
            "Editable whether or not Auto-Trading is enabled."
        ))
    new_margin_mode = st.selectbox(
        "Margin Mode", ["isolated", "cross"],
        index=0 if _snap_cfg.get("trade_margin_mode", "isolated") == "isolated" else 1,
        key="cfg_margin_mode",
        help=(
            "Margin mode for futures positions.\n\n"
            "  • isolated — each position uses its own collateral; SL is "
            "pinned at the liquidation price (1/leverage below entry). "
            "Est Liquidity column shows the per-trade liq price.\n"
            "  • cross    — positions share account equity as collateral; SL "
            "uses the configured SL %. Est Liquidity column shows '—'.\n\n"
            "Editable whether or not Auto-Trading is enabled — affects display "
            "columns (Est Liquidity) and SL placement logic."
        ))

    # ── Max DCA per Trade ─────────────────────────────────────────────────────
    # Dropdown (0-6). Doubling ladder; each add placed as a real market order.
    # Trigger math differs for isolated vs cross — see DEFAULT_CONFIG docstring.
    _dca_opts = [0, 1, 2, 3, 4, 5, 6]
    _cur_dca  = int(_snap_cfg.get("trade_max_dca", 1))
    if _cur_dca not in _dca_opts:
        _cur_dca = 1
    new_trade_max_dca = st.selectbox(
        "Max DCA per Trade", _dca_opts,
        index=_dca_opts.index(_cur_dca),
        key="cfg_trade_max_dca",
        help=(
            "How many automated DCA (Dollar-Cost Averaging) adds are allowed "
            "per trade.\n\n"
            "  • 0 — DCA OFF. Legacy behavior: sidebar TP/SL apply as usual.\n"
            "  • 1-6 — Allow up to N DCA adds. Each DCA doubles the previous "
            "add's margin (base → base → 2× → 4× → 8× …).\n\n"
            "Trigger:\n"
            "  • Isolated — DCA fires at 70% of the distance from blended "
            "average to current liquidation price.\n"
            "  • Cross — DCA fires at −7% below the current blended average.\n\n"
            "After N DCAs are consumed, the final SL sits at −3% below the "
            "blended average. Such closures route to the dedicated DCA SL Hit "
            "table. When DCA > 0 the sidebar SL % is IGNORED; only TP or the "
            "final −3% rule can close the position.\n\n"
            "Editable whether or not Auto-Trading is enabled."
        ))

    # Notional preview — always shown (even when Auto-Trading is off) so the
    # user can see the effective trade size driven by Size × Leverage.
    notional_usdt = new_trade_usdt * new_trade_lev
    _liq_pct_cap  = round(100 / new_trade_lev, 2) if new_trade_lev > 0 else 0
    _sl_caption   = (f"SL @ liq ~{_liq_pct_cap:.1f}% below entry"
                     if new_margin_mode == "isolated"
                     else f"SL -${notional_usdt * _snap_cfg.get('sl_pct',3.0)/100:.2f}")
    _preview_prefix = "📐" if new_trade_enabled else "📐 *(preview)*"
    st.caption(f"{_preview_prefix} Notional per trade: ~${notional_usdt:,.0f} USDT   "
               f"| TP +${notional_usdt * _snap_cfg.get('tp_pct',1.5)/100:.2f}   "
               f"| {_sl_caption}")
    if new_trade_enabled:
        st.caption("🔒 Credentials stored in scanner_config.json. "
                   "Use a trade-only API key — never withdrawal permissions.")

    # ── Connection test ────────────────────────────────────────────────────────
    _conn = getattr(_b, "_msc_api_conn_status",
                    {"status": "untested", "message": "", "tested_at": None,
                     "demo_mode": None, "uid": ""})
    _has_creds = bool(_snap_cfg.get("api_key") and _snap_cfg.get("api_secret")
                      and _snap_cfg.get("api_passphrase"))
    if st.button("🔌 Test Connection", use_container_width=True,
                 disabled=not _has_creds,
                 help="Verify your API credentials against MEXC right now."):
        with st.spinner("Testing…"):
            _result = test_api_connection(dict(_snap_cfg))
        _b._msc_api_conn_status = {
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
    _conn_now = getattr(_b, "_msc_api_conn_status",
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
            _cs = getattr(_b, "_msc_api_conn_status", {})
            _is_h = _cs.get("pos_mode", "net_mode") == "long_short_mode"
            _mode = _tc.get("trade_margin_mode", "isolated")

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

            _tresp = _trade_post("/api/v1/private/order/submit", _tbody, _tc)
            _b._msc_last_trade_raw = {
                "endpoint":  "/api/v1/private/order/submit",
                "body_sent": _tbody,
                "response":  _tresp,
                "is_hedge":  _is_h,
            }

            # Immediately cancel if placed
            _td0    = (_tresp.get("data") or [{}])[0]
            _tordid = _td0.get("ordId", "")
            if _tresp.get("code") == "0" and _tordid:
                try:
                    _trade_post("/api/v1/private/order/cancel_orders",
                                {"instId": "BTC-USDT-SWAP", "ordId": _tordid}, _tc)
                except Exception:
                    pass
                st.session_state["_test_trade_result"] = ("ok", f"✅ Test order placed & cancelled · ordId: {_tordid}")
            else:
                _terr = _okx_err(_tresp)
                _append_error("trade", f"Test trade failed: {_terr} | body={json.dumps(_tbody)} | resp={json.dumps(_tresp)[:300]}", endpoint="/api/v1/private/order/submit")
                st.session_state["_test_trade_result"] = ("err", f"❌ {_terr}")

    _ttr = st.session_state.get("_test_trade_result")
    if _ttr:
        if _ttr[0] == "ok":
            st.success(_ttr[1])
        else:
            st.error(_ttr[1])

    # ── Raw response debug expander ───────────────────────────────────────────
    _raw = getattr(_b, "_msc_last_trade_raw", {})
    if _raw:
        with st.expander("🔬 Last order raw response (debug)"):
            st.caption("Body sent to MEXC:")
            st.json(_raw.get("body_sent", {}))
            st.caption("MEXC response:")
            st.json(_raw.get("response", {}))
            st.caption(f"is_hedge={_raw.get('is_hedge')}  "
                       f"contracts={_raw.get('contracts','?')}  "
                       f"ct_val={_raw.get('ct_val','?')}")
    st.divider()

    st.markdown("**📊 Trade Settings**")
    c1, c2 = st.columns(2)
    new_tp = c1.number_input("TP %", min_value=0.1, max_value=20.0, step=0.1, value=float(_snap_cfg["tp_pct"]),    key="cfg_tp")
    _isolated_active = _snap_cfg.get("trade_margin_mode", "isolated") == "isolated"
    new_sl = c2.number_input(
        "SL %", min_value=0.1, max_value=20.0, step=0.1,
        value=float(_snap_cfg["sl_pct"]), key="cfg_sl",
        disabled=_isolated_active,
        help=("SL is the liquidation price in isolated mode (entry × (1 − 1/leverage)). "
              "Switch to Cross mode to use a custom SL %.")
        if _isolated_active else "Stop-loss as % below entry."
    )
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
    _macd_help = (
        "F7 — MACD Dark Green Histogram Filter\n\n"
        "Each timeframe is checked independently. Enable only the timeframes you want.\n\n"
        "For each enabled timeframe ALL of the following must be true:\n"
        "  • MACD line > 0\n"
        "  • Signal line > 0\n"
        "  • Histogram > 0 AND increasing (dark green — not fading)\n"
        "  • Bullish crossover within the last 12 candles"
    )
    st.markdown("**📊 F7 — MACD** (dark 🟢 histogram — per timeframe)", help=_macd_help)
    fm1, fm2, fm3 = st.columns(3)
    new_use_macd_3m  = fm1.checkbox("3m MACD",  value=bool(_snap_cfg.get("use_macd_3m",  True)), key="cfg_use_macd_3m")
    new_use_macd_5m  = fm2.checkbox("5m MACD",  value=bool(_snap_cfg.get("use_macd_5m",  True)), key="cfg_use_macd_5m")
    new_use_macd_15m = fm3.checkbox("15m MACD", value=bool(_snap_cfg.get("use_macd_15m", True)), key="cfg_use_macd_15m")
    _macd_on_tfs = [tf for tf, on in [("3m", new_use_macd_3m), ("5m", new_use_macd_5m), ("15m", new_use_macd_15m)] if on]
    if _macd_on_tfs:
        st.caption(f"✅ Checking MACD on: {' · '.join(_macd_on_tfs)}  |  MACD>0 · Signal>0 · Histogram 🟢↑ · Crossover ≤12")
    else:
        st.caption("⚫ MACD filter disabled (all timeframes off)")
    st.divider()

    # ── F8: Parabolic SAR ──────────────────────────────────────────────────────
    _sar_help = (
        "F8 — Parabolic SAR Filter\n\n"
        "Each timeframe is checked independently. Enable only the timeframes you want.\n\n"
        "For each enabled timeframe:\n"
        "  SAR must be positioned BELOW current price (bullish mode).\n"
        "  SAR above price = bearish trend → coin rejected on that timeframe."
    )
    st.markdown("**🪂 F8 — Parabolic SAR** (per timeframe)", help=_sar_help)
    fs1, fs2, fs3 = st.columns(3)
    new_use_sar_3m  = fs1.checkbox("3m SAR",  value=bool(_snap_cfg.get("use_sar_3m",  True)), key="cfg_use_sar_3m")
    new_use_sar_5m  = fs2.checkbox("5m SAR",  value=bool(_snap_cfg.get("use_sar_5m",  True)), key="cfg_use_sar_5m")
    new_use_sar_15m = fs3.checkbox("15m SAR", value=bool(_snap_cfg.get("use_sar_15m", True)), key="cfg_use_sar_15m")
    _sar_on_tfs = [tf for tf, on in [("3m", new_use_sar_3m), ("5m", new_use_sar_5m), ("15m", new_use_sar_15m)] if on]
    if _sar_on_tfs:
        st.caption(f"✅ Checking SAR on: {' · '.join(_sar_on_tfs)}  |  SAR must be below price (bullish)")
    else:
        st.caption("⚫ SAR filter disabled (all timeframes off)")
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

    # ── F10: 15m EMA Crossover ───────────────────────────────────────────────
    st.markdown("**📉 F10 — 15m EMA Crossover** (fast > slow)")
    new_use_ema_cross_15m = st.checkbox(
        "Enable F10 — 15m EMA Crossover",
        value=bool(_snap_cfg.get("use_ema_cross_15m", True)), key="cfg_use_ema_cross_15m",
        help=(
            "F10 — 15m EMA Crossover Filter\n\n"
            "On the 15m timeframe, the FAST EMA must be ABOVE the SLOW EMA.\n"
            "This confirms short-to-medium term bullish alignment before entry.\n\n"
            "Default: fast=12, slow=21 (both configurable below).\n"
            "Uses 15m candles already fetched — no extra API call."
        ))
    ex1, ex2 = st.columns(2)
    new_ema_cross_fast_15m = ex1.number_input(
        "Fast EMA (15m)", min_value=2, max_value=500, step=1,
        value=int(_snap_cfg.get("ema_cross_fast_15m", 12)),
        key="cfg_ema_cross_fast_15m", disabled=not new_use_ema_cross_15m)
    new_ema_cross_slow_15m = ex2.number_input(
        "Slow EMA (15m)", min_value=2, max_value=500, step=1,
        value=int(_snap_cfg.get("ema_cross_slow_15m", 21)),
        key="cfg_ema_cross_slow_15m", disabled=not new_use_ema_cross_15m)
    if new_use_ema_cross_15m:
        st.caption(f"✅ EMA{new_ema_cross_fast_15m} > EMA{new_ema_cross_slow_15m} on 15m required")
    st.divider()

    # ── Queue Size (max concurrent open trades) ──────────────────────────────
    st.markdown("**📊 Queue Size** (max concurrent open trades)")
    qs1, qs2 = st.columns(2)
    new_max_open_trades = qs1.number_input(
        "Max Open Trades", min_value=1, max_value=50, step=1,
        value=int(_snap_cfg.get("max_open_trades", 15)),
        key="cfg_max_open_trades",
        help=(
            "Hard cap on the number of concurrent open trades.\n\n"
            "When the limit is reached, new signals are logged as "
            "status='queue_limit' (no order placed) and the coin remains "
            "freely re-scannable on every future cycle.\n\n"
            "⚠️ Graceful reduction: Lowering this value does NOT force-close "
            "any currently open trades. If you reduce it below the current "
            "open count, overflow signals route to queue_limit until natural "
            "TP/SL closures bring the count down to the new cap."
        ))
    new_max_super_trades = qs2.number_input(
        "Max Super Trades", min_value=1, max_value=20, step=1,
        value=int(_snap_cfg.get("max_super_trades", 5)),
        key="cfg_max_super_trades",
        help=(
            "Hard cap on the number of concurrent open **Super Setup** trades "
            "(trades that bypass the F3–F10 filter chain because BOTH 15m and "
            "1h are in Discount zone).\n\n"
            "When this cap is reached, any new Super-eligible coin is NOT "
            "skipped — it falls through to the normal F3–F10 pipeline and is "
            "opened as a regular (non-super) trade if it passes all remaining "
            "filters. If it fails any filter, it is rejected like any other "
            "coin.\n\n"
            "This cap is independent of 'Max Open Trades' — Super trades still "
            "count toward that overall queue limit."
        ))
    st.caption(
        f"🔒 Open cap: {int(new_max_open_trades)} total · "
        f"⭐ Super cap: {int(new_max_super_trades)}"
    )
    st.divider()

    st.markdown("**⏱ Execution**")
    c5, c6 = st.columns(2)
    new_loop = c5.number_input(
        "Loop (min)", min_value=1, max_value=60, step=1,
        value=int(_snap_cfg["loop_minutes"]), key="cfg_loop",
        help=(
            "How often the scanner runs a full watchlist cycle.\n\n"
            "Every N minutes the scanner will:\n"
            "  1. Update all open signals (check TP/SL hits)\n"
            "  2. Re-scan the entire watchlist for new setups\n"
            "  3. Place orders for any passing signals\n\n"
            "Lower = more responsive but more API load. "
            "Higher = gentler on MEXC rate limits."
        ))
    new_cool = c6.number_input(
        "Cooldown (TP, min)", min_value=1, max_value=120, step=1,
        value=int(_snap_cfg["cooldown_minutes"]), key="cfg_cool",
        help=(
            "After a TP or SL close, skip this coin for N minutes before "
            "allowing re-entry.\n\n"
            "⚠️ Note: This is the **short TP cooldown**. SL losses have a "
            "separate, longer cooldown ('SL Cooldown (hrs)') so a freshly "
            "stopped-out coin is not re-entered too quickly."
        ))
    # ── SL-specific cooldown (per-coin blackout after SL hit) ────────────────
    new_sl_cooldown_hours = st.number_input(
        "SL Cooldown (hrs)", min_value=1, max_value=720, step=1,
        value=int(_snap_cfg.get("sl_cooldown_hours", 24)),
        key="cfg_sl_cooldown_hours",
        help=(
            "After an SL hit, block the same coin from re-entry for N hours.\n\n"
            "Default: 24 hours.\n\n"
            "⚠️ Applies **universally** — even to Super Setups. A coin that "
            "just lost on SL is blacklisted for this entire window regardless "
            "of how good the new setup looks.\n\n"
            "This is separate from the short TP cooldown (Cooldown (TP, min)) "
            "and only triggers on SL closes, not TP closes."
        ))
    st.divider()

    st.markdown("**📋 Watchlist** (one symbol per line)")
    wl_text = st.text_area("wl", value="\n".join(_snap_cfg["watchlist"]),
                            height=180, label_visibility="collapsed", key="cfg_wl")

    # Highlight any watchlist coins absent from the MEXC live SWAP instrument list
    _ct_cache = _b._msc_symbol_cache.get("ct_val", {})
    if _ct_cache:
        _wl_delisted = [s for s in _snap_cfg.get("watchlist", [])
                        if s not in _ct_cache]
        if _wl_delisted:
            st.warning(
                f"⚠️ {len(_wl_delisted)} coin(s) not found as live MEXC Futures instruments "
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
            _b._msc_log["signals"] = []
            _b._msc_log["health"]  = {
                "total_cycles": 0, "last_scan_at": None,
                "last_scan_duration_s": 0.0, "total_api_errors": 0,
                "watchlist_size": 0, "pre_filtered_out": 0, "deep_scanned": 0,
            }
            save_log(_b._msc_log)
        # 2 — filter funnel (use _reset_filter_counts so the in-place dict
        #     is reinitialised to zero-state, keeping the same object reference
        #     that the scanner thread uses — avoids the split-reference bug)
        _reset_filter_counts()
        # 3 — API error log
        with getattr(_b, "_msc_error_log_lock", threading.Lock()):
            if hasattr(_b, "_msc_error_log"):
                _b._msc_error_log.clear()
        # 4 — last trade debug panel + manual/test trade session results
        _b._msc_last_trade_raw = {}
        _b._msc_last_error     = ""
        for _ss_key in ("_mt_last_result", "_test_trade_result"):
            st.session_state.pop(_ss_key, None)
        st.success("✅ Flushed"); st.rerun()

    cd1, cd2 = st.columns(2)
    if cd1.button("📅 Clear 24h", use_container_width=True):
        cutoff = dubai_now() - timedelta(hours=24)
        with _log_lock:
            before = len(_b._msc_log["signals"])
            _b._msc_log["signals"] = [s for s in _b._msc_log["signals"]
                if datetime.fromisoformat(s["timestamp"].replace("Z","+00:00")) < cutoff]
            save_log(_b._msc_log)
        st.success(f"✅ Removed {before-len(_b._msc_log['signals'])}"); st.rerun()
    if cd2.button("📆 Clear 7d", use_container_width=True):
        cutoff = dubai_now() - timedelta(days=7)
        with _log_lock:
            before = len(_b._msc_log["signals"])
            _b._msc_log["signals"] = [s for s in _b._msc_log["signals"]
                if datetime.fromisoformat(s["timestamp"].replace("Z","+00:00")) < cutoff]
            save_log(_b._msc_log)
        st.success(f"✅ Removed {before-len(_b._msc_log['signals'])}"); st.rerun()
    st.divider()

    if st.button("↩️ Reset Defaults", use_container_width=True, type="secondary"):
        d = dict(DEFAULT_CONFIG)
        with _config_lock: _b._msc_cfg.clear(); _b._msc_cfg.update(d)
        save_config(d)
        # Also invalidate symbol cache so it re-verifies on next scan
        _b._msc_symbol_cache["fetched_at"] = 0
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
            "use_macd_3m":  bool(new_use_macd_3m),
            "use_macd_5m":  bool(new_use_macd_5m),
            "use_macd_15m": bool(new_use_macd_15m),
            "use_sar_3m":   bool(new_use_sar_3m),
            "use_sar_5m":   bool(new_use_sar_5m),
            "use_sar_15m":  bool(new_use_sar_15m),
            "use_vol_spike": bool(new_use_vol_spike),
            "vol_spike_mult": float(new_vol_mult), "vol_spike_lookback": int(new_vol_lookback),
            "use_pdz_5m": bool(new_use_pdz_5m),
            "use_pdz_15m": bool(new_use_pdz_15m),
            "use_ema_cross_15m":  bool(new_use_ema_cross_15m),
            "ema_cross_fast_15m": int(new_ema_cross_fast_15m),
            "ema_cross_slow_15m": int(new_ema_cross_slow_15m),
            "max_open_trades":    max(1, int(new_max_open_trades)),
            "max_super_trades":   max(1, int(new_max_super_trades)),
            "sl_cooldown_hours":  max(1, int(new_sl_cooldown_hours)),
            "watchlist": new_wl,
            # ── Auto-trading ─────────────────────────────────────────────────
            "trade_enabled":     bool(new_trade_enabled),
            "demo_mode":         (new_demo_mode == "Demo"),
            # When env vars supply credentials, the UI field is disabled and
            # shows a placeholder — never save placeholders / env-sourced values
            # back to disk. Use empty string so load_config() re-applies the env.
            "api_key":           ("" if _env_has["api_key"]
                                  else new_api_key.strip()),
            "api_secret":        ("" if _env_has["api_secret"]
                                  else new_api_secret.strip()),
            "api_passphrase":    ("" if _env_has["api_passphrase"]
                                  else new_api_passphrase.strip()),
            "trade_usdt_amount": float(new_trade_usdt),
            "trade_leverage":    int(new_trade_lev),
            "trade_margin_mode": new_margin_mode,
            "trade_max_dca":     max(0, min(6, int(new_trade_max_dca))),
        }
        with _config_lock: _b._msc_cfg.clear(); _b._msc_cfg.update(new_cfg)
        save_config(new_cfg)
        # Invalidate symbol cache if watchlist changed
        if new_wl != _snap_cfg.get("watchlist", []):
            _b._msc_symbol_cache["fetched_at"] = 0
        _b._msc_rescan_event.set()   # wake bg thread immediately — no waiting for next cycle
        st.success(f"✅ Saved — {len(new_wl)} coins — rescanning now…"); st.rerun()

# ─────────────────────────────────────────────────────────────────────────────
# MAIN AREA
# ─────────────────────────────────────────────────────────────────────────────
st.title("S&R — Crypto Intelligent Portal")
st.caption("🔧 MEXC v2.3 — 2026-04-22")

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
_api_cs   = getattr(_b, "_msc_api_conn_status",
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
open_count     = sum(1 for s in signals if s["status"]=="open")
tp_count       = sum(1 for s in signals if s["status"]=="tp_hit")
sl_count       = sum(1 for s in signals if s["status"]=="sl_hit")
dca_sl_count   = sum(1 for s in signals if s["status"]=="dca_sl_hit")
queue_count    = sum(1 for s in signals if s["status"]=="queue_limit")

# ── Warn if log loaded from a previous session already exceeds the cap
# (e.g. migrating from v1 which had no limit). No new trades will fire until
# open_count drops below the cap via TP or SL hits.
_max_open_cap = max(1, int(_snap_cfg.get("max_open_trades", 15)))
if open_count > _max_open_cap:
    st.warning(
        f"⚠️ **{open_count} open trades detected** — this exceeds the {_max_open_cap}-trade limit "
        f"(likely loaded from a log created before the queue-limit was enforced, or the limit was "
        f"recently lowered). No new trades will be opened until the open count drops to ≤{_max_open_cap} "
        f"via TP or SL hits. Existing trades will NOT be force-closed. "
        f"You can also use **Clear History** in the sidebar to reset the log."
    )
pre_out     = health.get("pre_filtered_out", 0)
deep_sc     = health.get("deep_scanned",     0)

# ── Capital Summary Box ───────────────────────────────────────────────────────
# When Auto-Trading is ON and API is connected:
#   Total    = live MEXC account equity (totalEq) fetched from /api/v1/private/account/assets
#   Invested = sum of collateral in currently-open trades (from signal log)
#   Remaining = Total − Invested
# When Auto-Trading is OFF (or API unreachable):
#   Total    = max_open_trades × trade_usdt_amount  (config-based estimate)
_cap_usdt_per_trade = float(_snap_cfg.get("trade_usdt_amount", 10) or 10)
_cap_max_trades     = max(1, int(_snap_cfg.get("max_open_trades", 15)))
_cap_invested       = sum(
    float(s.get("trade_usdt", _cap_usdt_per_trade) or _cap_usdt_per_trade)
    for s in signals if s.get("status") == "open"
)

# Try to fetch live MEXC balance when auto-trading is active and API is connected
_cap_total        = _cap_usdt_per_trade * _cap_max_trades   # fallback
_cap_total_source = f"config ({_cap_max_trades} trades × ${_cap_usdt_per_trade:,.0f})"
_cap_fetch_error  = ""

if _trade_on and _api_stat == "ok":
    try:
        _bal_resp  = _trade_get("/api/v1/private/account/assets", {}, _snap_cfg)
        if _bal_resp.get("success", False):
            _assets    = _bal_resp.get("data") or []
            _usdt_ast  = next((a for a in _assets if a.get("currency") == "USDT"), {})
            _total_eq  = float(_usdt_ast.get("equity", 0) or 0)
            if _total_eq > 0:
                _cap_total        = _total_eq
                _cap_total_source = "live MEXC account equity"
        else:
            _cap_fetch_error = f"MEXC error: {_bal_resp.get('msg','')}"
    except Exception as _bal_exc:
        _cap_fetch_error = str(_bal_exc)[:120]

_cap_remaining = max(0.0, _cap_total - _cap_invested)
_invest_pct    = (_cap_invested / _cap_total * 100) if _cap_total > 0 else 0.0

with st.container(border=True):
    _bc1, _bc2, _bc3, _bc4 = st.columns(4)
    _bc1.metric(
        "💰 Total Capital",
        f"${_cap_total:,.2f}",
        help=f"Source: {_cap_total_source}"
             + (f"\n\n⚠️ Balance fetch error: {_cap_fetch_error}" if _cap_fetch_error else "")
    )
    _bc2.metric(
        "📈 Invested",
        f"${_cap_invested:,.2f}",
        help=f"Collateral currently locked in {open_count} open trade(s)"
    )
    _bc3.metric(
        "🏦 Remaining",
        f"${_cap_remaining:,.2f}",
        help="Available capital for new trades (Total − Invested)"
    )
    _bc4.metric(
        "⚡ Utilisation",
        f"{_invest_pct:.1f}%",
        help="Percentage of total capital currently deployed"
    )

m1,m2,m3,m4,m5c,m6,m7,m8,m8b,m9 = st.columns(10)
m1.metric("Cycles",        health.get("total_cycles",0))
m2.metric("Scan Time",     f"{health.get('last_scan_duration_s',0)}s")
m3.metric("API Errors",    health.get("total_api_errors",0))
m4.metric("Pre-filtered ⚡", pre_out, help="Coins removed by bulk ticker pre-filter (saves API calls)")
m5c.metric("Deep Scanned", deep_sc, help="Coins that passed pre-filter and received full candle analysis")
m6.metric("Open",          open_count,  help=f"Active open trades (max {_max_open_cap} allowed simultaneously — configurable in sidebar)")
m7.metric("TP Hit ✅",     tp_count)
m8.metric("SL Hit ❌",     sl_count,   help="Regular SL hits (non-DCA trades, or DCA disabled)")
m8b.metric("DCA-SL ❌",    dca_sl_count, help="Ladder-exhausted SL hits — DCA trade closed at final SL (blended avg × 0.97) after max DCAs were consumed")
m9.metric("⏳ Queued",     queue_count, help=f"Signals detected while the {_max_open_cap}-trade limit was reached — no order placed, coin rescanned each cycle")

if getattr(_b, "_msc_last_error", ""):
    st.warning(f"⚠️ {_b._msc_last_error}")

# ── Active filter badges ───────────────────────────────────────────────────────
badges = []
if _snap_cfg.get("use_pdz_15m", True): badges.append(f"🎯 F2 — PDZ 15m")
if _snap_cfg.get("use_pdz_5m",  True): badges.append(f"🎯 F3 — PDZ 5m")
if _snap_cfg.get("use_rsi_5m",  True): badges.append(f"📈 F4 — 5m RSI ≥{_snap_cfg.get('rsi_5m_min',30)}")
if _snap_cfg.get("use_rsi_1h",  True): badges.append(f"📈 F5 — 1h RSI {_snap_cfg.get('rsi_1h_min',30)}–{_snap_cfg.get('rsi_1h_max',95)}")
if _snap_cfg.get("use_ema_3m"):    badges.append(f"📉 F6 — EMA{_snap_cfg.get('ema_period_3m',12)} 3m")
if _snap_cfg.get("use_ema_5m"):    badges.append(f"📉 F6 — EMA{_snap_cfg.get('ema_period_5m',12)} 5m")
if _snap_cfg.get("use_ema_15m"):   badges.append(f"📉 F6 — EMA{_snap_cfg.get('ema_period_15m',12)} 15m")
_macd_tfs = [tf for tf, k in [("3m","use_macd_3m"),("5m","use_macd_5m"),("15m","use_macd_15m")] if _snap_cfg.get(k, True)]
if _macd_tfs: badges.append(f"📊 F7 — MACD 🟢↑ {' · '.join(_macd_tfs)}")
_sar_tfs  = [tf for tf, k in [("3m","use_sar_3m"), ("5m","use_sar_5m"), ("15m","use_sar_15m")] if _snap_cfg.get(k, True)]
if _sar_tfs:  badges.append(f"🪂 F8 — SAR {' · '.join(_sar_tfs)}")
if _snap_cfg.get("use_vol_spike"): badges.append(
    f"📦 F9 — Vol ≥{_snap_cfg.get('vol_spike_mult',2.0)}× / {_snap_cfg.get('vol_spike_lookback',20)} 15m")
if _snap_cfg.get("use_ema_cross_15m", True):
    badges.append(f"📉 F10 — EMA{_snap_cfg.get('ema_cross_fast_15m',12)}>EMA{_snap_cfg.get('ema_cross_slow_15m',21)} 15m")
st.markdown("**Active Filters:**")
st.caption("  |  ".join(badges) if badges else "No advanced filters enabled")

# ── Shared PnL helper ──────────────────────────────────────────────────────
# Single source of truth for per-signal PnL ($). Used by:
#   • 24h Realized PnL summary (below)
#   • PnL $ column in the Open Signals / Closed Signals table
# Returns None if any input is missing or non-numeric — callers render "—".
def _calc_pnl_usd(sig: dict, ref_price, usdt_fallback: float, lev_fallback: int):
    """PnL $ for a signal — uses blended avg + total notional for DCA trades.

    For DCA trades (dca_count > 0) the reference is the blended average
    entry price and the notional is the cumulative total across all fills.
    For non-DCA trades this reduces to the original `(ref/entry − 1) × (usdt × lev)`.
    """
    try:
        _ref   = float(ref_price) if ref_price is not None else 0.0
    except (TypeError, ValueError):
        return None
    if _ref <= 0:
        return None
    # DCA trade — use blended avg + cumulative notional.
    _dca_count = int(sig.get("dca_count", 0) or 0)
    if _dca_count > 0:
        try:
            _avg   = float(sig.get("avg_entry", 0) or 0)
            _tnot  = float(sig.get("total_notional", 0) or 0)
        except (TypeError, ValueError):
            _avg = 0.0; _tnot = 0.0
        if _avg > 0 and _tnot > 0:
            return (_ref / _avg - 1.0) * _tnot
    # Non-DCA (or DCA with no fills yet beyond entry) — legacy formula.
    try:
        _entry = float(sig.get("entry", 0) or 0)
        _usdt  = float(sig.get("trade_usdt", usdt_fallback) or 0)
        _lev   = int(sig.get("trade_lev",    lev_fallback)  or 0)
    except (TypeError, ValueError):
        return None
    if _entry <= 0 or _usdt <= 0 or _lev <= 0:
        return None
    return (_ref / _entry - 1.0) * (_usdt * _lev)

# ── 24-hour realized PnL ($) ───────────────────────────────────────────────
# Sum realized PnL for all TP and SL closes in the last 24 hours. Uses the
# shared _calc_pnl_usd helper so this always matches the per-row "PnL $"
# column. Falls back to current config if the signal pre-dates the stored
# trade_usdt / trade_lev fields.
_pnl_cutoff_24h = dubai_now() - timedelta(hours=24)
_pnl_24h_total     = 0.0
_pnl_24h_wins      = 0.0
_pnl_24h_loss      = 0.0
_pnl_24h_tp_ct     = 0
_pnl_24h_sl_ct     = 0
_pnl_24h_dca_sl_ct = 0
_cfg_usdt_fallback = float(_snap_cfg.get("trade_usdt_amount", 0) or 0)
_cfg_lev_fallback  = int(_snap_cfg.get("trade_leverage", 10) or 0)
for _s in signals:
    if _s.get("status") not in ("tp_hit", "sl_hit", "dca_sl_hit"):
        continue
    _ct_raw = _s.get("close_time")
    if not _ct_raw:
        continue
    try:
        _ct = datetime.fromisoformat(str(_ct_raw).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        continue
    if _ct < _pnl_cutoff_24h:
        continue
    _pnl_val = _calc_pnl_usd(_s, _s.get("close_price"),
                             _cfg_usdt_fallback, _cfg_lev_fallback)
    if _pnl_val is None:
        continue
    _pnl_24h_total += _pnl_val
    if _s["status"] == "tp_hit":
        _pnl_24h_wins  += _pnl_val
        _pnl_24h_tp_ct += 1
    elif _s["status"] == "dca_sl_hit":
        _pnl_24h_loss      += _pnl_val      # negative
        _pnl_24h_dca_sl_ct += 1
    else:
        _pnl_24h_loss  += _pnl_val      # negative for SL hits
        _pnl_24h_sl_ct += 1

# Color based on sign: green for gain, red for loss, grey for exact zero / empty
if _pnl_24h_tp_ct == 0 and _pnl_24h_sl_ct == 0 and _pnl_24h_dca_sl_ct == 0:
    _pnl_color   = "#9CA3AF"  # grey
    _pnl_prefix  = "💰"
    _pnl_summary = "no closed trades in the last 24 h"
else:
    _pnl_color   = "#22C55E" if _pnl_24h_total >= 0 else "#EF4444"
    _pnl_prefix  = "💰" if _pnl_24h_total >= 0 else "📉"
    # Use backslash-escaped $ so Streamlit's markdown engine doesn't treat
    # pairs of dollar signs as LaTeX math delimiters (which would otherwise
    # swallow the inline <span> HTML between them).
    _pnl_summary = (
        f"{_pnl_24h_tp_ct} TP (+\\${_pnl_24h_wins:,.2f})  |  "
        f"{_pnl_24h_sl_ct} SL  |  "
        f"{_pnl_24h_dca_sl_ct} DCA-SL (\\${_pnl_24h_loss:,.2f})"
    )
st.markdown(
    f"{_pnl_prefix} **24h Realized PnL:** "
    f"<span style='color:{_pnl_color}; font-weight:600;'>"
    f"{_pnl_24h_total:+,.2f} \\$</span>  "
    f"<span style='opacity:0.7; font-size:0.85em;'>({_pnl_summary})</span>",
    unsafe_allow_html=True,
)

# ── Queue Size (current / max) ─────────────────────────────────────────────
_queue_indicator = f"📊 **Queue Size:** {open_count} / {_max_open_cap}"
if open_count >= _max_open_cap:
    _queue_indicator += "  🔒 *full — new signals will be queued*"
elif open_count > 0:
    _slots_left = _max_open_cap - open_count
    _queue_indicator += f"  ({_slots_left} slot{'s' if _slots_left != 1 else ''} free)"
st.markdown(_queue_indicator)

# ── Super Setup cap indicator (current Super trades / max) ─────────────────
_max_super_cap = max(0, int(_snap_cfg.get("max_super_trades", 5)))
_open_super_count = sum(
    1 for s in signals
    if s.get("status") == "open" and s.get("is_super_setup")
)
_super_indicator = f"⭐ **Super Cap:** {_open_super_count} / {_max_super_cap}"
if _max_super_cap > 0 and _open_super_count >= _max_super_cap:
    _super_indicator += "  *— new Super-eligible coins will fall through to F3-F10*"
st.caption(_super_indicator)

# ── SL Cooldown indicator ──────────────────────────────────────────────────
_sl_cd_hours_display = int(_snap_cfg.get("sl_cooldown_hours", 24))
st.caption(
    f"🔴 **SL Cooldown:** {_sl_cd_hours_display} h "
    f"*(per-coin blackout after SL hit — applies to all setups including Super)*"
)
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

# ── Shared helpers for row building ────────────────────────────────────────────
def _cv(v):
    """Format a criteria value: numeric → compact float, else as-is."""
    if v is None or v == "—" or v == "✅": return "—"
    try: return f"{float(v):.6f}".rstrip("0").rstrip(".")
    except (TypeError, ValueError): return str(v)

def _fmt_secs(secs: int) -> str:
    if secs < 0:   return "—"
    if secs < 3600: return f"{secs // 60}m {secs % 60}s"
    if secs < 86400:
        h, rem = divmod(secs, 3600); return f"{h}h {rem // 60}m"
    d, rem = divmod(secs, 86400);   return f"{d}d {rem // 3600}h"

def _build_signal_row(s: dict, is_open_table: bool = False,
                      show_pnl: bool = False) -> dict:
    """Convert one signal dict into a table row dict (all columns).

    Flags control which schema is emitted:
      • `is_open_table=True` — emit the Open-Signals-specific 26-column layout
        with its custom ordering and renamed columns:
          - "Current Status" (pos 7) — signed % change vs. entry, computed
            for EVERY open row (not just DL/TL-flagged). "—" only when live
            price data hasn't landed yet.
          - "Est Liquidity"  (pos 11) — estimated liquidation price for
            isolated trades, % distance from entry in parentheses. Cross
            trades show "—".
          - "Order Size"     (pos 26) — total dollar notional (margin ×
            leverage). Prefers the value MEXC actually used; falls back to
            sidebar settings for non-traded signals.

      • `is_open_table=False` — emit the legacy column layout used by TP Hit
        and SL Hit tables (and Queue Limit). Keeps the original "Fill $"
        column (actual market fill price), no "Current Status" / "Est
        Liquidity" / "Order Size" columns.

      • `show_pnl=True` adds the PnL column (after Sector in legacy layout,
        position 5 in Open layout):
          - "PnL $" — dollar PnL, formatted like "-2.76 $" / "+1.34 $".
            • Open trades    → live unrealized PnL (uses latest_price)
            • TP Hit trades  → realized gain   (uses close_price = TP)
            • SL Hit trades  → realized loss   (uses close_price = SL)
            • Queue Limit    → "—" (no trade was ever opened)

    Typical usage:
      Open Signals table  → is_open_table=True, show_pnl=True
      TP Hit table        → is_open_table=False, show_pnl=True
      SL Hit table        → is_open_table=False, show_pnl=True
      Queue Limit table   → both False
    """
    status      = s.get("status", "open")
    status_icon = {
        "open":        "🔵 Open",
        "tp_hit":      "✅ TP Hit",
        "sl_hit":      "❌ SL Hit",
        "dca_sl_hit":  "❌ DCA SL Hit",
        "queue_limit": "⏳ Queue Limit",
    }.get(status, status)

    # Alert column — only meaningful for open trades.
    # For DCA trades that have had ≥1 DCA fire, the alert shows the latest
    # DCA state (DCA-1, DCA-2, …) and supersedes the DL/TL tags so users can
    # tell at a glance that the trade has been averaged down.
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
            if _dl and _tl:   alert_col = "🔴 DL / TL"
            elif _dl:          alert_col = "🔴 DL"
            elif _tl:          alert_col = "🔴 TL"
    elif status in ("tp_hit", "sl_hit", "dca_sl_hit") and _dca_count_row > 0:
        # Preserve the DCA-N tag on closed DCA trades so TP Hit / DCA SL Hit
        # tables show it clearly.
        alert_col = f"DCA-{_dca_count_row}"

    # Current Status column — signed % change from entry for EVERY open trade
    # (previously "Current Drop", gated on DL/TL alerts — now always shown so
    # users can see real-time drift at a glance).
    #
    # price_alert_pct is populated by update_open_signals on every cycle
    # whenever candles are available, so nearly all open rows have a value.
    # Rows where it's missing (first cycle before update ran, or empty
    # candle fetch) fall back to "—".
    current_status_col = "—"
    if status == "open":
        _entry_rm = float(s.get("entry", 0) or 0)
        _pa_pct   = s.get("price_alert_pct")
        if _pa_pct is not None and _entry_rm > 0:
            # price_alert_pct = (entry - latest) / entry * 100 (positive = drop)
            # Flip sign so negative = drop, positive = rise — reads intuitively.
            try:
                _change = -float(_pa_pct)
                current_status_col = f"{_change:+.2f}%"
            except (TypeError, ValueError):
                current_status_col = "—"

    # ── Est Liquidity column (Open Signals only) ────────────────────────────
    # Estimated liquidation price + % distance from entry (negative, since liq
    # is below entry for LONG). Shown only for isolated trades. Cross-margin
    # trades show "—" because their liquidation depends on account-wide equity
    # and cannot be computed from per-trade info alone.
    #
    # Formula (LONG isolated, approximate):
    #     liq ≈ entry × (1 − 1/leverage)
    #     liq% from entry = -100 / leverage  (e.g. 10× → -10%, 20× → -5%)
    est_liq_col = "—"
    if status == "open":
        _margin_mode_rm = (s.get("order_margin_mode") or
                           _snap_cfg.get("trade_margin_mode", "isolated") or
                           "isolated").lower()
        # For DCA trades use the blended average; otherwise the original entry.
        _entry_liq = float(s.get("avg_entry", s.get("entry", 0)) or 0)
        _lev_liq   = int(s.get("trade_lev", _snap_cfg.get("trade_leverage", 10)) or 0)
        if _margin_mode_rm == "isolated" and _entry_liq > 0 and _lev_liq > 0:
            _liq_price = _entry_liq * (1.0 - 1.0 / _lev_liq)
            _liq_pct   = -100.0 / _lev_liq
            # Format liq price with enough precision for low-value coins
            if _liq_price >= 1:
                _liq_str = f"{_liq_price:.4f}"
            elif _liq_price >= 0.01:
                _liq_str = f"{_liq_price:.6f}"
            else:
                _liq_str = f"{_liq_price:.8f}"
            est_liq_col = f"{_liq_str} ({_liq_pct:+.2f}%)"

    # ── PnL $ column ─────────────────────────────────────────────────────────
    # Dollar PnL. Uses the per-signal trade_usdt and trade_lev stored at entry
    # (so PnL reflects actual position size, unaffected by later config changes).
    #   notional = trade_usdt × trade_lev
    #   qty      = notional / entry
    #   PnL $    = (ref_price − entry) × qty  =  (ref_price/entry − 1) × notional
    #
    # Reference price per status:
    #   • open    → sig["latest_price"] (live unrealized PnL)
    #   • tp_hit  → sig["close_price"] (= TP level — realized gain)
    #   • sl_hit  → sig["close_price"] (= SL level — realized loss)
    #   • queue   → "—" (no trade was ever opened)
    pnl_col = "—"
    if status in ("open", "tp_hit", "sl_hit", "dca_sl_hit"):
        if status == "open":
            _ref_pnl = s.get("latest_price")
            if _ref_pnl is None:
                # Fall back to deriving from price_alert_pct if latest_price
                # not stored yet (first cycle, before update_open_signals ran).
                # For DCA trades the pct is computed from blended avg; use
                # avg_entry as the reference too.
                _pa = s.get("price_alert_pct")
                _ref_entry_pnl = float(s.get("avg_entry",
                                             s.get("entry", 0)) or 0)
                if _pa is not None and _ref_entry_pnl > 0:
                    try:
                        _ref_pnl = _ref_entry_pnl * (1.0 - float(_pa) / 100.0)
                    except (TypeError, ValueError):
                        _ref_pnl = None
        else:
            # tp_hit / sl_hit / dca_sl_hit — close_price is the realized exit
            _ref_pnl = s.get("close_price")
        # Use the shared helper — single source of truth for PnL math.
        _pnl_val = _calc_pnl_usd(s, _ref_pnl, _cfg_usdt_fallback, _cfg_lev_fallback)
        if _pnl_val is not None:
            pnl_col = f"{_pnl_val:+.2f} $"

    ts_str    = fmt_dubai(s.get("timestamp", ""))
    close_str = fmt_dubai(s["close_time"]) if s.get("close_time") else "—"
    crit      = s.get("criteria", {})
    pdz_5m_val  = crit.get("pdz_zone_5m",  "—") or "—"
    pdz_15m_val = crit.get("pdz_zone_15m", "—") or "—"
    crit_str = (
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
        f"• PDZ 15m   : {pdz_15m_val}\n"
        f"• EMA12 15m : {_cv(crit.get('ema_cross_12_15m'))}\n"
        f"• EMA21 15m : {_cv(crit.get('ema_cross_21_15m'))}"
    ) if crit else "—"

    max_lev   = s.get("max_lev", get_max_leverage(s.get("symbol", "")))
    sl_reason = analyze_sl_reason(s) if status == "sl_hit" else "—"
    setup_type = "⭐ Super" if s.get("is_super_setup") else "Normal"

    _usdt    = float(s.get("trade_usdt", _snap_cfg.get("trade_usdt_amount", 0)))
    _lev     = int(s.get("trade_lev",   _snap_cfg.get("trade_leverage", 10)))
    _entry_p = float(s.get("entry", 0) or 0)
    _tp_p    = float(s.get("tp",    0) or 0)
    _sl_p    = float(s.get("sl",    0) or 0)
    # For DCA trades, TP $ / SL $ use the blended average and cumulative
    # notional so the dollar figures reflect the ACTUAL committed position
    # across all ladder fills.
    _dca_n_row = int(s.get("dca_count", 0) or 0)
    if _dca_n_row > 0:
        _avg_row = float(s.get("avg_entry", 0) or 0)
        _pos_row = float(s.get("total_notional", 0) or 0)
        if _avg_row > 0 and _pos_row > 0 and _tp_p > 0 and _sl_p > 0:
            tp_usd_str = f"+${_pos_row * (_tp_p - _avg_row) / _avg_row:.2f}"
            sl_usd_str = f"-${_pos_row * (_avg_row - _sl_p) / _avg_row:.2f}"
        else:
            tp_usd_str = sl_usd_str = "—"
    elif _usdt > 0 and _lev > 0 and _entry_p > 0:
        _pos = _usdt * _lev
        tp_usd_str = f"+${_pos * (_tp_p - _entry_p) / _entry_p:.2f}"
        sl_usd_str = f"-${_pos * (_entry_p - _sl_p) / _entry_p:.2f}"
    else:
        tp_usd_str = sl_usd_str = "—"

    ord_id_str  = s.get("order_id", "") or "—"
    algo_id_str = s.get("algo_id",  "") or "—"
    ord_env     = "🟡 Demo" if s.get("demo_mode") else "🔴 Live"
    ord_status  = s.get("order_status", "")
    ord_err     = s.get("order_error",  "")
    if   ord_status == "placed":  ord_status_str = f"✅ Entry+OCO {ord_env}"
    elif ord_status == "partial": ord_status_str = f"⚠️ Entry only {ord_env} · {ord_err[:80]}"
    elif ord_status == "error":   ord_status_str = f"❌ {ord_err[:80]}" if ord_err else "❌ Error"
    else:                         ord_status_str = "—"

    # ── MEXC Command column — shows exact parameters sent to MEXC ──────────────
    # A real order was placed iff we have an ordId OR order_status indicates
    # the call reached MEXC (placed / partial / error — an explicit rejection
    # counts because MEXC did see the request). Signals without these hallmarks
    # never actually talked to MEXC (auto-trading OFF, creds missing, or the
    # pre-flight validation in place_okx_order short-circuited); we must not
    # display a fabricated command string for them.
    _ord_sz       = int(s.get("order_sz", 0) or 0)
    _ct_val       = float(s.get("order_ct_val", 0) or 0)
    _notional     = float(s.get("order_notional", 0) or 0)
    _is_hedge     = bool(s.get("order_is_hedge", False))
    _order_id     = (s.get("order_id") or "").strip()
    _ord_status   = (s.get("order_status") or "").strip()
    _order_sent   = bool(_order_id) or _ord_status in ("placed", "partial", "error")
    # Fallback margin mode reflects the CURRENT sidebar setting — this matches
    # what `place_okx_order` would send RIGHT NOW, and keeps the Margin Mode
    # column (which uses the same fallback) in sync.
    _margin_mode  = (s.get("order_margin_mode") or "").strip().lower()
    if _margin_mode not in ("isolated", "cross"):
        _margin_mode = (_snap_cfg.get("trade_margin_mode") or "isolated").strip().lower()
    if not _notional and _usdt > 0 and _lev > 0:
        _notional = _usdt * _lev   # reconstruct for older signals without stored notional
    if status == "queue_limit":
        mexc_cmd_str = "No order placed — Queue Limit"
    elif not _order_sent:
        # Signal fired but no MEXC order was ever placed (auto-trading off,
        # missing creds, or early-return). Don't manufacture a fake command.
        mexc_cmd_str = "No order placed — auto-trading off or rejected pre-flight"
    elif _usdt > 0 or _ord_sz > 0:
        _sym_okx = _to_okx(s.get("symbol", ""))
        _ps_part = " | posSide: long" if _is_hedge else ""
        _ct_part = f" | ctVal: {_ct_val}" if _ct_val else ""
        mexc_cmd_str = (
            f"instId: {_sym_okx} | ordType: market"
            f" | tdMode: {_margin_mode}{_ps_part}"
            f" | sz: {_ord_sz} contracts"
            f" | collateral: ${_usdt:.2f} | lev: {_lev}×"
            f" | notional: ${_notional:.2f}{_ct_part}"
        )
    else:
        mexc_cmd_str = "—"

    duration_str = "—"
    if s.get("timestamp"):
        try:
            t_open = datetime.fromisoformat(s["timestamp"].replace("Z", "+00:00"))
            if status == "open":
                duration_str = _fmt_secs(int((dubai_now() - t_open).total_seconds()))
            elif s.get("close_time"):
                t_close = datetime.fromisoformat(s["close_time"].replace("Z", "+00:00"))
                duration_str = _fmt_secs(int((t_close - t_open).total_seconds()))
        except Exception:
            pass

    # ── Margin Mode display value (shared by Open and non-Open tables) ───────
    # When `order_margin_mode` is present on the signal, we show the value MEXC
    # actually used. When it's missing (auto-trading OFF at the time the
    # signal fired, creds missing, or `place_okx_order` short-circuited before
    # any request was sent), we fall back to the CURRENT sidebar setting so
    # this column stays consistent with the MEXC Command column and the active
    # config — prefixed with "⚠️" to signal that no live trade confirmation
    # exists for the row.
    _mm_val = ""
    if show_pnl:
        _mm_raw = (s.get("order_margin_mode") or "").strip().lower()
        if _mm_raw not in ("isolated", "cross"):
            _mm_raw = (_snap_cfg.get("trade_margin_mode") or "isolated").strip().lower()
            _mm_inferred = True
        else:
            _mm_inferred = False
        if _mm_raw == "isolated":
            _mm_val = ("⚠️ " if _mm_inferred else "") + "🔒 Isolated"
        else:  # "cross"
            _mm_val = ("⚠️ " if _mm_inferred else "") + "🔓 Cross"

    # ── Order Size (Open Signals only) ───────────────────────────────────────
    # Total dollar notional = margin × leverage. Prefers the value actually
    # sent to MEXC (`order_notional`), then per-signal trade_usdt × trade_lev
    # (captured at trade placement), then current sidebar settings as a last
    # resort (so non-traded signals still show what WOULD have been sent).
    # For DCA trades the cumulative total_notional (sum across all fills) is
    # the authoritative position size. Fall back to single-order fields for
    # non-DCA trades.
    _os_notional = float(s.get("total_notional", 0) or 0)
    if _os_notional <= 0:
        _os_notional = float(s.get("order_notional", 0) or 0)
    if _os_notional <= 0:
        _os_usdt = float(s.get("trade_usdt", _snap_cfg.get("trade_usdt_amount", 0)) or 0)
        _os_lev  = int(s.get("trade_lev",   _snap_cfg.get("trade_leverage",     0)) or 0)
        if _os_usdt > 0 and _os_lev > 0:
            _os_notional = _os_usdt * _os_lev
    order_size_col = f"${_os_notional:,.2f}" if _os_notional > 0 else "—"

    # Signal Entry displays the latest working entry price:
    #   • DCA trades (dca_count > 0) → blended average (avg_entry)
    #   • Non-DCA / pre-first-DCA → signal_entry (actual fill) or entry
    # The Original Entry column (inserted right after Signal Entry) holds
    # the very first fill price so the original reference is never lost.
    if _dca_count_row > 0:
        _sig_entry_display = s.get("avg_entry",
                                   s.get("signal_entry", s.get("entry", "")))
    else:
        _sig_entry_display = s.get("signal_entry", s.get("entry", ""))
    _orig_entry_display = s.get("original_entry",
                                s.get("signal_entry", s.get("entry", ""))) \
        if s.get("original_entry") is not None \
        else s.get("signal_entry", s.get("entry", ""))

    if is_open_table:
        # Open-Signals-specific column order (27 columns — adds Original Entry
        # after Signal Entry for DCA trade visibility).
        # TP Hit / SL Hit / DCA SL Hit / Queue Limit tables use the non-Open
        # branch below.
        row: dict = {
            "Time (GST)":      ts_str,
            "Symbol":          s.get("symbol", ""),
            "Alert":           alert_col,
            "Setup":           setup_type,
            "PnL $":           pnl_col,
            "Margin Mode":     _mm_val,
            "Current Status":  current_status_col,
            "Signal Entry":    _sig_entry_display,
            "Original Entry":  _orig_entry_display,
            "TP":              s.get("tp", ""),
            "SL":              s.get("sl", ""),
            "Est Liquidity":   est_liq_col,
            "Duration":        duration_str,
            "TP $":            tp_usd_str,
            "SL $":            sl_usd_str,
            "Status":          status_icon,
            "Close Time":      close_str,
            "Sector":          s.get("sector", "Other"),
            "Close $":         s.get("close_price") or "—",
            "Max Lev":         f"{max_lev}×",
            "Order":           ord_status_str,
            "MEXC Command":     mexc_cmd_str,
            "Entry Criteria":  crit_str,
            "Order ID":        ord_id_str,
            "Algo ID":         algo_id_str,
            "⚠️ SL Reason":   sl_reason,
            "Order Size":      order_size_col,
        }
        return row

    # ── Non-Open tables (TP Hit, SL Hit, Queue Limit) — unchanged ordering ──
    # Optional columns (inserted via the flags):
    #   • "Margin Mode"  → right after Setup — only when show_pnl=True
    #                       (TP Hit and SL Hit tables set show_pnl=True;
    #                        Queue Limit table leaves it False, since queued
    #                        signals never actually placed a trade.)
    #   • "PnL $"        → right after Sector — only when show_pnl=True
    row = {
        "Time (GST)":     ts_str,
        "Alert":          alert_col,
    }
    row["Symbol"] = s.get("symbol", "")
    row["Setup"]  = setup_type
    if show_pnl:
        row["Margin Mode"] = _mm_val
    row["Sector"] = s.get("sector", "Other")
    if show_pnl:
        row["PnL $"] = pnl_col
    row.update({
        "Signal Entry":   _sig_entry_display,
        "Original Entry": _orig_entry_display,
        "Fill $":         s.get("entry", "") if s.get("signal_entry") else "—",
        "TP":             s.get("tp", ""),
        "TP $":           tp_usd_str,
        "SL":             s.get("sl", ""),
        "SL $":           sl_usd_str,
        "Status":         status_icon,
        "Duration":       duration_str,
        "Close Time":     close_str,
        "Close $":        s.get("close_price") or "—",
        "Max Lev":        f"{max_lev}×",
        "Order":          ord_status_str,
        "MEXC Command":    mexc_cmd_str,
        "Order ID":       ord_id_str,
        "Algo ID":        algo_id_str,
        "Entry Criteria": crit_str,
        "⚠️ SL Reason":  sl_reason,
    })
    # For TP Hit / SL Hit / DCA SL Hit, surface the cumulative Order Size so
    # DCA-exhausted closures show the full committed notional.
    if show_pnl:
        row["Order Size"] = order_size_col
    return row

# Shared column_config used by all four tables
_SIG_COL_CFG = {
    "Alert":          st.column_config.TextColumn(
                          "🚨 Alert", width="small",
                          help="🔴 DL = price ≥3% below entry  |  🔴 TL = open ≥2 hours"),
    "Current Status": st.column_config.TextColumn(
                          "📈 Current Status", width="small",
                          help="Signed % change of current price vs. entry for "
                               "every open trade — negative = drop, positive = "
                               "rise. Computed on each scan cycle from the "
                               "latest close price; rows show \"—\" only when "
                               "live price data hasn't landed yet (typically "
                               "the very first cycle after a signal fires)."),
    "Est Liquidity":  st.column_config.TextColumn(
                          "💥 Est Liquidity", width="medium",
                          help="Estimated liquidation price for isolated trades, "
                               "with % distance from entry in parentheses.\n\n"
                               "Formula (LONG isolated, approximate): "
                               "liq ≈ entry × (1 − 1/leverage).\n"
                               "Distance from entry = -100 / leverage "
                               "(e.g. 10× → -10%, 20× → -5%).\n\n"
                               "Cross-margin trades show \"—\" because liquidation "
                               "depends on account-wide equity, not per-trade info."),
    "Order Size":     st.column_config.TextColumn(
                          "💵 Order Size", width="small",
                          help="Total dollar notional for this trade "
                               "(collateral × leverage).\n\n"
                               "Source priority:\n"
                               "  1. `order_notional` from the actual MEXC "
                               "placement (authoritative).\n"
                               "  2. Per-signal `trade_usdt × trade_lev` "
                               "captured at placement time.\n"
                               "  3. Current sidebar settings (for signals "
                               "that never opened a real trade — shows what "
                               "WOULD have been sent).\n\n"
                               "\"—\" means neither a trade amount nor a "
                               "leverage is available to compute from."),
    "Setup":          st.column_config.TextColumn(width="small"),
    "Margin Mode":    st.column_config.TextColumn(
                          "Margin Mode", width="small",
                          help="Margin mode for this signal's trade.\n\n"
                               "  • 🔒 Isolated — per-position liquidation; "
                               "only this trade's collateral is at risk.\n"
                               "  • 🔓 Cross    — liquidation depends on "
                               "account-wide equity.\n"
                               "  • ⚠️ prefix — no `order_margin_mode` was "
                               "recorded on this signal (e.g. auto-trading "
                               "was OFF when it fired, or credentials missing, "
                               "or the order short-circuited before any MEXC "
                               "call). The value shown is the CURRENT sidebar "
                               "setting — NOT a confirmation of what was sent "
                               "to MEXC (nothing was).\n\n"
                               "Rows without the ⚠️ reflect what MEXC actually "
                               "used at trade time; changing the sidebar "
                               "default later does not relabel them."),
    "PnL $":          st.column_config.TextColumn(
                          "💰 PnL $", width="small",
                          help="Dollar PnL based on the collateral and leverage "
                               "stored on the signal at entry time.\n\n"
                               "Formula: (ref_price / entry − 1) × (trade_usdt × leverage).\n\n"
                               "Reference price by status:\n"
                               "  • Open    → live latest price (unrealized PnL)\n"
                               "  • TP Hit  → TP level (realized gain)\n"
                               "  • SL Hit  → SL level (realized loss)\n"
                               "  • Queue   → — (no trade opened)\n\n"
                               "Changing global settings later does not affect "
                               "the PnL of already-opened trades."),
    "Signal Entry":   st.column_config.NumberColumn(format="%.8f",
                          help="Latest working entry reference.\n\n"
                               "  • Non-DCA trades → actual market fill price\n"
                               "  • DCA trades     → blended average across "
                               "all fills (updated on each DCA add).\n\n"
                               "The ORIGINAL first-fill price is preserved in "
                               "the Original Entry column next to this one so "
                               "no information is lost when a ladder runs."),
    "Original Entry": st.column_config.NumberColumn(
                          "🎯 Original Entry", format="%.8f",
                          help="The very first fill price for this trade, "
                               "frozen at entry time. Useful for seeing how "
                               "far a DCA ladder has averaged the position "
                               "down from the initial entry.\n\n"
                               "For non-DCA trades this matches Signal Entry."),
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
    "MEXC Command":    st.column_config.TextColumn(
                          "MEXC Command",
                          width="large",
                          help="Exact parameters sent to MEXC when the order was placed. "
                               "Collateral = your USDT setting. "
                               "Notional = collateral × leverage (this is what MEXC shows as position size)."),
    "Order ID":       st.column_config.TextColumn(width="medium"),
    "Algo ID":        st.column_config.TextColumn(width="medium"),
    "Entry Criteria": st.column_config.TextColumn(width="medium"),
    "⚠️ SL Reason":  st.column_config.TextColumn(width="medium"),
}

def _render_sig_table(sig_list: list, header: str, empty_msg: str,
                      auto_height: bool = False, is_open_table: bool = False,
                      show_pnl: bool = False):
    rows = [_build_signal_row(s, is_open_table=is_open_table, show_pnl=show_pnl)
            for s in sig_list]
    st.markdown(f"### {header} ({len(rows)})")
    if rows:
        if auto_height:
            # Expand so ALL rows are visible without internal scrolling.
            # Each row ≈ 35 px, header ≈ 38 px, +10 px buffer.
            st.dataframe(rows, use_container_width=True, hide_index=True,
                         height=len(rows) * 35 + 48,
                         column_config=_SIG_COL_CFG)
        else:
            st.dataframe(rows, use_container_width=True, hide_index=True,
                         column_config=_SIG_COL_CFG)
    else:
        st.info(empty_msg)

# ── Filter by sector then split into four status buckets ───────────────────────
filtered = signals if selected_sector == "All" else \
           [s for s in signals if s.get("sector") == selected_sector]
filtered_sorted = sorted(filtered, key=lambda x: x.get("timestamp", ""), reverse=True)

_open_sigs    = [s for s in filtered_sorted if s.get("status") == "open"]
_tp_sigs      = [s for s in filtered_sorted if s.get("status") == "tp_hit"]
_sl_sigs      = [s for s in filtered_sorted if s.get("status") == "sl_hit"]
_dca_sl_sigs  = [s for s in filtered_sorted if s.get("status") == "dca_sl_hit"]
_queue_sigs   = [s for s in filtered_sorted if s.get("status") == "queue_limit"]

# ── Table 1: Open Signals ───────────────────────────────────────────────────────
_render_sig_table(_open_sigs,  "🔵 Open Signals",  "No open signals right now.",
                  auto_height=True, is_open_table=True, show_pnl=True)
st.divider()

# ── Table 2: TP Hit ─────────────────────────────────────────────────────────────
# show_pnl=True → realized gain column, using close_price (= TP level).
# For DCA trades, the row naturally picks up DCA-N in Alert, blended avg in
# Signal Entry, original entry in Original Entry, and cumulative Order Size.
_render_sig_table(_tp_sigs,    "✅ TP Hit",         "No TP hits yet.",
                  show_pnl=True)
st.divider()

# ── Table 3: SL Hit (non-DCA trades only) ──────────────────────────────────────
# Trades that exhausted a DCA ladder and hit the −3% final SL are routed to
# the dedicated "DCA SL Hit" table below, not this one.
_render_sig_table(_sl_sigs,    "❌ SL Hit",         "No SL hits yet.",
                  show_pnl=True)
st.divider()

# ── Table 4: DCA SL Hit (ladder-exhausted closures) ────────────────────────────
# Dedicated table for trades that consumed every allowed DCA add and then hit
# the final −3%-below-blended-average SL. Keeping these separate from regular
# SL hits makes it easy to audit DCA-strategy performance in isolation.
_render_sig_table(_dca_sl_sigs, "❌ DCA SL Hit (ladder exhausted)",
                  "No DCA ladder-exhausted SL hits yet.",
                  show_pnl=True)
st.divider()

# ── Table 5: Queue Limit ────────────────────────────────────────────────────────
# No PnL shown — queue_limit signals never opened a real trade.
_render_sig_table(_queue_sigs, "⏳ Queue Limit",    "No queued signals.")

if _queue_sigs:
    if st.button("🗑️ Clear Queue Limit Records", key="clear_queue_limit"):
        with _log_lock:
            _b._msc_log["signals"] = [
                s for s in _b._msc_log["signals"]
                if s.get("status") != "queue_limit"
            ]
            save_log(_b._msc_log)
        st.success("✅ Queue Limit records cleared.")
        st.rerun()

# ─────────────────────────────────────────────────────────────────────────────
# MEXC Live Positions Panel
# ─────────────────────────────────────────────────────────────────────────────
_has_api_creds = bool(
    _snap_cfg.get("api_key") and _snap_cfg.get("api_secret")
    and _snap_cfg.get("api_passphrase")
)

st.divider()
with st.expander("📡 MEXC Live Positions", expanded=False):
    if not _has_api_creds:
        st.warning("Enter API credentials in the sidebar to use this panel.")
    else:
        _env_label = "🟡 Demo" if _snap_cfg.get("demo_mode", True) else "🔴 Live"
        _col_btn, _col_info = st.columns([1, 4])
        with _col_btn:
            _do_refresh = st.button("🔄 Refresh from MEXC", key="refresh_okx_live")
        with _col_info:
            _last_ts = st.session_state.get("okx_pos_ts", "")
            if _last_ts:
                st.caption(f"Last fetched: {_last_ts}  ·  {_env_label}")
            else:
                st.caption(f"Press Refresh to load live data directly from MEXC  ·  {_env_label}")

        if _do_refresh:
            try:
                _pos_resp  = _trade_get("/api/v1/private/position/open_positions",
                                        {"instType": "SWAP"}, _snap_cfg)
                _algo_resp = _trade_get("/api/v1/private/order/list_orders_pending",
                                        {"instType": "SWAP", "ordType": "oco"},
                                        _snap_cfg)
                st.session_state["okx_pos_data"]  = _pos_resp
                st.session_state["okx_algo_data"] = _algo_resp
                st.session_state["okx_pos_ts"]    = \
                    dubai_now().strftime("%d %b %Y  %H:%M:%S GST")
                st.rerun()
            except Exception as _refresh_exc:
                st.error(f"❌ MEXC API error: {_refresh_exc}")

        _pos_data  = st.session_state.get("okx_pos_data")
        _algo_data = st.session_state.get("okx_algo_data")

        if _pos_data is None:
            st.info("No data yet — press **🔄 Refresh from MEXC** above.")
        else:
            # ── Positions table ───────────────────────────────────────────
            st.markdown("#### 📊 Open Positions")
            if not _pos_data.get("success", False):
                st.error(f"MEXC error: {_pos_data.get('msg', 'Unknown error')}")
            else:
                _positions = _pos_data.get("data", [])
                if _positions:
                    _pos_rows = []
                    for _p in _positions:
                        _upnl     = float(_p.get("upl",      0) or 0)
                        _upnl_pct = float(_p.get("uplRatio", 0) or 0) * 100
                        _liq_raw  = float(_p.get("liqPx",    0) or 0)
                        # MEXC: 'margin' is only populated in Isolated mode.
                        # In Cross mode use 'imr' (Initial Margin Requirement).
                        _margin_isolated = float(_p.get("margin", 0) or 0)
                        _margin_imr      = float(_p.get("imr",    0) or 0)
                        _margin_display  = _margin_isolated if _margin_isolated > 0 else _margin_imr
                        _mgn_mode        = _p.get("mgnMode", "cross")
                        _margin_label    = f"{'IMR' if _mgn_mode == 'cross' else 'Margin'} $"
                        _pos_rows.append({
                            "Symbol":        _p.get("instId", ""),
                            "Side":          _p.get("posSide", "net").capitalize(),
                            "Mode":          _mgn_mode.capitalize(),
                            "Contracts":     int(_p.get("pos", 0) or 0),
                            "Avg Entry":     float(_p.get("avgPx",       0) or 0),
                            "Mark Price":    float(_p.get("markPx",      0) or 0),
                            "Unreal PnL":    round(_upnl, 4),
                            "PnL %":         f"{_upnl_pct:+.2f}%",
                            "Notional $":    round(float(_p.get("notionalUsd", 0) or 0), 2),
                            "IMR / Margin $": round(_margin_display, 4),
                            "Leverage":      f"{_p.get('lever', '')}×",
                            "Liq Price":     _liq_raw if _liq_raw > 0 else "—",
                        })
                    st.dataframe(
                        _pos_rows,
                        use_container_width=True,
                        hide_index=True,
                        height=len(_pos_rows) * 35 + 48,
                        column_config={
                            "Avg Entry":       st.column_config.NumberColumn(format="%.6f"),
                            "Mark Price":      st.column_config.NumberColumn(format="%.6f"),
                            "Liq Price":       st.column_config.NumberColumn(format="%.6f"),
                            "Unreal PnL":      st.column_config.NumberColumn(
                                                   "Unreal PnL $", format="%.4f"),
                            "IMR / Margin $":  st.column_config.NumberColumn(
                                                   "IMR / Margin $", format="%.4f",
                                                   help="Cross margin → IMR (Initial Margin Requirement). "
                                                        "Isolated margin → Margin held per position."),
                        },
                    )
                else:
                    st.info("No open SWAP positions on MEXC right now.")

            # ── Active OCO Algo Orders table ──────────────────────────────
            st.markdown("#### 🎯 Active TP/SL Orders (OCO)")
            _algo_code = (_algo_data or {}).get("code", "")
            if _algo_code and not _algo_data.get("success", False):
                st.error(f"MEXC algo error: {(_algo_data or {}).get('msg', '')}")
            else:
                _algos = (_algo_data or {}).get("data", [])
                if _algos:
                    _algo_rows = []
                    for _a in _algos:
                        # MEXC returns cTime as Unix ms string
                        _ct_ms = int(_a.get("cTime", 0) or 0)
                        try:
                            from datetime import timezone
                            _ct_str = datetime.fromtimestamp(
                                _ct_ms / 1000, tz=timezone.utc
                            ).astimezone(
                                __import__("zoneinfo").ZoneInfo("Asia/Dubai")
                            ).strftime("%d %b %H:%M:%S")
                        except Exception:
                            _ct_str = str(_ct_ms)
                        _algo_rows.append({
                            "Symbol":     _a.get("instId", ""),
                            "Algo ID":    _a.get("algoId", ""),
                            "Contracts":  int(_a.get("sz", 0) or 0),
                            "TP Trigger": float(_a.get("tpTriggerPx", 0) or 0) or "—",
                            "SL Trigger": float(_a.get("slTriggerPx", 0) or 0) or "—",
                            "State":      _a.get("state", ""),
                            "Created":    _ct_str,
                        })
                    st.dataframe(
                        _algo_rows,
                        use_container_width=True,
                        hide_index=True,
                        height=len(_algo_rows) * 35 + 48,
                        column_config={
                            "TP Trigger": st.column_config.NumberColumn(format="%.6f"),
                            "SL Trigger": st.column_config.NumberColumn(format="%.6f"),
                        },
                    )
                else:
                    st.info("No active OCO algo orders on MEXC right now.")

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
                   "the full MEXC request and response are shown immediately below.")

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
            "Margin mode", ["isolated", "cross"],
            index=0 if _snap_cfg.get("trade_margin_mode","isolated") == "isolated" else 1,
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
            _mt_sl_hint = (mt_sl if mt_sl > 0
                           else mt_entry * (1 - 1/max(1,mt_lev)) if mt_mode == "isolated"
                           else mt_entry * (1 - _snap_cfg.get("sl_pct",3.0)/100))
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
            _mt_sl_use = (mt_sl if mt_sl > 0
                          else (_ref * (1 - 1/max(1,mt_lev)) if mt_mode == "isolated"
                                else _ref * (1 - _snap_cfg.get("sl_pct",3.0)/100))
                          if _ref else 0)

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

            # Show full raw MEXC request/response for debugging
            _mt_raw = getattr(_b, "_msc_last_trade_raw", {})
            if _mt_raw:
                st.markdown(f"**Entry order — {_mt_raw.get('endpoint','')}**")
                st.json(_mt_raw.get("body_sent", {}))
                st.markdown("**MEXC entry response:**")
                st.json(_mt_raw.get("response", {}))
                if _mt_raw.get("algo_body_sent"):
                    st.markdown("**OCO algo order sent:**")
                    st.json(_mt_raw["algo_body_sent"])
                    st.markdown("**MEXC OCO response:**")
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
        # F7 MACD — per timeframe running totals
        after_f7_macd_3m  = after_f6  - fc.get("f7_macd_3m",  0)
        after_f7_macd_5m  = after_f7_macd_3m  - fc.get("f7_macd_5m",  0)
        after_f7_macd_15m = after_f7_macd_5m  - fc.get("f7_macd_15m", 0)
        # F8 SAR — per timeframe running totals
        after_f8_sar_3m   = after_f7_macd_15m - fc.get("f8_sar_3m",  0)
        after_f8_sar_5m   = after_f8_sar_3m   - fc.get("f8_sar_5m",  0)
        after_f8_sar_15m  = after_f8_sar_5m   - fc.get("f8_sar_15m", 0)
        after_f9           = after_f8_sar_15m  - fc.get("f9_vol",       0)
        after_f10          = after_f9          - fc.get("f10_ema_cross", 0)
        after_empty        = after_f10         - fc.get("f_empty_data", 0)

        # Use the config that was ACTIVE during the last scan for labels
        sc = fc.get("scan_cfg") or _snap_cfg

        if sc is not _snap_cfg and sc != _snap_cfg:
            st.caption("⚠️ Config changed since last scan — funnel reflects the previous settings. "
                       "Rescan is in progress with the new config.")

        pre_lbl    = "⚡ After Bulk Pre-filter" if sc.get("use_pre_filter", True) else "⚡ Pre-filter (disabled)"
        pdz15m_lbl = "F2 — PDZ Zones (15m)" if sc.get("use_pdz_15m", True) else "F2 — PDZ 15m (off)"
        pdz5m_lbl  = "F3 — PDZ Zones (5m)"  if sc.get("use_pdz_5m",  True) else "F3 — PDZ 5m (off)"
        f4_lbl     = f"F4 — 5m RSI \u2265{sc.get('rsi_5m_min',30)}" if sc.get("use_rsi_5m", True) else "F4 — 5m RSI (off)"
        f5_lbl     = (f"F5 — 1h RSI {sc.get('rsi_1h_min',30)}\u2013{sc.get('rsi_1h_max',95)}"
                      if sc.get("use_rsi_1h", True) else "F5 — 1h RSI (off)")
        ema_parts = []
        if sc.get("use_ema_3m"):  ema_parts.append(f"3m EMA{sc.get('ema_period_3m',12)}")
        if sc.get("use_ema_5m"):  ema_parts.append(f"5m EMA{sc.get('ema_period_5m',12)}")
        if sc.get("use_ema_15m"): ema_parts.append(f"15m EMA{sc.get('ema_period_15m',12)}")
        ema_lbl = ("F6 — EMA (" + (" · ".join(ema_parts)) + ")") if ema_parts else "F6 — EMA (off)"
        vol_lbl = (f"F9 — Vol \u2265{sc.get('vol_spike_mult',2.0)}\xd7 / {sc.get('vol_spike_lookback',20)} 15m"
                   if sc.get("use_vol_spike") else "F9 — Vol (off)")
        ema_cross_lbl = (f"F10 — EMA{sc.get('ema_cross_fast_15m',12)}>EMA{sc.get('ema_cross_slow_15m',21)} 15m"
                          if sc.get("use_ema_cross_15m", True) else "F10 — EMA Cross (off)")

        # Build funnel rows — only include MACD/SAR timeframes that are enabled
        funnel_data = [
            (f"Watchlist ({total})",       total),
            (pre_lbl,                      after_pre),
            ("Entered Deep Scan",          checked),
            (f"After {pdz15m_lbl}",        after_f2),
            (f"After {pdz5m_lbl}",         after_f3),
            (f"After {f4_lbl}",            after_f4),
            (f"After {f5_lbl}",            after_f5),
            (f"After {ema_lbl}",           after_f6),
        ]
        # F7 MACD — add a row per enabled timeframe
        _prev_macd = after_f6
        for _tf, _key, _after in [
            ("3m",  "use_macd_3m",  after_f7_macd_3m),
            ("5m",  "use_macd_5m",  after_f7_macd_5m),
            ("15m", "use_macd_15m", after_f7_macd_15m),
        ]:
            if sc.get(_key, True):
                funnel_data.append((f"F7 — MACD \U0001f7e2\u2191 {_tf}", _after))
                _prev_macd = _after
            # If disabled keep running value the same (no row added, count unchanged)
        # F8 SAR — add a row per enabled timeframe
        _prev_sar = _prev_macd
        for _tf, _key, _after in [
            ("3m",  "use_sar_3m",  after_f8_sar_3m),
            ("5m",  "use_sar_5m",  after_f8_sar_5m),
            ("15m", "use_sar_15m", after_f8_sar_15m),
        ]:
            if sc.get(_key, True):
                funnel_data.append((f"F8 — SAR {_tf}", _after))
                _prev_sar = _after

        funnel_data.append((f"After {vol_lbl}",           after_f9))
        funnel_data.append((f"After {ema_cross_lbl}",     after_f10))
        funnel_data.append(("After \u26a0\ufe0f Empty Candle Drop", after_empty))

        # Colour palette — generate enough colours for variable row count
        _palette = ["#1f6feb","#388bfd","#58a6ff","#79c0ff","#a5d6ff",
                    "#3fb950","#56d364","#7ee787","#40c8b8","#26a69a",
                    "#d29922","#e3b341","#f0a500","#f85149","#ff6b6b"]
        _colours = (_palette * ((len(funnel_data) // len(_palette)) + 1))[:len(funnel_data)]

        fig_funnel = go.Figure(go.Funnel(
            y=[d[0] for d in funnel_data],
            x=[d[1] for d in funnel_data],
            marker=dict(color=_colours),
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

        def _coin_str(s: set) -> str:
            return ", ".join(sorted(s)) if s else "—"

        # Build remaining sets by subtracting eliminated coins stage-by-stage
        _pre  = set(fc.get("pre_filter_passed_syms", []))
        _chk  = set(fc.get("checked_syms", []))
        _f2e  = set(fc.get("f2_elim_syms",  []))
        _f3e  = set(fc.get("f3_elim_syms",  []))
        _f4e  = set(fc.get("f4_elim_syms",  []))
        _f5e  = set(fc.get("f5_elim_syms",  []))
        _f6e  = set(fc.get("f6_elim_syms",  []))
        # F7 MACD — per timeframe elimination sets
        _f7e_3m  = set(fc.get("f7_macd_3m_elim_syms",  []))
        _f7e_5m  = set(fc.get("f7_macd_5m_elim_syms",  []))
        _f7e_15m = set(fc.get("f7_macd_15m_elim_syms", []))
        # F8 SAR — per timeframe elimination sets
        _f8e_3m  = set(fc.get("f8_sar_3m_elim_syms",  []))
        _f8e_5m  = set(fc.get("f8_sar_5m_elim_syms",  []))
        _f8e_15m = set(fc.get("f8_sar_15m_elim_syms", []))
        _f9e    = set(fc.get("f9_elim_syms",      []))
        _f10e   = set(fc.get("f10_elim_syms",     []))
        _fempty = set(fc.get("f_empty_data_syms", []))

        _after_f2        = _chk          - _f2e
        _after_f3        = _after_f2     - _f3e
        _after_f4        = _after_f3     - _f4e
        _after_f5        = _after_f4     - _f5e
        _after_f6        = _after_f5     - _f6e
        # MACD per timeframe — only subtract if that timeframe is enabled
        _after_f7_macd_3m  = _after_f6          - (_f7e_3m  if sc.get("use_macd_3m",  True) else set())
        _after_f7_macd_5m  = _after_f7_macd_3m  - (_f7e_5m  if sc.get("use_macd_5m",  True) else set())
        _after_f7_macd_15m = _after_f7_macd_5m  - (_f7e_15m if sc.get("use_macd_15m", True) else set())
        # SAR per timeframe
        _after_f8_sar_3m   = _after_f7_macd_15m - (_f8e_3m  if sc.get("use_sar_3m",  True) else set())
        _after_f8_sar_5m   = _after_f8_sar_3m   - (_f8e_5m  if sc.get("use_sar_5m",  True) else set())
        _after_f8_sar_15m  = _after_f8_sar_5m   - (_f8e_15m if sc.get("use_sar_15m", True) else set())
        _after_f9          = _after_f8_sar_15m  - _f9e
        _after_f10         = _after_f9          - (_f10e if sc.get("use_ema_cross_15m", True) else set())
        _fempty_syms = {s.split("(")[0] for s in _fempty}
        _after_empty = _after_f10 - _fempty_syms

        _process_err_count = max(0, fc.get("errors", 0))
        _new_sig_s    = set(fc.get("new_signal_syms",         []))
        _blk_active_s = set(fc.get("blocked_by_active_syms", []))
        _blk_cool_s   = set(fc.get("blocked_by_cooldown_syms",[]))
        _blk_sl_cool_s = set(fc.get("blocked_by_sl_cooldown_syms", []))
        _super_demoted_s = set(fc.get("super_cap_demoted_syms", []))
        _returned_syms = _new_sig_s | _blk_active_s | _blk_cool_s | _blk_sl_cool_s

        # Fixed opening rows
        stage_rows = [
            ("⚡ After Bulk Pre-filter",  len(_pre),      _coin_str(_pre)),
            ("🔬 Entered Deep Scan",      len(_chk),      _coin_str(_chk)),
            (f"After {pdz15m_lbl}",       len(_after_f2), _coin_str(_after_f2)),
            (f"After {pdz5m_lbl}",        len(_after_f3), _coin_str(_after_f3)),
            (f"After {f4_lbl}",           len(_after_f4), _coin_str(_after_f4)),
            (f"After {f5_lbl}",           len(_after_f5), _coin_str(_after_f5)),
            (f"After {ema_lbl}",          len(_after_f6), _coin_str(_after_f6)),
        ]
        # F7 MACD — one row per enabled timeframe
        for _tf, _key, _after_set in [
            ("3m",  "use_macd_3m",  _after_f7_macd_3m),
            ("5m",  "use_macd_5m",  _after_f7_macd_5m),
            ("15m", "use_macd_15m", _after_f7_macd_15m),
        ]:
            if sc.get(_key, True):
                stage_rows.append((
                    f"F7 — MACD \U0001f7e2\u2191 {_tf}",
                    len(_after_set),
                    _coin_str(_after_set),
                ))
        # F8 SAR — one row per enabled timeframe
        for _tf, _key, _after_set in [
            ("3m",  "use_sar_3m",  _after_f8_sar_3m),
            ("5m",  "use_sar_5m",  _after_f8_sar_5m),
            ("15m", "use_sar_15m", _after_f8_sar_15m),
        ]:
            if sc.get(_key, True):
                stage_rows.append((
                    f"F8 — SAR {_tf}",
                    len(_after_set),
                    _coin_str(_after_set),
                ))
        # Fixed closing rows
        stage_rows += [
            (f"After {vol_lbl}",               len(_after_f9),        _coin_str(_after_f9)),
            (f"After {ema_cross_lbl}",         len(_after_f10),       _coin_str(_after_f10)),
            ("⚠️ Dropped — Empty Candle Data", len(_fempty),          ", ".join(sorted(_fempty)) if _fempty else "—"),
            ("💥 Dropped — Process Error",     _process_err_count,    "See API Error Log below ↓"),
            ("✅ Returned Signal",             len(_returned_syms),   _coin_str(_returned_syms)),
            ("⭐ Super cap demoted → F3-F10",  len(_super_demoted_s), _coin_str(_super_demoted_s)),
            ("🔵 Blocked — Open trade exists", len(_blk_active_s),    _coin_str(_blk_active_s)),
            ("🟡 Blocked — TP Cooldown",       len(_blk_cool_s),      _coin_str(_blk_cool_s)),
            ("🔴 Blocked — SL Cooldown (24h)", len(_blk_sl_cool_s),   _coin_str(_blk_sl_cool_s)),
            ("🟢 New Signals Fired",           len(_new_sig_s),       _coin_str(_new_sig_s)),
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
# API Error Log  (bottom of page)
# ─────────────────────────────────────────────────────────────────────────────
st.divider()

with getattr(_b, "_msc_error_log_lock", threading.Lock()):
    _err_snap = list(getattr(_b, "_msc_error_log", []))

_TYPE_ICON  = {"scan": "🔴", "trade": "🟠", "loop": "🟣", "http": "🔵",
               "signal_update": "🟤", "io": "💾"}
_TYPE_LABEL = {"scan": "Scan/Candle", "trade": "Trade/Order",
               "loop": "Scanner loop", "http": "HTTP/Network",
               "signal_update": "Signal Update", "io": "File I/O"}

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
            with _b._msc_error_log_lock:
                _b._msc_error_log.clear()
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
        _shown    = [e for e in reversed(_err_snap)
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

# ─────────────────────────────────────────────────────────────────────────────
# Auto-refresh
# ─────────────────────────────────────────────────────────────────────────────
time.sleep(30)
st.rerun()
