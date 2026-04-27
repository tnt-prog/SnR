#!/usr/bin/env python3
"""
OKX Futures Scanner — Streamlit Dashboard  v2
Performance improvements:
  A. Bulk ticker pre-filter  — 1 API call eliminates ~70% of coins before any candle fetch
  B. Parallel timeframe fetch — 4 timeframes fetched simultaneously per coin
  C. Staged candle fetch      — quick 50-candle RSI/resistance check before full fetch
  D. Symbol cache             — OKX instrument list cached for 6 h, not re-fetched every cycle
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
BASE          = "https://www.okx.com"
OKX_INTERVALS = {"30m": "30m", "3m": "3m", "5m": "5m", "15m": "15m", "1h": "1H"}

# ─────────────────────────────────────────────────────────────────────────────
# API rate-limiter  — hard cap: 8 concurrent HTTP requests at any time
# OKX public-market limit is 20 req / 2 s.  Staying well below keeps us safe.
# ─────────────────────────────────────────────────────────────────────────────
_api_sem = threading.Semaphore(20)  # max concurrent in-flight requests
# Rate limiter (_rate_wait) already caps at 15 req/s; semaphore just needs to
# allow enough concurrent in-flight so the pipeline stays full at any latency.
# Formula: slots = target_rps × round_trip_latency → 15 × 1.3s ≈ 20 slots.

# ── Token-bucket rate limiter ─────────────────────────────────────────────────
# OKX public candle endpoint: 40 req / 2 s = 20 req/s hard limit.
# We target 15 req/s (75 % of limit) to stay safe regardless of network latency.
# Semaphore alone is not enough — if OKX responds in <67 ms we still exceed 20/s.
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
    "loop_minutes":         4,
    "cooldown_minutes":     2,
    "use_ema_3m":           False,
    "ema_period_3m":      12,
    "use_ema_5m":         False,
    "ema_period_5m":      12,
    "use_ema_15m":        True,
    "ema_period_15m":     12,
    "use_macd_3m":        False,  # F7 — MACD dark-green (3m)
    "use_macd_5m":        False,  # F7 — MACD dark-green (5m)
    "use_macd_15m":       True,   # F7 — MACD dark-green (15m)
    "use_sar_3m":         False,  # F8 — Parabolic SAR (3m)
    "use_sar_5m":         True,   # F8 — Parabolic SAR (5m)
    "use_sar_15m":        True,   # F8 — Parabolic SAR (15m)
    "use_vol_spike":      False,
    "vol_spike_mult":     2.0,
    "vol_spike_lookback": 20,
    "use_pdz_5m":            False,  # F3 — PDZ (5m)
    "use_pdz_15m":           True,   # F2 — PDZ (15m)
    "use_atr_filter":        False,  # F5b — ATR(14) 15m TP-reachability filter
    "atr_mode":              "Normal",  # "Strict" (≤1.5×) | "Normal" (≤2.0×) | "Relaxed" (≤3.0×)
    "use_ema_cross_15m":     True,   # F10 — 15m EMA crossover (fast > slow)
    "ema_cross_fast_15m":    12,
    "ema_cross_slow_15m":    21,
    # ── Queue limit (max concurrent open trades) ──────────────────────────────
    "max_open_trades":       7,      # Hard cap on concurrent open trades.
                                     # Lowering this does NOT close existing
                                     # trades — new signals during overflow are
                                     # logged as queue_limit until natural TP/SL
                                     # closures bring the count down.
    # ── Super Setup cap (max concurrent Super trades) ─────────────────────────
    "max_super_trades":      2,      # Hard cap on concurrent open Super trades.
                                     # When cap is reached, Super-eligible coins
                                     # fall through to the normal F3–F10 pipeline
                                     # and open as regular trades if they pass.
    # ── SL cooldown (per-coin blackout after SL hit) ──────────────────────────
    "sl_cooldown_hours":     4,      # Hours to skip a coin after an SL hit.
                                     # Applies UNIVERSALLY — even to Super Setups.
                                     # Separate from TP cooldown (cooldown_minutes).
    # ── Auto-trading (OKX Demo / Live) ────────────────────────────────────────
    "trade_enabled":         False,
    "demo_mode":             True,   # True = Demo API, False = Live API
    "api_key":               "",
    "api_secret":            "",
    "api_passphrase":        "",
    "trade_usdt_amount":     5.0,    # USDT collateral per trade (before leverage)
    "trade_leverage":        20,     # leverage applied (capped by MAX_LEVERAGE)
    "trade_margin_mode":     "isolated",  # "cross" or "isolated"
    # ── DCA (Dollar-Cost Averaging) ──────────────────────────────────────────
    # 0 = DCA OFF (legacy behavior — sidebar TP/SL apply normally).
    # 1-6 = Allow up to N automated DCA adds per trade.
    # Each DCA doubles the previous add's margin (10 → 10 → 20 → 40 → 80 …).
    # Isolated: DCA triggers when price reaches 70% of the distance from the
    #           current blended average to the current liquidation price.
    # Cross:    DCA triggers when price drops 7% below the current blended avg.
    # After N DCAs are consumed, the final SL uses the same formula as all
    # earlier DCAs: blended_avg × (1 − sl_distance_pct). A breach routes
    # the trade into a dedicated DCA SL Hit table. When DCA > 0, the sidebar
    # SL % is ignored — only TP and this SL can close the trade.
    "trade_max_dca":         3,
    # ── DCA trigger drop percentages ──────────────────────────────────────────
    # Two separate configurables, shown in the sidebar based on selected
    # margin mode.
    #   • Isolated: % DISTANCE ABOVE LIQUIDATION toward entry (dropdown
    #     10/15/…/95, step 5). Higher = conservative (fires near entry);
    #     lower = aggressive (fires near liq).
    #       drop_pct = (1/lev) × (1 − iso_dist/100)
    #       DCA price = entry × (1 − drop_pct)
    #       Example: entry $100, 10× lev, liq ≈ $90
    #         10% → $91  · 50% → $95  · 70% → $97  · 95% → $99.50
    #       default 70 → at 10× lev, fires at −3% from avg.
    #                    at 20× lev, fires at −1.5% from avg. (leverage-aware)
    #   • Cross:    % drop below blended avg (leverage-independent).
    #       default 7 → fires at −7% from avg, regardless of leverage.
    # Snapshotted onto the signal at entry time so sidebar tweaks don't
    # retroactively change already-open trades.
    "dca_iso_distance_pct":  80.0,   # 10–95 (step 5), isolated
    "dca_cross_drop_pct":    7.0,    # 0.1–50, cross
    # ── Fixed Dollar TP/SL after any DCA fires ────────────────────────────────
    # After any DCA fill the TP and SL switch to fixed dollar targets so the
    # profit goal and maximum loss are always expressed in USDT, not as a % of
    # an ever-changing blended average.
    # TP price = avg_entry + (dca_tp_usd  / total_coins_held)
    # SL price = avg_entry − (dca_sl_usd  / total_coins_held)
    # The SL replaces the percentage-based SL entirely for DCA trades.
    # Priority: fixed dollar SL beats the next DCA trigger — the bot will never
    # add more margin into a position that has already hit its loss limit.
    "dca_tp_usd":            0.50,   # Fixed $ profit target after any DCA
    "dca_sl_usd":            5.00,   # Fixed $ max loss   after any DCA (replaces % SL)
    # ── Open-Trade Watcher loop (1-minute DCA/TP checker) ─────────────────────
    # When > 0 AND < loop_minutes: a dedicated background thread runs every
    # `watcher_minutes` minutes, fetching 1m candles for each open trade and
    # executing DCA triggers + TP exits in real time. The main scan loop's
    # open-trade check is DISABLED while the watcher is active to avoid
    # double-execution. When 0 or ≥ loop_minutes, watcher is disabled and
    # main loop handles open-trade checks as before (legacy behaviour).
    "watcher_minutes":       1,
    # ── Hours of operation (GST / Dubai UTC+4) ────────────────────────────────
    # When enabled, new signal scanning is paused outside the defined window.
    # Open-trade monitoring (TP/SL/DCA) always runs regardless of this setting.
    # Supports midnight-crossing windows (e.g. start=22, end=06).
    "scan_hour_enabled":     False,
    "scan_hour_start":       0,    # 0–23 GST
    "scan_hour_end":         23,   # 0–23 GST
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
CONFIG_FILE  = _SCRIPT_DIR / "scanner_config.json"
LOG_FILE     = _SCRIPT_DIR / "scanner_log.json"
_config_lock = threading.Lock()

def load_config() -> dict:
    """Load persisted config, applying env-var overrides for API credentials.

    Environment variables (preferred — never written back to disk):
      • OKX_API_KEY
      • OKX_API_SECRET
      • OKX_API_PASSPHRASE
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
    for _cfg_k, _env_k in (("api_key",        "OKX_API_KEY"),
                           ("api_secret",     "OKX_API_SECRET"),
                           ("api_passphrase", "OKX_API_PASSPHRASE")):
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
    for _cfg_k, _env_k in (("api_key",        "OKX_API_KEY"),
                           ("api_secret",     "OKX_API_SECRET"),
                           ("api_passphrase", "OKX_API_PASSPHRASE")):
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
        # ── Open-Trade Watcher (1-minute DCA/TP checker) ──────────────────────
        # Separate thread that polls open trades on a shorter interval than the
        # main scan loop. When active, the main loop skips its own open-trade
        # update (avoids duplicate candle fetches + double DCA execution).
        _b._bsc_watcher_thread  = None
        _b._bsc_watcher_event   = threading.Event()   # wake watcher immediately
        _b._bsc_watcher_last_ts = 0                   # unix ts of last tick (for UI)
        _b._bsc_watcher_last_dur = 0.0                # seconds — last tick duration
        # ── Circuit-breaker halt flag ─────────────────────────────────────────
        # Set to True by the background loop when the last 3 closed trades are
        # all sl_hit. CONTRACT: once True, ONLY the manual "▶️ Resume Scanning"
        # button (in the sidebar) may set this back to False. No TP hit, no
        # cycle, no other event — manual resume only. Do not add any code path
        # that clears this flag automatically.
        _b._bsc_sl_paused     = False
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
# ── Defensive hot-reload backfill ────────────────────────────────────────────
# The main init block above is gated by a persistent builtins flag, so when
# the app was already running before new attributes were added to the module,
# those attributes won't exist on _b. Add any missing ones here so older
# sessions keep working after a code update (no process restart required).
if not hasattr(_b, "_bsc_watcher_thread"):
    _b._bsc_watcher_thread = None
if not hasattr(_b, "_bsc_watcher_event"):
    _b._bsc_watcher_event = threading.Event()
if not hasattr(_b, "_bsc_watcher_last_ts"):
    _b._bsc_watcher_last_ts = 0
if not hasattr(_b, "_bsc_watcher_last_dur"):
    _b._bsc_watcher_last_dur = 0.0

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
                r = get_session().get(url, params=params, timeout=10)  # was 20s
            if r.status_code == 429:
                # Cap sleep at 5 s — Retry-After can be up to 30 s which cascades
                # into 300-second scan times when multiple threads hit it at once.
                wait = min(int(r.headers.get("Retry-After", 5)), 5)
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
            if attempt < _retries - 1: time.sleep(1); continue
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
                _cv = float(s.get("ctVal") or 0)
                ct_vals[sym] = _cv if _cv > 0 else 0.0
                # Store 0 on parse failure so _get_ct_val treats it as a cache
                # miss and forces a re-fetch rather than silently using 1.0.
            except (TypeError, ValueError):
                ct_vals[sym] = 0.0
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
    try:
        return r.json()
    except (json.JSONDecodeError, ValueError) as _je:
        # OKX returned non-JSON (e.g. an HTML 503 page during an outage).
        # Surface a structured error instead of propagating a cryptic
        # JSONDecodeError up to the background loop.
        _snippet = r.text[:200].replace("\n", " ") if r.text else ""
        raise RuntimeError(
            f"OKX POST {path} returned non-JSON response "
            f"(status={r.status_code}): {_snippet!r}"
        ) from _je

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
    try:
        return r.json()
    except (json.JSONDecodeError, ValueError) as _je:
        _snippet = r.text[:200].replace("\n", " ") if r.text else ""
        raise RuntimeError(
            f"OKX GET {path} returned non-JSON response "
            f"(status={r.status_code}): {_snippet!r}"
        ) from _je

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

    IMPORTANT: Never falls back to 1.0.  If ctVal cannot be confirmed from
    OKX, raises ValueError so the caller can refuse the order.  Using 1.0 as
    a fallback for a token whose real ctVal is e.g. 10 would send 10× the
    intended position size.
    """
    ct = _b._bsc_symbol_cache.get("ct_val", {}).get(sym)
    if ct and ct > 0:
        return ct
    # Cache miss — fetch on-demand (rare, e.g. first run before any scan)
    try:
        data = safe_get(f"{BASE}/api/v5/public/instruments",
                        {"instType": "SWAP", "instId": _to_okx(sym)})
        for s in data.get("data", []):
            raw = s.get("ctVal", "")
            val = float(raw) if raw not in ("", None) else 0.0
            if val > 0:
                _b._bsc_symbol_cache.setdefault("ct_val", {})[sym] = val
                _append_error("info",
                    f"ctVal for {_to_okx(sym)} fetched on-demand: {val} "
                    f"(cache was cold — populated now)")
                return val
    except Exception as _e:
        raise ValueError(
            f"ctVal for {_to_okx(sym)} could not be fetched from OKX "
            f"({_e}) — order refused to prevent wrong position sizing"
        ) from _e
    raise ValueError(
        f"ctVal for {_to_okx(sym)} not found in OKX instruments response "
        f"— order refused to prevent wrong position sizing"
    )

def _set_leverage_okx(sym: str, cfg: dict) -> dict:
    """Set leverage for a symbol before placing an order.

    Returns the raw OKX API response dict.
    Raises RuntimeError if OKX rejects the leverage change so callers can
    decide whether to abort the order or treat it as a warning.
    """
    lev = str(min(int(cfg.get("trade_leverage", 10)), get_max_leverage(sym)))
    resp = _trade_post("/api/v5/account/set-leverage", {
        "instId":  _to_okx(sym),
        "lever":   lev,
        "mgnMode": cfg.get("trade_margin_mode", "isolated"),
    }, cfg)
    if resp.get("code") != "0":
        okx_msg = resp.get("msg", "") or str(resp)
        raise RuntimeError(
            f"OKX rejected set-leverage {lev}x for {_to_okx(sym)}: {okx_msg} "
            f"(code={resp.get('code','')})"
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

def _okx_log_entry(sig: dict, label: str, **fields) -> None:
    """Append a timestamped OKX command entry to sig['okx_log'].

    Each entry is a single line: "[MM/DD HH:MM:SS] LABEL | key: val | ..."
    Entries are stored in sig["okx_log"] (a list[str]) and joined with
    "\\n########\\n" when rendered in the OKX Command column.
    """
    ts    = dubai_now().strftime("%m/%d %H:%M:%S")
    parts = [f"[{ts}] {label}"]
    for k, v in fields.items():
        parts.append(f"{k}: {v}")
    entry = " | ".join(parts)
    if not isinstance(sig.get("okx_log"), list):
        sig["okx_log"] = []
    sig["okx_log"].append(entry)


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
        # ── Input validation — fail fast with a clear error, BEFORE touching
        # any OKX endpoint. A malformed signal (missing field, None, unparseable
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
        raw_contracts = notional / (ct_val * entry)
        contracts     = max(1, int(raw_contracts))
        # Guard against absurdly large values (e.g. micro-priced tokens with wrong ct_val)
        if contracts > 100_000:
            return {"ordId": "", "algoId": "", "sz": contracts, "status": "error",
                    "error": (f"Contract count too large ({contracts}) — "
                              f"ct_val={ct_val}, entry={entry}, notional={notional}. "
                              f"Check ctVal for {sym}."),
                    **_base_info}

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
        _base_info["is_hedge"] = is_hedge

        # ── Step 1: Set leverage — HARD FAIL if OKX rejects ─────────────────
        # If OKX refuses to apply the requested leverage we must NOT place the
        # order — otherwise it would open at whatever leverage OKX currently
        # has set for this symbol (could be far lower than intended).
        try:
            _set_leverage_okx(sym, cfg)
        except Exception as lev_exc:
            _err = f"set-leverage failed — order aborted: {lev_exc}"
            _append_error("trade", _err,
                          symbol=sym, endpoint="/api/v5/account/set-leverage")
            return {"ordId": "", "algoId": "", "sz": 0,
                    "status": "error", "error": _err, **_base_info}

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
                    "status": "error", "error": err, **_base_info}
        if d0.get("sCode", "0") != "0":
            err = f"Entry order: {d0.get('sCode')}: {d0.get('sMsg','')}"
            _append_error("trade",
                          f"{err} | body={json.dumps(order_body)}",
                          symbol=sym, endpoint="/api/v5/trade/order")
            return {"ordId": ord_id, "algoId": "", "sz": contracts,
                    "status": "error", "error": err, **_base_info}

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

        # Recalculate TP and SL from the actual fill price.
        # Isolated mode: SL = liquidation price (entry × (1 − 1/leverage)).
        # Cross mode:    SL = entry × (1 − sl_pct%).
        tp_pct    = float(cfg.get("tp_pct", 1.5)) / 100
        actual_tp = _pround(actual_entry * (1 + tp_pct))
        if mode == "isolated":
            actual_sl = _pround(actual_entry * (1 - 1 / lev))
        else:
            sl_pct    = float(cfg.get("sl_pct", 3.0)) / 100
            actual_sl = _pround(actual_entry * (1 - sl_pct))

        # ── Step 4: Place TP/SL algo using actual fill price ────────────────
        # Cross margin: TP-only conditional order (no SL on OKX — user accepts
        # OKX liquidation as the only backstop). DCA levels are triggered by
        # price detection in the watcher and placed as market orders, not as
        # pre-placed limit orders.
        # Isolated margin: unchanged OCO (TP + SL together).
        if mode == "cross":
            # ── Cross: TP-only algo at the entry TP price ─────────────────────
            _tp_sig = _place_tp_only_order(
                {"symbol": sym, "order_margin_mode": mode,
                 "order_is_hedge": is_hedge},
                cfg, actual_tp, contracts
            )
            if not _tp_sig:
                return {"ordId": ord_id, "algoId": "", "sz": contracts,
                        "status": "partial",
                        "error": "Entry ✅ · TP algo ❌ (cross mode, no SL)",
                        "actual_entry": actual_entry,
                        "actual_tp": actual_tp, "actual_sl": actual_sl,
                        **_base_info}

            return {"ordId": ord_id, "algoId": _tp_sig, "sz": contracts,
                    "status": "placed", "error": "",
                    "actual_entry": actual_entry,
                    "actual_tp": actual_tp, "actual_sl": actual_sl,
                    "tp_algo_id":    _tp_sig,
                    **_base_info}

        # ── Isolated: OCO (TP + SL) ───────────────────────────────────────────
        # In hedge mode, closing a long requires posSide="long" on the sell too.
        algo_body: dict = {
            "instId":          _to_okx(sym),
            "tdMode":          mode,
            "side":            "sell",
            "ordType":         "oco",
            "sz":              str(contracts),
            "tpTriggerPx":     str(actual_tp),
            "tpTriggerPxType": "mark",
            "tpOrdPx":         "-1",    # market fill when TP triggers
            "slTriggerPx":     str(actual_sl),
            "slTriggerPxType": "mark",
            "slOrdPx":         "-1",    # market fill when SL triggers
        }
        if is_hedge:
            algo_body["posSide"] = "long"    # closing a long in hedge mode
        else:
            # Prevent oversell flipping LONG → SHORT in cross net mode.
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
                    "actual_tp": actual_tp, "actual_sl": actual_sl,
                    **_base_info}

        return {"ordId": ord_id, "algoId": algo_id, "sz": contracts,
                "status": "placed", "error": "",
                "actual_entry": actual_entry,
                "actual_tp": actual_tp, "actual_sl": actual_sl,
                **_base_info}

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
        mode   = cfg.get("trade_margin_mode", "isolated")

        # Pre-trade instrument check
        ct_cache = _b._bsc_symbol_cache.get("ct_val", {})
        if ct_cache and sym not in ct_cache:
            err = f"Instrument {_to_okx(sym)} not found in OKX live SWAP list."
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

        # Set leverage — HARD FAIL if OKX rejects
        try:
            _set_leverage_okx(sym, cfg)
        except Exception as lev_exc:
            _err = f"set-leverage failed — order aborted: {lev_exc}"
            _append_error("trade", _err, symbol=sym,
                          endpoint="/api/v5/account/set-leverage")
            return {"ordId": "", "algoId": "", "sz": 0,
                    "status": "error", "error": _err}

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

def calc_atr(candles: list, period: int = 14) -> list:
    """
    Average True Range (ATR) using Wilder's smoothing.
    Returns a list of ATR values (same length as candles minus the warm-up).
    Each candle must have 'high', 'low', 'close' keys.
    """
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
    # Seed with simple average of first `period` TRs
    atr = [sum(trs[:period]) / period]
    for tr in trs[period:]:
        atr.append((atr[-1] * (period - 1) + tr) / period)
    return atr


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
def calc_pdz_zone(candles: list, price: float, buffer_pct: float = 0.015) -> tuple:
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
      • Band A (equil_top < price < prem_bot) → QUALIFIES if room ≥ buffer_pct (= tp_pct)
      • Band B (disc_top  < price < equil_bot)→ QUALIFIES if room ≥ buffer_pct (= tp_pct)

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

    lookback = candles[-290:]  # 290 candles — 1 OKX API call (~72 hrs on 15m, ~24 hrs on 5m)
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

    band_a_ceil = premium_bottom * (1 - buffer_pct)   # buffer below Premium boundary
    band_b_ceil = equil_bottom   * (1 - buffer_pct)   # buffer below Equilibrium boundary

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
        "f5b_atr":          0,   # F5b — ATR(14) 15m TP-reachability
        "f6_ema_3m":        0,   # F6 — EMA 3m
        "f6_ema_5m":        0,   # F6 — EMA 5m
        "f6_ema_15m":       0,   # F6 — EMA 15m
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
        "f5b_elim_syms":          [],
        "f6_ema_3m_elim_syms":    [],
        "f6_ema_5m_elim_syms":    [],
        "f6_ema_15m_elim_syms":   [],
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
        # ── Flush/scan timing (used to keep funnel hidden until a full post-flush scan) ─
        "flushed_at":        0.0,
        "scan_completed_at": 0.0,
    }
    with _filter_lock:
        _filter_counts.clear()
        _filter_counts.update(counts)
        _b._bsc_filter_counts = _filter_counts   # keep reference in sync under lock

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
        # Candle counts (all single API call — OKX cap is 300/call):
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

        closes_5m_q = [c["close"] for c in m5_quick]
        entry_q     = _pround(m5_quick[-1]["close"])

        # ── F2: PDZ 15m — Super Setup requires 15m Discount ──────────────────
        pdz_zone_15m       = "—"
        pdz_zone_1h        = "—"
        is_super_eligible  = False
        if cfg.get("use_pdz_15m", True):
            pdz_pass_15m, pdz_zone_15m = calc_pdz_zone(m15_quick, entry_q, float(cfg.get("tp_pct", 1.2)) / 100.0)
            if pdz_zone_15m == "Discount":
                # 15m is Discount — now we need 1h also in Discount for Super
                # (if 1h fetch failed, can't verify → treat as not-super, fall
                # through to F3-F10 rather than blocking entry).
                if m1h_quick:
                    _, pdz_zone_1h = calc_pdz_zone(m1h_quick, entry_q, float(cfg.get("tp_pct", 1.2)) / 100.0)
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
                        "atr_15m": "—",
                        "atr_ratio": "—",
                    },
                }
            # else: super cap exhausted — demote and fall through to F3-F10
            _record_elim("super_cap_demoted", "super_cap_demoted_syms", sym)

        # ── F3: PDZ 5m ────────────────────────────────────────────────────────
        pdz_zone_5m = "—"
        if cfg.get("use_pdz_5m", True):
            pdz_pass_5m, pdz_zone_5m = calc_pdz_zone(m5_quick, entry_q, float(cfg.get("tp_pct", 1.2)) / 100.0)
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

        # ── F5b: ATR(14) 15m — TP reachability filter ───────────────────────
        # ATR is ALWAYS computed and stored in criteria so that the Difficulty
        # column in the Open Signals table works regardless of whether the
        # filter toggle is on or off.
        # ratio = (TP distance %) / (ATR %)
        # Strict: ratio ≤ 1.5  → TP within 1.5× ATR (very reachable)
        # Normal: ratio ≤ 2.0  → TP within 2× ATR
        # Relaxed: ratio ≤ 3.0 → TP within 3× ATR
        atr_15m_val = None
        atr_ratio_val = None
        _atr_series = calc_atr(m15, 14)
        if _atr_series and entry > 0:
            atr_15m_val   = _atr_series[-1]
            _tp_dist_pct  = float(cfg.get("tp_pct", 1.5))  # TP% from config
            _atr_pct      = (atr_15m_val / entry) * 100.0
            atr_ratio_val = (_tp_dist_pct / _atr_pct) if _atr_pct > 0 else None
        # Only REJECT the coin when the filter is explicitly enabled
        if cfg.get("use_atr_filter", False) and atr_ratio_val is not None:
            _atr_thresh = {"Strict": 1.5, "Normal": 2.0, "Relaxed": 3.0}.get(
                              cfg.get("atr_mode", "Normal"), 2.0)
            if atr_ratio_val > _atr_thresh:
                _record_elim("f5b_atr", "f5b_elim_syms", sym)
                return None

        # ── F6: EMA (per-timeframe tracking) ────────────────────────────────
        ema_3m_val = ema_5m_val = ema_15m_val = None
        if cfg.get("use_ema_3m"):
            ema = calc_ema(closes_3m, max(2, int(cfg.get("ema_period_3m", 12))))
            if not ema or entry < ema[-1]:
                _record_elim("f6_ema_3m", "f6_ema_3m_elim_syms", sym)
                return None
            ema_3m_val = _pround(ema[-1])
        if cfg.get("use_ema_5m"):
            ema = calc_ema(closes_5m, max(2, int(cfg.get("ema_period_5m", 12))))
            if not ema or entry < ema[-1]:
                _record_elim("f6_ema_5m", "f6_ema_5m_elim_syms", sym)
                return None
            ema_5m_val = _pround(ema[-1])
        if cfg.get("use_ema_15m"):
            ema = calc_ema(closes_15m, max(2, int(cfg.get("ema_period_15m", 12))))
            if not ema or entry < ema[-1]:
                _record_elim("f6_ema_15m", "f6_ema_15m_elim_syms", sym)
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
            "atr_15m":          round(atr_15m_val, 8)  if atr_15m_val   is not None else "—",
            "atr_ratio":        round(atr_ratio_val, 3) if atr_ratio_val is not None else "—",
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

    with _filter_lock:
        _filter_counts["scan_completed_at"] = time.time()
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
def _init_signal_trade_snapshot(sig: dict, cfg: dict) -> None:
    """Snapshot trade + DCA config onto a freshly-created signal.

    This MUST be called for every new signal the scanner adds to the log —
    not just signals that pass through the auto-trading path — so that:

      • Paper-mode / credential-less signals still carry `dca_enabled`,
        `dca_max`, and the snapshotted drop percentages. Without this
        snapshot, `_dca_compute_trigger` returns 0 → the Next DCA column
        renders "—" for every open trade, and the paper-mode DCA simulator
        has nothing to act on.
      • `trade_usdt`, `trade_lev`, `order_margin_mode`, `demo_mode` reflect
        what WOULD have been sent to OKX, so PnL, Margin Mode, and OKX
        Command columns show meaningful values even without a live order.
      • `avg_entry`, `dca_fills[0]`, `total_notional`, `original_entry` are
        populated so DCA math works the instant a signal fires.

    Idempotent-ish: the DCA fields are always (re)set to a fresh state for
    fill 0. Callers in the auto-trade path call this AFTER overwriting
    `entry` with the actual OKX fill price, so fill 0 reflects the real
    execution price rather than the scanner's signal price.
    """
    _dca_max_snap = int(cfg.get("trade_max_dca", 0) or 0)
    _dca_max_snap = max(0, min(6, _dca_max_snap))
    _entry_px     = float(sig.get("entry", 0) or 0)
    # Prefer values already set on the signal (from auto-trade path); else
    # fall back to the current cfg so paper/no-creds signals still show
    # what would have been used.
    _entry_usdt   = float(sig.get("trade_usdt",
                                  cfg.get("trade_usdt_amount", 0)) or 0)
    _entry_lev    = int(sig.get("trade_lev",
                                cfg.get("trade_leverage", 10)) or 10)
    _entry_notnl  = _entry_usdt * _entry_lev if _entry_lev > 0 else 0.0

    # Trade config — only set if not already populated by auto-trade path.
    sig.setdefault("trade_usdt",        _entry_usdt)
    sig.setdefault("trade_lev",         _entry_lev)
    sig.setdefault("demo_mode",         bool(cfg.get("demo_mode", True)))
    sig.setdefault("order_margin_mode",
                   (cfg.get("trade_margin_mode", "isolated") or "isolated"))

    # DCA state — always overwritten (this is the source of truth).
    sig["dca_enabled"]    = _dca_max_snap > 0
    sig["dca_max"]        = _dca_max_snap
    sig["dca_count"]      = 0
    sig["dca_iso_distance_pct"] = float(
        cfg.get("dca_iso_distance_pct", 70.0) or 70.0
    )
    sig["dca_cross_drop_pct"]   = float(
        cfg.get("dca_cross_drop_pct",   7.0)  or 7.0
    )
    # Snapshot the TP/SL active at entry time so the Trade History column
    # can show the original levels on the first line, even after later DCAs
    # have recomputed sig["tp"] / sig["sl"].
    _entry_tp_snap = float(sig.get("tp", 0) or 0)
    _entry_sl_snap = float(sig.get("sl", 0) or 0)
    sig["dca_fills"]      = [{
        "price":    _entry_px,
        "usdt":     _entry_usdt,
        "leverage": _entry_lev,
        "notional": _entry_notnl,
        "ts":       sig.get("timestamp", ""),
        "order_id": sig.get("order_id", "") or "",
        "dca_idx":  0,
        "tp":       _entry_tp_snap,
        "sl":       _entry_sl_snap,
    }] if _entry_px > 0 else []
    sig["avg_entry"]      = _entry_px
    sig["total_usdt"]     = _entry_usdt
    sig["total_notional"] = _entry_notnl
    sig["original_entry"] = _entry_px
    # final_sl_price is populated only after the DCA ladder is fully
    # exhausted (dca_count == dca_max).
    sig["final_sl_price"] = None

    # ── SL distance snapshot (isolated → 1/lev; cross → sl_pct/100) ──
    # Snapshotted at entry time so post-DCA SL recompute stays proportional
    # to the initial SL distance even if the sidebar SL % is changed later.
    # Formula is identical regardless of mode — the distance just differs.
    # Post-DCA: sig["sl"] = avg_entry × (1 − sl_distance_pct).
    _mode = (sig.get("order_margin_mode") or "isolated").strip().lower()
    if _mode == "isolated" and _entry_lev > 0:
        _sl_dist = 1.0 / _entry_lev
    else:
        _sl_dist = float(cfg.get("sl_pct", 3.0) or 3.0) / 100.0
    # Prefer deriving from the actual entry SL if available (more accurate
    # when entry/sl were already set by the auto-trade path with fill price).
    if _entry_px > 0 and 0 < _entry_sl_snap < _entry_px:
        _sl_dist = (_entry_px - _entry_sl_snap) / _entry_px
    sig["sl_distance_pct"] = float(_sl_dist)

    # ── next_dca_px: the price at which the NEXT DCA add will fire. ──
    # Separate from sig["sl"] so sig["sl"] can always be a real stop.
    # Recomputed on each DCA fill in _execute_dca_fill_paper / _execute_dca_fill.
    try:
        sig["next_dca_px"] = float(_dca_compute_trigger(sig, cfg) or 0.0)
    except Exception:
        sig["next_dca_px"] = 0.0

    # ── DCA ladder (cross margin only) ───────────────────────────────────────
    # Pre-calculate all DCA trigger prices and blended averages at entry time.
    # Stored on the signal so the DCA Levels column can display planned prices
    # for all levels (past and future) without recomputing on every render.
    # Actual fill prices are recorded in dca_fills after each market DCA order.
    _snap_mode2 = (sig.get("order_margin_mode") or "isolated").strip().lower()
    if _dca_max_snap > 0 and _snap_mode2 == "cross" and _entry_px > 0:
        _tp_pct_l  = float(cfg.get("tp_pct", 1.5) or 1.5)
        _cdrop_l   = float(cfg.get("dca_cross_drop_pct", 7.0) or 7.0)
        sig["dca_ladder"] = _calc_cross_dca_ladder(
            _entry_px, _entry_usdt, _entry_lev,
            _dca_max_snap, _cdrop_l, _tp_pct_l
        )
    else:
        sig.setdefault("dca_ladder", [])


def _migrate_legacy_signals(log: dict, cfg: dict) -> int:
    """Backfill the DCA/trade snapshot onto pre-existing OPEN signals that
    were created before `_init_signal_trade_snapshot` was wired into the
    non-traded path (or before the DCA fields even existed in the schema).

    Only touches signals where:
      • status == "open"  (closed trades are historical and must not change)
      • At least one of the DCA snapshot fields is missing / legacy —
        specifically, `dca_fills` is empty OR `dca_enabled` is absent OR
        `original_entry` is missing.

    Uses the CURRENT sidebar config to fill in trade_usdt, trade_lev,
    margin_mode, DCA max, and drop percentages. This is the pragmatic
    trade-off: we can't retroactively know what the config was at the
    moment the signal fired (it may have been different), but it's better
    than the current state of the Next DCA column showing "—" forever.

    Returns the number of signals migrated so it can be logged/reported.
    """
    if not log or not isinstance(log.get("signals"), list):
        return 0
    _n = 0
    for _sig in log["signals"]:
        if not isinstance(_sig, dict):
            continue
        if _sig.get("status") != "open":
            continue
        # Detect legacy / missing snapshot
        _missing_dca = (
            "dca_enabled" not in _sig
            or not _sig.get("dca_fills")
            or "original_entry" not in _sig
            or ((_sig.get("order_margin_mode") or "isolated").strip().lower() == "cross"
                and not _sig.get("dca_ladder"))
        )
        if not _missing_dca:
            continue
        try:
            _init_signal_trade_snapshot(_sig, cfg)
            # Also populate the tag that surfaces in the UI fallback paths.
            _sig.setdefault("signal_entry", _sig.get("entry"))
            _n += 1
        except Exception:
            # Never let one bad row abort the rest of the migration.
            continue
    return _n


def _dca_compute_trigger(sig: dict, cfg: dict = None) -> float:
    """Compute the next-DCA trigger price for an open signal.

    Two percentages are configurable from the sidebar and snapshotted onto
    the signal at entry time so later sidebar changes don't retroactively
    alter already-open trades:

      Isolated: `dca_iso_distance_pct` % = HOW FAR PRICE MUST DROP from
                avg_entry toward liquidation before next DCA fires, expressed
                as a fraction of the full entry-to-liq range.
                10 → fires very close to entry (conservative / early).
                80 → fires 80% of the way toward liquidation (aggressive).
                95 → fires almost at liquidation (very aggressive).

                Formula:
                  dca_drop % = (1 / leverage) × (iso_dist / 100)
                  DCA price  = avg_entry × (1 − dca_drop %)

                Example — entry $100 · 10× → liq ≈ $90:
                  iso_dist=10  →  drop 1%    →  DCA at $99
                  iso_dist=50  →  drop 5%    →  DCA at $95
                  iso_dist=80  →  drop 8%    →  DCA at $92
                  iso_dist=95  →  drop 9.5%  →  DCA at $90.50

      Cross:    `dca_cross_drop_pct` % fixed drop below blended avg (default 7%).
                Leverage-independent.

    Fallback chain for each %: sig snapshot → cfg → hardcoded default.
    Returns 0.0 if the trigger cannot be computed.
    """
    avg_entry = float(sig.get("avg_entry", sig.get("entry", 0)) or 0)
    if avg_entry <= 0:
        return 0.0
    mode = (sig.get("order_margin_mode") or "isolated").strip().lower()
    lev  = int(sig.get("trade_lev", 0) or 0)
    if lev <= 0:
        lev = 10
    cfg = cfg or {}
    if mode == "isolated":
        # sig snapshot wins; fall back to current cfg; then hardcoded 70.
        iso_dist = float(sig.get("dca_iso_distance_pct",
                                 cfg.get("dca_iso_distance_pct", 70.0)) or 70.0)
        iso_dist = max(10.0, min(95.0, iso_dist))  # clamp to dropdown range
        # iso_dist = % of the entry-to-liq range price must drop before next DCA fires.
        # drop_pct = (1/lev) × (iso_dist/100)
        # At 20×, iso_dist=80 → drop 4% → trigger at entry × 0.96.
        dca_drop_pct = (1.0 / lev) * (iso_dist / 100.0)
    else:
        cross_drop = float(sig.get("dca_cross_drop_pct",
                                   cfg.get("dca_cross_drop_pct", 7.0)) or 7.0)
        cross_drop = max(0.1, min(50.0, cross_drop))  # clamp 0.1–50
        dca_drop_pct = cross_drop / 100.0
    return _pround(avg_entry * (1.0 - dca_drop_pct))


def _calc_cross_dca_ladder(entry_px: float, base_usdt: float, leverage: int,
                            max_dca: int, cross_drop_pct: float,
                            tp_pct: float) -> list:
    """Pre-calculate the full DCA price cascade for cross margin mode.

    All values are deterministic at entry time because we know:
      • entry price, base USDT, leverage
      • cross_drop_pct  — fixed % drop below blended avg for each DCA trigger
      • tp_pct          — fixed % above blended avg for TP

    Assumes each DCA fills exactly at its trigger price (actual slippage is
    tiny and does not materially change the cascade).

    Returns a list of dicts, one per DCA level (1-indexed):
        {
            "level":       int,    # 1, 2, 3 …
            "trigger_px":  float,  # price at which this DCA fires
            "usdt":        float,  # USDT margin added at this level
            "blended_avg": float,  # blended avg AFTER this DCA fills
            "tp_px":       float,  # TP price after this DCA (tp_pct% above blended_avg)
        }
    """
    if entry_px <= 0 or base_usdt <= 0 or leverage <= 0 or max_dca <= 0:
        return []
    drop_frac = cross_drop_pct / 100.0
    tp_frac   = tp_pct / 100.0
    lev       = max(1, int(leverage))

    # Seed with entry fill
    fills = [{"price": entry_px, "notional": base_usdt * lev}]
    ladder = []

    for i in range(1, max_dca + 1):
        # Blended average BEFORE this DCA fires (from fills so far)
        tot_not  = sum(f["notional"] for f in fills)
        tot_unit = sum(f["notional"] / f["price"] for f in fills if f["price"] > 0)
        blended_before = tot_not / tot_unit if tot_unit > 0 else fills[-1]["price"]

        trigger_px = _pround(blended_before * (1.0 - drop_frac))
        dca_usdt   = base_usdt * (2 ** (i - 1))   # DCA 1=base, 2=2×, 3=4×, …
        dca_not    = dca_usdt * lev

        # Blended average AFTER this DCA fills at trigger_px
        fills_after  = fills + [{"price": trigger_px, "notional": dca_not}]
        tot_not_a    = sum(f["notional"] for f in fills_after)
        tot_unit_a   = sum(f["notional"] / f["price"] for f in fills_after if f["price"] > 0)
        blend_after  = tot_not_a / tot_unit_a if tot_unit_a > 0 else trigger_px
        tp_after     = _pround(blend_after * (1.0 + tp_frac))

        ladder.append({
            "level":       i,
            "trigger_px":  trigger_px,
            "usdt":        dca_usdt,
            "blended_avg": _pround(blend_after),
            "tp_px":       tp_after,
        })
        # Add this fill for the next iteration
        fills.append({"price": trigger_px, "notional": dca_not})

    return ladder


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
    """Place a DCA market-buy add on OKX using the sig's existing leverage/mode.

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
            _pm_resp = _trade_get("/api/v5/account/config", {}, cfg)
            if _pm_resp.get("code") == "0":
                _pm = _pm_resp.get("data", [{}])[0].get("posMode", "net_mode")
                is_hedge = (_pm == "long_short_mode")
        except Exception:
            pass

        # Re-apply leverage (idempotent; warning-only for DCA — position already
        # exists at its original leverage so a rejection here is non-critical)
        try:
            _set_leverage_okx(sym, cfg)
        except Exception as lev_exc:
            _append_error("trade", f"DCA set-leverage warning (non-critical): {lev_exc}",
                          symbol=sym, endpoint="/api/v5/account/set-leverage")

        order_body: dict = {
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
            "endpoint":  "/api/v5/trade/order (DCA add)",
            "body_sent": order_body,
            "response":  resp,
            "is_hedge":  is_hedge,
            "contracts": contracts,
            "ct_val":    ct_val,
            "dca_usdt":  dca_usdt,
        }
        d0     = (resp.get("data") or [{}])[0]
        ord_id = d0.get("ordId", "")

        if resp.get("code") != "0":
            err = _okx_err(resp)
            _append_error("trade",
                          f"DCA {err} | body={json.dumps(order_body)} | "
                          f"resp={json.dumps(resp)[:300]}",
                          symbol=sym, endpoint="/api/v5/trade/order")
            return {"ordId": "", "sz": contracts, "status": "error",
                    "error": err, "ct_val": ct_val, "notional": notional,
                    "tdMode": mode, "is_hedge": is_hedge}
        if d0.get("sCode", "0") != "0":
            err = f"DCA order: {d0.get('sCode')}: {d0.get('sMsg','')}"
            _append_error("trade",
                          f"{err} | body={json.dumps(order_body)}",
                          symbol=sym, endpoint="/api/v5/trade/order")
            return {"ordId": ord_id, "sz": contracts, "status": "error",
                    "error": err, "ct_val": ct_val, "notional": notional,
                    "tdMode": mode, "is_hedge": is_hedge}

        # Fetch actual fill price
        actual_entry = ref_price
        time.sleep(0.3)
        try:
            fill_resp = _trade_get("/api/v5/trade/order",
                                   {"instId": _to_okx(sym), "ordId": ord_id},
                                   cfg)
            if fill_resp.get("code") == "0":
                fill_d = fill_resp.get("data", [{}])[0]
                avg_px = float(fill_d.get("avgPx", 0) or 0)
                if avg_px > 0:
                    actual_entry = avg_px
        except Exception as fill_exc:
            _append_error("trade",
                          f"DCA fill fetch failed: {fill_exc}",
                          symbol=sym, endpoint="/api/v5/trade/order[GET]")

        return {"ordId": ord_id, "sz": contracts,
                "status": "placed", "error": "",
                "actual_entry": actual_entry,
                "ct_val": ct_val, "notional": notional,
                "tdMode": mode, "is_hedge": is_hedge}

    except Exception as exc:
        _append_error("trade", f"DCA exception: {exc}",
                      symbol=sig.get("symbol", ""),
                      endpoint="/api/v5/trade/order")
        return {"ordId": "", "sz": 0, "status": "error", "error": str(exc)}


def _cancel_algo_best_effort(algo_id: str, sym: str, cfg: dict) -> None:
    """Cancel an OCO algo order on OKX — log failure, never raise."""
    if not algo_id or not sym:
        return
    try:
        _trade_post("/api/v5/trade/cancel-algos",
                    [{"algoId": algo_id, "instId": _to_okx(sym)}],
                    cfg)
    except Exception as exc:
        _append_error("trade",
                      f"cancel-algo failed for {sym} algoId={algo_id}: {exc}",
                      symbol=sym, endpoint="/api/v5/trade/cancel-algos")




def _force_close_position(sig: dict, cfg: dict) -> dict:
    """Force-close a signal — works for both paper and Demo/Live trades.

    Paper path  (no order_id): updates the log only — no OKX API calls.
    Live path   (order_id set):
      1. Fetch the actual live position size from OKX (authoritative source).
      2. Cancel the OCO/TP algo order attached to the signal.
      3. Cancel any pending DCA preorders (cross margin mode).
      4. Place a market sell for the full contract count.
      5. Mark the signal as closed_okx in the shared log.

    Returns {"success": bool, "message": str}
    """
    sym      = sig.get("symbol", "?")
    is_paper = not sig.get("order_id")

    try:
        # ── PAPER PATH ─────────────────────────────────────────────────────────
        if is_paper:
            _close_px = float(sig.get("latest_price") or
                              sig.get("avg_entry") or sig.get("entry") or 0)
            with _log_lock:
                for _s in _b._bsc_log["signals"]:
                    if (_s.get("timestamp") == sig.get("timestamp") and
                            _s.get("symbol") == sym):
                        _s["status"]      = "closed_okx"
                        _s["close_price"] = _close_px
                        _s["close_time"]  = dubai_now().isoformat()
                        break
                save_log(_b._bsc_log)
            return {"success": True,
                    "message": f"✅ {sym} paper trade closed at {_close_px}."}

        # ── LIVE PATH (Demo or Live) ────────────────────────────────────────────
        # Step 1: Fetch live position size from OKX ──────────────────────────
        _pos_resp = _trade_get(
            "/api/v5/account/positions",
            {"instType": "SWAP", "instId": _to_okx(sym)},
            cfg,
        )
        if _pos_resp.get("code") != "0":
            return {"success": False,
                    "message": f"Position fetch failed: {_pos_resp.get('msg', 'unknown')}"}

        _live = [p for p in _pos_resp.get("data", [])
                 if float(p.get("pos", 0) or 0) != 0]
        if not _live:
            # Position already gone on OKX — just clean up the log
            with _log_lock:
                for _s in _b._bsc_log["signals"]:
                    if (_s.get("timestamp") == sig.get("timestamp") and
                            _s.get("symbol") == sym):
                        _s["status"]      = "closed_okx"
                        _s["close_price"] = float(sig.get("latest_price") or
                                                   sig.get("avg_entry") or
                                                   sig.get("entry") or 0)
                        _s["close_time"]  = dubai_now().isoformat()
                        break
                save_log(_b._bsc_log)
            return {"success": True,
                    "message": f"✅ {sym}: No open position on OKX — log updated."}

        _pos      = _live[0]
        contracts = abs(int(float(_pos.get("pos", 0) or 0)))
        mode      = (sig.get("order_margin_mode") or
                     cfg.get("trade_margin_mode", "isolated")).strip().lower()
        is_hedge  = bool(sig.get("order_is_hedge", False))

        _fc_env_tag = "DEMO" if sig.get("demo_mode") else "LIVE"

        # Step 2: Cancel OCO / TP algo ────────────────────────────────────────
        for _aid_key in ("algo_id", "tp_algo_id"):
            _aid = sig.get(_aid_key, "")
            if _aid:
                _cancel_algo_best_effort(_aid, sym, cfg)
                _okx_log_entry(
                    sig, f"ALGO CANCEL [{_fc_env_tag}]",
                    algoId = _aid,
                    reason = "force close",
                )

        # Step 3: Market sell ─────────────────────────────────────────────────
        _sell_body: dict = {
            "instId":  _to_okx(sym),
            "tdMode":  mode,
            "side":    "sell",
            "ordType": "market",
            "sz":      str(contracts),
        }
        if is_hedge:
            _sell_body["posSide"] = "long"

        _sell_resp = _trade_post("/api/v5/trade/order", _sell_body, cfg)
        _sd0       = (_sell_resp.get("data") or [{}])[0]

        if _sell_resp.get("code") != "0" or _sd0.get("sCode", "0") != "0":
            _err = (_okx_err(_sell_resp) if _sell_resp.get("code") != "0"
                    else f"{_sd0.get('sCode')}: {_sd0.get('sMsg', '')}")
            _append_error("trade", f"Force close sell failed for {sym}: {_err}",
                          symbol=sym, endpoint="/api/v5/trade/order")
            return {"success": False, "message": f"Sell order rejected: {_err}"}

        # ── OKX Command log — force close sell ────────────────────────────────
        _okx_log_entry(
            sig, f"FORCE CLOSE [{_fc_env_tag}]",
            instId  = _to_okx(sym),
            ordType = "market",
            side    = "sell",
            tdMode  = mode,
            sz      = f"{contracts} contracts",
            ordId   = _sd0.get("ordId", "—"),
        )

        # Step 5: Update log ──────────────────────────────────────────────────
        _close_px = float(sig.get("latest_price") or
                          sig.get("avg_entry") or sig.get("entry") or 0)
        with _log_lock:
            for _s in _b._bsc_log["signals"]:
                if (_s.get("timestamp") == sig.get("timestamp") and
                        _s.get("symbol") == sym):
                    _s["status"]      = "closed_okx"
                    _s["close_price"] = _close_px
                    _s["close_time"]  = dubai_now().isoformat()
                    break
            save_log(_b._bsc_log)

        _append_error("trade",
                      f"Force closed {sym} — {contracts} contracts at market.",
                      symbol=sym, endpoint="/api/v5/trade/order")
        return {"success": True,
                "message": f"✅ {sym} force closed ({contracts} contracts at market)."}

    except Exception as exc:
        _append_error("trade", f"Force close exception for {sym}: {exc}",
                      symbol=sym, endpoint="_force_close_position")
        return {"success": False, "message": f"Exception: {exc}"}

def _place_dca_oco_algo(sig: dict, cfg: dict, new_tp: float,
                         new_sl: float, total_contracts: int) -> str:
    """Place a fresh OCO (TP + SL) algo on OKX after a DCA add.

    Returns the new algoId, or "" on failure (non-fatal — DCA already placed).
    """
    try:
        sym  = sig.get("symbol", "")
        mode = (sig.get("order_margin_mode") or
                cfg.get("trade_margin_mode", "isolated")).strip().lower()
        is_hedge = bool(sig.get("order_is_hedge", False))
        algo_body: dict = {
            "instId":          _to_okx(sym),
            "tdMode":          mode,
            "side":            "sell",
            "ordType":         "oco",
            "sz":              str(int(total_contracts)),
            "tpTriggerPx":     str(_pround(new_tp)),
            "tpTriggerPxType": "mark",
            "tpOrdPx":         "-1",
            "slTriggerPx":     str(_pround(new_sl)),
            "slTriggerPxType": "mark",
            "slOrdPx":         "-1",
        }
        if is_hedge:
            algo_body["posSide"] = "long"
        else:
            # Prevent oversell flipping LONG → SHORT in cross net mode.
            algo_body["reduceOnly"] = "true"
        algo_resp = _trade_post("/api/v5/trade/order-algo", algo_body, cfg)
        ad        = (algo_resp.get("data") or [{}])[0]
        if algo_resp.get("code") != "0" or (ad.get("sCode","0") != "0" and ad.get("sCode","")):
            err = _okx_err(algo_resp) if algo_resp.get("code") != "0" \
                  else f"{ad.get('sCode')}: {ad.get('sMsg','')}"
            _append_error("trade", f"DCA OCO algo failed: {err}",
                          symbol=sym, endpoint="/api/v5/trade/order-algo")
            return ""
        return ad.get("algoId", "")
    except Exception as exc:
        _append_error("trade", f"DCA OCO exception: {exc}",
                      symbol=sig.get("symbol", ""),
                      endpoint="/api/v5/trade/order-algo")
        return ""


def _place_tp_only_order(sig: dict, cfg: dict,
                         tp_price: float, total_contracts: int) -> str:
    """Place a standalone conditional TP sell order on OKX (cross margin mode).

    No SL attached — the user has chosen OKX liquidation as the only backstop.
    Returns algoId on success, "" on failure (non-fatal, logged).
    """
    try:
        sym      = sig.get("symbol", "")
        mode     = (sig.get("order_margin_mode") or
                    cfg.get("trade_margin_mode", "isolated")).strip().lower()
        is_hedge = bool(sig.get("order_is_hedge", False))
        algo_body: dict = {
            "instId":         _to_okx(sym),
            "tdMode":         mode,
            "side":           "sell",
            "ordType":        "conditional",
            "sz":             str(int(max(1, total_contracts))),
            "tpTriggerPx":    str(_pround(tp_price)),
            "tpTriggerPxType": "mark",   # use mark price — more reliable than
                                         # last price for low-liquidity tokens
            "tpOrdPx":        "-1",      # market fill when TP triggers
        }
        if is_hedge:
            algo_body["posSide"] = "long"
        else:
            # Prevent oversell flipping LONG → SHORT in cross net mode.
            # OKX caps execution at the actual position size when reduceOnly=true.
            algo_body["reduceOnly"] = "true"
        resp = _trade_post("/api/v5/trade/order-algo", algo_body, cfg)
        ad   = (resp.get("data") or [{}])[0]
        if resp.get("code") != "0" or (ad.get("sCode", "0") not in ("0", "") and ad.get("sCode")):
            err = (_okx_err(resp) if resp.get("code") != "0"
                   else f"{ad.get('sCode')}: {ad.get('sMsg', '')}")
            _append_error("trade", f"TP-only algo failed: {err}",
                          symbol=sym, endpoint="/api/v5/trade/order-algo")
            return ""
        return ad.get("algoId", "")
    except Exception as exc:
        _append_error("trade", f"TP-only algo exception: {exc}",
                      symbol=sig.get("symbol", ""),
                      endpoint="/api/v5/trade/order-algo")
        return ""


def _execute_dca_fill_paper(sig: dict, cfg: dict) -> bool:
    """Paper-mode DCA fill (no OKX API calls).

    Runs the SAME math as `_execute_dca_fill` — appends a synthetic fill
    record, recomputes blended avg / total_usdt / total_notional / tp / sl,
    increments dca_count — but skips the real market-buy order AND the
    OCO cancel/replace. The simulated fill price is taken from the detected
    trigger (`sig["_dca_trigger_px"]`, set by `_update_one_signal` /
    `_watcher_update_one_signal`) so the displayed blended avg matches what
    a live fill at market would have produced.

    Used automatically whenever:
      • auto-trading is OFF  (cfg["trade_enabled"] is False), OR
      • the signal has no live OKX order  (no order_id — e.g. a paper-mode
        entry that never placed a real order).

    Returns True on successful simulated add, False otherwise.
    Each paper fill record is tagged with "paper": True so the UI / logs
    can distinguish it from real fills.
    """
    sym = sig.get("symbol", "?")
    try:
        dca_usdt = _dca_next_usdt(sig)
        if dca_usdt <= 0:
            sig.pop("_dca_pending", None)
            return False

        # Use the detected trigger price as the simulated fill — this is the
        # price the low wick reached on the candle that flagged the trigger,
        # which is a faithful approximation of what a market order would have
        # filled at. Fallback: recompute the current trigger.
        fill_px = float(sig.get("_dca_trigger_px", 0) or 0)
        if fill_px <= 0:
            fill_px = _dca_compute_trigger(sig, cfg)
        if fill_px <= 0:
            sig.pop("_dca_pending", None)
            return False

        lev = int(sig.get("trade_lev", 0) or 0)
        if lev <= 0:
            lev = int(cfg.get("trade_leverage", 10) or 10)
        fill_not = dca_usdt * lev

        fills = sig.get("dca_fills") or []
        next_idx = len(fills)
        fills.append({
            "price":    _pround(fill_px),
            "usdt":     dca_usdt,
            "leverage": lev,
            "notional": fill_not,
            "ts":       dubai_now().isoformat(),
            "order_id": "",
            "dca_idx":  next_idx,
            "paper":    True,
        })
        sig["dca_fills"] = fills

        # Recompute blended average as notional-weighted mean of fills.
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
            prices = [float(f.get("price", 0) or 0) for f in fills
                      if float(f.get("price", 0) or 0) > 0]
            avg_entry = (sum(prices) / len(prices)
                         if prices else float(sig.get("avg_entry", 0) or 0))

        sig["avg_entry"]      = _pround(avg_entry)
        sig["total_usdt"]     = total_usdt
        sig["total_notional"] = total_notional
        sig["dca_count"]      = next_idx

        # ── Fixed dollar TP/SL after any DCA (paper path) ───────────────────
        # TP = avg + ($dca_tp_usd / total_coins)
        # SL = avg − ($dca_sl_usd / total_coins)
        # Replaces the legacy percentage-based formulas entirely for DCA trades.
        _dca_tp_usd_p = float(cfg.get("dca_tp_usd", 0.50) or 0.50)
        _dca_sl_usd_p = float(cfg.get("dca_sl_usd", 5.00) or 5.00)
        _avg_p        = float(sig["avg_entry"])
        _tot_not_p    = float(sig.get("total_notional", 0) or 0)
        if _avg_p > 0 and _tot_not_p > 0:
            _total_coins_p = _tot_not_p / _avg_p
            new_tp = _pround(_avg_p + _dca_tp_usd_p / _total_coins_p)
            new_sl = _pround(_avg_p - _dca_sl_usd_p / _total_coins_p)
        else:
            # Fallback to percentage if avg or notional unavailable
            _tp_pct_fb = float(cfg.get("tp_pct", 1.5)) / 100.0
            _sl_d_fb   = float(sig.get("sl_distance_pct",
                                        cfg.get("sl_pct", 3.0) / 100.0) or 0.03)
            new_tp = _pround(_avg_p * (1.0 + _tp_pct_fb))
            new_sl = _pround(_avg_p * (1.0 - _sl_d_fb))
        sig["tp"]            = new_tp
        sig["sl"]            = new_sl
        sig["fc_trigger_px"] = new_tp   # paper FC detection uses the same price

        dca_max          = int(sig.get("dca_max", 0) or 0)
        ladder_exhausted = (sig.get("dca_count", 0) >= dca_max)
        if ladder_exhausted:
            sig["final_sl_price"] = new_sl
            sig["next_dca_px"]    = 0.0
        else:
            sig["final_sl_price"] = None
            try:
                sig["next_dca_px"] = float(_dca_compute_trigger(sig, cfg) or 0.0)
            except Exception:
                sig["next_dca_px"] = 0.0

        # Snapshot the post-recompute Entry / TP / SL onto THIS DCA fill so
        # the Trade History column always shows all three values for every row.
        try:
            fills[-1]["entry"] = float(sig.get("avg_entry", 0) or 0)
            fills[-1]["tp"]    = float(sig.get("tp", 0) or 0)
            fills[-1]["sl"]    = float(sig.get("sl", 0) or 0)
        except Exception:
            pass

        # No order_id / algo_id change — paper trade has no OKX position.
        # Mark display-level status so the UI can reflect the paper fill.
        sig["order_status"] = "paper_filled"

        # ── OKX Command log — paper DCA fill ──────────────────────────────────
        _okx_log_entry(
            sig, f"DCA-{next_idx} [PAPER]",
            instId     = _to_okx(sym),
            ordType    = "market (simulated)",
            fill_px    = _pround(fill_px),
            collateral = f"${dca_usdt:.2f}",
            avg_entry  = sig.get("avg_entry", ""),
        )
        _okx_log_entry(
            sig, f"DCA-{next_idx} TP/SL [PAPER]",
            new_tp  = sig.get("tp", ""),
            new_sl  = sig.get("sl", ""),
            tp_usd  = f"+${_dca_tp_usd_p:.2f}",
            sl_usd  = f"-${_dca_sl_usd_p:.2f}",
        )

        sig.pop("_dca_pending", None)
        sig.pop("_dca_trigger_px", None)
        sig.pop("_dca_trigger_time", None)
        return True

    except Exception as exc:
        _append_error("trade", f"Paper DCA exception: {exc}",
                      symbol=sym, endpoint="_execute_dca_fill_paper")
        sig.pop("_dca_pending", None)
        return False


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
        # ── Position pre-check: verify OKX still holds an open position before
        # adding margin.  If the position was closed externally (manual close,
        # OCO triggered) skip the DCA, mark the signal closed, and log clearly.
        #
        # Grace-period guard: skip the check entirely for the first 90 seconds
        # after the entry timestamp.  OKX's position API can take several seconds
        # to reflect a freshly filled market order, so firing the check too soon
        # after entry produces a false "no position" result and incorrectly closes
        # the signal as sl_hit (race condition observed on ACTUSDT 27-Apr-2026).
        _entry_ts_str = sig.get("timestamp", "")
        _now_utc      = dubai_now()
        _age_secs     = 9999  # default: assume old enough to check
        if _entry_ts_str:
            try:
                _entry_dt = datetime.fromisoformat(
                    _entry_ts_str.replace("Z", "+00:00"))
                _age_secs = (_now_utc - _entry_dt).total_seconds()
            except Exception:
                pass

        _GRACE_SECS = 90   # configurable constant — do not check within 90 s of entry

        if _age_secs < _GRACE_SECS:
            # Too soon after entry — skip position check, proceed with DCA.
            _append_error(
                "trade",
                f"DCA position pre-check skipped for {sym} — signal is only "
                f"{int(_age_secs)}s old (grace period = {_GRACE_SECS}s). "
                f"OKX may not have confirmed the position yet.",
                symbol=sym,
                endpoint="grace_period_skip",
            )
        else:
            try:
                _pos_chk = _trade_get("/api/v5/account/positions",
                                      {"instType": "SWAP", "instId": _to_okx(sym)},
                                      cfg)
                if _pos_chk.get("code") == "0":
                    _live_pos = [p for p in _pos_chk.get("data", [])
                                 if float(p.get("pos", 0) or 0) != 0]
                    if not _live_pos:
                        _append_error(
                            "trade",
                            f"DCA aborted for {sym} — no open OKX position found "
                            f"(signal age {int(_age_secs)}s, past grace period). "
                            f"Position may have been closed externally (manual "
                            f"close or OCO hit). Marking signal as closed.",
                            symbol=sym,
                            endpoint="/api/v5/account/positions",
                        )
                        sig["status"]      = "sl_hit"
                        sig["close_price"] = float(sig.get("latest_price") or
                                                   sig.get("avg_entry") or
                                                   sig.get("entry") or 0)
                        sig["close_time"]  = dubai_now().isoformat()
                        sig.pop("_dca_pending", None)
                        return False
            except Exception as _pos_exc:
                # Position check failed — proceed with DCA rather than blocking.
                _append_error("trade",
                              f"DCA position pre-check failed for {sym}: {_pos_exc} "
                              f"— proceeding with DCA anyway.",
                              symbol=sym, endpoint="/api/v5/account/positions")

        dca_usdt = _dca_next_usdt(sig)
        if dca_usdt <= 0:
            sig.pop("_dca_pending", None)
            return False

        # ── Both cross and isolated: place a live market DCA buy ─────────────
        # Cross mode no longer pre-places limit orders at entry; DCA is now
        # triggered by price detection in the watcher and placed as a market
        # order — identical to isolated mode.
        result = place_okx_dca_order(sig, cfg, dca_usdt)
        if result.get("status") != "placed":
            sig.pop("_dca_pending", None)
            sig["_dca_last_error"] = result.get("error", "")
            _append_error("trade",
                          f"DCA market order failed for {sym}: "
                          f"{result.get('error', 'unknown error')}",
                          symbol=sym, endpoint="/api/v5/trade/order")
            return False
        fill_px    = float(result.get("actual_entry", 0) or 0)
        fill_sz    = int(result.get("sz", 0) or 0)
        fill_not   = float(result.get("notional", 0) or 0) or (dca_usdt * int(sig.get("trade_lev", 10) or 10))
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

        # ── OKX Command log — DCA market order ────────────────────────────────
        _dca_env_tag = "DEMO" if sig.get("demo_mode") else "LIVE"
        _okx_log_entry(
            sig, f"DCA-{next_idx} [{_dca_env_tag}]",
            instId     = _to_okx(sym),
            ordType    = "market",
            tdMode     = (sig.get("order_margin_mode") or "isolated"),
            sz         = f"{fill_sz} contracts",
            ordId      = fill_ordid or "—",
            fill_px    = fill_px,
            collateral = f"${dca_usdt:.2f}",
        )

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

        # ── Fixed dollar TP/SL after any DCA (live path) ────────────────────
        # TP = avg + ($dca_tp_usd / total_coins)
        # SL = avg − ($dca_sl_usd / total_coins)
        # Replaces the legacy percentage-based formulas entirely for DCA trades.
        _dca_tp_usd_l = float(cfg.get("dca_tp_usd", 0.50) or 0.50)
        _dca_sl_usd_l = float(cfg.get("dca_sl_usd", 5.00) or 5.00)
        _avg_l        = float(sig["avg_entry"])
        if _avg_l > 0 and total_notional > 0:
            _total_coins_l = total_notional / _avg_l
            new_tp = _pround(_avg_l + _dca_tp_usd_l / _total_coins_l)
            new_sl = _pround(_avg_l - _dca_sl_usd_l / _total_coins_l)
        else:
            # Fallback to percentage if avg or notional unavailable
            _tp_pct_fb = float(cfg.get("tp_pct", 1.5)) / 100.0
            _sl_d_fb   = float(sig.get("sl_distance_pct",
                                        cfg.get("sl_pct", 3.0) / 100.0) or 0.03)
            new_tp = _pround(_avg_l * (1.0 + _tp_pct_fb))
            new_sl = _pround(_avg_l * (1.0 - _sl_d_fb))
        sig["tp"]            = new_tp
        sig["sl"]            = new_sl
        sig["fc_trigger_px"] = new_tp   # keeps FC detection consistent

        dca_max = int(sig.get("dca_max", 0) or 0)
        if sig["dca_count"] >= dca_max:
            sig["final_sl_price"] = new_sl
            sig["next_dca_px"]    = 0.0
        else:
            sig["final_sl_price"] = None
            try:
                sig["next_dca_px"] = float(_dca_compute_trigger(sig, cfg) or 0.0)
            except Exception:
                sig["next_dca_px"] = 0.0

        # Snapshot the post-recompute Entry / TP / SL onto THIS DCA fill so
        # the Trade History column always shows all three values for every row.
        try:
            fills[-1]["entry"] = float(sig.get("avg_entry", 0) or 0)
            fills[-1]["tp"]    = float(sig.get("tp", 0) or 0)
            fills[-1]["sl"]    = float(sig.get("sl", 0) or 0)
        except Exception:
            pass

        # Update last-order/display fields so OKX Command / Order ID reflect
        # the newest add.
        sig["order_id"]       = fill_ordid or sig.get("order_id", "")
        sig["order_sz"]       = fill_sz
        sig["order_notional"] = fill_not
        sig["order_status"]   = "placed"

        # Cancel old OCO and place fresh one at new levels, sized to the
        # TOTAL contract count across all fills.
        # Change 2: per-fill reconstruction — use sz where captured (fill-0
        # now carries sz since Change 1); fall back to notional math only for
        # fills that still lack it (pre-Change-1 open trades in the log).
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
            total_contracts = fill_sz   # last-resort: latest DCA fill only

        _old_algo = sig.get("algo_id", "") or sig.get("tp_algo_id", "")
        if _old_algo:
            _cancel_algo_best_effort(_old_algo, sym, cfg)
            _okx_log_entry(
                sig, f"ALGO CANCEL [{_dca_env_tag}]",
                algoId  = _old_algo,
                reason  = f"replaced after DCA-{next_idx}",
            )

        # ── Place fresh OCO (TP + SL) at fixed dollar levels ─────────────────
        # Both cross and isolated mode now use a full OCO so OKX holds BOTH
        # the fixed dollar TP and the fixed dollar SL simultaneously.
        # fc_trigger_px is already set to sig["tp"] above, so the existing
        # OKX-sync / FC-detection path continues to work unchanged.
        new_algo = _place_dca_oco_algo(sig, cfg, sig["tp"], sig["sl"],
                                       total_contracts)
        if new_algo:
            sig["algo_id"]    = new_algo
            sig["tp_algo_id"] = new_algo
            _okx_log_entry(
                sig, f"DCA OCO ALGO [{_dca_env_tag}]",
                ordType         = "oco",
                tpTriggerPx     = sig["tp"],
                slTriggerPx     = sig["sl"],
                tpTriggerPxType = "mark",
                slTriggerPxType = "mark",
                sz              = f"{total_contracts} contracts",
                algoId          = new_algo,
                tp_usd          = f"+${_dca_tp_usd_l:.2f}",
                sl_usd          = f"-${_dca_sl_usd_l:.2f}",
            )
        else:
            _okx_log_entry(
                sig, f"DCA OCO ALGO FAILED [{_dca_env_tag}]",
                tpTriggerPx = sig["tp"],
                slTriggerPx = sig["sl"],
                sz          = f"{total_contracts} contracts",
                error       = "algo placement returned no algoId",
            )

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
        # Snapshot cfg for DCA-trigger fallback chain. `_dca_compute_trigger`
        # prefers the sig's own snapshot fields; cfg covers legacy trades
        # created before those fields existed.
        with _config_lock:
            _cfg_snap = dict(_b._bsc_cfg)
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
            _dca_trigger_price = _dca_compute_trigger(sig, _cfg_snap)
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
            _post_dca  = [c for c in post if c["time"] >= _last_fill_ts_ms]
            tp_time    = None
            dca_time   = None
            sl_time    = None
            _dca_sl_px = float(sig.get("sl", 0) or 0)   # fixed dollar SL price
            for c in _post_dca:
                if tp_time is None and c["high"] >= sig["tp"]:
                    tp_time = c["time"]
                if (dca_time is None and _dca_trigger_price > 0
                        and c["low"] <= _dca_trigger_price):
                    dca_time = c["time"]
                if (sl_time is None and _dca_sl_px > 0
                        and c["low"] <= _dca_sl_px):
                    sl_time = c["time"]
            # ── FC-B: check fc_trigger_px (= fixed dollar TP price) ──────────
            _fc_px_w = float(sig.get("fc_trigger_px", 0) or 0)
            fc_time  = None
            if _fc_px_w > 0:
                for _c in _post_dca:
                    if fc_time is None and _c["high"] >= _fc_px_w:
                        fc_time = _c["time"]
                        break

            # Priority: SL (fixed dollar — beats everything, including DCA) →
            #           FC / TP → DCA trigger.
            _sl_wins = (sl_time is not None
                        and (tp_time  is None or sl_time <= tp_time)
                        and (fc_time  is None or sl_time <= fc_time)
                        and (dca_time is None or sl_time <= dca_time))
            _fc_wins = (not _sl_wins
                        and fc_time is not None
                        and (dca_time is None or fc_time <= dca_time)
                        and (tp_time  is None or fc_time <= tp_time))
            _tp_wins = (not _sl_wins
                        and not _fc_wins
                        and tp_time is not None
                        and (dca_time is None or tp_time <= dca_time))

            if _sl_wins:
                # Fixed dollar SL hit — routes to DCA SL Hit table.
                sig.update(status="dca_sl_hit", close_price=_dca_sl_px,
                           close_time=to_dubai(datetime.fromtimestamp(
                               sl_time/1000, tz=timezone.utc)).isoformat())
                sig.pop("price_alert", None)
                sig.pop("_dca_pending", None)
            elif _fc_wins:
                sig.update(status="fc_hit", close_price=_fc_px_w,
                           close_time=to_dubai(datetime.fromtimestamp(
                               fc_time/1000, tz=timezone.utc)).isoformat())
                sig.pop("price_alert", None)
                sig.pop("_dca_pending", None)
            elif _tp_wins:
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
        # When ladder is exhausted, sig["sl"] == final_sl_price
        # (blended avg × (1 − sl_distance_pct)) and a hit routes to
        # "dca_sl_hit" instead of "sl_hit".
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

            # ── ATR backfill: populate atr_ratio for signals that predate ATR ──
            # If this open signal was created before ATR computation was added,
            # its criteria dict will have atr_ratio == "—" or missing entirely.
            # We fetch 15m candles here (one extra call per affected signal,
            # only done once — result is stored so subsequent cycles skip it).
            _crit = sig.get("criteria", {})
            _stored_ratio = _crit.get("atr_ratio", "—")
            if _stored_ratio in (None, "—", ""):
                try:
                    _m15_back = get_klines(sig["symbol"], "15m", 50)
                    _atr_back = calc_atr(_m15_back, 14)
                    _entry_b  = float(sig.get("entry", 0) or 0)
                    if _atr_back and _entry_b > 0:
                        _atr_val_b  = _atr_back[-1]
                        _atr_pct_b  = (_atr_val_b / _entry_b) * 100.0
                        _tp_b       = float(sig.get("tp", 0) or 0)
                        _tp_pct_b   = ((_tp_b - _entry_b) / _entry_b * 100.0) if _tp_b > 0 else None
                        _ratio_b    = (_tp_pct_b / _atr_pct_b) if (_tp_pct_b and _atr_pct_b > 0) else None
                        if _ratio_b is not None:
                            _crit["atr_15m"]   = round(_atr_val_b, 8)
                            _crit["atr_ratio"] = round(_ratio_b, 3)
                            sig["criteria"]    = _crit
                except Exception:
                    pass   # best-effort — leave "—" if fetch fails
    except Exception as _upd_exc:
        # Previously swallowed silently — surfaced now so malformed signals
        # (bad timestamp, missing tp/sl, OKX fetch failure) become visible
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
# Open-Trade Watcher thread (1-minute DCA/TP checker)
# ─────────────────────────────────────────────────────────────────────────────
# A lightweight second loop that polls every `watcher_minutes` minutes — much
# faster than the main scan (`loop_minutes`). It only touches OPEN signals:
# fetches 1m candles, detects TP hits (candle high) and DCA triggers
# (candle LOW — the wick captures fast intra-candle drops), and executes the
# full DCA pipeline inline. The main _bg_loop skips its own open-trade update
# while the watcher is active, so there's no double-fetch and no race.
# ─────────────────────────────────────────────────────────────────────────────
def _watcher_update_one_signal(sig: dict) -> None:
    """1-minute variant of `_update_one_signal`.

    Fetches 1m candles and runs the same TP / DCA / SL detection logic, but
    anchored on the last fill timestamp rather than the original entry. Uses
    candle LOW (wick/tail) for DCA trigger — this catches fast downward spikes
    that never print as a close. Mutates `sig` in place.
    """
    _sym = sig.get("symbol", "?")
    try:
        # Snapshot cfg so `_dca_compute_trigger` can fall back to the live
        # sidebar value for any signals without a per-entry snapshot.
        with _config_lock:
            _cfg_snap = dict(_b._bsc_cfg)
        sig_dt    = _parse_iso_safe(sig["timestamp"])
        sig_ts_ms = int(sig_dt.timestamp() * 1000)
        # 1m × 120 bars ≈ 2h history — enough to catch anything missed between
        # watcher ticks even if the thread was momentarily delayed.
        candles   = get_klines(sig["symbol"], "1m", 120)
        if not candles:
            return
        post = [c for c in candles if c["time"] >= sig_ts_ms]

        _dca_enabled = bool(sig.get("dca_enabled", False))
        _dca_max     = int(sig.get("dca_max", 0) or 0)
        _dca_count   = int(sig.get("dca_count", 0) or 0)
        _ladder_slots_left = _dca_enabled and _dca_count < _dca_max

        if _ladder_slots_left:
            _dca_trigger_price = _dca_compute_trigger(sig, _cfg_snap)
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
                sig.update(status="tp_hit", close_price=sig["tp"],
                           close_time=to_dubai(datetime.fromtimestamp(
                               tp_time/1000, tz=timezone.utc)).isoformat())
                sig.pop("price_alert", None)
                sig.pop("_dca_pending", None)
            elif dca_time is not None:
                sig["_dca_pending"]      = True
                sig["_dca_trigger_px"]   = _dca_trigger_price
                sig["_dca_trigger_time"] = to_dubai(datetime.fromtimestamp(
                    dca_time/1000, tz=timezone.utc)).isoformat()
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
            return

        # ── Legacy branch OR DCA ladder fully exhausted ─────────────────────
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
            if candles:
                latest_price = candles[-1]["close"]
                _ref = float(sig.get("avg_entry",
                                     sig.get("entry", 0)) or 0)
                sig["latest_price"] = float(latest_price)
                if _ref > 0:
                    drop_pct = (_ref - latest_price) / _ref * 100
                    sig["price_alert"]     = drop_pct >= _PRICE_ALERT_PCT
                    sig["price_alert_pct"] = round(drop_pct, 2)
                else:
                    sig["price_alert"]     = False
                    sig["price_alert_pct"] = 0.0
    except Exception as _upd_exc:
        _append_error("signal_update",
                      f"[watcher] {_sym}: {type(_upd_exc).__name__}: {_upd_exc}",
                      symbol=_sym)


def _watcher_update_open_signals(signals):
    """Parallel 1m candle fetch for all open signals."""
    open_sigs = [s for s in signals if s["status"] == "open"]
    if not open_sigs:
        return signals
    with ThreadPoolExecutor(max_workers=min(len(open_sigs), 15)) as pool:
        futs = [pool.submit(_watcher_update_one_signal, s) for s in open_sigs]
        for f in as_completed(futs):
            f.result()
    return signals


def _okx_sync_open_signals(open_snapshot: list, cfg: dict) -> bool:
    """Sync open signals against OKX positions-history (one API call).

    For each open signal that has an order_id (live OKX trade), checks whether
    OKX has already closed that position (TP, SL, liquidation, or manual).
    Mutates signals in-place and returns True if any signal was updated.

    Close-type mapping (OKX positions-history 'type' field):
        2 / 4         → tp_hit    (Take-Profit / Partial TP)
        1 / 3 / 5     → sl_hit    (Stop-Loss / Liquidation / ADL)
        anything else → closed_okx (manually closed on OKX)
    """
    # Only run when auto-trading is ON and credentials are present
    if not cfg.get("trade_enabled"):
        return False
    if not (cfg.get("api_key") and cfg.get("api_secret") and cfg.get("api_passphrase")):
        return False

    # Only consider signals that have a live OKX order_id
    live_sigs = [s for s in open_snapshot
                 if s.get("status") == "open" and s.get("order_id")]
    if not live_sigs:
        return False

    try:
        ph_resp = _trade_get(
            "/api/v5/account/positions-history",
            {"instType": "SWAP", "limit": "50"},
            cfg,
        )
        if ph_resp.get("code") != "0":
            return False

        ph_data = ph_resp.get("data", [])
        if not ph_data:
            return False

        # Build lookup: instId → list of closed position records
        ph_by_inst: dict = {}
        for rec in ph_data:
            inst = rec.get("instId", "")
            if inst:
                ph_by_inst.setdefault(inst, []).append(rec)

        _updated = False
        _close_type_map = {
            "2": "tp_hit",
            "4": "tp_hit",
            "1": "sl_hit",
            "3": "sl_hit",
            "5": "sl_hit",
        }

        for sig in live_sigs:
            sym      = sig.get("symbol", "")
            inst_id  = _to_okx(sym)   # e.g. "BTCUSDT" → "BTC-USDT-SWAP"
            records  = ph_by_inst.get(inst_id, [])
            if not records:
                continue

            # Parse signal entry time as ms timestamp for comparison
            try:
                _entry_ms = int(_parse_iso_safe(sig["timestamp"]).timestamp() * 1000)
            except Exception:
                _entry_ms = 0

            # Find the most recent closed record AFTER this signal's entry
            _match = None
            for rec in records:
                try:
                    _rec_ms = int(rec.get("uTime", 0) or 0)
                except Exception:
                    _rec_ms = 0
                if _rec_ms > _entry_ms:
                    if _match is None or _rec_ms > int(_match.get("uTime", 0) or 0):
                        _match = rec

            if _match is None:
                continue

            # Determine new status
            _close_type  = str(_match.get("type", ""))
            _new_status  = _close_type_map.get(_close_type, "closed_okx")
            _close_px    = float(_match.get("closeAvgPx", 0) or 0) or None
            _close_ts    = ""
            try:
                _close_ts = to_dubai(datetime.fromtimestamp(
                    int(_match.get("uTime", 0)) / 1000, tz=timezone.utc
                )).isoformat()
            except Exception:
                pass

            # Guard: only update if still open (watcher may have already closed it)
            if sig.get("status") != "open":
                continue

            # ── FC-B detection ────────────────────────────────────────────────
            # If the signal had a DCA fill, the TP algo was replaced with an
            # fc_trigger_px order.  OKX reports this close as type "2" (TP) but
            # the close price will be at or near fc_trigger_px, not sig["tp"].
            # We distinguish the two by comparing close_price to the stored
            # fc_trigger_px with a small slippage tolerance (0.5 %).
            if _new_status == "tp_hit" and sig.get("fc_trigger_px") and _close_px:
                _fc_ref = float(sig["fc_trigger_px"])
                if _close_px <= _fc_ref * 1.005:
                    _new_status = "fc_hit"

            sig["status"]      = _new_status
            sig["close_price"] = _close_px if _close_px else sig.get(
                "tp" if _new_status in ("tp_hit", "fc_hit") else "sl")
            sig["close_time"]  = _close_ts
            sig.pop("price_alert", None)
            sig.pop("_dca_pending", None)
            _append_error(
                "trade",
                f"OKX sync: {sym} marked {_new_status} "
                f"(OKX type={_close_type}, closeAvgPx={_close_px})",
                symbol=sym,
            )
            _updated = True

        return _updated

    except Exception as _sync_exc:
        _append_error("watcher", f"OKX sync error: {_sync_exc}")
        return False


def _watcher_loop():
    """Background thread: polls open trades every `watcher_minutes`.

    Skips its own work when:
      • scanner is paused (manual or circuit breaker)
      • watcher_minutes is 0 or >= loop_minutes (main loop handles it)
    Otherwise fetches 1m candles, detects TP/DCA/SL, and executes DCA fills
    inline — same pipeline the main loop uses, just on a faster interval.
    """
    while True:
        try:
            if not _scanner_running.is_set() or getattr(_b, "_bsc_sl_paused", False):
                time.sleep(2); continue

            with _config_lock:
                cfg = dict(_b._bsc_cfg)

            _watcher_min = int(cfg.get("watcher_minutes", 1) or 0)
            _loop_min    = int(cfg.get("loop_minutes", 5) or 5)

            # Watcher is only ACTIVE if it ticks faster than the main loop.
            # Otherwise, idle — main loop does the work.
            if _watcher_min <= 0 or _watcher_min >= _loop_min:
                # Recheck every 10s — the user might change the sidebar value.
                _b._bsc_watcher_event.wait(timeout=10)
                _b._bsc_watcher_event.clear()
                continue

            t0 = time.time()
            try:
                with _log_lock:
                    _open_snapshot = [s for s in _b._bsc_log["signals"]
                                      if s.get("status") == "open"]
                if _open_snapshot:
                    _watcher_update_open_signals(_open_snapshot)

                    # ── OKX sync: close signals whose positions OKX already closed ──
                    # Runs one positions-history API call to catch TP/SL/manual
                    # closures that happened on OKX but weren't detected by candles.
                    _sync_updated = _okx_sync_open_signals(_open_snapshot, cfg)
                    if _sync_updated:
                        with _log_lock:
                            save_log(_b._bsc_log)

                    # Execute any DCA triggers inline. Uses the REAL fill
                    # pipeline when auto-trading is ON and the signal has a
                    # live OKX order to anchor to; otherwise falls through to
                    # the PAPER simulator which runs the same math (blended
                    # avg, tp, sl, dca_count) without any exchange calls.
                    _dca_pending_sigs = [s for s in _open_snapshot
                                         if s.get("_dca_pending")]
                    if _dca_pending_sigs:
                        for _dsig in _dca_pending_sigs:
                            # Change 4: if live trading is ON but order_id is
                            # missing the entry order failed — skip DCA and log
                            # clearly rather than silently paper-simulating.
                            if cfg.get("trade_enabled") and not _dsig.get("order_id"):
                                # ── Fix: retroactively close legacy failed-entry signals ──
                                if _dsig.get("order_status") == "error":
                                    _dsig["status"] = "entry_failed"
                                _append_error(
                                    "trade",
                                    f"DCA skipped for {_dsig.get('symbol','?')} — "
                                    f"trade_enabled is ON but order_id is empty. "
                                    f"Entry order may have failed. Check Error Log.",
                                    symbol=_dsig.get("symbol", ""),
                                )
                                _dsig.pop("_dca_pending", None)
                                continue
                            _is_paper = (not cfg.get("trade_enabled")
                                         or not _dsig.get("order_id"))
                            if _is_paper:
                                _execute_dca_fill_paper(_dsig, cfg)
                            else:
                                _execute_dca_fill(_dsig, cfg)
                        with _log_lock:
                            save_log(_b._bsc_log)
                    else:
                        # No DCA to place — still persist closed trades
                        # (TP/SL) if any flipped during this tick.
                        if any(s.get("status") != "open"
                               for s in _open_snapshot):
                            with _log_lock:
                                save_log(_b._bsc_log)
            except Exception as e:
                _append_error("watcher", str(e))

            elapsed = time.time() - t0
            _b._bsc_watcher_last_ts  = int(time.time())
            _b._bsc_watcher_last_dur = round(elapsed, 2)
            sleep_sec = max(0, _watcher_min * 60 - elapsed)
            _b._bsc_watcher_event.wait(timeout=sleep_sec)
            _b._bsc_watcher_event.clear()
        except Exception as e:
            _append_error("watcher", f"outer loop: {e}")
            time.sleep(5)


# ─────────────────────────────────────────────────────────────────────────────
# Background scanner thread
# ─────────────────────────────────────────────────────────────────────────────
def _check_sl_circuit_breaker():
    """Return True if the last 3 closed trades are all sl_hit (triggers auto-pause)."""
    with _log_lock:
        _closed = sorted(
            [s for s in _b._bsc_log["signals"]
             if s.get("status") in ("tp_hit", "sl_hit") and s.get("close_time")],
            key=lambda x: x.get("close_time", ""),
        )
    return len(_closed) >= 3 and all(s["status"] == "sl_hit" for s in _closed[-3:])


def _bg_loop():
    while True:
        # Pause if user manually stopped OR circuit breaker fired (3 consec SL).
        # NOTE: When _bsc_sl_paused is True, the loop is fully idle — no
        # open-signal updates, no scans, no cycle logic. The flag is cleared
        # ONLY by the manual "▶️ Resume Scanning" button (see sidebar UI). TP
        # hits do NOT auto-resume the scanner.
        if not _scanner_running.is_set() or getattr(_b, "_bsc_sl_paused", False):
            time.sleep(2); continue
        with _config_lock:
            cfg = dict(_b._bsc_cfg)
        t0 = time.time()
        try:
            # ── Phase 1: Update open signals (candle fetch per open trade) ──
            # This is network-bound (0.3–0.8 s typical, up to several seconds
            # on slow connections). We intentionally run it OUTSIDE `_log_lock`
            # so the UI snapshot in the main render thread is never blocked
            # waiting for OKX. Signal dicts are shared references, so in-place
            # mutations performed by _update_one_signal are visible to the
            # main log automatically — no write-back required.
            # Safety: the only path that REPLACES the signals list wholesale
            # (Flush All / Clear 24h) holds `_log_lock` while doing so, so
            # taking a shallow copy under the lock here gives us a stable
            # snapshot even if the user flushes mid-cycle. Any dicts that
            # got removed from the live list won't be re-added — we only
            # mutate dict fields, never reassign the list.
            # When the watcher is ACTIVE (ticks faster than the main loop)
            # it takes over open-trade updates + DCA execution — skip this
            # block entirely to avoid duplicate candle fetches & double DCA.
            _watcher_min = int(cfg.get("watcher_minutes", 1) or 0)
            _loop_min    = int(cfg.get("loop_minutes", 5) or 5)
            _watcher_active = 0 < _watcher_min < _loop_min

            with _log_lock:
                _open_snapshot = [s for s in _b._bsc_log["signals"]
                                  if s.get("status") == "open"]
            if _open_snapshot and not _watcher_active:
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
                    # Real fill when auto-trading is ON AND the sig has a
                    # live OKX order_id to anchor to; otherwise paper sim.
                    for _dsig in _dca_pending_sigs:
                        # Change 4: if live trading is ON but order_id is
                        # missing the entry order failed — skip DCA and log
                        # clearly rather than silently paper-simulating.
                        if cfg.get("trade_enabled") and not _dsig.get("order_id"):
                            # ── Fix: retroactively close legacy failed-entry signals ──
                            if _dsig.get("order_status") == "error":
                                _dsig["status"] = "entry_failed"
                            _append_error(
                                "trade",
                                f"DCA skipped for {_dsig.get('symbol','?')} — "
                                f"trade_enabled is ON but order_id is empty. "
                                f"Entry order may have failed. Check Error Log.",
                                symbol=_dsig.get("symbol", ""),
                            )
                            _dsig.pop("_dca_pending", None)
                            continue
                        _is_paper = (not cfg.get("trade_enabled")
                                     or not _dsig.get("order_id"))
                        if _is_paper:
                            _execute_dca_fill_paper(_dsig, cfg)
                        else:
                            _execute_dca_fill(_dsig, cfg)
                    with _log_lock:
                        save_log(_b._bsc_log)

            # ── Circuit breaker: check AFTER updating signals so newly-closed
            # sl_hit trades are counted immediately ─────────────────────────
            if _check_sl_circuit_breaker():
                _b._bsc_sl_paused = True
                continue   # halt this cycle; UI will show the resume banner

            # ── Super Setup cap: count currently-open Super trades ───────────
            # and compute how many additional Super slots are available this cycle.
            with _log_lock:
                _current_supers = sum(
                    1 for s in _b._bsc_log["signals"]
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
                cooled_tp = {s["symbol"] for s in _b._bsc_log["signals"]
                             if s.get("close_time")
                             and s.get("status") == "tp_hit"
                             and datetime.fromisoformat(
                                 s["close_time"].replace("Z","+00:00")) >= tp_cutoff}
                # SL cooldown — long (hours, default 24), applies UNIVERSALLY
                # including to Super Setups. A stopped-out coin is blacklisted
                # for the full sl_cooldown_hours window regardless of what new
                # setup it might qualify for.
                cooled_sl = {s["symbol"] for s in _b._bsc_log["signals"]
                             if s.get("close_time")
                             and s.get("status") == "sl_hit"
                             and datetime.fromisoformat(
                                 s["close_time"].replace("Z","+00:00")) >= sl_cutoff}
                cooled = cooled_tp | cooled_sl
                active = {s["symbol"] for s in _b._bsc_log["signals"] if s["status"]=="open"}

            # ── Hours of operation gate ──────────────────────────────────────
            # If the user has enabled the hours filter, skip the new-signal
            # scan phase when we are outside the configured GST window.
            # Open-trade monitoring (Phase 1 above + watcher thread) is NOT
            # gated — it continues around the clock regardless of this setting.
            if cfg.get("scan_hour_enabled", False):
                _now_h   = dubai_now().hour
                _h_start = int(cfg.get("scan_hour_start", 0))
                _h_end   = int(cfg.get("scan_hour_end", 23))
                if _h_start <= _h_end:
                    _in_window = _h_start <= _now_h <= _h_end
                else:          # window crosses midnight (e.g. 22 → 06)
                    _in_window = _now_h >= _h_start or _now_h <= _h_end
                if not _in_window:
                    # Use the rescan event so a Save & Apply that changes the
                    # window re-checks immediately rather than waiting 60 s.
                    # If the event fires but we're still outside the window,
                    # the next loop cycle will hit this gate again straight away.
                    _rescan_event.wait(timeout=60)
                    _rescan_event.clear()
                    continue         # skip scan, keep loop alive

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
                            _b._bsc_log["signals"].append(queued_sig)
                            _queued_syms.append(sig["symbol"])
                            # Add to skip only for THIS cycle to prevent duplicate
                            # queue_limit entries within the same scan batch.
                            skip.add(sig["symbol"])
                        else:
                            # Snapshot trade + DCA config onto EVERY new signal,
                            # regardless of whether an OKX order will be placed.
                            # This guarantees paper-mode / credential-less signals
                            # still carry dca_enabled / dca_max / drop % so the
                            # Next DCA column and paper DCA simulator work.
                            _init_signal_trade_snapshot(sig, cfg)
                            _b._bsc_log["signals"].append(sig)
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
                    sig["order_id"]          = result.get("ordId", "")
                    sig["algo_id"]           = result.get("algoId", "")
                    sig["order_sz"]          = result.get("sz", 0)
                    sig["order_status"]      = result.get("status", "")
                    sig["order_error"]       = result.get("error", "")
                    # ── Fix: mark failed entries so watcher excludes them ──────
                    # Without this, order_id="" signals stay "open" forever and
                    # hit the DCA guard every tick in Demo/Live mode, permanently
                    # blocking DCA while paper mode works fine.
                    if result.get("status") == "error":
                        sig["status"] = "entry_failed"
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
                    # Cross mode TP algo id
                    sig["tp_algo_id"]        = result.get("tp_algo_id", "")
                    # Overwrite entry/TP/SL with actual fill values if available.
                    # Market orders fill at the live price, not the signal price —
                    # the OCO algo order was placed using these corrected levels.
                    if result.get("actual_entry"):
                        sig["entry"]        = result["actual_entry"]
                        sig["tp"]           = result["actual_tp"]
                        sig["sl"]           = result["actual_sl"]
                        sig["signal_entry"] = sig.get("entry")  # keep original for reference
                    # ── DCA state (re)initialization ────────────────────────
                    # The helper was already called once before append() with the
                    # scanner's signal price. Now that OKX has filled and
                    # (potentially) corrected `entry`, re-run it so fill 0 /
                    # avg_entry / original_entry reflect the ACTUAL execution
                    # price, and trade_usdt / trade_lev / order_margin_mode are
                    # left as-is (setdefault preserves the values set above from
                    # cfg / OKX result).
                    _init_signal_trade_snapshot(sig, cfg)
                    # ── Change 1: backfill sz into fill-0 so OCO contract
                    # count is exact on every future DCA for this trade.
                    if sig.get("dca_fills") and sig.get("order_sz"):
                        sig["dca_fills"][0]["sz"] = int(sig["order_sz"])
                    if result.get("tp_algo_id"):
                        sig["tp_algo_id"] = result["tp_algo_id"]
                    # ── OKX Command log — entry order ──────────────────────────
                    _env_tag = "DEMO" if sig.get("demo_mode") else "LIVE"
                    if result.get("status") in ("placed", "partial", "error"):
                        _okx_log_entry(
                            sig, f"ENTRY [{_env_tag}]",
                            instId   = _to_okx(sig.get("symbol", "")),
                            ordType  = "market",
                            tdMode   = sig.get("order_margin_mode", ""),
                            sz       = f"{sig.get('order_sz', 0)} contracts",
                            ordId    = result.get("ordId", "—"),
                            entry_px = result.get("actual_entry", sig.get("entry", "")),
                            status   = result.get("status", ""),
                        )
                    # ── OKX Command log — TP/OCO algo ─────────────────────────
                    _algo_placed = result.get("algoId", "") or result.get("tp_algo_id", "")
                    if _algo_placed:
                        _is_cross_log = (sig.get("order_margin_mode", "").strip().lower() == "cross")
                        if _is_cross_log:
                            _okx_log_entry(
                                sig, f"TP ALGO [{_env_tag}]",
                                ordType  = "conditional",
                                tpTriggerPx = result.get("actual_tp", sig.get("tp", "")),
                                tpTriggerPxType = "mark",
                                algoId   = _algo_placed,
                            )
                        else:
                            _okx_log_entry(
                                sig, f"OCO ALGO [{_env_tag}]",
                                ordType  = "oco",
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
        # Wait for sleep_sec, but wake immediately if Save & Apply triggers rescan
        _rescan_event.wait(timeout=sleep_sec)
        _rescan_event.clear()

def _ensure_scanner():
    if _b._bsc_thread is None or not _b._bsc_thread.is_alive():
        t = threading.Thread(target=_bg_loop, daemon=True, name="okx-scanner")
        t.start(); _b._bsc_thread = t
    # ── Start/restart the 1-minute open-trade watcher thread ─────────────────
    if _b._bsc_watcher_thread is None or not _b._bsc_watcher_thread.is_alive():
        wt = threading.Thread(target=_watcher_loop, daemon=True,
                              name="okx-watcher")
        wt.start(); _b._bsc_watcher_thread = wt

# ─────────────────────────────────────────────────────────────────────────────
# STREAMLIT UI
# ─────────────────────────────────────────────────────────────────────────────
st.set_page_config(page_title="S&R Crypto Intelligent Portal", page_icon="💎",
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

# ── Legacy-signal migration ─────────────────────────────────────────────────
# Runs on EVERY Streamlit script execution. The internal `_missing_dca` guard
# inside `_migrate_legacy_signals` ensures already-migrated signals are left
# untouched (the function only rewrites signals that still lack dca_enabled /
# dca_fills / original_entry). Running on every load means that as soon as
# the new code is loaded — even without a full Python restart — the next
# page view will backfill any pre-existing open signals.
try:
    with _log_lock, _config_lock:
        _migrated = _migrate_legacy_signals(_b._bsc_log, _b._bsc_cfg)
        if _migrated > 0:
            save_log(_b._bsc_log)
    _b._bsc_legacy_migration_last_count = _migrated
    _b._bsc_legacy_migration_last_ts    = dubai_now().isoformat()
    if _migrated > 0:
        _append_error(
            "loop",
            f"Legacy DCA snapshot migration: backfilled {_migrated} "
            f"open signal(s) with current sidebar DCA config.",
        )
except Exception as _mig_exc:
    # Never block page render over a migration error.
    try:
        _append_error("loop", f"Legacy migration failed: {_mig_exc}")
    except Exception:
        pass
    _b._bsc_legacy_migration_last_count = -1

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

    # ── Circuit Breaker Status ─────────────────────────────────────────────────
    _sl_paused = getattr(_b, "_bsc_sl_paused", False)
    if _sl_paused:
        st.warning(
            "🔴 **Paused — 3 consecutive SL hit**\n\n"
            "Review market conditions before resuming.",
            icon=None,
        )
        if st.button("▶️ Resume Scanning", key="resume_sl_circuit",
                     type="primary", use_container_width=True):
            # ONLY place in the codebase where _bsc_sl_paused is cleared back
            # to False (apart from fresh process startup). Auto-resume on TP
            # hit is intentionally disabled — the halt sticks until a human
            # reviews market conditions and clicks this button.
            _b._bsc_sl_paused = False
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
        help="Automatically place a market LONG order on OKX when a signal fires.")

    new_demo_mode = st.radio(
        "Environment", ["Demo", "Live"],
        index=0 if _snap_cfg.get("demo_mode", True) else 1,
        horizontal=True, key="cfg_demo_mode",
        help="Demo uses x-simulated-trading header. Live places real orders.",
        disabled=not new_trade_enabled)
    if new_trade_enabled and new_demo_mode == "Live":
        st.warning("⚠️ LIVE mode — real funds at risk!")

    # ── Credential source indicator ────────────────────────────────────────
    # Env vars (OKX_API_KEY / OKX_API_SECRET / OKX_API_PASSPHRASE) take
    # precedence over plaintext fields. Show the user which source is active
    # so they know whether their secrets are sitting in scanner_config.json.
    import os as _os_env
    _env_has = {
        "api_key":        bool(_os_env.environ.get("OKX_API_KEY", "").strip()),
        "api_secret":     bool(_os_env.environ.get("OKX_API_SECRET", "").strip()),
        "api_passphrase": bool(_os_env.environ.get("OKX_API_PASSPHRASE", "").strip()),
    }
    _any_env = any(_env_has.values())
    _all_env = all(_env_has.values())
    _has_plaintext = any(
        str(_snap_cfg.get(k, "")).strip() for k in ("api_key", "api_secret", "api_passphrase")
    )
    if _all_env:
        st.success(
            "🔐 All 3 credentials loaded from environment variables "
            "(`OKX_API_KEY`, `OKX_API_SECRET`, `OKX_API_PASSPHRASE`). "
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
            "`scanner_config.json`. Consider exporting `OKX_API_KEY`, "
            "`OKX_API_SECRET`, and `OKX_API_PASSPHRASE` as environment "
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
        placeholder=("Loaded from OKX_API_KEY env var" if _env_has["api_key"]
                     else "Your OKX API key"),
        help=(
            "OKX API key used to place and manage orders.\n\n"
            "Env-var override: `OKX_API_KEY` (preferred — not written to disk)."
        ))
    new_api_secret = st.text_input(
        "API Secret", value=_sec_display,
        key="cfg_api_secret", type="password",
        disabled=not new_trade_enabled or _env_has["api_secret"],
        placeholder=("Loaded from OKX_API_SECRET env var" if _env_has["api_secret"]
                     else "Your OKX API secret"),
        help=(
            "OKX API secret used to sign requests.\n\n"
            "Env-var override: `OKX_API_SECRET` (preferred — not written to disk)."
        ))
    new_api_passphrase = st.text_input(
        "API Passphrase", value=_pass_display,
        key="cfg_api_passphrase", type="password",
        disabled=not new_trade_enabled or _env_has["api_passphrase"],
        placeholder=("Loaded from OKX_API_PASSPHRASE env var" if _env_has["api_passphrase"]
                     else "Your OKX API passphrase"),
        help=(
            "OKX API passphrase (set when the key was created).\n\n"
            "Env-var override: `OKX_API_PASSPHRASE` (preferred — not written to disk)."
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
            "Leverage applied (capped by OKX max for each coin).\n\n"
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

    # ── DCA trigger drop percentages ─────────────────────────────────────────
    # Two fields, shown based on margin mode. Both are snapshotted onto the
    # signal at entry time so sidebar changes don't retroactively alter
    # already-open trades.
    if new_margin_mode == "isolated":
        # Fixed dropdown — % distance FROM liquidation price toward entry.
        # 10% = DCA fires close to liquidation (aggressive); 95% = fires
        # very close to entry (conservative).
        _iso_options = list(range(10, 100, 5))  # 10, 15, …, 95
        _cur_iso_pct = float(_snap_cfg.get("dca_iso_distance_pct", 70.0) or 70.0)
        # Snap the saved value to the nearest dropdown option so the widget
        # can always render (free-input legacy values get normalized).
        _cur_iso_int = int(round(_cur_iso_pct / 5.0) * 5)
        _cur_iso_int = max(10, min(95, _cur_iso_int))
        try:
            _cur_iso_idx = _iso_options.index(_cur_iso_int)
        except ValueError:
            _cur_iso_idx = _iso_options.index(70)
        new_dca_iso_distance_pct = float(st.selectbox(
            "DCA Trigger — Isolated (% distance from liquidation)",
            options=_iso_options,
            index=_cur_iso_idx,
            format_func=lambda v: f"{v}%",
            key="cfg_dca_iso_distance_pct",
            help=(
                "Only applies when Max DCA > 0 AND Margin Mode = Isolated.\n\n"
                "Meaning: DCA fires at the chosen % DISTANCE ABOVE the "
                "liquidation price (toward entry). Lower % = closer to "
                "liquidation (aggressive — you sit deep in the hole before "
                "averaging). Higher % = closer to entry (conservative — "
                "average down quickly on small drawdowns).\n\n"
                "Formula:\n"
                "  dca_drop %  = (1 / leverage) × (1 − chosen_pct / 100)\n"
                "  DCA price   = entry × (1 − dca_drop %)\n\n"
                "Example — Entry $100, 10× leverage → liquidation ≈ $90:\n"
                "  • 10% → DCA fires at $91   (10% above liq, toward entry)\n"
                "  • 50% → DCA fires at $95   (midpoint)\n"
                "  • 70% → DCA fires at $97   (70% above liq, toward entry)\n"
                "  • 95% → DCA fires at $99.50 (very close to entry)\n\n"
                "The chosen value is snapshotted onto each signal at entry "
                "time, so changing it later does not retroactively move the "
                "trigger on already-open trades."
            )))
        new_dca_cross_drop_pct = float(_snap_cfg.get("dca_cross_drop_pct", 7.0) or 7.0)
    else:
        _cur_cross_pct = float(_snap_cfg.get("dca_cross_drop_pct", 7.0) or 7.0)
        _cur_cross_pct = max(0.1, min(50.0, _cur_cross_pct))
        new_dca_cross_drop_pct = st.number_input(
            "DCA Trigger — Cross (% fixed drop below avg)",
            min_value=0.1, max_value=50.0,
            value=float(_cur_cross_pct), step=0.5,
            key="cfg_dca_cross_drop_pct",
            help=(
                "Only applies when Max DCA > 0 AND Margin Mode = Cross.\n\n"
                "DCA fires at a fixed percentage drop below the current "
                "blended average entry. Default 7%.\n\n"
                "Example: avg entry $100, drop % = 7 → DCA triggers at $93. "
                "After the fill, the next DCA uses the new (lower) blended "
                "avg × (1 - drop%) as its trigger.\n\n"
                "Higher values = wait for a deeper drawdown before adding."
            ))
        new_dca_iso_distance_pct = float(_snap_cfg.get("dca_iso_distance_pct", 70.0) or 70.0)

    # ── Fixed dollar TP/SL after DCA ──────────────────────────────────────────
    _cur_dca_tp_usd = float(_snap_cfg.get("dca_tp_usd", 0.50) or 0.50)
    new_dca_tp_usd = st.number_input(
        "DCA TP — Fixed $ profit target",
        min_value=0.10, max_value=50.0,
        value=float(max(0.10, min(50.0, _cur_dca_tp_usd))), step=0.10,
        key="cfg_dca_tp_usd",
        help=(
            "After any DCA fill (DCA-1 through max ladder), the TP is set to "
            "close the position at exactly this dollar profit.\n\n"
            "Formula: TP price = blended avg + (this value ÷ total coins held)\n\n"
            "Example: avg $0.0504, $400 notional (7,936 coins) →\n"
            "  TP = avg + ($0.50 ÷ 7,936) ≈ avg + $0.000063 per coin.\n\n"
            "Applies to BOTH paper and live modes. Default: $0.50."
        ))
    _cur_dca_sl_usd = float(_snap_cfg.get("dca_sl_usd", 5.00) or 5.00)
    new_dca_sl_usd = st.number_input(
        "DCA SL — Fixed $ loss limit",
        min_value=0.50, max_value=500.0,
        value=float(max(0.50, min(500.0, _cur_dca_sl_usd))), step=0.50,
        key="cfg_dca_sl_usd",
        help=(
            "After any DCA fill, the SL is set to close the position at this "
            "maximum dollar loss from the blended average. Fully replaces the "
            "percentage-based SL for DCA trades.\n\n"
            "Formula: SL price = blended avg − (this value ÷ total coins held)\n\n"
            "Example: avg $0.0504, $400 notional (7,936 coins) →\n"
            "  SL = avg − ($5.00 ÷ 7,936) ≈ avg − $0.000630 per coin.\n\n"
            "Priority: SL hit takes precedence over the next DCA trigger — the "
            "bot will NOT add more margin into a position whose loss has already "
            "reached this limit.\n\n"
            "Applies to BOTH paper and live modes. Default: $5.00."
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
    _isolated_active = _snap_cfg.get("trade_margin_mode", "isolated") == "isolated"
    new_sl = c2.number_input(
        "SL % (Cross only)", min_value=0.1, max_value=20.0, step=0.1,
        value=float(_snap_cfg["sl_pct"]), key="cfg_sl",
        disabled=_isolated_active,
        help=(
            "Applies only to CROSS margin trades.\n\n"
            "• Cross mode → SL = entry × (1 − SL%/100). "
            "After each DCA, SL = blended_avg × (1 − SL%/100), "
            "so the stop moves down with the new average.\n\n"
            "• Isolated mode → SL is pinned to the liquidation price "
            "(entry × (1 − 1/leverage)). This field is ignored. "
            "After each DCA, SL = blended_avg × (1 − 1/leverage), "
            "which equals the NEW liquidation of the averaged position.\n\n"
            "Final DCA rule (both modes): once the ladder is fully "
            "consumed, SL continues using the same formula as all earlier "
            "DCAs (blended_avg × (1 − sl_distance_pct)) and a breach "
            "routes the trade into the DCA SL Hit table."
        )
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

    # ── F5b: ATR(14) 15m TP-Reachability Filter ────────────────────────────────
    st.markdown("**📐 F5b — ATR Filter** (15m TP reachability)",
                help=(
                    "F5b — ATR(14) TP-Reachability Filter\n\n"
                    "Rejects coins whose TP target is too far relative to the current "
                    "Average True Range (ATR) on the 15m timeframe.\n\n"
                    "Formula: ratio = TP% / ATR%\n\n"
                    "  • Strict  (≤1.5×) — TP must be within 1.5× the ATR. "
                    "Only coins where the market regularly moves at least TP/1.5 per candle pass.\n"
                    "  • Normal  (≤2.0×) — TP within 2× ATR. Balanced setting.\n"
                    "  • Relaxed (≤3.0×) — TP within 3× ATR. Accepts coins with lower volatility.\n\n"
                    "A higher ratio means TP is harder to reach given current market movement. "
                    "ATR is computed on 15m candles (already fetched) — no extra API call."
                ))
    new_use_atr_filter = st.checkbox(
        "Enable F5b — ATR Filter",
        value=bool(_snap_cfg.get("use_atr_filter", False)), key="cfg_use_atr_filter")
    new_atr_mode = st.selectbox(
        "ATR Mode",
        options=["Strict", "Normal", "Relaxed"],
        index=["Strict", "Normal", "Relaxed"].index(_snap_cfg.get("atr_mode", "Normal")),
        key="cfg_atr_mode",
        disabled=not new_use_atr_filter,
        help="Strict=ratio≤1.5  ·  Normal=ratio≤2.0  ·  Relaxed=ratio≤3.0")
    _atr_thresh_disp = {"Strict": "≤1.5×", "Normal": "≤2.0×", "Relaxed": "≤3.0×"}.get(new_atr_mode, "≤2.0×")
    if new_use_atr_filter:
        st.caption(f"✅ ATR filter ON — {new_atr_mode} mode: TP%/ATR% {_atr_thresh_disp}")
    else:
        st.caption("⚫ ATR filter disabled")
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
            "Higher = gentler on OKX rate limits."
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
    # ── Open-Trade Watcher interval ──────────────────────────────────────────
    # Runs a dedicated short-interval thread that only updates OPEN trades
    # (1m candles, with tail/wick captured). When watcher < loop, the main
    # loop skips its open-trade update so there's no duplicate work.
    new_watcher_minutes = st.number_input(
        "Open-Trade Check Interval (min)", min_value=0, max_value=60, step=1,
        value=int(_snap_cfg.get("watcher_minutes", 1) or 0),
        key="cfg_watcher_minutes",
        help=(
            "How often the 1-minute watcher checks open trades for TP hits "
            "and DCA triggers, INDEPENDENTLY of the main scan.\n\n"
            "  • 0  — Disabled. The main scan loop handles everything (legacy).\n"
            "  • 1+ (but less than 'Loop (min)') — ACTIVE. A dedicated thread "
            "polls open trades every N minutes using 1-minute candles. This "
            "catches fast price spikes (via candle tail/wick) between main "
            "scans and fires DCA / TP much sooner.\n\n"
            "When ACTIVE, the main loop stops updating open trades itself — "
            "no duplicate fetches, no double DCA execution.\n"
            "When ≥ 'Loop (min)', this setting is ignored and the main loop "
            "handles opens as before."
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

    # ── Hours of Operation ────────────────────────────────────────────────────
    st.markdown("**🕐 Hours of Operation (GST)**",
                help=(
                    "Restrict new signal scanning to a specific time window "
                    "(Dubai / GST, UTC+4).\n\n"
                    "When enabled, the scanner pauses new signal detection "
                    "outside the defined window. Open-trade monitoring "
                    "(TP / SL / DCA) always runs 24/7 regardless of this setting.\n\n"
                    "Midnight-crossing windows are supported — e.g. Start 22, "
                    "End 06 means the scanner is active 22:00–23:59 and "
                    "00:00–06:00 GST."
                ))
    new_scan_hour_enabled = st.checkbox(
        "Enable hours of operation",
        value=bool(_snap_cfg.get("scan_hour_enabled", False)),
        key="cfg_scan_hour_enabled",
    )
    _sh1, _sh2 = st.columns(2)
    new_scan_hour_start = _sh1.number_input(
        "From (GST)", min_value=0, max_value=23, step=1,
        value=int(_snap_cfg.get("scan_hour_start", 0)),
        key="cfg_scan_hour_start",
        disabled=not new_scan_hour_enabled,
    )
    new_scan_hour_end = _sh2.number_input(
        "Until (GST)", min_value=0, max_value=23, step=1,
        value=int(_snap_cfg.get("scan_hour_end", 23)),
        key="cfg_scan_hour_end",
        disabled=not new_scan_hour_enabled,
    )
    if new_scan_hour_enabled:
        _s = int(new_scan_hour_start)
        _e = int(new_scan_hour_end)
        if _s == _e:
            st.caption("⚠️ Start and End are the same — scanner will only run during that single hour.")
        elif _s < _e:
            st.caption(f"Active: {_s:02d}:00 – {_e:02d}:59 GST")
        else:
            st.caption(f"Active: {_s:02d}:00 – 23:59 and 00:00 – {_e:02d}:59 GST (crosses midnight)")

    # ── Live gate status (reads from the RUNNING config, not the UI widgets) ──
    # This is the source of truth — it shows what the scanner thread actually
    # sees. If this says Disabled but your checkbox is ticked, you haven't
    # pressed Save & Apply yet.
    _live_hour_enabled = _snap_cfg.get("scan_hour_enabled", False)
    _live_h_start      = int(_snap_cfg.get("scan_hour_start", 0))
    _live_h_end        = int(_snap_cfg.get("scan_hour_end", 23))
    _live_now_h        = dubai_now().hour
    _live_now_str      = dubai_now().strftime("%H:%M GST")
    if not _live_hour_enabled:
        st.info(f"⚪ Gate: **Disabled** — scanner runs 24/7  ·  Now: {_live_now_str}")
    else:
        if _live_h_start <= _live_h_end:
            _live_in_window = _live_h_start <= _live_now_h <= _live_h_end
        else:
            _live_in_window = _live_now_h >= _live_h_start or _live_now_h <= _live_h_end
        if _live_in_window:
            st.success(
                f"🟢 Gate: **IN WINDOW** — scanning active  ·  "
                f"Now: {_live_now_str}  ·  "
                f"Window: {_live_h_start:02d}:00 – {_live_h_end:02d}:59"
            )
        else:
            st.warning(
                f"🔴 Gate: **BLOCKED** — scan suppressed  ·  "
                f"Now: {_live_now_str}  ·  "
                f"Window: {_live_h_start:02d}:00 – {_live_h_end:02d}:59"
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
        with _filter_lock:
            _filter_counts["flushed_at"] = time.time()
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
            "use_atr_filter":      bool(new_use_atr_filter),
            "atr_mode":            new_atr_mode,
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
            "watcher_minutes":    max(0, min(60, int(new_watcher_minutes))),
            "scan_hour_enabled":  bool(new_scan_hour_enabled),
            "scan_hour_start":    int(new_scan_hour_start),
            "scan_hour_end":      int(new_scan_hour_end),
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
            "dca_iso_distance_pct": max(10.0, min(95.0, float(new_dca_iso_distance_pct))),
            "dca_cross_drop_pct":   max(0.1, min(50.0,  float(new_dca_cross_drop_pct))),
            "dca_tp_usd":           max(0.10, min(50.0,  float(new_dca_tp_usd))),
            "dca_sl_usd":           max(0.50, min(500.0, float(new_dca_sl_usd))),
        }
        with _config_lock: _b._bsc_cfg.clear(); _b._bsc_cfg.update(new_cfg)
        save_config(new_cfg)
        # Invalidate symbol cache if watchlist changed
        if new_wl != _snap_cfg.get("watchlist", []):
            _b._bsc_symbol_cache["fetched_at"] = 0
        _b._bsc_rescan_event.set()   # wake bg thread immediately — no waiting for next cycle
        _b._bsc_watcher_event.set()  # wake watcher so new watcher_minutes applies right away
        st.success(f"✅ Saved — {len(new_wl)} coins — rescanning now…"); st.rerun()

# ─────────────────────────────────────────────────────────────────────────────
# MAIN AREA
# ─────────────────────────────────────────────────────────────────────────────
_CODE_UPDATED = "27 Apr 2026  23:30 GST"
st.title(f"S&R — Crypto Intelligent Portal   ·   🕐 {_CODE_UPDATED}")

# ── Total Realized PnL computation ─────────────────────────────────────────────
# Moved above the account summary box so _total_pnl is available for the
# Realized PnL metric card. Display banner (st.markdown) remains below.
# Sums realized PnL across every closed signal (tp_hit + sl_hit + dca_sl_hit):
#   • DCA trades  → (close / avg_entry − 1) × total_notional
#   • Non-DCA     → (close / entry     − 1) × (trade_usdt × trade_lev)
def _pnl_topline(sig: dict, usdt_fb: float, lev_fb: int):
    try:
        _close = float(sig.get("close_price") or 0)
    except (TypeError, ValueError):
        return None
    if _close <= 0:
        return None
    _dcn = int(sig.get("dca_count", 0) or 0)
    if _dcn > 0:
        try:
            _avg = float(sig.get("avg_entry", 0) or 0)
            _tnl = float(sig.get("total_notional", 0) or 0)
        except (TypeError, ValueError):
            _avg, _tnl = 0.0, 0.0
        if _avg > 0 and _tnl > 0:
            return (_close / _avg - 1.0) * _tnl
    try:
        _ent = float(sig.get("entry", 0) or 0)
        _usd = float(sig.get("trade_usdt", usdt_fb) or 0)
        _lev = int(sig.get("trade_lev", lev_fb) or 0)
    except (TypeError, ValueError):
        return None
    if _ent <= 0 or _usd <= 0 or _lev <= 0:
        return None
    return (_close / _ent - 1.0) * (_usd * _lev)

_total_pnl       = 0.0
_total_pnl_wins  = 0.0
_total_pnl_loss  = 0.0
_total_tp_ct     = 0
_total_sl_ct     = 0
_total_dca_sl_ct = 0
_cfg_usdt_fb_top = float(_snap_cfg.get("trade_usdt_amount", 0) or 0)
_cfg_lev_fb_top  = int(_snap_cfg.get("trade_leverage", 10) or 0)
for _s in signals:
    if _s.get("status") not in ("tp_hit", "sl_hit", "dca_sl_hit"):
        continue
    _v = _pnl_topline(_s, _cfg_usdt_fb_top, _cfg_lev_fb_top)
    if _v is None:
        continue
    _total_pnl += _v
    if _s["status"] == "tp_hit":
        _total_pnl_wins += _v
        _total_tp_ct    += 1
    elif _s["status"] == "dca_sl_hit":
        _total_pnl_loss  += _v
        _total_dca_sl_ct += 1
    else:
        _total_pnl_loss += _v
        _total_sl_ct    += 1

# ── OKX Account Summary ────────────────────────────────────────────────────────
_acct_has_creds = bool(
    _snap_cfg.get("api_key") and _snap_cfg.get("api_secret")
    and _snap_cfg.get("api_passphrase")
)
if _acct_has_creds:
    try:
        _bal_resp  = _trade_get("/api/v5/account/balance", {"ccy": "USDT"}, _snap_cfg)
        # Fetch realized PnL directly from OKX settled position history.
        # limit=100 covers the most recent closed positions (~last 3 months).
        _hist_resp = _trade_get("/api/v5/account/positions-history",
                                {"instType": "SWAP", "limit": "100"}, _snap_cfg)
        if _bal_resp.get("code") == "0":
            _bal_d   = _bal_resp["data"][0]
            _bal_det = _bal_d.get("details", [{}])[0]
            _avail   = float(_bal_det.get("availBal", 0) or 0)
            _tot_eq  = float(_bal_d.get("totalEq",   0) or 0)
            _invested = _tot_eq - _avail
            _upl      = float(_bal_det.get("upl",    0) or 0)
            _upl_sign = "+" if _upl >= 0 else ""
            # Sum OKX-settled realizedPnl across returned position records.
            _realized = 0.0
            if _hist_resp.get("code") == "0":
                for _hp in _hist_resp.get("data", []):
                    _realized += float(_hp.get("realizedPnl", 0) or 0)
            _rpnl_sign = "+" if _realized >= 0 else ""
            _ac1, _ac2, _ac3, _ac4 = st.columns(4)
            _ac1.metric("💰 Available",      f"${_avail:,.2f} USDT")
            _ac2.metric("📊 Invested",       f"${_invested:,.2f} USDT")
            _ac3.metric("📈 Unrealized PnL", f"${_upl_sign}{_upl:,.2f} USDT")
            _ac4.metric("💵 Realized PnL",   f"${_rpnl_sign}{_realized:,.2f} USDT")
    except Exception:
        pass

# ── Total Realized PnL banner ──────────────────────────────────────────────
# _pnl_topline() and accumulation loop moved above the account summary box
# so _total_pnl is available for the Realized PnL metric card.
# Display banner only — computation already done above.
_closed_total = _total_tp_ct + _total_sl_ct + _total_dca_sl_ct
if _closed_total == 0:
    _total_color = "#9CA3AF"
    _total_prefix = "💼"
    _total_sub    = "no closed trades yet"
else:
    _total_color  = "#22C55E" if _total_pnl >= 0 else "#EF4444"
    _total_prefix = "💼" if _total_pnl >= 0 else "📉"
    _total_sub    = (
        f"{_closed_total} closed trades  ·  "
        f"{_total_tp_ct} TP (+\\${_total_pnl_wins:,.2f})  |  "
        f"{_total_sl_ct} SL  |  "
        f"{_total_dca_sl_ct} DCA-SL (\\${_total_pnl_loss:,.2f})"
    )

st.markdown(
    f"<div style='padding:10px 14px; margin:4px 0 10px 0; "
    f"border:1px solid {_total_color}33; border-radius:10px; "
    f"background:linear-gradient(90deg, {_total_color}14, transparent);'>"
    f"<span style='font-size:0.95em; opacity:0.75;'>{_total_prefix} Total Realized PnL</span><br>"
    f"<span style='font-size:2.2em; font-weight:700; color:{_total_color};'>"
    f"{_total_pnl:+,.2f} $</span>  "
    f"<span style='opacity:0.75; font-size:0.9em; margin-left:8px;'>{_total_sub}</span>"
    f"</div>",
    unsafe_allow_html=True,
)

last_scan = health.get("last_scan_at", "never")
if last_scan and last_scan != "never":
    try:
        ts       = datetime.fromisoformat(last_scan.replace("Z","+00:00"))
        ts_dubai = to_dubai(ts)
        ago      = int((dubai_now()-ts_dubai).total_seconds()/60)
        last_scan = f"{ago}m ago  ({ts_dubai.strftime('%H:%M')} GST)"
    except Exception: pass

col_h1, col_h2, col_h3 = st.columns([3, 1, 1])
col_h1.caption(f"Last scan: {last_scan}   |   🕐 Dubai / GST (UTC+4)")
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

# ── Capital requirement summary ────────────────────────────────────────────────
_cap_base      = float(_snap_cfg.get("trade_usdt_amount", 5.0))
_cap_dca_max   = int(_snap_cfg.get("trade_max_dca", 3))
_cap_pool      = int(_snap_cfg.get("max_open_trades", 7))
_cap_per_trade = _cap_base * (2 ** _cap_dca_max)   # original + all DCA adds
_cap_minimum   = _cap_per_trade * _cap_pool
_cap_buffer    = _cap_minimum * 0.25
_cap_total     = _cap_minimum + _cap_buffer
st.markdown(
    f"<p style='color:#4da6ff;font-weight:700;margin:2px 0;font-size:14px;'>"
    f"💰 Minimum Required: <span style='font-size:15px'>${_cap_minimum:,.2f} USDT</span>"
    f"&nbsp;&nbsp;|&nbsp;&nbsp;"
    f"🛡️ Buffer (25%): <span style='font-size:15px'>${_cap_buffer:,.2f} USDT</span>"
    f"&nbsp;&nbsp;|&nbsp;&nbsp;"
    f"✅ Total Recommended: <span style='font-size:15px'>${_cap_total:,.2f} USDT</span>"
    f"</p>",
    unsafe_allow_html=True,
)

# ── Health metrics ─────────────────────────────────────────────────────────────
open_count     = sum(1 for s in signals if s["status"]=="open")
tp_count       = sum(1 for s in signals if s["status"]=="tp_hit")
sl_count       = sum(1 for s in signals if s["status"]=="sl_hit")
dca_sl_count   = sum(1 for s in signals if s["status"]=="dca_sl_hit")
fc_count       = sum(1 for s in signals if s["status"]=="fc_hit")
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

m1,m2,m3,m4,m5c,m6,m7,m8,m8b,m9 = st.columns(10)
m1.metric("Cycles",        health.get("total_cycles",0))
m2.metric("Scan Time",     f"{health.get('last_scan_duration_s',0)}s")
m3.metric("API Errors",    health.get("total_api_errors",0))
m4.metric("Pre-filtered ⚡", pre_out, help="Coins removed by bulk ticker pre-filter (saves API calls)")
m5c.metric("Deep Scanned", deep_sc, help="Coins that passed pre-filter and received full candle analysis")
m6.metric("Open",          open_count,  help=f"Active open trades (max {_max_open_cap} allowed simultaneously — configurable in sidebar)")
m7.metric("TP Hit ✅",     tp_count)
m8.metric("SL Hit ❌",     sl_count,   help="Regular SL hits (non-DCA trades, or DCA disabled)")
m8b.metric("DCA-SL ❌",    dca_sl_count, help="Ladder-exhausted SL hits — DCA trade closed at final SL (blended avg × (1 − sl_distance_pct)) after max DCAs were consumed")
m9.metric("⏳ Queued",     queue_count, help=f"Signals detected while the {_max_open_cap}-trade limit was reached — no order placed, coin rescanned each cycle")

if getattr(_b, "_bsc_last_error", ""):
    st.warning(f"⚠️ {_b._bsc_last_error}")

# ── Active config + filter panel ───────────────────────────────────────────────
def _cfg_panel(cfg: dict) -> str:
    """Render a full configuration + active-filter summary as HTML."""
    _c = cfg  # shorthand

    # ── helpers ────────────────────────────────────────────────────────────────
    def _pill(text, active=True):
        bg  = "#1f3d5c" if active else "#21262d"
        col = "#79c0ff" if active else "#8b949e"
        return (f"<span style='display:inline-block;font-size:11px;"
                f"padding:2px 9px;border-radius:12px;margin:2px 3px 2px 0;"
                f"background:{bg};color:{col};font-weight:500'>{text}</span>")

    def _pill_off(text):
        return _pill(text, active=False)

    def _kv_cell(label, value, highlight=False):
        vc = "#79c0ff" if highlight else "#e6edf3"
        return (f"<td style='padding:6px 8px 6px 0;vertical-align:top;white-space:nowrap;'>"
                f"<span style='font-size:11px;color:#8b949e'>{label}</span><br>"
                f"<span style='font-size:13px;font-weight:500;color:{vc}'>{value}</span></td>")

    def _section(title):
        return (f"<tr><td colspan='20' style='padding:10px 0 4px 0;"
                f"font-size:11px;font-weight:500;color:#8b949e;"
                f"border-top:1px solid #30363d;letter-spacing:0.05em'>"
                f"{title}</td></tr>")

    # ── collect values ─────────────────────────────────────────────────────────
    margin   = str(_c.get("trade_margin_mode", "isolated")).capitalize()
    lev      = int(_c.get("trade_leverage", 10))
    usdt     = float(_c.get("trade_usdt_amount", 5.0))
    tp_pct   = float(_c.get("tp_pct", 1.2))
    sl_pct   = float(_c.get("sl_pct", 3.0))
    max_open = int(_c.get("max_open_trades", 15))
    max_sup  = int(_c.get("max_super_trades", 1))
    demo     = bool(_c.get("demo_mode", True))
    loop_m   = int(_c.get("loop_minutes", 4))
    cooldown = int(_c.get("cooldown_minutes", 2))
    sl_cool  = float(_c.get("sl_cooldown_hours", 6))

    dca_en   = bool(_c.get("dca_enabled", True))
    dca_max  = int(_c.get("trade_max_dca", 4))
    iso_dist = float(_c.get("dca_iso_distance_pct", 70.0))
    cross_d  = float(_c.get("dca_cross_drop_pct", 7.0))
    pdz_buf  = tp_pct   # buffer = tp_pct (Change #1)

    # liq distance for isolated
    liq_pct  = round(1.0 / lev * 100, 1)
    dca_trig = round((1.0 / lev) * (iso_dist / 100.0) * 100, 2)

    # ── filter badge lists ─────────────────────────────────────────────────────
    filter_pills = []
    def _fpill(text, on): filter_pills.append(_pill(text, on))
    _fpill(f"F2 PDZ 15m",                       _c.get("use_pdz_15m", True))
    _fpill(f"F3 PDZ 5m",                        _c.get("use_pdz_5m",  True))
    _fpill(f"F4 RSI5m ≥{_c.get('rsi_5m_min',30)}",  _c.get("use_rsi_5m",  True))
    _fpill(f"F5 RSI1h {_c.get('rsi_1h_min',30)}–{_c.get('rsi_1h_max',95)}", _c.get("use_rsi_1h", True))
    _fpill(f"F5b ATR {_c.get('atr_mode','Normal')}", _c.get("use_atr_filter", False))
    _fpill(f"F6 EMA{_c.get('ema_period_3m',12)} 3m",  _c.get("use_ema_3m"))
    _fpill(f"F6 EMA{_c.get('ema_period_5m',12)} 5m",  _c.get("use_ema_5m"))
    _fpill(f"F6 EMA{_c.get('ema_period_15m',12)} 15m", _c.get("use_ema_15m"))
    _macd_on = [tf for tf, k in [("3m","use_macd_3m"),("5m","use_macd_5m"),("15m","use_macd_15m")] if _c.get(k, True)]
    _fpill(f"F7 MACD {' · '.join(_macd_on) if _macd_on else 'off'}", bool(_macd_on))
    _sar_on  = [tf for tf, k in [("3m","use_sar_3m"),("5m","use_sar_5m"),("15m","use_sar_15m")] if _c.get(k, True)]
    _fpill(f"F8 SAR {' · '.join(_sar_on) if _sar_on else 'off'}", bool(_sar_on))
    _fpill(f"F9 Vol ≥{_c.get('vol_spike_mult',2.0)}× / {_c.get('vol_spike_lookback',20)}",
           _c.get("use_vol_spike"))
    _fpill(f"F10 EMA{_c.get('ema_cross_fast_15m',12)}>EMA{_c.get('ema_cross_slow_15m',21)} 15m",
           _c.get("use_ema_cross_15m", True))

    # ── build HTML ─────────────────────────────────────────────────────────────
    _mode_col = "#f85149" if demo else "#3fb950"
    _mode_lbl = "DEMO" if demo else "LIVE"
    _html = (
        f"<div style='background:#161b22;border:1px solid #30363d;"
        f"border-radius:8px;padding:14px 18px;margin-bottom:12px;'>"
        f"<div style='display:flex;align-items:center;gap:10px;margin-bottom:10px;'>"
        f"<span style='font-size:13px;font-weight:500;color:#e6edf3'>Scanner Config</span>"
        f"<span style='font-size:11px;font-weight:500;padding:2px 8px;"
        f"border-radius:10px;background:{_mode_col}22;color:{_mode_col}'>{_mode_lbl}</span>"
        f"</div>"
        f"<div style='overflow-x:auto;-webkit-overflow-scrolling:touch;'>"
        f"<table style='border-collapse:collapse;min-width:100%'><tbody>"
    )

    # Row 1 — Trade setup
    _html += _section("TRADE SETUP")
    _html += "<tr>"
    _html += _kv_cell("Margin",        margin)
    _html += _kv_cell("Leverage",      f"{lev}×",        highlight=True)
    _html += _kv_cell("Trade Size",    f"${usdt} USDT")
    _html += _kv_cell("TP",            f"{tp_pct}%",     highlight=True)
    _html += _kv_cell("SL (cross)",    f"{sl_pct}%")
    _html += _kv_cell("Liq distance",  f"{liq_pct}%")
    _html += _kv_cell("Max open",      str(max_open))
    _html += _kv_cell("Max super",     str(max_sup))
    _html += _kv_cell("Loop",          f"{loop_m} min")
    _html += _kv_cell("Cooldown",      f"{cooldown} min")
    _html += _kv_cell("SL cooldown",   f"{sl_cool}h")
    _html += "</tr>"

    # Row 2 — DCA setup
    _html += _section("DCA SETUP")
    _html += "<tr>"
    _dca_col = "#3fb950" if dca_en else "#8b949e"
    _html += _kv_cell("DCA enabled",   f"<span style='color:{_dca_col}'>"
                                        f"{'YES' if dca_en else 'NO'}</span>")
    _html += _kv_cell("Max DCAs",      str(dca_max),     highlight=True)
    _html += _kv_cell("Trigger (iso)", f"{iso_dist}%",   highlight=True)
    _html += _kv_cell("Trigger drop",  f"{dca_trig}% below avg entry")
    _html += _kv_cell("Trigger (cross)", f"{cross_d}% drop")
    _html += _kv_cell("PDZ buffer",    f"{pdz_buf}% (= TP%)")
    _html += "</tr>"

    # Row 3 — Active filters
    _html += _section("ACTIVE FILTERS")
    _html += f"<tr><td colspan='20' style='padding:4px 0 2px 0'>{''.join(filter_pills)}</td></tr>"

    _html += "</tbody></table></div></div>"
    return _html

st.markdown(_cfg_panel(_snap_cfg), unsafe_allow_html=True)

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

def _fmt_px_auto(px) -> str:
    """Auto-precision price formatter — 4/6/8 decimals depending on magnitude."""
    try:
        p = float(px or 0)
    except (TypeError, ValueError):
        return "—"
    if p <= 0:
        return "—"
    if p >= 1:      return f"{p:.4f}"
    if p >= 0.01:   return f"{p:.6f}"
    return f"{p:.8f}"


def _fmt_trade_history(s: dict) -> str:
    """Render a multi-line Trade History string for the Open / TP Hit /
    SL Hit / DCA SL Hit tables.

    Format (one line per fill — Entry then DCA 1, DCA 2, …):

        Entry : $<price> | TP $<tp> | SL $<sl> | MM/DD HH:MM
        DCA 1 : $<price> | TP $<tp> | SL $<sl> | MM/DD HH:MM
        DCA 2 : $<price> | TP $<tp> | SL $<sl> | MM/DD HH:MM

    Past fills that were recorded BEFORE we started storing per-fill TP/SL
    show the abbreviated form (price + time only). Newer fills show the
    full Entry/TP/SL/time line.

    Returns "—" if there's nothing to show (no fills AND no entry data).
    """
    fills = s.get("dca_fills") or []
    lines: list = []

    if fills:
        _last_idx = len(fills) - 1
        for i, f in enumerate(fills):
            idx   = int(f.get("dca_idx", 0) or 0)
            label = "Entry" if idx == 0 else f"DCA {idx}"
            px_s  = _fmt_px_auto(f.get("price", 0))
            ts_s  = fmt_dubai(f.get("ts", "")) or "—"
            tp_v  = f.get("tp")
            sl_v  = f.get("sl")
            # Fallback ONLY for the most recent fill: sig["tp"]/sig["sl"]
            # reflect the state immediately after this fill (no later DCAs
            # have rewritten them yet), so they're accurate for this row.
            # Older fills can't borrow from sig — the values would be wrong.
            if i == _last_idx:
                if tp_v is None or _fmt_px_auto(tp_v) == "—":
                    tp_v = s.get("tp")
                if sl_v is None or _fmt_px_auto(sl_v) == "—":
                    sl_v = s.get("sl")
            _has_tp = tp_v is not None and _fmt_px_auto(tp_v) != "—"
            _has_sl = sl_v is not None and _fmt_px_auto(sl_v) != "—"
            if _has_tp and _has_sl:
                lines.append(
                    f"{label:<6}: ${px_s} | TP ${_fmt_px_auto(tp_v)} | "
                    f"SL ${_fmt_px_auto(sl_v)} | {ts_s}"
                )
            else:
                # Pre-migration fill with no reliable TP/SL — price + time only.
                lines.append(f"{label:<6}: ${px_s} | {ts_s}")
        return "\n".join(lines)

    # No dca_fills array at all (legacy signal, never snapshotted). Fall
    # back to sig-level entry/tp/sl for a single-line display.
    _ent = s.get("original_entry") or s.get("signal_entry") or s.get("entry")
    _tp  = s.get("tp")
    _sl  = s.get("sl")
    _ts  = fmt_dubai(s.get("timestamp", "")) or "—"
    if _ent:
        _px_s = _fmt_px_auto(_ent)
        if _tp and _sl and _fmt_px_auto(_tp) != "—" and _fmt_px_auto(_sl) != "—":
            return f"Entry : ${_px_s} | TP ${_fmt_px_auto(_tp)} | SL ${_fmt_px_auto(_sl)} | {_ts}"
        return f"Entry : ${_px_s} | {_ts}"
    return "—"


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
            leverage). Prefers the value OKX actually used; falls back to
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
        "fc_hit":      "🟣 FC Hit",
        "queue_limit": "⏳ Queue Limit",
        "closed_okx":  "🟠 Closed on OKX",
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
    elif status in ("tp_hit", "sl_hit", "dca_sl_hit", "fc_hit") and _dca_count_row > 0:
        # Preserve the DCA-N tag on closed DCA trades so TP Hit / DCA SL Hit /
        # FC Hit tables show it clearly.
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
    if status in ("open", "tp_hit", "sl_hit", "dca_sl_hit", "fc_hit"):
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
            # tp_hit / sl_hit / dca_sl_hit / fc_hit — close_price is the realized exit
            _ref_pnl = s.get("close_price")
        # Use the shared helper — single source of truth for PnL math.
        _pnl_val = _calc_pnl_usd(s, _ref_pnl, _cfg_usdt_fallback, _cfg_lev_fallback)
        if _pnl_val is not None:
            pnl_col = f"{_pnl_val:+.2f} $"

    # ── Exit % column (TP Hit / SL Hit / DCA SL Hit / FC Hit) ─────────────────
    # Signed percentage of close_price vs the effective entry (blended avg for
    # DCA trades, original entry otherwise).
    #   Positive = closed above entry (TP or FC)  Negative = closed below (SL)
    exit_pct_col = "—"
    if status in ("tp_hit", "sl_hit", "dca_sl_hit", "fc_hit"):
        try:
            _close_ep  = float(s.get("close_price", 0) or 0)
            _ref_ep    = float(s.get("avg_entry", 0) or 0) if _dca_count_row > 0 \
                         else float(s.get("signal_entry", s.get("entry", 0)) or 0)
            if _close_ep > 0 and _ref_ep > 0:
                _ep_pct = (_close_ep - _ref_ep) / _ref_ep * 100.0
                exit_pct_col = f"{_ep_pct:+.2f}%"
        except (TypeError, ValueError):
            exit_pct_col = "—"

    # ── FC Trigger Price column ───────────────────────────────────────────────
    # Displayed in Open Signals (so user can see the FC close target while live)
    # and in FC Hit (showing what price actually closed the trade).
    # For non-FC, non-open signals this column is "—".
    _fc_px_raw  = s.get("fc_trigger_px")
    fc_trig_col = "—"
    if _fc_px_raw:
        try:
            _fcp = float(_fc_px_raw)
            if _fcp > 0:
                fc_trig_col = _fmt_px_auto(_fcp)
                # For FC Hit, also annotate the delta above avg_entry
                if status == "fc_hit":
                    _avg_fc_disp = float(s.get("avg_entry", 0) or 0)
                    if _avg_fc_disp > 0:
                        _fc_delta = (_fcp - _avg_fc_disp) / _avg_fc_disp * 100.0
                        fc_trig_col += f"  (+{_fc_delta:.3f}%)"
        except (TypeError, ValueError):
            fc_trig_col = "—"

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
        f"• EMA21 15m : {_cv(crit.get('ema_cross_21_15m'))}\n"
        f"• ATR 15m   : {_cv(crit.get('atr_15m'))}\n"
        f"• ATR ratio : {_cv(crit.get('atr_ratio'))}"
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
    _is_cross_ord = (s.get("order_margin_mode") or
                     _snap_cfg.get("trade_margin_mode","isolated")).strip().lower() == "cross"
    if   ord_status == "placed":
        ord_status_str = (f"✅ Entry+TP {ord_env}" if _is_cross_ord
                          else f"✅ Entry+OCO {ord_env}")
    elif ord_status == "partial": ord_status_str = f"⚠️ Entry only {ord_env} · {ord_err[:80]}"
    elif ord_status == "error":   ord_status_str = f"❌ {ord_err[:80]}" if ord_err else "❌ Error"
    else:                         ord_status_str = "—"

    # ── OKX Command column ────────────────────────────────────────────────────
    # Prefer the structured okx_log list (appended at every real OKX call).
    # Fall back to the legacy static string for older signals that pre-date
    # the log (no okx_log key or empty list).
    _okx_log_list = s.get("okx_log")
    if isinstance(_okx_log_list, list) and _okx_log_list:
        okx_cmd_str = "\n########\n".join(_okx_log_list)
    else:
        # ── Legacy fallback: reconstruct a single summary line ─────────────
        _ord_sz       = int(s.get("order_sz", 0) or 0)
        _ct_val       = float(s.get("order_ct_val", 0) or 0)
        _notional     = float(s.get("order_notional", 0) or 0)
        _is_hedge     = bool(s.get("order_is_hedge", False))
        _order_id     = (s.get("order_id") or "").strip()
        _ord_status   = (s.get("order_status") or "").strip()
        _order_sent   = bool(_order_id) or _ord_status in ("placed", "partial", "error")
        _margin_mode  = (s.get("order_margin_mode") or "").strip().lower()
        if _margin_mode not in ("isolated", "cross"):
            _margin_mode = (_snap_cfg.get("trade_margin_mode") or "isolated").strip().lower()
        if not _notional and _usdt > 0 and _lev > 0:
            _notional = _usdt * _lev
        if status == "queue_limit":
            okx_cmd_str = "No order placed — Queue Limit"
        elif not _order_sent:
            okx_cmd_str = "No order placed — auto-trading off or rejected pre-flight"
        elif _usdt > 0 or _ord_sz > 0:
            _sym_okx = _to_okx(s.get("symbol", ""))
            _ps_part = " | posSide: long" if _is_hedge else ""
            _ct_part = f" | ctVal: {_ct_val}" if _ct_val else ""
            okx_cmd_str = (
                f"instId: {_sym_okx} | ordType: market"
                f" | tdMode: {_margin_mode}{_ps_part}"
                f" | sz: {_ord_sz} contracts"
                f" | collateral: ${_usdt:.2f} | lev: {_lev}×"
                f" | notional: ${_notional:.2f}{_ct_part}"
            )
        else:
            okx_cmd_str = "—"

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
    # When `order_margin_mode` is present on the signal, we show the value OKX
    # actually used. When it's missing (auto-trading OFF at the time the
    # signal fired, creds missing, or `place_okx_order` short-circuited before
    # any request was sent), we fall back to the CURRENT sidebar setting so
    # this column stays consistent with the OKX Command column and the active
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
    # sent to OKX (`order_notional`), then per-signal trade_usdt × trade_lev
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

    # ── Current Price column (Open Signals only) ─────────────────────────────
    # Live latest close, refreshed every Open-Trade Check cycle by
    # `_update_one_signal` (main loop) and `_watcher_update_one_signal`
    # (1-minute watcher) — both write `sig["latest_price"] = candles[-1].close`
    # on every scan. Auto-precision formatting so small-value coins show
    # enough digits.
    current_price_col = "—"
    if status == "open":
        _latest_px = s.get("latest_price")
        if _latest_px is not None:
            try:
                _lp = float(_latest_px)
                if _lp > 0:
                    if _lp >= 1:
                        current_price_col = f"{_lp:.4f}"
                    elif _lp >= 0.01:
                        current_price_col = f"{_lp:.6f}"
                    else:
                        current_price_col = f"{_lp:.8f}"
            except (TypeError, ValueError):
                pass

    # ── DCA Levels column (all tables when DCA is enabled) ──────────────────
    # Shows the full pre-calculated DCA ladder for every signal — all levels,
    # past and future — so it acts as both a forecast and a history column.
    # ✅ = already triggered   ⏳ = still pending
    dca_levels_col = "—"
    _dca_en_lv  = bool(s.get("dca_enabled", False))
    _dca_max_lv = int(s.get("dca_max", 0) or 0)
    if _dca_en_lv and _dca_max_lv > 0:
        _ladder_lv  = s.get("dca_ladder") or []
        _count_lv   = int(s.get("dca_count", 0) or 0)
        _mode_lv    = (s.get("order_margin_mode") or
                       _snap_cfg.get("trade_margin_mode", "isolated") or
                       "isolated").strip().lower()
        if _ladder_lv:
            # Build a map: DCA level → actual fill price from dca_fills.
            # dca_fills[0] = entry fill; dca_fills[N] = DCA-N fill.
            _fills_lv    = s.get("dca_fills") or []
            _fill_px_map = {
                _fi: float(_ff.get("price", 0) or 0)
                for _fi, _ff in enumerate(_fills_lv)
                if _fi > 0 and float(_ff.get("price", 0) or 0) > 0
            }
            _lines = []
            for _item in _ladder_lv:
                _lvl        = int(_item.get("level", 0))
                _planned_px = float(_item.get("trigger_px", 0) or 0)
                if _planned_px <= 0:
                    continue
                _filled   = _lvl <= _count_lv
                _actual   = _fill_px_map.get(_lvl, 0)
                # Filled levels: show actual market fill price (append planned in
                # parentheses so history is preserved).  Pending: show planned.
                if _filled and _actual > 0:
                    _icon = "✅"
                    _px   = _actual
                    def _fmt(p):
                        return f"{p:.4f}" if p >= 1 else (f"{p:.6f}" if p >= 0.01 else f"{p:.8f}")
                    _lines.append(f"{_icon} DCA-{_lvl}: {_fmt(_actual)} (plan {_fmt(_planned_px)})")
                    continue
                else:
                    _icon = "✅" if _filled else "⏳"
                    _px   = _planned_px
                if _px >= 1:
                    _px_str = f"{_px:.4f}"
                elif _px >= 0.01:
                    _px_str = f"{_px:.6f}"
                else:
                    _px_str = f"{_px:.8f}"
                _lines.append(f"{_icon} DCA-{_lvl}: {_px_str}")
            dca_levels_col = "\n".join(_lines) if _lines else "—"
        elif _mode_lv == "cross":
            # Legacy signal without stored ladder — compute on the fly
            _cdrop_lv = float(s.get("dca_cross_drop_pct",
                                    _snap_cfg.get("dca_cross_drop_pct", 7.0)) or 7.0)
            _tp_lv    = float(_snap_cfg.get("tp_pct", 1.5) or 1.5)
            _base_lv  = float(s.get("trade_usdt", _snap_cfg.get("trade_usdt_amount", 5)) or 5)
            _lev_lv   = int(s.get("trade_lev", _snap_cfg.get("trade_leverage", 10)) or 10)
            _entry_lv = float(s.get("original_entry", s.get("entry", 0)) or 0)
            if _entry_lv > 0:
                try:
                    _ladder_fly = _calc_cross_dca_ladder(
                        _entry_lv, _base_lv, _lev_lv, _dca_max_lv, _cdrop_lv, _tp_lv)
                    _lines = []
                    for _item in _ladder_fly:
                        _lvl = int(_item.get("level", 0))
                        _px  = float(_item.get("trigger_px", 0) or 0)
                        if _px <= 0:
                            continue
                        _icon = "✅" if _lvl <= _count_lv else "⏳"
                        if _px >= 1:
                            _px_str = f"{_px:.4f}"
                        elif _px >= 0.01:
                            _px_str = f"{_px:.6f}"
                        else:
                            _px_str = f"{_px:.8f}"
                        _lines.append(f"{_icon} DCA-{_lvl}: {_px_str}")
                    dca_levels_col = "\n".join(_lines) if _lines else "—"
                except Exception:
                    dca_levels_col = "—"

    # ── Next DCA price column (Open Signals only) ────────────────────────────
    # Shows the price at which the NEXT DCA add would trigger, using the
    # signal's own snapshotted drop percentages (isolated / cross) so the
    # value is consistent with what the watcher/main loop will actually use.
    # Displayed ONLY when:
    #   • Trade is still open
    #   • DCA is enabled on the signal (dca_enabled=True)
    #   • Ladder has slots left (dca_count < dca_max)
    # Otherwise shown as "—".
    next_dca_col = "—"
    if status == "open":
        _dca_en_row  = bool(s.get("dca_enabled", False))
        _dca_max_row = int(s.get("dca_max", 0) or 0)
        if _dca_en_row and _dca_max_row > 0 and _dca_count_row < _dca_max_row:
            try:
                # Prefer the stored sig["next_dca_px"] (set on each DCA
                # fill + at signal open). Fall back to live computation
                # for legacy signals that predate the field.
                _next_dca_px = float(s.get("next_dca_px", 0) or 0)
                if _next_dca_px <= 0:
                    _next_dca_px = _dca_compute_trigger(s, _snap_cfg)
                if _next_dca_px and _next_dca_px > 0:
                    # Auto-precision formatting matches Est Liquidity so low-
                    # value coins (BASEDUSDT, etc.) show enough digits.
                    if _next_dca_px >= 1:
                        next_dca_col = f"{_next_dca_px:.4f}"
                    elif _next_dca_px >= 0.01:
                        next_dca_col = f"{_next_dca_px:.6f}"
                    else:
                        next_dca_col = f"{_next_dca_px:.8f}"
                    # Append the ladder progress (e.g. "DCA 2/3") for context.
                    next_dca_col = (
                        f"{next_dca_col} "
                        f"(DCA {_dca_count_row + 1}/{_dca_max_row})"
                    )
            except Exception:
                next_dca_col = "—"

    # ── Trade History column ────────────────────────────────────────────────
    # Multi-line lifecycle: Entry line + one line per DCA fill. Each line
    # shows price | TP | SL | time (going-forward; past DCAs w/o stored
    # TP/SL show price | time only). Appears in Open / TP Hit / SL Hit /
    # DCA SL Hit tables — closed trades retain the full ladder history.
    try:
        trade_history_col = _fmt_trade_history(s)
    except Exception:
        trade_history_col = "—"

    # ── Difficulty column (Open Signals only) ────────────────────────────────
    # Ratio = (remaining TP distance %) / (ATR% at entry time)
    # Uses atr_ratio stored in criteria at signal open. Difficulty reflects
    # how far price still needs to travel vs. typical 15m market movement:
    #   🟢 Easy   — ratio ≤ 1.5  (TP within 1.5× ATR — market moves this far routinely)
    #   🟡 Medium — ratio ≤ 2.5  (moderate stretch — achievable but needs a push)
    #   🔴 Hard   — ratio >  2.5 (TP far beyond typical volatility)
    diff_col = "—"
    if status == "open":
        _atr_ratio_s = crit.get("atr_ratio")
        _entry_d = float(s.get("entry", 0) or 0)
        _tp_d    = float(s.get("tp",    0) or 0)
        _latest  = s.get("latest_price")
        try:
            # Try to use live ratio (remaining TP% vs stored ATR%)
            # Fall back to the stored atr_ratio if live price unavailable
            if _atr_ratio_s not in (None, "—") and _latest is not None and _entry_d > 0:
                _latest_f  = float(_latest)
                _rem_tp_pct = ((_tp_d - _latest_f) / _latest_f * 100.0) if _latest_f > 0 else None
                # Reconstruct ATR% from stored ratio and TP% at entry
                _tp_pct_orig = ((_tp_d - _entry_d) / _entry_d * 100.0) if _entry_d > 0 else None
                _ratio_orig  = float(_atr_ratio_s)
                _atr_pct_orig = (_tp_pct_orig / _ratio_orig) if (_tp_pct_orig and _ratio_orig > 0) else None
                if _rem_tp_pct is not None and _atr_pct_orig and _atr_pct_orig > 0:
                    _live_ratio = _rem_tp_pct / _atr_pct_orig
                    if _live_ratio <= 1.5:   diff_col = "🟢 Easy"
                    elif _live_ratio <= 2.5: diff_col = "🟡 Medium"
                    else:                    diff_col = "🔴 Hard"
            elif _atr_ratio_s not in (None, "—"):
                _r = float(_atr_ratio_s)
                if _r <= 1.5:   diff_col = "🟢 Easy"
                elif _r <= 2.5: diff_col = "🟡 Medium"
                else:           diff_col = "🔴 Hard"
        except (TypeError, ValueError):
            diff_col = "—"

    if is_open_table:
        # Open-Signals-specific column order — adds Difficulty as first column
        # and Original Entry after Signal Entry for DCA trade visibility.
        # TP Hit / SL Hit / DCA SL Hit / Queue Limit tables use the non-Open
        # branch below.
        row: dict = {
            "Difficulty":        diff_col,
            "Time (GST)":        ts_str,
            "Symbol":            s.get("symbol", ""),
            "Alert":             alert_col,
            "Setup":             setup_type,
            "PnL $":             pnl_col,
            "Margin Mode":       _mm_val,
            "Current Status":    current_status_col,
            "Signal Entry":      _sig_entry_display,
            "Original Entry":    _orig_entry_display,
            "Current Price":     current_price_col,
            "DCA Levels":        dca_levels_col,
            "Next DCA":          next_dca_col,
            "FC Trigger Price":  fc_trig_col,
            "TP":                s.get("tp", ""),
            "SL":                s.get("sl", ""),
            "Trade History":     trade_history_col,
            "Est Liquidity":     est_liq_col,
            "Duration":          duration_str,
            "TP $":              tp_usd_str,
            "SL $":              sl_usd_str,
            "Status":            status_icon,
            "Close Time":        close_str,
            "Sector":            s.get("sector", "Other"),
            "Close $":           s.get("close_price") or "—",
            "Max Lev":           f"{max_lev}×",
            "Order":             ord_status_str,
            "OKX Command":       okx_cmd_str,
            "Entry Criteria":    crit_str,
            "Order ID":          ord_id_str,
            "Algo ID":           algo_id_str,
            "⚠️ SL Reason":     sl_reason,
            "Order Size":        order_size_col,
        }
        return row

    # ── Non-Open tables (TP Hit, SL Hit, FC Hit, Queue Limit) ───────────────
    # FC Hit gets a tailored column set:
    #   • FC Trigger Price column added (the price that actually closed the trade)
    #   • TP $ renamed to "Max TP $" (planned TP never reached — label clarifies)
    #   • SL $ removed (trade wasn't stopped out)
    #   • ⚠️ SL Reason removed (not applicable)
    #   • Exit % added (actual close % above avg entry)
    #   • Difficulty added (post-trade insight)
    _is_fc = (status == "fc_hit")
    row = {
        "Time (GST)":     ts_str,
        "Alert":          alert_col,
    }
    row["Symbol"]     = s.get("symbol", "")
    row["Difficulty"] = diff_col
    row["Setup"]      = setup_type
    if show_pnl:
        row["Margin Mode"] = _mm_val
    row["Sector"] = s.get("sector", "Other")
    if show_pnl:
        row["PnL $"] = pnl_col
    if show_pnl:
        row["Exit %"] = exit_pct_col
    row.update({
        "DCA Levels":        dca_levels_col,
        "Signal Entry":      _sig_entry_display,
        "Original Entry":    _orig_entry_display,
        "Fill $":            s.get("entry", "") if s.get("signal_entry") else "—",
        "TP":                s.get("tp", ""),
        "Max TP $" if _is_fc else "TP $": tp_usd_str,
        "Trade History":     trade_history_col,
        "Status":            status_icon,
        "Duration":          duration_str,
        "Close Time":        close_str,
        "Close $":           s.get("close_price") or "—",
        "Max Lev":           f"{max_lev}×",
        "Order":             ord_status_str,
        "OKX Command":       okx_cmd_str,
        "Order ID":          ord_id_str,
        "Algo ID":           algo_id_str,
        "Entry Criteria":    crit_str,
    })
    # FC Hit: show FC Trigger Price; include SL/SL$/SL Reason for all others
    if _is_fc:
        row["FC Trigger Price"] = fc_trig_col
    else:
        row["SL"]             = s.get("sl", "")
        row["SL $"]           = sl_usd_str
        row["⚠️ SL Reason"]  = sl_reason
    # Order Size for trades that had real positions (TP/SL/FC/DCA SL)
    if show_pnl:
        row["Order Size"] = order_size_col
    return row

# Shared column_config used by all four tables
_SIG_COL_CFG = {
    "Difficulty":     st.column_config.TextColumn(
                          "🎯 Difficulty", width="small",
                          help="TP reachability relative to ATR(14) on 15m at entry time.\n\n"
                               "🟢 Easy   — TP within 1.5× ATR (market moves this far routinely)\n"
                               "🟡 Medium — TP within 2.5× ATR (achievable with a good push)\n"
                               "🔴 Hard   — TP beyond 2.5× ATR (needs unusually strong move)\n\n"
                               "Updates live: ratio is recomputed from remaining TP distance "
                               "vs. the ATR% measured at entry, so it gets easier as price "
                               "moves toward TP. Shows '—' when ATR was not computed (ATR "
                               "filter disabled at entry time)."),
    "DCA Levels":     st.column_config.TextColumn(
                          "📊 DCA Levels", width="medium",
                          help=(
                               "Pre-calculated DCA trigger prices for all levels.\n\n"
                               "✅ = already triggered   ⏳ = pending\n\n"
                               "Prices are exact — calculated at entry time from the "
                               "blended average cascade. Cross margin only; shows '—' "
                               "for isolated mode or when DCA is disabled.")),
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
                               "  1. `order_notional` from the actual OKX "
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
                               "or the order short-circuited before any OKX "
                               "call). The value shown is the CURRENT sidebar "
                               "setting — NOT a confirmation of what was sent "
                               "to OKX (nothing was).\n\n"
                               "Rows without the ⚠️ reflect what OKX actually "
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
    "Current Price":  st.column_config.TextColumn(
                          "💹 Current Price", width="small",
                          help="Live latest close for the symbol, refreshed "
                               "on every Open-Trade Check cycle — both the "
                               "main loop and the 1-minute watcher write "
                               "`latest_price` onto the signal from the most "
                               "recent 1m candle close.\n\n"
                               "Compare against Next DCA to see how close the "
                               "next DCA trigger is. Precision auto-scales "
                               "(4/6/8 decimals) so low-value coins still "
                               "show enough digits to read meaningfully.\n\n"
                               "Shows \"—\" only on the very first cycle after "
                               "a signal fires, before any candle fetch has "
                               "populated the field."),
    "Next DCA":       st.column_config.TextColumn(
                          "🪜 Next DCA", width="medium",
                          help="Price at which the NEXT DCA add will trigger "
                               "on this trade, plus the ladder slot "
                               "(e.g. \"DCA 2/3\" means the upcoming fire "
                               "will be the 2nd DCA of a max-3 ladder).\n\n"
                               "Computed from the blended average and the "
                               "% snapshotted onto the signal at entry:\n"
                               "  • Isolated → `dca_iso_distance_pct` = "
                               "% DISTANCE ABOVE LIQUIDATION toward entry. "
                               "Higher = conservative (fires near entry), "
                               "lower = aggressive (fires near liq).\n"
                               "     e.g. entry $100 · 10× · 70% → $97\n"
                               "  • Cross    → `dca_cross_drop_pct` % fixed "
                               "drop below avg.\n\n"
                               "Shown only when DCA is enabled on the trade "
                               "and ladder slots remain (dca_count < dca_max). "
                               "Otherwise \"—\"."),
    "FC Trigger Price": st.column_config.TextColumn(
                          "🟣 FC Trigger", width="medium",
                          help="Force-Close trigger price set on OKX after a DCA fill.\n\n"
                               "Formula: avg_entry + ($0.50 / total_contracts / ct_val)\n\n"
                               "This is the price at which the OKX conditional algo will close "
                               "the position, locking in at least +$0.50 above breakeven.\n\n"
                               "In Open Signals: shows the live target (update each DCA).\n"
                               "In FC Hit: shows the price that triggered the close, with "
                               "the % above avg entry in parentheses.\n\n"
                               "Shows '—' for non-DCA trades and isolated-mode trades."),
    "Exit %":           st.column_config.TextColumn(
                          "📊 Exit %", width="small",
                          help="Signed % of close_price vs effective entry price.\n\n"
                               "  • Positive → closed above entry (TP or FC hit)\n"
                               "  • Negative → closed below entry (SL hit)\n\n"
                               "Entry reference:\n"
                               "  • DCA trades  → blended avg_entry (reflects actual cost)\n"
                               "  • Non-DCA     → signal_entry (actual market fill)\n\n"
                               "Useful for spotting consistent TP distance vs SL distance "
                               "across your trade history."),
    "Max TP $":         st.column_config.TextColumn(
                          "🎯 Max TP $", width="small",
                          help="Dollar gain that WOULD have been realized at the planned TP "
                               "price — this target was NOT reached (the trade was closed "
                               "earlier by the FC mechanism).\n\n"
                               "Compare this against PnL $ to see how much of the planned "
                               "gain was captured by the FC close vs what was left on the table."),
    "Fill $":           st.column_config.NumberColumn(format="%.8f",
                          help="Actual market fill price (may differ from signal entry)"),
    "Trade History":  st.column_config.TextColumn(
                          "📜 Trade History", width="large",
                          help="Full trade lifecycle — one line per fill:\n\n"
                               "  • Entry : ${price} | TP ${tp} | SL ${sl} | {time}\n"
                               "  • DCA N : ${price} | TP ${tp} | SL ${sl} | {time}\n\n"
                               "Each DCA add is appended on a new line with the "
                               "TP/SL that were in effect AFTER that add (post-"
                               "recompute). Past DCAs recorded before this "
                               "feature landed show only price and time — no "
                               "fabricated TP/SL values.\n\n"
                               "Closed trades (TP Hit / SL Hit / DCA SL Hit) "
                               "retain the full ladder history for after-action "
                               "review."),
    "TP":             st.column_config.NumberColumn(format="%.8f"),
    "TP $":           st.column_config.TextColumn(width="small"),
    "SL":             st.column_config.NumberColumn(format="%.8f"),
    "SL $":           st.column_config.TextColumn(width="small"),
    "Duration":       st.column_config.TextColumn(width="small"),
    "Close Time":     st.column_config.TextColumn(width="small"),
    "Max Lev":        st.column_config.TextColumn(width="small"),
    "Order":          st.column_config.TextColumn(width="medium"),
    "OKX Command":    st.column_config.TextColumn(
                          "OKX Command",
                          width="large",
                          help="Exact parameters sent to OKX when the order was placed. "
                               "Collateral = your USDT setting. "
                               "Notional = collateral × leverage (this is what OKX shows as position size)."),
    "Order ID":       st.column_config.TextColumn(width="medium"),
    "Algo ID":        st.column_config.TextColumn(width="medium"),
    "Entry Criteria": st.column_config.TextColumn(width="medium"),
    "⚠️ SL Reason":  st.column_config.TextColumn(width="medium"),
}

def _style_alert_cell(val) -> str:
    """Return CSS for the Alert column: orange + bold when the cell
    contains a DCA tag (e.g. "DCA-1", "DCA-2/3"). Otherwise no styling.

    Case-insensitive substring match — catches "DCA-N", "DCA N", "DCA
    X/Y filled", etc.
    """
    try:
        if val is None:
            return ""
        if "dca" in str(val).lower():
            return "color: #FF8C00; font-weight: 700;"
    except Exception:
        pass
    return ""


def _style_pnl_cell(val) -> str:
    """Return CSS for the PnL $ column: green for positive, red for negative."""
    try:
        if val is None or str(val).strip() in ("—", "", "N/A"):
            return ""
        s = str(val).strip()
        if s.startswith("-"):
            return "color: #EF4444; font-weight: 600;"   # red
        if s.startswith("+"):
            return "color: #22C55E; font-weight: 600;"   # green
    except Exception:
        pass
    return ""


def _render_sig_table(sig_list: list, header: str, empty_msg: str,
                      auto_height: bool = False, is_open_table: bool = False,
                      show_pnl: bool = False, scroll_height: int = None,
                      use_expander: bool = False, expander_open: bool = True,
                      show_header: bool = True):
    """Render a signal table.

    Parameters
    ----------
    use_expander  : wrap the whole table in a collapsed st.expander whose label
                    is the header string + row count.  The internal ### markdown
                    heading is suppressed to avoid duplication with the expander
                    label.  expander_open controls whether it starts expanded.
    show_header   : when False, suppresses the ### markdown heading.  Useful
                    when the caller wraps the table in its own st.expander and
                    wants to avoid a redundant heading inside.
    """
    rows = [_build_signal_row(s, is_open_table=is_open_table, show_pnl=show_pnl)
            for s in sig_list]

    def _draw_table_content():
        # Heading is shown in non-expander mode (unless explicitly suppressed).
        if show_header and not use_expander:
            st.markdown(f"### {header} ({len(rows)})")
        if rows:
            # Wrap the rows in a pandas DataFrame so we can apply Styler to
            # color the Alert cell orange+bold when it contains DCA text.
            # Fall back to plain dict rendering if pandas styling fails.
            try:
                import pandas as _pd
                _df = _pd.DataFrame(rows)
                _styled = _df.style
                if "Alert" in _df.columns:
                    _styled = _styled.applymap(_style_alert_cell, subset=["Alert"])
                if "PnL $" in _df.columns:
                    _styled = _styled.applymap(_style_pnl_cell, subset=["PnL $"])
                _render_obj = _styled
            except Exception:
                _render_obj = rows

            if scroll_height is not None:
                # Fixed-height scrollable table — internal vertical scroll bar,
                # the rest of the page stays still.
                st.dataframe(_render_obj, use_container_width=True,
                             hide_index=True,
                             height=scroll_height,
                             column_config=_SIG_COL_CFG)
            elif auto_height:
                # Expand so ALL rows are visible without internal scrolling.
                st.dataframe(_render_obj, use_container_width=True,
                             hide_index=True,
                             height=len(rows) * 35 + 48,
                             column_config=_SIG_COL_CFG)
            else:
                st.dataframe(_render_obj, use_container_width=True,
                             hide_index=True,
                             column_config=_SIG_COL_CFG)
        else:
            st.info(empty_msg)

    if use_expander:
        # Expander label carries the count so it's visible while collapsed.
        with st.expander(f"{header} ({len(rows)})", expanded=expander_open):
            _draw_table_content()
    else:
        _draw_table_content()

# ── Signal tables — auto-refresh fragment ──────────────────────────────────────
# Wrapped in @st.fragment(run_every=30) so Streamlit re-renders ONLY this
# section every 30 seconds without touching the sidebar or any other widget.
# Requires Streamlit ≥ 1.37. Falls back gracefully on older versions.
@st.fragment(run_every=30)
def _signal_tables_fragment():
    # Re-read fresh data on every fragment run (every 30 s)
    with _log_lock:
        _frag_log = json.loads(json.dumps(_b._bsc_log))
    with _config_lock:
        _frag_cfg = dict(_b._bsc_cfg)

    _frag_signals       = _frag_log.get("signals", [])
    _frag_sector        = st.session_state.get("sector_filter", "All")

    # ── Filter by sector then split into four status buckets ─────────────────
    filtered = _frag_signals if _frag_sector == "All" else \
               [s for s in _frag_signals if s.get("sector") == _frag_sector]
    filtered_sorted = sorted(filtered, key=lambda x: x.get("timestamp", ""), reverse=True)

    _open_sigs       = [s for s in filtered_sorted if s.get("status") == "open"]
    _tp_sigs         = [s for s in filtered_sorted if s.get("status") == "tp_hit"]
    _sl_sigs         = [s for s in filtered_sorted if s.get("status") == "sl_hit"]
    _dca_sl_sigs     = [s for s in filtered_sorted if s.get("status") == "dca_sl_hit"]
    _fc_sigs         = [s for s in filtered_sorted if s.get("status") == "fc_hit"]
    _queue_sigs      = [s for s in filtered_sorted if s.get("status") == "queue_limit"]
    _closed_okx_sigs = [s for s in filtered_sorted if s.get("status") == "closed_okx"]

    # ── Table 1: Open Signals ───────────────────────────────────────────────────────
    # ── Open Signals table with row selection + Force Close ─────────────────────
    st.markdown(f"### 🔵 Open Signals ({len(_open_sigs)})")
    if _open_sigs:
        try:
            import pandas as _pd
            _open_rows = [_build_signal_row(s, is_open_table=True, show_pnl=True)
                          for s in _open_sigs]
            _open_df   = _pd.DataFrame(_open_rows)
            _open_styled = _open_df.style
            if "Alert" in _open_df.columns:
                _open_styled = _open_styled.applymap(_style_alert_cell, subset=["Alert"])
            if "PnL $" in _open_df.columns:
                _open_styled = _open_styled.applymap(_style_pnl_cell, subset=["PnL $"])
            _open_render = _open_styled
        except Exception:
            _open_rows   = [_build_signal_row(s, is_open_table=True, show_pnl=True)
                            for s in _open_sigs]
            _open_render = _open_rows

        _open_event = st.dataframe(
            _open_render,
            use_container_width=True,
            hide_index=True,
            height=450,
            column_config=_SIG_COL_CFG,
            selection_mode="single-row",
            on_select="rerun",
            key="open_signals_table",
        )

        # ── Force Close action bar — appears when a row is selected ──────────
        _sel_rows = (_open_event.selection.rows
                     if _open_event and hasattr(_open_event, "selection")
                     else [])
        # Guard: the fragment reruns every 30 s and _open_sigs may shrink
        # (signals close) while Streamlit still holds the old selection index.
        if _sel_rows and _sel_rows[0] < len(_open_sigs):
            _fc_sig   = _open_sigs[_sel_rows[0]]
            _fc_sym   = _fc_sig.get("symbol", "?")
            _fc_entry = float(_fc_sig.get("avg_entry") or _fc_sig.get("entry") or 0)
            _fc_price = float(_fc_sig.get("latest_price") or _fc_entry or 0)
            _fc_upnl  = ((_fc_price - _fc_entry) / _fc_entry * 100
                         if _fc_entry > 0 else 0)
            _fc_mode  = ("📄 Paper" if not _fc_sig.get("order_id") else
                         "🟡 Demo"  if _fc_sig.get("demo_mode") else "🔴 Live")
            _fc_color = "🟢" if _fc_upnl >= 0 else "🔴"
            _fca1, _fca2, _fca3 = st.columns([3, 2, 2])
            with _fca1:
                st.info(f"**{_fc_sym}** selected  ·  {_fc_mode}")
            with _fca2:
                st.markdown(f"{_fc_color} uPnL: `{_fc_upnl:+.2f}%`")
            with _fca3:
                if st.button("⚡ Force Close", type="primary",
                             key="fc_execute", use_container_width=True):
                    with st.spinner(f"Closing {_fc_sym}…"):
                        _fc_result = _force_close_position(_fc_sig, _frag_cfg)
                    if _fc_result["success"]:
                        st.success(_fc_result["message"])
                        st.rerun()
                    else:
                        st.error(_fc_result["message"])
        else:
            st.caption("👆 Click a row to select it, then use ⚡ Force Close.")
    else:
        st.info("No open signals right now.")

    # ── OKX Live Positions (inline, auto-fetch) ────────────────────────────────────
    # Shown only when auto-trading is ON. Fetches on every render using a dedicated
    # session key so it always reflects the current OKX state independently of the
    # manual-refresh expander panel below.
    _inline_trade_on = _snap_cfg.get("trade_enabled", False)
    _inline_has_creds = bool(
        _snap_cfg.get("api_key") and _snap_cfg.get("api_secret")
        and _snap_cfg.get("api_passphrase")
    )
    if _inline_trade_on and _inline_has_creds:
        # Auto-fetch on every render into a dedicated key.
        try:
            _inline_pos_resp = _trade_get(
                "/api/v5/account/positions", {"instType": "SWAP"}, _snap_cfg
            )
            st.session_state["okx_inline_pos_data"] = _inline_pos_resp
            st.session_state["okx_inline_pos_ts"] = dubai_now().strftime(
                "%d %b %Y  %H:%M:%S GST"
            )
        except Exception as _inline_exc:
            st.session_state["okx_inline_pos_data"] = None
            _append_error("trade", f"Inline positions fetch failed: {_inline_exc}",
                          endpoint="/api/v5/account/positions")

        _inline_pos_data = st.session_state.get("okx_inline_pos_data")
        _inline_pos_ts   = st.session_state.get("okx_inline_pos_ts", "")
        _env_inline      = "🟡 Demo" if _snap_cfg.get("demo_mode", True) else "🔴 Live"

        st.markdown(
            f"**📡 OKX Positions (Live)**"
            f"<span style='font-size:0.8em; color:gray; margin-left:12px;'>"
            f"auto-fetched · {_inline_pos_ts} · {_env_inline}</span>",
            unsafe_allow_html=True,
        )

        if _inline_pos_data is None:
            st.warning("⚠️ Could not fetch OKX positions — check Error Log.")
        elif _inline_pos_data.get("code") != "0":
            st.error(f"OKX error: {_inline_pos_data.get('msg', 'Unknown error')}")
        else:
            _inline_positions = [
                p for p in _inline_pos_data.get("data", [])
                if float(p.get("pos", 0) or 0) != 0
            ]

            # Build set of open signal symbols for the Match column.
            _open_sig_syms = {s.get("symbol", "") for s in _open_sigs}

            if _inline_positions:
                _inline_rows = []
                _ghost_syms  = []   # on OKX but no matching open signal
                for _p in _inline_positions:
                    _inst   = _p.get("instId", "")
                    # Normalise OKX instId (BTC-USDT-SWAP) → signal symbol (BTCUSDT)
                    _sym_norm = _inst.replace("-USDT-SWAP", "USDT").replace("-", "")
                    _upnl     = float(_p.get("upl",      0) or 0)
                    _upnl_pct = float(_p.get("uplRatio", 0) or 0) * 100
                    _liq_px   = float(_p.get("liqPx",    0) or 0)
                    _mgn_mode = _p.get("mgnMode", "cross")
                    _margin   = float(_p.get("margin", 0) or 0) or float(_p.get("imr", 0) or 0)
                    _matched  = _sym_norm in _open_sig_syms or _inst in _open_sig_syms
                    if not _matched:
                        _ghost_syms.append(_inst)
                    _inline_rows.append({
                        "Symbol":     _inst,
                        "Contracts":  int(float(_p.get("pos", 0) or 0)),
                        "Avg Entry":  float(_p.get("avgPx",       0) or 0),
                        "Mark Price": float(_p.get("markPx",      0) or 0),
                        "Unreal PnL": round(_upnl, 4),
                        "PnL %":      f"{_upnl_pct:+.2f}%",
                        "Leverage":   f"{_p.get('lever', '')}×",
                        "Liq Price":  _liq_px if _liq_px > 0 else "—",
                        "Mode":       _mgn_mode.capitalize(),
                        "Match":      "✅" if _matched else "⚠️ no signal",
                    })

                st.dataframe(
                    _inline_rows,
                    use_container_width=True,
                    hide_index=True,
                    height=len(_inline_rows) * 35 + 48,
                    column_config={
                        "Avg Entry":  st.column_config.NumberColumn(format="%.6f"),
                        "Mark Price": st.column_config.NumberColumn(format="%.6f"),
                        "Liq Price":  st.column_config.NumberColumn(format="%.6f"),
                        "Unreal PnL": st.column_config.NumberColumn(
                                          "Unreal PnL $", format="%.4f"),
                    },
                )

                # Warn for signals that have no OKX position.
                _sig_no_pos = [
                    s.get("symbol", "") for s in _open_sigs
                    if s.get("symbol", "") not in _open_sig_syms - {
                        _p2.replace("-USDT-SWAP", "USDT").replace("-", "")
                        for _p2 in [_r["Symbol"] for _r in _inline_rows]
                    }
                ]
                # Simpler: find open signals whose symbol has no OKX position row.
                _okx_norm_syms = {
                    r["Symbol"].replace("-USDT-SWAP", "USDT").replace("-", "")
                    for r in _inline_rows
                }
                _unmatched_sigs = [
                    s.get("symbol", "") for s in _open_sigs
                    if s.get("symbol", "") not in _okx_norm_syms
                ]
                if _ghost_syms:
                    st.caption(
                        f"⚠️ {len(_ghost_syms)} OKX position(s) have no matching open signal: "
                        + ", ".join(_ghost_syms)
                    )
                if _unmatched_sigs:
                    st.caption(
                        f"⚠️ {len(_unmatched_sigs)} open signal(s) have no matching OKX position: "
                        + ", ".join(_unmatched_sigs)
                    )
            else:
                st.info("No open SWAP positions on OKX right now.")

    st.divider()

    # ── OKX Closed Positions history (single fetch, shared by TP + SL tables) ──────
    # Fetched once per render when auto-trading is ON; stored in two session-state
    # keys (_okx_tp_hist / _okx_sl_hist) split by close reason so each table can
    # render independently without a second API call.
    #   type "2" / "4"       → TP / partial-TP  → fulfilled orders table
    #   type "1" / "5" / "3" → SL / liquidated / manual → SL closed table
    _hist_trade_on   = _snap_cfg.get("trade_enabled", False)
    _hist_has_creds  = bool(
        _snap_cfg.get("api_key") and _snap_cfg.get("api_secret")
        and _snap_cfg.get("api_passphrase")
    )
    if _hist_trade_on and _hist_has_creds:
        try:
            _ph_resp = _trade_get(
                "/api/v5/account/positions-history",
                {"instType": "SWAP", "limit": "100"},
                _snap_cfg,
            )
            _ph_ts = dubai_now().strftime("%d %b %Y  %H:%M:%S GST")
            if _ph_resp.get("code") == "0":
                _ph_all = _ph_resp.get("data", [])
                st.session_state["okx_tp_hist"]   = [
                    p for p in _ph_all if p.get("type") in ("2", "4")
                ]
                st.session_state["okx_sl_hist"]   = [
                    p for p in _ph_all if p.get("type") in ("1", "3", "5")
                ]
                st.session_state["okx_hist_ts"]   = _ph_ts
            else:
                st.session_state["okx_tp_hist"]  = None
                st.session_state["okx_sl_hist"]  = None
                st.session_state["okx_hist_ts"]  = _ph_ts
        except Exception as _ph_exc:
            st.session_state["okx_tp_hist"]  = None
            st.session_state["okx_sl_hist"]  = None
            _append_error("trade", f"Positions-history fetch failed: {_ph_exc}",
                          endpoint="/api/v5/account/positions-history")

    # ── Table 2: TP Hit ─────────────────────────────────────────────────────────────
    # show_pnl=True → realized gain column, using close_price (= TP level).
    # For DCA trades, the row naturally picks up DCA-N in Alert, blended avg in
    # Signal Entry, original entry in Original Entry, and cumulative Order Size.
    _render_sig_table(_tp_sigs,    "✅ TP Hit",         "No TP hits yet.",
                      show_pnl=True)

    # ── OKX Fulfilled Orders (auto-trading only) ────────────────────────────────────
    if _hist_trade_on and _hist_has_creds:
        _tp_hist      = st.session_state.get("okx_tp_hist")
        _hist_ts_tp   = st.session_state.get("okx_hist_ts", "")
        _env_hist     = "🟡 Demo" if _snap_cfg.get("demo_mode", True) else "🔴 Live"

        # Filter to last 24 hours using the position uTime (ms epoch).
        _24h_cutoff_ms = (time.time() - 86400) * 1000
        _tp_hist_24h = []
        if _tp_hist:
            for _ph in _tp_hist:
                try:
                    if int(_ph.get("uTime", 0)) >= _24h_cutoff_ms:
                        _tp_hist_24h.append(_ph)
                except Exception:
                    pass

        _fulfilled_count = len(_tp_hist_24h) if _tp_hist else 0
        _exp_label_tp = (
            f"📋 OKX Fulfilled Orders — last 24 h ({_fulfilled_count})"
            f"   ·   {_hist_ts_tp} · {_env_hist}"
        )
        with st.expander(_exp_label_tp, expanded=False):
            if _tp_hist is None:
                st.warning("⚠️ Could not fetch OKX position history — check Error Log.")
            elif not _tp_hist_24h:
                st.info("No fulfilled (TP-closed) positions in the last 24 hours.")
            else:
                # Build set of TP signal symbols for the Match column.
                _tp_sig_syms = {s.get("symbol", "") for s in _tp_sigs}
                _tp_close_map = {
                    "2": "Take Profit",
                    "4": "Partial TP",
                }
                _tp_rows = []
                for _ph in _tp_hist_24h:
                    _inst_tp     = _ph.get("instId", "")
                    _sym_tp      = _inst_tp.replace("-USDT-SWAP", "USDT").replace("-", "")
                    _rpnl_tp     = float(_ph.get("realizedPnl", 0) or 0)
                    _close_ts_tp = ""
                    try:
                        _close_ts_tp = datetime.fromtimestamp(
                            int(_ph.get("uTime", 0)) / 1000,
                            tz=dubai_now().tzinfo
                        ).strftime("%d %b %Y  %H:%M GST")
                    except Exception:
                        pass
                    _matched_tp = _sym_tp in _tp_sig_syms or _inst_tp in _tp_sig_syms
                    _tp_rows.append({
                        "Symbol":       _inst_tp,
                        "Direction":    _ph.get("direction", "").capitalize(),
                        "Close Reason": _tp_close_map.get(_ph.get("type", ""), "TP"),
                        "Contracts":    int(float(_ph.get("closeTotalPos", 0) or 0)),
                        "Avg Entry":    float(_ph.get("openAvgPx",  0) or 0),
                        "Close Price":  float(_ph.get("closeAvgPx", 0) or 0),
                        "Realized PnL": round(_rpnl_tp, 4),
                        "Close Time":   _close_ts_tp,
                        "Match":        "✅" if _matched_tp else "⚠️ no signal",
                    })

                st.dataframe(
                    _tp_rows,
                    use_container_width=True,
                    hide_index=True,
                    height=len(_tp_rows) * 35 + 48,
                    column_config={
                        "Avg Entry":    st.column_config.NumberColumn(format="%.6f"),
                        "Close Price":  st.column_config.NumberColumn(format="%.6f"),
                        "Realized PnL": st.column_config.NumberColumn(
                                            "Realized PnL $", format="%.4f"),
                    },
                )

    st.divider()

    # ── Table 3: SL Hit (non-DCA trades only) ──────────────────────────────────────
    # Trades that exhausted a DCA ladder and hit the −3% final SL are routed to
    # the dedicated "DCA SL Hit" table below, not this one.
    _render_sig_table(_sl_sigs,    "❌ SL Hit",         "No SL hits yet.",
                      show_pnl=True)

    # ── OKX Liquidated / SL Closed Orders (auto-trading only) ───────────────────────
    if _hist_trade_on and _hist_has_creds:
        _sl_hist      = st.session_state.get("okx_sl_hist")
        _hist_ts_sl   = st.session_state.get("okx_hist_ts", "")
        _env_hist_sl  = "🟡 Demo" if _snap_cfg.get("demo_mode", True) else "🔴 Live"

        st.markdown(
            f"**📋 OKX Liquidated / SL Closed Orders**"
            f"<span style='font-size:0.8em; color:gray; margin-left:12px;'>"
            f"auto-fetched · {_hist_ts_sl} · {_env_hist_sl}</span>",
            unsafe_allow_html=True,
        )

        if _sl_hist is None:
            st.warning("⚠️ Could not fetch OKX position history — check Error Log.")
        elif not _sl_hist:
            st.info("No SL-closed or liquidated positions found in the last 100 records.")
        else:
            # Build combined set of SL signal symbols for the Match column.
            _sl_sig_syms = {s.get("symbol", "") for s in _sl_sigs + _dca_sl_sigs}
            _sl_close_map = {
                "1": "Stop-Loss",
                "3": "Manual Close",
                "5": "Liquidated",
            }
            _sl_rows = []
            _sl_ghost_syms = []
            for _ph in _sl_hist:
                _inst_sl   = _ph.get("instId", "")
                _sym_sl    = _inst_sl.replace("-USDT-SWAP", "USDT").replace("-", "")
                _rpnl_sl   = float(_ph.get("realizedPnl", 0) or 0)
                _close_ts_sl = ""
                try:
                    _close_ts_sl = datetime.fromtimestamp(
                        int(_ph.get("uTime", 0)) / 1000,
                        tz=dubai_now().tzinfo
                    ).strftime("%d %b %Y  %H:%M GST")
                except Exception:
                    pass
                _reason_sl  = _sl_close_map.get(_ph.get("type", ""), "SL")
                _matched_sl = _sym_sl in _sl_sig_syms or _inst_sl in _sl_sig_syms
                if not _matched_sl:
                    _sl_ghost_syms.append(_inst_sl)
                _sl_rows.append({
                    "Symbol":       _inst_sl,
                    "Direction":    _ph.get("direction", "").capitalize(),
                    "Close Reason": _reason_sl,
                    "Contracts":    int(float(_ph.get("closeTotalPos", 0) or 0)),
                    "Avg Entry":    float(_ph.get("openAvgPx",  0) or 0),
                    "Close Price":  float(_ph.get("closeAvgPx", 0) or 0),
                    "Realized PnL": round(_rpnl_sl, 4),
                    "Close Time":   _close_ts_sl,
                    "Match":        "✅" if _matched_sl else "⚠️ no signal",
                })

            st.dataframe(
                _sl_rows,
                use_container_width=True,
                hide_index=True,
                height=len(_sl_rows) * 35 + 48,
                column_config={
                    "Avg Entry":    st.column_config.NumberColumn(format="%.6f"),
                    "Close Price":  st.column_config.NumberColumn(format="%.6f"),
                    "Realized PnL": st.column_config.NumberColumn(
                                        "Realized PnL $", format="%.4f"),
                },
            )
            if _sl_ghost_syms:
                st.caption(
                    f"⚠️ {len(_sl_ghost_syms)} OKX SL/liquidated position(s) have no matching signal: "
                    + ", ".join(_sl_ghost_syms)
                )

    st.divider()

    # ── Table 4: DCA SL Hit (ladder-exhausted closures) ────────────────────────────
    # Dedicated table for trades that consumed every allowed DCA add and then hit
    # the final −3%-below-blended-average SL. Keeping these separate from regular
    # SL hits makes it easy to audit DCA-strategy performance in isolation.
    _render_sig_table(_dca_sl_sigs, "❌ DCA SL Hit (ladder exhausted)",
                      "No DCA ladder-exhausted SL hits yet.",
                      show_pnl=True)
    st.divider()

    # ── Table 4b: FC Hit (Force Close — PnL ≥ +$0.50 after DCA) ───────────────────
    # Closed by the FC-B mechanism: OKX conditional algo set to fc_trigger_px
    # (avg_entry + $0.50 / total_contracts / ct_val) fires and closes the position
    # at market.  Distinct from TP Hit — the original TP was NOT reached.
    _render_sig_table(_fc_sigs, "🟣 FC Hit (DCA breakeven close)",
                      "No FC closes yet.",
                      show_pnl=True)
    st.divider()

    # ── Table 5: Queue Limit ────────────────────────────────────────────────────────
    # No PnL shown — queue_limit signals never opened a real trade.
    # Collapsed by default; clear button lives inside the same expander.
    _ql_count = len(_queue_sigs)
    with st.expander(f"⏳ Queue Limit ({_ql_count})", expanded=False):
        _render_sig_table(_queue_sigs, "⏳ Queue Limit", "No queued signals.",
                          show_header=False)
        if _queue_sigs:
            if st.button("🗑️ Clear Queue Limit Records", key="clear_queue_limit"):
                with _log_lock:
                    _b._bsc_log["signals"] = [
                        s for s in _b._bsc_log["signals"]
                        if s.get("status") != "queue_limit"
                    ]
                    save_log(_b._bsc_log)
                st.success("✅ Queue Limit records cleared.")
                st.rerun()

    # ── Table 6: Closed on OKX ─────────────────────────────────────────────────────
    # Positions that OKX closed (manually or otherwise) but weren't caught by the
    # candle-based TP/SL detector. Shown separately so they're easy to review.
    if _closed_okx_sigs:
        st.divider()
        _render_sig_table(_closed_okx_sigs, "🟠 Closed on OKX",
                          "No manually closed positions.", show_pnl=True)


_signal_tables_fragment()

# ── Page-load signal buckets (used by debug panel below) ───────────────────────
# The fragment above has its own live copies; these are page-load snapshots used
# only by the static debug/snapshot panel further down the page.
_filtered_pg   = signals if st.session_state.get("sector_filter","All") == "All" else \
                 [s for s in signals if s.get("sector") == st.session_state.get("sector_filter","All")]
_fsorted_pg    = sorted(_filtered_pg, key=lambda x: x.get("timestamp",""), reverse=True)
_open_sigs       = [s for s in _fsorted_pg if s.get("status") == "open"]
_tp_sigs         = [s for s in _fsorted_pg if s.get("status") == "tp_hit"]
_sl_sigs         = [s for s in _fsorted_pg if s.get("status") == "sl_hit"]
_dca_sl_sigs     = [s for s in _fsorted_pg if s.get("status") == "dca_sl_hit"]
_fc_sigs_pg      = [s for s in _fsorted_pg if s.get("status") == "fc_hit"]
_queue_sigs      = [s for s in _fsorted_pg if s.get("status") == "queue_limit"]
_closed_okx_sigs = [s for s in _fsorted_pg if s.get("status") == "closed_okx"]

# ─────────────────────────────────────────────────────────────────────────────
# OKX Live Positions Panel
# ─────────────────────────────────────────────────────────────────────────────
_has_api_creds = bool(
    _snap_cfg.get("api_key") and _snap_cfg.get("api_secret")
    and _snap_cfg.get("api_passphrase")
)

st.divider()
with st.expander("📡 OKX Live Positions", expanded=False):
    if not _has_api_creds:
        st.warning("Enter API credentials in the sidebar to use this panel.")
    else:
        _env_label = "🟡 Demo" if _snap_cfg.get("demo_mode", True) else "🔴 Live"
        _col_btn, _col_info = st.columns([1, 4])
        with _col_btn:
            _do_refresh = st.button("🔄 Refresh from OKX", key="refresh_okx_live")
        with _col_info:
            _last_ts = st.session_state.get("okx_pos_ts", "")
            if _last_ts:
                st.caption(f"Last fetched: {_last_ts}  ·  {_env_label}")
            else:
                st.caption(f"Press Refresh to load live data directly from OKX  ·  {_env_label}")

        if _do_refresh:
            try:
                _pos_resp  = _trade_get("/api/v5/account/positions",
                                        {"instType": "SWAP"}, _snap_cfg)
                _algo_resp = _trade_get("/api/v5/trade/orders-algo-pending",
                                        {"instType": "SWAP", "ordType": "oco"},
                                        _snap_cfg)
                st.session_state["okx_pos_data"]  = _pos_resp
                st.session_state["okx_algo_data"] = _algo_resp
                st.session_state["okx_pos_ts"]    = \
                    dubai_now().strftime("%d %b %Y  %H:%M:%S GST")
                st.rerun()
            except Exception as _refresh_exc:
                st.error(f"❌ OKX API error: {_refresh_exc}")

        _pos_data  = st.session_state.get("okx_pos_data")
        _algo_data = st.session_state.get("okx_algo_data")

        if _pos_data is None:
            st.info("No data yet — press **🔄 Refresh from OKX** above.")
        else:
            # ── Positions table ───────────────────────────────────────────
            st.markdown("#### 📊 Open Positions")
            if _pos_data.get("code") != "0":
                st.error(f"OKX error: {_pos_data.get('msg', 'Unknown error')}")
            else:
                _positions = _pos_data.get("data", [])
                if _positions:
                    _pos_rows = []
                    for _p in _positions:
                        _upnl     = float(_p.get("upl",      0) or 0)
                        _upnl_pct = float(_p.get("uplRatio", 0) or 0) * 100
                        _liq_raw  = float(_p.get("liqPx",    0) or 0)
                        # OKX: 'margin' is only populated in Isolated mode.
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
                            "Contracts":     int(float(_p.get("pos", 0) or 0)),
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
                    st.info("No open SWAP positions on OKX right now.")

            # ── Active OCO Algo Orders table ──────────────────────────────
            st.markdown("#### 🎯 Active TP/SL Orders (OCO)")
            _algo_code = (_algo_data or {}).get("code", "")
            if _algo_code and _algo_code != "0":
                st.error(f"OKX algo error: {(_algo_data or {}).get('msg', '')}")
            else:
                _algos = (_algo_data or {}).get("data", [])
                if _algos:
                    _algo_rows = []
                    for _a in _algos:
                        # OKX returns cTime as Unix ms string
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
                    st.info("No active OCO algo orders on OKX right now.")

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
    outcome = {"Open": open_count, "TP Hit": tp_count,
               "SL Hit": sl_count, "FC Hit": fc_count}
    ch2.plotly_chart(go.Figure(go.Bar(
        x=list(outcome.keys()), y=list(outcome.values()),
        marker_color=["#58a6ff", "#3fb950", "#f85149", "#a371f7"],
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
if (
    fc.get("total_watchlist", 0) > 0
    and fc.get("scan_completed_at", 0.0) >= fc.get("flushed_at", 0.0)
):
    with st.expander("🔬 Last scan filter funnel"):
        total     = fc.get("total_watchlist", 0)
        pre_out_n = fc.get("pre_filtered_out", 0)
        after_pre = total - pre_out_n
        checked   = fc.get("checked", after_pre)
        after_f2  = checked   - fc.get("f2_pdz15m", 0)
        after_f3  = after_f2  - fc.get("f3_pdz5m",  0)
        after_f4  = after_f3  - fc.get("f4_rsi5m",  0)
        after_f5  = after_f4  - fc.get("f5_rsi1h",  0)
        after_f5b = after_f5  - fc.get("f5b_atr",   0)
        after_f6_ema_3m  = after_f5b        - fc.get("f6_ema_3m",  0)
        after_f6_ema_5m  = after_f6_ema_3m  - fc.get("f6_ema_5m",  0)
        after_f6_ema_15m = after_f6_ema_5m  - fc.get("f6_ema_15m", 0)
        after_f6         = after_f6_ema_15m  # final EMA stage output
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
        _atr_mode_lbl = sc.get("atr_mode", "Normal")
        _atr_thresh_lbl = {"Strict": "≤1.5×", "Normal": "≤2.0×", "Relaxed": "≤3.0×"}.get(_atr_mode_lbl, "≤2.0×")
        f5b_lbl    = (f"F5b — ATR(14) 15m {_atr_mode_lbl} {_atr_thresh_lbl}"
                      if sc.get("use_atr_filter", False) else "F5b — ATR (off)")
        ema_parts = []
        if sc.get("use_ema_3m"):  ema_parts.append(f"3m EMA{sc.get('ema_period_3m',12)}")
        if sc.get("use_ema_5m"):  ema_parts.append(f"5m EMA{sc.get('ema_period_5m',12)}")
        if sc.get("use_ema_15m"): ema_parts.append(f"15m EMA{sc.get('ema_period_15m',12)}")
        ema_lbl = ("F6 — EMA (" + (" · ".join(ema_parts)) + ")") if ema_parts else "F6 — EMA (off)"
        vol_lbl = (f"F9 — Vol \u2265{sc.get('vol_spike_mult',2.0)}\xd7 / {sc.get('vol_spike_lookback',20)} 15m"
                   if sc.get("use_vol_spike") else "F9 — Vol (off)")
        ema_cross_lbl = (f"F10 — EMA{sc.get('ema_cross_fast_15m',12)}>EMA{sc.get('ema_cross_slow_15m',21)} 15m"
                          if sc.get("use_ema_cross_15m", True) else "F10 — EMA Cross (off)")

        # ── Table-style funnel (replaces Plotly chart) ─────────────────────
        def _funnel_table_html(rows, total_n):
            """Render the filter funnel as a readable HTML table."""
            _css = (
                "<style>"
                ".ftbl{width:100%;border-collapse:collapse;font-size:13px;margin-bottom:1rem;}"
                ".ftbl th{font-size:11px;font-weight:500;padding:6px 10px;"
                "border-bottom:1px solid #30363d;color:#8b949e;text-align:left;}"
                ".ftbl td{padding:7px 10px;border-bottom:1px solid #21262d;"
                "color:#e6edf3;vertical-align:middle;}"
                ".ftbl tr:last-child td{border-bottom:none;}"
                ".ftbl tr.total-row td{background:#161b22;font-weight:500;}"
                ".ftbl tr.signal-row td{background:#0d1117;font-weight:500;}"
                ".ft-stage{font-size:12px;color:#8b949e;margin-top:2px;}"
                ".ft-on{display:inline-block;font-size:10px;padding:1px 7px;"
                "border-radius:4px;background:#1f3d5c;color:#79c0ff;font-weight:500;}"
                ".ft-off{display:inline-block;font-size:10px;padding:1px 7px;"
                "border-radius:4px;background:#21262d;color:#8b949e;font-weight:500;}"
                ".ft-in{color:#8b949e;}"
                ".ft-rem{color:#3fb950;font-weight:500;}"
                ".ft-drop0{color:#8b949e;}"
                ".ft-drop1{color:#d29922;font-weight:500;}"
                ".ft-drop2{color:#f85149;font-weight:500;}"
                ".ft-bar-wrap{display:inline-block;width:90px;height:7px;"
                "background:#21262d;border-radius:4px;vertical-align:middle;margin-right:6px;}"
                ".ft-bar{height:7px;border-radius:4px;background:#388bfd;}"
                "</style>"
            )
            _hdr = (
                "<table class='ftbl'>"
                "<thead><tr>"
                "<th style='width:32%'>Stage</th>"
                "<th style='width:9%'>Status</th>"
                "<th style='width:35%'>In → Dropped → Remaining</th>"
                "<th style='width:24%'>Survival</th>"
                "</tr></thead><tbody>"
            )
            _body = ""
            for _stage, _status, _in, _dropped, _rem, _pct, _desc, _row_cls in rows:
                _badge = f"<span class='ft-on'>ON</span>" if _status == "on" else (
                         f"<span class='ft-off'>OFF</span>" if _status == "off" else "")
                _drop_cls = "ft-drop0" if _dropped == 0 else (
                            "ft-drop2" if _dropped > 20 else "ft-drop1")
                _drop_str = (f"<span class='{_drop_cls}'>{_dropped} dropped</span>"
                             if _dropped > 0 else "<span class='ft-drop0'>—</span>")
                _flow = (f"<span class='ft-in'>{_in}</span>"
                         f" <span style='color:#8b949e'>→</span> "
                         f"{_drop_str}"
                         f" <span style='color:#8b949e'>→</span> "
                         f"<span class='ft-rem'>{_rem}</span>")
                _bar_w = max(1, _pct)
                _bar = (f"<div class='ft-bar-wrap'>"
                        f"<div class='ft-bar' style='width:{_bar_w}%'></div></div>"
                        f"<span style='font-size:12px;color:#8b949e'>{_pct}%</span>")
                _stage_cell = (_stage if not _desc else
                               f"{_stage}<div class='ft-stage'>{_desc}</div>")
                _row_style = f" class='{_row_cls}'" if _row_cls else ""
                _body += (f"<tr{_row_style}>"
                          f"<td>{_stage_cell}</td>"
                          f"<td>{_badge}</td>"
                          f"<td>{_flow}</td>"
                          f"<td>{_bar}</td>"
                          f"</tr>")
            return _css + _hdr + _body + "</tbody></table>"

        # Build rows: (stage, status, in_n, dropped, remaining, pct, desc, row_css_class)
        def _pct(n): return round(n / total * 100) if total > 0 else 0
        _ft_rows = []
        _ft_rows.append(("Watchlist", "", total, 0, total, 100, "Starting pool", "total-row"))
        _ft_rows.append(("\u26a1 Bulk pre-filter",
                         "on" if sc.get("use_pre_filter", True) else "off",
                         total, pre_out_n, after_pre, _pct(after_pre),
                         "Volume \xb7 price vs 24h low", ""))
        _blacklisted = after_pre - checked
        _ft_rows.append(("Cooldown / blacklist", "on",
                         after_pre, _blacklisted, checked, _pct(checked),
                         "SL cooldown \xb7 open trades", ""))
        _ft_rows.append((pdz15m_lbl,
                         "on" if sc.get("use_pdz_15m", True) else "off",
                         checked, fc.get("f2_pdz15m", 0), after_f2, _pct(after_f2),
                         "Premium \xb7 Equil \xb7 BandA \xb7 BandB", ""))
        _ft_rows.append((pdz5m_lbl,
                         "on" if sc.get("use_pdz_5m", True) else "off",
                         after_f2, fc.get("f3_pdz5m", 0), after_f3, _pct(after_f3),
                         "Same logic on 5m candles", ""))
        _ft_rows.append((f4_lbl,
                         "on" if sc.get("use_rsi_5m", True) else "off",
                         after_f3, fc.get("f4_rsi5m", 0), after_f4, _pct(after_f4),
                         "5m RSI floor", ""))
        _ft_rows.append((f5_lbl,
                         "on" if sc.get("use_rsi_1h", True) else "off",
                         after_f4, fc.get("f5_rsi1h", 0), after_f5, _pct(after_f5),
                         "1h RSI range", ""))
        _ft_rows.append((f5b_lbl,
                         "on" if sc.get("use_atr_filter", False) else "off",
                         after_f5, fc.get("f5b_atr", 0), after_f5b, _pct(after_f5b),
                         f"TP%/ATR% ratio {_atr_thresh_lbl}", ""))
        # F6 EMA — per enabled timeframe
        _ft_prev_ema = after_f5b
        for _ema_tf, _ema_key, _ema_pkey, _ema_dkey, _ema_aftn in [
            ("3m",  "use_ema_3m",  "ema_period_3m",  "f6_ema_3m",  after_f6_ema_3m),
            ("5m",  "use_ema_5m",  "ema_period_5m",  "f6_ema_5m",  after_f6_ema_5m),
            ("15m", "use_ema_15m", "ema_period_15m", "f6_ema_15m", after_f6_ema_15m),
        ]:
            if sc.get(_ema_key):
                _ema_lbl = f"F6 \u2014 EMA{sc.get(_ema_pkey, 12)} {_ema_tf}"
                _ft_rows.append((_ema_lbl, "on",
                                 _ft_prev_ema, fc.get(_ema_dkey, 0), _ema_aftn, _pct(_ema_aftn),
                                 f"Price above EMA{sc.get(_ema_pkey, 12)} on {_ema_tf}", ""))
                _ft_prev_ema = _ema_aftn
        if not ema_parts:
            _ft_rows.append(("F6 \u2014 EMA (off)", "off",
                             after_f5b, 0, after_f5b, _pct(after_f5b),
                             "All EMA timeframes disabled", ""))
        # F7 MACD — per enabled timeframe
        _ft_prev = after_f6
        for _tf, _key, _dkey, _aftn in [
            ("3m",  "use_macd_3m",  "f7_macd_3m",  after_f7_macd_3m),
            ("5m",  "use_macd_5m",  "f7_macd_5m",  after_f7_macd_5m),
            ("15m", "use_macd_15m", "f7_macd_15m", after_f7_macd_15m),
        ]:
            if sc.get(_key, True):
                _ft_rows.append((f"F7 \u2014 MACD {_tf}", "on",
                                 _ft_prev, fc.get(_dkey, 0), _aftn, _pct(_aftn),
                                 "MACD histogram bullish crossover", ""))
                _ft_prev = _aftn
        # F8 SAR — per enabled timeframe
        for _tf, _key, _dkey, _aftn in [
            ("3m",  "use_sar_3m",  "f8_sar_3m",  after_f8_sar_3m),
            ("5m",  "use_sar_5m",  "f8_sar_5m",  after_f8_sar_5m),
            ("15m", "use_sar_15m", "f8_sar_15m", after_f8_sar_15m),
        ]:
            if sc.get(_key, True):
                _ft_rows.append((f"F8 \u2014 SAR {_tf}", "on",
                                 _ft_prev, fc.get(_dkey, 0), _aftn, _pct(_aftn),
                                 "Price above Parabolic SAR", ""))
                _ft_prev = _aftn
        _ft_rows.append((vol_lbl,
                         "on" if sc.get("use_vol_spike") else "off",
                         after_f8_sar_15m, fc.get("f9_vol", 0), after_f9, _pct(after_f9),
                         "Volume spike check", ""))
        _ft_rows.append((ema_cross_lbl,
                         "on" if sc.get("use_ema_cross_15m", True) else "off",
                         after_f9, fc.get("f10_ema_cross", 0), after_f10, _pct(after_f10),
                         "Fast EMA above slow EMA 15m", ""))
        _ft_rows.append(("\u26a0\ufe0f Empty candle drop", "on",
                         after_f10, fc.get("f_empty_data", 0), after_empty, _pct(after_empty),
                         "Missing timeframe data", ""))
        _ft_rows.append(("\u2705 Signals generated", "", after_empty, 0, after_empty,
                         _pct(after_empty), "Passed all active filters", "signal-row"))

        st.markdown(_funnel_table_html(_ft_rows, total), unsafe_allow_html=True)
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
        _f5be = set(fc.get("f5b_elim_syms", []))
        _f6e_3m  = set(fc.get("f6_ema_3m_elim_syms",  []))
        _f6e_5m  = set(fc.get("f6_ema_5m_elim_syms",  []))
        _f6e_15m = set(fc.get("f6_ema_15m_elim_syms", []))
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
        _after_f5b       = _after_f5    - (_f5be if sc.get("use_atr_filter", False) else set())
        _after_f6_ema_3m  = _after_f5b       - (_f6e_3m  if sc.get("use_ema_3m")  else set())
        _after_f6_ema_5m  = _after_f6_ema_3m - (_f6e_5m  if sc.get("use_ema_5m")  else set())
        _after_f6_ema_15m = _after_f6_ema_5m - (_f6e_15m if sc.get("use_ema_15m") else set())
        _after_f6         = _after_f6_ema_15m
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

        # stage_rows: (stage_name, in_count, remaining_count, coin_str)
        # dropped = in_count - remaining_count (computed at render time)
        stage_rows = [
            ("⚡ After Bulk Pre-filter",  total,       len(_pre),      _coin_str(_pre)),
            ("🔬 Entered Deep Scan",      len(_pre),   len(_chk),      _coin_str(_chk)),
            (f"After {pdz15m_lbl}",       len(_chk),   len(_after_f2), _coin_str(_after_f2)),
            (f"After {pdz5m_lbl}",        len(_after_f2), len(_after_f3), _coin_str(_after_f3)),
            (f"After {f4_lbl}",           len(_after_f3), len(_after_f4), _coin_str(_after_f4)),
            (f"After {f5_lbl}",           len(_after_f4), len(_after_f5), _coin_str(_after_f5)),
            (f"After {f5b_lbl}",          len(_after_f5), len(_after_f5b), _coin_str(_after_f5b)),
        ]
        # F6 EMA — one row per enabled timeframe
        _sr_ema_prev = _after_f5b
        for _ema_tf, _ema_key, _ema_pkey, _ema_after_set in [
            ("3m",  "use_ema_3m",  "ema_period_3m",  _after_f6_ema_3m),
            ("5m",  "use_ema_5m",  "ema_period_5m",  _after_f6_ema_5m),
            ("15m", "use_ema_15m", "ema_period_15m", _after_f6_ema_15m),
        ]:
            if sc.get(_ema_key):
                stage_rows.append((
                    f"F6 — EMA{sc.get(_ema_pkey, 12)} {_ema_tf}",
                    len(_sr_ema_prev), len(_ema_after_set),
                    _coin_str(_ema_after_set),
                ))
                _sr_ema_prev = _ema_after_set
        if not ema_parts:
            stage_rows.append(("F6 — EMA (off)", len(_after_f5b), len(_after_f5b), _coin_str(_after_f5b)))
        stage_rows += [
        ]
        # F7 MACD — one row per enabled timeframe
        _sr_prev = _after_f6
        for _tf, _key, _after_set in [
            ("3m",  "use_macd_3m",  _after_f7_macd_3m),
            ("5m",  "use_macd_5m",  _after_f7_macd_5m),
            ("15m", "use_macd_15m", _after_f7_macd_15m),
        ]:
            if sc.get(_key, True):
                stage_rows.append((
                    f"F7 — MACD \U0001f7e2\u2191 {_tf}",
                    len(_sr_prev), len(_after_set),
                    _coin_str(_after_set),
                ))
                _sr_prev = _after_set
        # F8 SAR — one row per enabled timeframe
        for _tf, _key, _after_set in [
            ("3m",  "use_sar_3m",  _after_f8_sar_3m),
            ("5m",  "use_sar_5m",  _after_f8_sar_5m),
            ("15m", "use_sar_15m", _after_f8_sar_15m),
        ]:
            if sc.get(_key, True):
                stage_rows.append((
                    f"F8 — SAR {_tf}",
                    len(_sr_prev), len(_after_set),
                    _coin_str(_after_set),
                ))
                _sr_prev = _after_set
        # Fixed closing rows
        stage_rows += [
            (f"After {vol_lbl}",               len(_sr_prev),         len(_after_f9),   _coin_str(_after_f9)),
            (f"After {ema_cross_lbl}",         len(_after_f9),        len(_after_f10),  _coin_str(_after_f10)),
            ("⚠️ Dropped — Empty Candle Data", len(_after_f10),       len(_fempty),     ", ".join(sorted(_fempty)) if _fempty else "—"),
            ("💥 Dropped — Process Error",     "—", _process_err_count, "See API Error Log below ↓"),
            ("✅ Returned Signal",             "—", len(_returned_syms),   _coin_str(_returned_syms)),
            ("⭐ Super cap demoted → F3-F10",  "—", len(_super_demoted_s), _coin_str(_super_demoted_s)),
            ("🔵 Blocked — Open trade exists", "—", len(_blk_active_s),    _coin_str(_blk_active_s)),
            ("🟡 Blocked — TP Cooldown",       "—", len(_blk_cool_s),      _coin_str(_blk_cool_s)),
            ("🔴 Blocked — SL Cooldown (24h)", "—", len(_blk_sl_cool_s),   _coin_str(_blk_sl_cool_s)),
            ("🟢 New Signals Fired",           "—", len(_new_sig_s),       _coin_str(_new_sig_s)),
        ]

        st.dataframe(
            [{"Filter Stage":    r[0],
              "In":              r[1],
              "Dropped":         (r[1] - r[2]) if isinstance(r[1], int) and isinstance(r[2], int) else "—",
              "Remaining":       r[2],
              "Qualified Coins": r[3]} for r in stage_rows],
            use_container_width=True,
            hide_index=True,
            column_config={
                "Filter Stage":    st.column_config.TextColumn(width="medium"),
                "In":              st.column_config.NumberColumn(width="small"),
                "Dropped":         st.column_config.NumberColumn(width="small"),
                "Remaining":       st.column_config.NumberColumn(width="small"),
                "Qualified Coins": st.column_config.TextColumn(width="large"),
            }
        )

# ─────────────────────────────────────────────────────────────────────────────
# API Error Log  (bottom of page)
# ─────────────────────────────────────────────────────────────────────────────
st.divider()

with getattr(_b, "_bsc_error_log_lock", threading.Lock()):
    _err_snap = list(getattr(_b, "_bsc_error_log", []))

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
# Download Diagnostics  (bottom of page — plain-text snapshot for debug/share)
# ─────────────────────────────────────────────────────────────────────────────
# Produces a single .txt file containing everything needed to diagnose issues:
#   • Runtime state (scanner running / halted, cooldown state, thread status)
#   • Full config snapshot with API credentials REDACTED
#   • Active filters summary + filter funnel from last scan
#   • Every signal bucket (open / TP / SL / DCA SL / queue) with all key fields
#   • Recent API error log
#
# The user can download this and paste/attach to a chat for troubleshooting,
# saving the back-and-forth of copying filters + trade details manually.
st.divider()
st.markdown("### 📥 Download Diagnostics")
st.caption(
    "One-click text snapshot of every filter, sidebar setting, open/closed "
    "trade, and recent error — useful for sharing the exact state of the app "
    "when reporting a bug or asking for code changes. API credentials are "
    "redacted automatically."
)


def _build_diagnostics_text() -> str:
    """Assemble a readable plain-text dump of the full app state."""
    _SENSITIVE_KEYS = {"api_key", "api_secret", "api_passphrase"}
    _lines: list = []
    _push = _lines.append

    def _hdr(title: str):
        _push("")
        _push("=" * 78)
        _push(title)
        _push("=" * 78)

    def _sub(title: str):
        _push("")
        _push("-- " + title + " " + "-" * max(3, 74 - len(title)))

    def _kv(k, v):
        _push(f"  {k:<32} : {v}")

    def _fmt_ts(v):
        try:
            return fmt_dubai(v) if v else "—"
        except Exception:
            return str(v or "—")

    def _redact(cfg: dict) -> dict:
        out = {}
        for k, v in cfg.items():
            if k in _SENSITIVE_KEYS and v:
                out[k] = f"<redacted · len={len(str(v))}>"
            else:
                out[k] = v
        return out

    # ── Header ───────────────────────────────────────────────────────────────
    _push("DCA_SMACORSS DIAGNOSTICS SNAPSHOT")
    _push("Generated: " + dubai_now().strftime("%Y-%m-%d %H:%M:%S GST"))
    _push("User: " + (os.environ.get("USER") or os.environ.get("USERNAME") or "?"))

    # ── Runtime state ────────────────────────────────────────────────────────
    _hdr("RUNTIME STATE")
    try:
        _kv("scanner_running",        _scanner_running.is_set())
    except Exception:
        _kv("scanner_running",        "<unavailable>")
    _kv("bsc_sl_paused",              getattr(_b, "_bsc_sl_paused", False))
    _kv("bsc_sl_paused_reason",       getattr(_b, "_bsc_sl_paused_reason", "") or "—")
    _kv("bsc_sl_paused_ts",           _fmt_ts(getattr(_b, "_bsc_sl_paused_ts", "")))
    try:
        _bg_t = getattr(_b, "_bsc_thread", None)
        _kv("bg_thread_alive", bool(_bg_t is not None and _bg_t.is_alive()))
    except Exception:
        _kv("bg_thread_alive", "<unavailable>")
    try:
        _wt_t = getattr(_b, "_bsc_watcher_thread", None)
        _kv("watcher_thread_alive", bool(_wt_t is not None and _wt_t.is_alive()))
    except Exception:
        _kv("watcher_thread_alive", "<unavailable>")
    try:
        _health_local = _b._bsc_log.get("health", {}) if hasattr(_b, "_bsc_log") else {}
    except Exception:
        _health_local = {}
    _kv("last_scan_ts",               _fmt_ts(_health_local.get("last_scan_at", "")))
    _kv("total_cycles",               _health_local.get("total_cycles", 0))
    _kv("last_scan_duration_sec",     round(float(getattr(_b, "_bsc_last_dur", 0) or 0), 2))
    _kv("last_watcher_ts",            _fmt_ts(getattr(_b, "_bsc_watcher_last_ts", 0)))
    _kv("last_watcher_duration_sec",  round(float(getattr(_b, "_bsc_watcher_last_dur", 0) or 0), 2))
    _kv("legacy_migration_last_count", getattr(_b, "_bsc_legacy_migration_last_count", "n/a"))
    _kv("legacy_migration_last_ts",    _fmt_ts(getattr(_b, "_bsc_legacy_migration_last_ts", "")))

    # ── Configuration (redacted) ──────────────────────────────────────────────
    _hdr("CONFIGURATION (API credentials redacted)")
    try:
        _cfg_snap_local = _redact(dict(_snap_cfg))
    except Exception as _e:
        _cfg_snap_local = {"<error>": str(_e)}
    for _k in sorted(_cfg_snap_local.keys()):
        _v = _cfg_snap_local[_k]
        if isinstance(_v, list):
            _v = f"[{len(_v)} items] " + ", ".join(str(x) for x in _v[:20])
            if len(_cfg_snap_local[_k]) > 20:
                _v += " … (+" + str(len(_cfg_snap_local[_k]) - 20) + " more)"
        _kv(_k, _v)

    # ── Signal bucket counts ────────────────────────────────────────────────────────
    _hdr("SIGNAL COUNTS")
    _kv("open",        len(_open_sigs))
    _kv("tp_hit",      len(_tp_sigs))
    _kv("sl_hit",      len(_sl_sigs))
    _kv("dca_sl_hit",  len(_dca_sl_sigs))
    _kv("fc_hit",      len(_fc_sigs_pg))
    _kv("queue_limit", len(_queue_sigs))
    _kv("closed_okx",  len(_closed_okx_sigs))
    _kv("total",       len(signals))


    # ── Capital Requirement Summary ──────────────────────────────────────────
    _hdr("CAPITAL REQUIREMENT SUMMARY")
    try:
        _dc_base    = float(_snap_cfg.get("trade_usdt_amount", 5.0))
        _dc_dca_max = int(_snap_cfg.get("trade_max_dca", 3))
        _dc_pool    = int(_snap_cfg.get("max_open_trades", 7))
        _dc_per     = _dc_base * (2 ** _dc_dca_max)
        _dc_min     = _dc_per * _dc_pool
        _dc_buf     = _dc_min * 0.25
        _dc_tot     = _dc_min + _dc_buf
        _kv("base_margin_per_trade",    f"${_dc_base:.2f}")
        _kv("max_dca_levels",           _dc_dca_max)
        _kv("max_open_trades",          _dc_pool)
        _kv("worst_case_per_trade",     f"${_dc_per:.2f}  (base × 2^dca_max)")
        _kv("minimum_required",         f"${_dc_min:,.2f}  (per_trade × max_trades)")
        _kv("buffer_25pct",             f"${_dc_buf:,.2f}")
        _kv("total_recommended",        f"${_dc_tot:,.2f}")
        _kv("margin_mode",              _snap_cfg.get("trade_margin_mode", "isolated"))
    except Exception as _ce:
        _push(f"  <error: {_ce}>")

    # ── DCA Fixed Dollar TP/SL ────────────────────────────────────────────────
    _hdr("DCA FIXED DOLLAR TP / SL")
    try:
        _dca_tp_usd_d = float(_snap_cfg.get("dca_tp_usd", 0.50) or 0.50)
        _dca_sl_usd_d = float(_snap_cfg.get("dca_sl_usd", 5.00) or 5.00)
        _kv("dca_tp_usd  (fixed $ profit after DCA)", f"${_dca_tp_usd_d:.2f}")
        _kv("dca_sl_usd  (fixed $ max loss  after DCA)", f"${_dca_sl_usd_d:.2f}")
        _kv("note", "SL takes priority over DCA trigger if both fire on same candle")
    except Exception as _de:
        _push(f"  <error: {_de}>")

    # ── Live OKX Positions (real-time API call) ───────────────────────────────
    _hdr("LIVE OKX POSITIONS (from /api/v5/account/positions)")
    try:
        _has_creds = bool(
            _snap_cfg.get("api_key") and
            _snap_cfg.get("api_secret") and
            _snap_cfg.get("api_passphrase")
        )
        if not _has_creds:
            _push("  <skipped — API credentials not configured>")
        else:
            _pos_resp = _trade_get(
                "/api/v5/account/positions",
                {"instType": "SWAP"},
                _snap_cfg,
            )
            if _pos_resp.get("code") != "0":
                _push(f"  <OKX API error: code={_pos_resp.get('code')} "
                      f"msg={_pos_resp.get('msg', '?')}>")
            else:
                _pos_data = [p for p in (_pos_resp.get("data") or [])
                             if float(p.get("pos", 0) or 0) != 0]
                if not _pos_data:
                    _push("  (no open swap positions on OKX)")
                else:
                    _kv("total_open_positions", len(_pos_data))
                    _push("")
                    for _pi, _p in enumerate(_pos_data, 1):
                        _inst      = _p.get("instId", "?")
                        _pos_sz    = _p.get("pos", "?")
                        _avg_px    = _p.get("avgPx", "?")
                        _upnl      = _p.get("upl", "?")
                        _upnl_r    = _p.get("uplRatio", "?")
                        _margin    = _p.get("imr", "?") or _p.get("margin", "?")
                        _lev       = _p.get("lever", "?")
                        _liq_px    = _p.get("liqPx", "?")
                        _mm_mode   = _p.get("mgnMode", "?")
                        _pos_side  = _p.get("posSide", "net")
                        _ctime     = _p.get("cTime", "")
                        _utime     = _p.get("uTime", "")
                        # Format timestamps
                        try:
                            _ct_fmt = (datetime.fromtimestamp(int(_ctime)/1000,
                                       tz=timezone.utc)
                                       .strftime("%m/%d %H:%M:%S") if _ctime else "—")
                        except Exception:
                            _ct_fmt = str(_ctime or "—")
                        try:
                            _ut_fmt = (datetime.fromtimestamp(int(_utime)/1000,
                                       tz=timezone.utc)
                                       .strftime("%m/%d %H:%M:%S") if _utime else "—")
                        except Exception:
                            _ut_fmt = str(_utime or "—")
                        # Format uPnL with sign
                        try:
                            _upnl_f = float(_upnl or 0)
                            _upnl_r_f = float(_upnl_r or 0)
                            _upnl_str = f"{_upnl_f:+.4f} USDT ({_upnl_r_f*100:+.2f}%)"
                        except Exception:
                            _upnl_str = str(_upnl)
                        _push(f"  [{_pi}] {_inst}")
                        _push(f"        pos_sz      : {_pos_sz}  |  posSide: {_pos_side}")
                        _push(f"        avg_px      : {_avg_px}")
                        _push(f"        uPnL        : {_upnl_str}")
                        _push(f"        margin      : {_margin} USDT  |  lev: {_lev}×  |  mode: {_mm_mode}")
                        _push(f"        liq_px      : {_liq_px}")
                        _push(f"        opened_at   : {_ct_fmt}  |  updated: {_ut_fmt}")

                # ── Cross-reference with bot's open signals ────────────────────
                _push("")
                _push("  -- Cross-reference vs bot open signals " + "-" * 36)
                _bot_open = {s.get("symbol", ""): s for s in _open_sigs}
                _okx_inst_set = {p.get("instId", "") for p in _pos_data}
                # Bot says open but OKX has no position
                for _bsym, _bsig in _bot_open.items():
                    _okx_inst = _bsig.get("symbol", "").replace("USDT", "-USDT-SWAP")
                    if _bsig.get("order_id") and _okx_inst not in _okx_inst_set:
                        _push(f"  ⚠️  MISMATCH — bot=OPEN  okx=NO POSITION : {_bsym} "
                              f"(order_id={_bsig.get('order_id','')})")
                # OKX has position but bot doesn't track it
                for _p in _pos_data:
                    _inst = _p.get("instId", "")
                    _bsym_chk = _inst.replace("-USDT-SWAP", "USDT")
                    if _bsym_chk not in _bot_open:
                        _push(f"  ⚠️  MISMATCH — okx=OPEN  bot=NOT TRACKED : {_inst}")
                if not any(True for _bsym, _bsig in _bot_open.items()
                           if _bsig.get("order_id") and
                           _bsig.get("symbol","").replace("USDT","-USDT-SWAP")
                           not in _okx_inst_set) and \
                   not any(True for _p in _pos_data
                           if _p.get("instId","").replace("-USDT-SWAP","USDT")
                           not in _bot_open):
                    _push("  ✅  All bot open signals match OKX positions")
    except Exception as _pe:
        _push(f"  <error fetching OKX positions: {_pe}>")

    # ── Filter Funnel (last scan) ─────────────────────────────────────────────
    _hdr("FILTER FUNNEL — LAST SCAN")
    try:
        _fc = getattr(_b, "_bsc_filter_counts", {}) or {}
        _fmap = [
            ("pre_filtered_out",  "Pre-filter eliminated"),
            ("checked",           "Deep-scanned"),
            ("f_empty_data",      "F0  — Empty/bad data"),
            ("f1_resistance",     "F1  — Resistance"),
            ("f2_super_setup",    "F2  — Super setup passed"),
            ("super_cap_demoted", "F2  — Super cap demoted"),
            ("f3_pdz5m",          "F3  — PDZ 5m"),
            ("f4_rsi5m",          "F4  — RSI 5m"),
            ("f5_rsi1h",          "F5  — RSI 1h"),
            ("f6_ema",            "F6  — EMA"),
            ("f7_macd",           "F7  — MACD"),
            ("f7b_macd5m",        "F7b — MACD 5m"),
            ("f7c_macd15m",       "F7c — MACD 15m"),
            ("f8_sar",            "F8  — SAR"),
            ("f9_vol",            "F9  — Volume"),
            ("f10_ema_cross",     "F10 — EMA Cross 15m"),
            ("passed",            "✅ Passed all filters"),
            ("super_setup",       "⭐ Super setup"),
        ]
        for _fk, _fl in _fmap:
            _fv = _fc.get(_fk, 0)
            if _fv:
                _kv(_fl, _fv)
        _kv("watchlist_size",   _fc.get("watchlist_size", "—"))
    except Exception as _fe:
        _push(f"  <error: {_fe}>")

    # ── Per-signal detail (all buckets) ──────────────────────────────────────
    def _sig_lines(sig: dict, idx: int):
        _push(f"  [{idx}] {sig.get('symbol','?')} | status={sig.get('status','?')} "
              f"| entry={sig.get('entry','—')} | tp={sig.get('tp','—')} "
              f"| sl={sig.get('sl','—')} | avg_entry={sig.get('avg_entry','—')} "
              f"| original_entry={sig.get('original_entry','—')} "
              f"| next_dca_px={sig.get('next_dca_px','—')} "
              f"| final_sl_price={sig.get('final_sl_price','—')} "
              f"| sl_distance_pct={sig.get('sl_distance_pct','—')} "
              f"| fc_trigger_px={sig.get('fc_trigger_px','—')} "
              f"| dca_count={sig.get('dca_count',0)}/{sig.get('dca_max',0)} "
              f"| mode={sig.get('order_margin_mode','—')} "
              f"| lev={sig.get('trade_lev','—')}x "
              f"| usdt=${sig.get('trade_usdt','—')} "
              f"| dca_cross_drop={sig.get('dca_cross_drop_pct','—')}% "
              f"| dca_iso_dist={sig.get('dca_iso_distance_pct','—')}% "
              f"| ts={_fmt_ts(sig.get('timestamp',''))} "
              f"| close_ts={_fmt_ts(sig.get('close_time',''))} "
              f"| close_px={sig.get('close_price','—')} "
              f"| order_id={sig.get('order_id','—')} "
              f"| algo_id={sig.get('algo_id','—')} "
              f"| tp_algo_id={sig.get('tp_algo_id','—')} "
              f"| demo={sig.get('demo_mode','—')} "
              f"| is_super={sig.get('is_super_setup',False)}")
        # OKX Command log — one sub-line per entry
        _log_entries = sig.get("okx_log")
        if isinstance(_log_entries, list) and _log_entries:
            _push(f"    okx_log ({len(_log_entries)} entries):")
            for _li, _le in enumerate(_log_entries, 1):
                _push(f"      #{_li:>2}  {_le}")

    _entry_failed_sigs = [s for s in signals if s.get("status") == "entry_failed"]
    for _bucket_name, _bucket in [
        ("OPEN SIGNALS",    _open_sigs),
        ("TP HIT",          _tp_sigs),
        ("SL HIT",          _sl_sigs),
        ("DCA SL HIT",      _dca_sl_sigs),
        ("FC HIT",          _fc_sigs_pg),
        ("QUEUE LIMIT",     _queue_sigs),
        ("CLOSED ON OKX",   _closed_okx_sigs),
        ("ENTRY FAILED",    _entry_failed_sigs),
    ]:
        _hdr(_bucket_name + f"  ({len(_bucket)} signals)")
        if not _bucket:
            _push("  (none)")
        else:
            for _i, _s in enumerate(_bucket, 1):
                _sig_lines(_s, _i)
                # Full entry criteria
                _crit = _s.get("criteria") or {}
                if _crit:
                    _push(f"    criteria  : rsi_5m={_crit.get('rsi_5m','—')} "
                          f"rsi_1h={_crit.get('rsi_1h','—')} "
                          f"ema_3m={_crit.get('ema_3m','—')} "
                          f"ema_5m={_crit.get('ema_5m','—')} "
                          f"ema_15m={_crit.get('ema_15m','—')} "
                          f"macd_3m={_crit.get('macd_3m','—')} "
                          f"macd_5m={_crit.get('macd_5m','—')} "
                          f"macd_15m={_crit.get('macd_15m','—')} "
                          f"sar_3m={_crit.get('sar_3m','—')} "
                          f"sar_5m={_crit.get('sar_5m','—')} "
                          f"sar_15m={_crit.get('sar_15m','—')} "
                          f"vol_ratio={_crit.get('vol_ratio','—')} "
                          f"pdz_5m={_crit.get('pdz_zone_5m','—')} "
                          f"pdz_15m={_crit.get('pdz_zone_15m','—')} "
                          f"pdz_1h={_crit.get('pdz_zone_1h','—')} "
                          f"ema_cross_12={_crit.get('ema_cross_12_15m','—')} "
                          f"ema_cross_21={_crit.get('ema_cross_21_15m','—')} "
                          f"atr_15m={_crit.get('atr_15m','—')} "
                          f"atr_ratio={_crit.get('atr_ratio','—')}")
                # DCA ladder — use stored ladder or compute on-the-fly if missing
                _ladder = _s.get("dca_ladder") or []
                if not _ladder:
                    _diag_mode  = (_s.get("order_margin_mode") or "isolated").strip().lower()
                    _diag_entry = float(_s.get("original_entry", _s.get("entry", 0)) or 0)
                    _diag_max   = int(_s.get("dca_max", 0) or 0)
                    if _diag_mode == "cross" and _diag_entry > 0 and _diag_max > 0:
                        try:
                            _diag_usdt  = float(_s.get("trade_usdt", 5) or 5)
                            _diag_lev   = int(_s.get("trade_lev", 20) or 20)
                            _diag_cdrop = float(_s.get("dca_cross_drop_pct", 7.0) or 7.0)
                            _diag_tp    = float(_snap_cfg.get("tp_pct", 1.0) or 1.0)
                            _ladder = _calc_cross_dca_ladder(
                                _diag_entry, _diag_usdt, _diag_lev,
                                _diag_max, _diag_cdrop, _diag_tp)
                        except Exception:
                            _ladder = []
                if _ladder:
                    _lvls = [f"DCA-{item['level']}@{item['trigger_px']} "
                             f"(blend={item.get('blended_avg','?')} tp={item.get('tp_px','?')})"
                             for item in _ladder]
                    _push(f"    dca_ladder: {' | '.join(_lvls)}")
                # DCA fills
                _fills = _s.get("dca_fills") or []
                if len(_fills) > 1:
                    _push(f"    dca_fills : {len(_fills)-1} DCA(s) fired — "
                          + " | ".join(f"DCA-{f.get('dca_idx',i)}@{f.get('price','?')} "
                                       f"usdt=${f.get('usdt','?')} "
                                       f"{'[paper]' if f.get('paper') else '[live]'}"
                                       for i, f in enumerate(_fills[1:], 1)))
    # ── Active Watchlist ──────────────────────────────────────────────────────
    _hdr("WATCHLIST")
    try:
        _wl = list(_snap_cfg.get("watchlist") or [])
        _push(f"  Total: {len(_wl)} symbols")
        for _wi, _wsym in enumerate(_wl, 1):
            _push(f"  {_wi:>3}. {_wsym}")
    except Exception as _we:
        _push(f"  <error reading watchlist: {_we}>")

    # ── API Error Log ─────────────────────────────────────────────────────────
    _hdr("API ERROR LOG (last 200 entries, newest first)")
    try:
        with getattr(_b, "_bsc_error_log_lock", threading.Lock()):
            _err_entries = list(reversed(getattr(_b, "_bsc_error_log", [])))[:200]
        if not _err_entries:
            _push("  (no errors)")
        else:
            for _ei, _err in enumerate(_err_entries, 1):
                _push(f"  [{_ei:>3}] {_fmt_ts(_err.get('ts',''))} "
                      f"| {_err.get('type','?'):8} "
                      f"| {_err.get('symbol',''):15} "
                      f"| {_err.get('endpoint',''):40} "
                      f"| {str(_err.get('msg',''))[:120]}")
    except Exception as _ele:
        _push(f"  <error reading error log: {_ele}>")

    _push("")
    _push("=" * 78)
    _push("END OF DIAGNOSTICS")
    _push("=" * 78)
    return "\n".join(_lines)


# ─────────────────────────────────────────────────────────────────────────────
# Download button — calls the builder and streams the result
# ─────────────────────────────────────────────────────────────────────────────
try:
    _diag_text = _build_diagnostics_text()
except Exception as _diag_exc:
    _diag_text = f"Error building diagnostics: {_diag_exc}"

st.text_area("📋 Diagnostics Preview", _diag_text, height=300, key="debug_snap_area")

_diag_filename = f"diagnostics_{dubai_now().strftime('%Y%m%d_%H%M%S')}.txt"
st.download_button(
    label="⬇️ Download Diagnostics",
    data=_diag_text.encode("utf-8"),
    file_name=_diag_filename,
    mime="text/plain",
    key="diag_download_btn",
)
