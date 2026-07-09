"""
XAUUSD AI Signal Bot - Scoring Edition v6 FINAL+ (Scalping M1)
===============================================================
- SCORE_MARGIN = 3
- Threshold = base sesi + adjustment ATR relatif
- Duplicate detector: arah + entry
- Index SQLite pada timestamp
- Outcome tracker: semak High/Low sejak signal
- Same‑candle ambiguity → tandakan "AMBIGUOUS"
- Tiada forced LOSS; jika 60 minit dan tiada TP/SL → "TIMEOUT"
- Satu scheduler untuk analysis + outcome (kemas)
- Statistik ringkas (Win Rate, last 30) dalam Telegram
- Database WAL mode + timeout
- Cooldown persistent SQLite
"""

import os
import json
import time
import logging
import sqlite3
import requests
from datetime import datetime, timezone, timedelta
from threading import Thread
from flask import Flask
import schedule

# ─────────────────────────────────────────────
# CONFIG — semua dari Environment Variables
# ─────────────────────────────────────────────
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")
GROQ_API_KEY = os.environ.get("GROQ_API_KEY")
TWELVEDATA_API_KEY = os.environ.get("TWELVEDATA_API_KEY")

SYMBOL = "XAU/USD"
CHECK_INTERVAL_MINUTES = 3
CONFIDENCE_THRESHOLD = 75
NEWS_BLACKOUT_MINUTES = 60
PIP_SIZE = 0.1
TP_PIPS = 10
DEFAULT_SL_PIPS = 8

M1_OUTPUTSIZE = 900

# --- Scoring weights ---
WEIGHT_M15 = 3
WEIGHT_M5 = 5
WEIGHT_EMA_CROSS = 2
WEIGHT_PRICE_VS_EMA = 2
WEIGHT_ADX = 2
WEIGHT_RSI = 1
WEIGHT_STRUCTURE = 2

SCORE_MARGIN = 3

# --- Minimum ATR dan ADX ---
MIN_ATR_M1_TRADE = 0.40
MIN_ADX_M1 = 20

# --- Entry drift ---
MAX_ENTRY_DRIFT_PIPS = 3
MAX_DISTANCE_FROM_EMA_FACTOR = 1.2

# --- Cooldown & duplicate ---
COOLDOWN_MINUTES = 20
DUPLICATE_WINDOW_MINUTES = 5

# --- Breakeven / Timeout ---
BE_MINUTES = 30          # mula semak BE selepas 30 minit
TIMEOUT_MINUTES = 60     # selepas 60 minit tanpa TP/SL → tandakan "TIMEOUT"
BE_PROFIT_THRESHOLD = 0.5   # 5 pip untung minimum sebelum BE layak
BE_PRICE_DISTANCE = 0.15    # 1.5 pip dari entry untuk dianggap BE

# --- Session base thresholds (UTC) ---
SESSION_BASE = {
    "ASIA": 11,
    "LONDON": 9,
    "NEWYORK": 8,
}

# --- Database ---
DB_FILE = "signals.db"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger("xauusd-bot")

# ─────────────────────────────────────────────
# 1. DATA COLLECTION (sama)
# ─────────────────────────────────────────────

def fetch_candles(interval, outputsize):
    try:
        url = "https://api.twelvedata.com/time_series"
        params = {
            "symbol": SYMBOL,
            "interval": interval,
            "outputsize": outputsize,
            "apikey": TWELVEDATA_API_KEY
        }
        resp = requests.get(url, params=params, timeout=15)
        data = resp.json()
        if "values" not in data:
            log.error(f"TwelveData error ({interval}): {data}")
            return None
        raw = list(reversed(data["values"]))
        return [{
            "datetime": c["datetime"],
            "open": float(c["open"]),
            "high": float(c["high"]),
            "low": float(c["low"]),
            "close": float(c["close"]),
        } for c in raw]
    except Exception as e:
        log.error(f"Error fetching {interval} candles: {e}")
        return None

def _parse_dt(dt_str):
    return datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")

def aggregate_candles(m1_candles, group_size):
    if not m1_candles:
        return []
    buckets = {}
    for c in m1_candles:
        dt = _parse_dt(c["datetime"])
        bucket_minute = (dt.minute // group_size) * group_size
        bucket_start = dt.replace(minute=bucket_minute, second=0, microsecond=0)
        buckets.setdefault(bucket_start, []).append(c)

    keys = list(buckets.keys())
    aggregated = []
    for idx, key in enumerate(keys):
        chunk = buckets[key]
        is_edge = (idx == 0 or idx == len(keys) - 1)
        if is_edge and len(chunk) < group_size:
            continue
        aggregated.append({
            "datetime": key.strftime("%Y-%m-%d %H:%M:%S"),
            "open": chunk[0]["open"],
            "high": max(c["high"] for c in chunk),
            "low": min(c["low"] for c in chunk),
            "close": chunk[-1]["close"],
        })
    return aggregated

def get_today_news():
    try:
        url = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"
        resp = requests.get(url, timeout=15)
        events = resp.json()
        today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        return [
            e for e in events
            if e.get("country") == "USD"
            and e.get("impact") in ("High", "Medium")
            and str(e.get("date", "")).startswith(today_str)
        ]
    except Exception as e:
        log.error(f"Error fetching calendar: {e}")
        return []

def minutes_until_next_high_impact(events):
    now = datetime.now(timezone.utc)
    soonest = None
    for e in events:
        if e.get("impact") != "High":
            continue
        try:
            event_time = datetime.fromisoformat(str(e.get("date")).replace("Z", "+00:00"))
        except Exception:
            continue
        delta_min = (event_time - now).total_seconds() / 60
        if -5 <= delta_min <= 180:
            if soonest is None or delta_min < soonest:
                soonest = delta_min
    return soonest

# ─────────────────────────────────────────────
# 2. INDICATORS (dioptimumkan) – SAMA
# ─────────────────────────────────────────────

def ema_series(closes, period):
    if len(closes) < period:
        return []
    k = 2 / (period + 1)
    vals = [sum(closes[:period]) / period]
    for price in closes[period:]:
        vals.append(price * k + vals[-1] * (1 - k))
    return vals

def rsi_latest(closes, period=14):
    if len(closes) < period + 1:
        return None
    gains, losses = [], []
    for i in range(1, period + 1):
        ch = closes[i] - closes[i - 1]
        gains.append(max(ch, 0))
        losses.append(max(-ch, 0))
    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period
    for i in range(period + 1, len(closes)):
        ch = closes[i] - closes[i - 1]
        gain, loss = max(ch, 0), max(-ch, 0)
        avg_gain = (avg_gain * (period - 1) + gain) / period
        avg_loss = (avg_loss * (period - 1) + loss) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return round(100 - 100 / (1 + rs), 1)

def atr_series(candles, period=14):
    if len(candles) < period + 1:
        return []
    trs = []
    for i in range(1, len(candles)):
        h, l, pc = candles[i]["high"], candles[i]["low"], candles[i - 1]["close"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    if len(trs) < period:
        return []
    atr_values = []
    atr = sum(trs[:period]) / period
    atr_values.append(atr)
    for i in range(period, len(trs)):
        atr = (atr * (period - 1) + trs[i]) / period
        atr_values.append(atr)
    return atr_values

def atr_latest(candles, period=14):
    atr_list = atr_series(candles, period)
    return atr_list[-1] if atr_list else None

def atr_average(candles, period=14, lookback=100):
    atr_list = atr_series(candles, period)
    if len(atr_list) < lookback:
        return None
    return round(sum(atr_list[-lookback:]) / lookback, 3)

def adx_latest(candles, period=14):
    if len(candles) < period * 2:
        return None

    plus_dm, minus_dm, trs = [], [], []
    for i in range(1, len(candles)):
        high, low = candles[i]["high"], candles[i]["low"]
        prev_high, prev_low = candles[i - 1]["high"], candles[i - 1]["low"]
        prev_close = candles[i - 1]["close"]

        up_move = high - prev_high
        down_move = prev_low - low
        plus_dm.append(up_move if (up_move > down_move and up_move > 0) else 0)
        minus_dm.append(down_move if (down_move > up_move and down_move > 0) else 0)
        trs.append(max(high - low, abs(high - prev_close), abs(low - prev_close)))

    if len(trs) < period:
        return None

    atr = sum(trs[:period]) / period
    plus_di_smooth = sum(plus_dm[:period]) / period
    minus_di_smooth = sum(minus_dm[:period]) / period

    dx_values = []
    for i in range(period, len(trs)):
        atr = (atr * (period - 1) + trs[i]) / period
        plus_di_smooth = (plus_di_smooth * (period - 1) + plus_dm[i]) / period
        minus_di_smooth = (minus_di_smooth * (period - 1) + minus_dm[i]) / period

        if atr == 0:
            continue
        plus_di = 100 * plus_di_smooth / atr
        minus_di = 100 * minus_di_smooth / atr
        di_sum = plus_di + minus_di
        dx = 0 if di_sum == 0 else 100 * abs(plus_di - minus_di) / di_sum
        dx_values.append(dx)

    if not dx_values:
        return None
    if len(dx_values) < period:
        return round(sum(dx_values) / len(dx_values), 1)

    adx = sum(dx_values[:period]) / period
    for dx in dx_values[period:]:
        adx = (adx * (period - 1) + dx) / period
    return round(adx, 1)

def get_di(candles, period=14):
    if len(candles) < period + 1:
        return None, None

    plus_dm, minus_dm, trs = [], [], []
    for i in range(1, len(candles)):
        high, low = candles[i]["high"], candles[i]["low"]
        prev_high, prev_low = candles[i - 1]["high"], candles[i - 1]["low"]
        prev_close = candles[i - 1]["close"]

        up_move = high - prev_high
        down_move = prev_low - low
        plus_dm.append(up_move if (up_move > down_move and up_move > 0) else 0)
        minus_dm.append(down_move if (down_move > up_move and down_move > 0) else 0)
        trs.append(max(high - low, abs(high - prev_close), abs(low - prev_close)))

    if len(trs) < period:
        return None, None

    atr = sum(trs[:period]) / period
    plus_di_smooth = sum(plus_dm[:period]) / period
    minus_di_smooth = sum(minus_dm[:period]) / period

    for i in range(period, len(trs)):
        atr = (atr * (period - 1) + trs[i]) / period
        plus_di_smooth = (plus_di_smooth * (period - 1) + plus_dm[i]) / period
        minus_di_smooth = (minus_di_smooth * (period - 1) + minus_dm[i]) / period

    if atr == 0:
        return None, None
    plus_di = 100 * plus_di_smooth / atr
    minus_di = 100 * minus_di_smooth / atr
    return round(plus_di, 2), round(minus_di, 2)

# ─────────────────────────────────────────────
# 3. SCORING SYSTEM (SAMA)
# ─────────────────────────────────────────────

def get_market_structure(candles, lookback=5, majority=0.6):
    if len(candles) < lookback + 1:
        return "NEUTRAL"
    recent = candles[-lookback-1:]
    bullish_count = 0
    bearish_count = 0
    for i in range(1, len(recent)):
        if recent[i]["high"] > recent[i-1]["high"] and recent[i]["low"] > recent[i-1]["low"]:
            bullish_count += 1
        elif recent[i]["high"] < recent[i-1]["high"] and recent[i]["low"] < recent[i-1]["low"]:
            bearish_count += 1
    total = bullish_count + bearish_count
    if total == 0:
        return "NEUTRAL"
    if bullish_count / total >= majority:
        return "BULLISH"
    if bearish_count / total >= majority:
        return "BEARISH"
    return "NEUTRAL"

def get_session_base():
    now = datetime.now(timezone.utc)
    hour = now.hour
    if 0 <= hour < 8:
        return SESSION_BASE["ASIA"]
    elif 8 <= hour < 16:
        return SESSION_BASE["LONDON"]
    else:
        return SESSION_BASE["NEWYORK"]

def get_atr_adjustment(atr, atr_avg):
    if atr_avg is None or atr_avg == 0:
        return 0
    ratio = atr / atr_avg
    if ratio < 0.7:
        return 1
    elif ratio < 0.9:
        return 0
    elif ratio < 1.1:
        return 0
    elif ratio < 1.3:
        return -1
    else:
        return -1

def compute_scoring(m15_candles, m5_candles, m1_candles, m1_indicators, current_price, plus_di=None, minus_di=None):
    bull_score = 0.0
    bear_score = 0.0

    def simple_trend(candles, fast=10, slow=20):
        if not candles or len(candles) < slow + 2:
            return "NEUTRAL"
        closes = [c["close"] for c in candles]
        ema_fast = ema_series(closes, fast)
        ema_slow = ema_series(closes, slow)
        if not ema_fast or not ema_slow:
            return "NEUTRAL"
        if ema_fast[-1] > ema_slow[-1]:
            return "BULLISH"
        elif ema_fast[-1] < ema_slow[-1]:
            return "BEARISH"
        return "NEUTRAL"

    m15_dir = simple_trend(m15_candles)
    m5_dir = simple_trend(m5_candles)

    if m15_dir == "BULLISH":
        bull_score += WEIGHT_M15
    elif m15_dir == "BEARISH":
        bear_score += WEIGHT_M15

    if m5_dir == "BULLISH":
        bull_score += WEIGHT_M5
    elif m5_dir == "BEARISH":
        bear_score += WEIGHT_M5

    ema20 = m1_indicators.get("ema20")
    ema50 = m1_indicators.get("ema50")
    if ema20 is not None and ema50 is not None:
        if ema20 > ema50:
            bull_score += WEIGHT_EMA_CROSS
        elif ema20 < ema50:
            bear_score += WEIGHT_EMA_CROSS

    if ema20 is not None:
        if current_price > ema20:
            bull_score += WEIGHT_PRICE_VS_EMA
        elif current_price < ema20:
            bear_score += WEIGHT_PRICE_VS_EMA

    adx = m1_indicators.get("adx14")
    if adx is not None and adx > 25 and plus_di is not None and minus_di is not None:
        if plus_di > minus_di:
            bull_score += WEIGHT_ADX
        elif minus_di > plus_di:
            bear_score += WEIGHT_ADX

    rsi = m1_indicators.get("rsi14")
    if rsi is not None:
        if rsi < 30:
            bull_score += WEIGHT_RSI * 2
        elif rsi < 50:
            bull_score += WEIGHT_RSI
        elif rsi < 65:
            pass
        elif rsi < 70:
            bear_score += WEIGHT_RSI
        else:
            bear_score += WEIGHT_RSI * 2

    struct = get_market_structure(m1_candles, lookback=5, majority=0.6)
    if struct == "BULLISH":
        bull_score += WEIGHT_STRUCTURE
    elif struct == "BEARISH":
        bear_score += WEIGHT_STRUCTURE

    base = get_session_base()
    atr = m1_indicators.get("atr14")
    atr_avg = m1_indicators.get("atr_avg")
    adj = get_atr_adjustment(atr, atr_avg) if (atr is not None and atr_avg is not None) else 0
    threshold = base + adj

    margin = bull_score - bear_score
    if bull_score >= threshold and margin >= SCORE_MARGIN:
        direction = "BULLISH"
    elif bear_score >= threshold and -margin >= SCORE_MARGIN:
        direction = "BEARISH"
    else:
        direction = "NEUTRAL"

    return {
        "direction": direction,
        "bull_score": bull_score,
        "bear_score": bear_score,
        "margin": margin,
        "threshold": threshold,
        "base_threshold": base,
        "atr_adjustment": adj,
        "m15_dir": m15_dir,
        "m5_dir": m5_dir,
        "struct": struct,
    }

# ─────────────────────────────────────────────
# 4. DATABASE (SQLite) – WAL mode + timeout
# ─────────────────────────────────────────────

def get_db_connection():
    conn = sqlite3.connect(DB_FILE, timeout=10)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

def init_db():
    conn = get_db_connection()
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS cooldown (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            direction TEXT,
            time TEXT
        )
    ''')
    c.execute("INSERT OR IGNORE INTO cooldown (id, direction, time) VALUES (1, NULL, NULL)")
    c.execute('''
        CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            direction TEXT,
            entry REAL,
            sl REAL,
            tp REAL,
            confidence INTEGER,
            bull_score REAL,
            bear_score REAL,
            atr REAL,
            adx REAL,
            outcome TEXT DEFAULT NULL
        )
    ''')
    c.execute("CREATE INDEX IF NOT EXISTS idx_signal_time ON signals(timestamp)")
    conn.commit()
    conn.close()

def get_cooldown():
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("SELECT direction, time FROM cooldown WHERE id=1")
    row = c.fetchone()
    conn.close()
    if row:
        return row[0], row[1]
    return None, None

def set_cooldown(direction, dt):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("UPDATE cooldown SET direction=?, time=? WHERE id=1", (direction, dt.isoformat()))
    conn.commit()
    conn.close()

def is_duplicate(direction, entry, window_minutes=DUPLICATE_WINDOW_MINUTES):
    conn = get_db_connection()
    c = conn.cursor()
    cutoff = (datetime.now(timezone.utc) - timedelta(minutes=window_minutes)).isoformat()
    c.execute('''
        SELECT entry FROM signals
        WHERE timestamp > ? AND direction = ?
        ORDER BY timestamp DESC LIMIT 10
    ''', (cutoff, direction))
    rows = c.fetchall()
    conn.close()
    for row in rows:
        if abs(row[0] - entry) < 0.05:
            return True
    return False

def save_signal(direction, entry, sl, tp, confidence, bull_score, bear_score, atr, adx):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute('''
        INSERT INTO signals (timestamp, direction, entry, sl, tp, confidence, bull_score, bear_score, atr, adx)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        datetime.now(timezone.utc).isoformat(),
        direction,
        entry,
        sl,
        tp,
        confidence,
        bull_score,
        bear_score,
        atr,
        adx
    ))
    conn.commit()
    conn.close()

def update_outcome(signal_id, outcome):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("UPDATE signals SET outcome=? WHERE id=?", (outcome, signal_id))
    conn.commit()
    conn.close()

def get_pending_signals():
    conn = get_db_connection()
    c = conn.cursor()
    c.execute('''
        SELECT id, direction, entry, sl, tp, timestamp FROM signals
        WHERE outcome IS NULL
    ''')
    rows = c.fetchall()
    conn.close()
    return rows

def get_stats():
    """Kira Win Rate BUY/SELL dan last 30 outcomes."""
    conn = get_db_connection()
    c = conn.cursor()
    # Total win/loss by direction
    c.execute('''
        SELECT direction, outcome, COUNT(*) FROM signals
        WHERE outcome IN ('WIN','LOSS')
        GROUP BY direction, outcome
    ''')
    rows = c.fetchall()
    stats = {"BUY": {"W":0,"L":0}, "SELL": {"W":0,"L":0}}
    for direction, outcome, count in rows:
        if direction in stats:
            if outcome == "WIN":
                stats[direction]["W"] = count
            else:
                stats[direction]["L"] = count

    # Last 30 trades (latest first)
    c.execute('''
        SELECT direction, outcome FROM signals
        WHERE outcome IN ('WIN','LOSS')
        ORDER BY timestamp DESC LIMIT 30
    ''')
    last30 = c.fetchall()
    conn.close()
    return stats, last30

# ─────────────────────────────────────────────
# 5. OUTCOME TRACKER (DIPERBAIKI)
# ─────────────────────────────────────────────

def check_outcomes():
    try:
        candles = fetch_candles("1min", M1_OUTPUTSIZE)
        if not candles:
            return
        pending = get_pending_signals()
        now = datetime.now(timezone.utc)
        for sig in pending:
            signal_id, direction, entry, sl, tp, ts_str = sig
            try:
                sig_time = datetime.fromisoformat(ts_str)
            except:
                continue

            relevant = [c for c in candles if _parse_dt(c["datetime"]) >= sig_time]
            if not relevant:
                continue

            high_since = max(c["high"] for c in relevant)
            low_since = min(c["low"] for c in relevant)
            last_close = relevant[-1]["close"]

            outcome = None

            # Tentukan WIN/LOSS/AMBIGUOUS
            if direction == "BUY":
                hit_tp = high_since >= tp
                hit_sl = low_since <= sl
                if hit_tp and hit_sl:
                    outcome = "AMBIGUOUS"
                elif hit_tp:
                    outcome = "WIN"
                elif hit_sl:
                    outcome = "LOSS"
            else:  # SELL
                hit_tp = low_since <= tp
                hit_sl = high_since >= sl
                if hit_tp and hit_sl:
                    outcome = "AMBIGUOUS"
                elif hit_tp:
                    outcome = "WIN"
                elif hit_sl:
                    outcome = "LOSS"

            # Jika tiada outcome, semak BE / TIMEOUT
            if outcome is None:
                elapsed = (now - sig_time).total_seconds() / 60
                current_price = candles[-1]["close"]

                # BE jika kembali ke entry selepas BE_MINUTES dan pernah untung
                if elapsed >= BE_MINUTES and abs(current_price - entry) <= BE_PRICE_DISTANCE:
                    if direction == "BUY" and (high_since - entry) >= BE_PROFIT_THRESHOLD:
                        outcome = "BE"
                    elif direction == "SELL" and (entry - low_since) >= BE_PROFIT_THRESHOLD:
                        outcome = "BE"

                # TIMEOUT jika melebihi TIMEOUT_MINUTES dan tiada outcome
                if outcome is None and elapsed >= TIMEOUT_MINUTES:
                    outcome = "TIMEOUT"

            if outcome:
                update_outcome(signal_id, outcome)
                log.info(f"Signal ID {signal_id} {direction} updated to {outcome} (high={high_since}, low={low_since})")
    except Exception as e:
        log.error(f"Error in outcome checker: {e}")

# ─────────────────────────────────────────────
# 6. STATISTICS untuk Telegram
# ─────────────────────────────────────────────

def format_stats():
    stats, last30 = get_stats()
    lines = []
    for dir_name in ("BUY", "SELL"):
        w = stats[dir_name]["W"]
        l = stats[dir_name]["L"]
        total = w + l
        wr = f"{round(w/total*100)}%" if total > 0 else "N/A"
        lines.append(f"{dir_name}: {wr} ({w}W/{l}L)")
    # Last 30
    wins = sum(1 for _, o in last30 if o == "WIN")
    losses = sum(1 for _, o in last30 if o == "LOSS")
    lines.append(f"Last 30: {wins}W/{losses}L")
    return "\n".join(lines)

# ─────────────────────────────────────────────
# 7. AI ANALYSIS LAYER (SAMA)
# ─────────────────────────────────────────────

def build_prompt(current_price, m1_candles, indicators, score_result, news_events, expected_decision):
    candles_summary = "\n".join([
        f"  {c['datetime']}: O={c['open']} H={c['high']} L={c['low']} C={c['close']}"
        for c in m1_candles[-20:]
    ])

    news_summary = "\n".join([
        f"  {e.get('date', '')} {e.get('title', '')} (impact: {e.get('impact', '')})"
        for e in news_events
    ]) or "  Tiada high/medium impact USD news hari ini."

    prompt = f"""Kau seorang SCALPER XAUUSD berpengalaman di timeframe M1. **ARAH sudah DIPUTUSKAN oleh sistem scoring: {expected_decision}**.
Tugas kau HANYA:
- Tentukan sama ada masuk SEKARANG (jawab {expected_decision}) atau HOLD.
- Cadangkan SL (dalam pip) berdasarkan ATR14 dan struktur candle M1 terkini.
- Beri confidence yang jujur.

Sistem scoring memberikan:
- Markah BUY: {score_result['bull_score']:.1f}
- Markah SELL: {score_result['bear_score']:.1f}
- Margin: {score_result['margin']:.1f} (>= {SCORE_MARGIN} diperlukan)
- Threshold: {score_result['threshold']} (base {score_result['base_threshold']} + adjustment {score_result['atr_adjustment']})
- M15 trend: {score_result['m15_dir']}
- M5 trend: {score_result['m5_dir']}
- Struktur M1: {score_result['struct']}

INDIKATOR M1 SEMASA:
- EMA20: {indicators['ema20']}
- EMA50: {indicators['ema50']}
- RSI14: {indicators['rsi14']}
- ATR14: {indicators['atr14']}
- ADX14: {indicators['adx14']} (>= {MIN_ADX_M1} menunjukkan pasaran trending)

HARGA SEMASA: {current_price}

20 CANDLE M1 TERKINI (dari terbaru ke lama):
{candles_summary}

ECONOMIC CALENDAR HARI INI (USD, High/Medium impact):
{news_summary}

PERATURAN KETAT:
1. ENTRY MESTI harga semasa ({current_price}) - market order SEKARANG.
2. TP sentiasa TEPAT 10 pips dari entry, ke arah {expected_decision}.
3. Cadangkan sl_pips (nombor pip sahaja, biasanya 6-10 pips) berdasarkan ATR14 dan struktur candle M1 terkini.
4. Kalau RSI overbought (>70) untuk BUY, atau oversold (<30) untuk SELL - risiko reversal tinggi, bagi HOLD.
5. Cari titik masuk M1 yang confirm arah {expected_decision} (breakout micro-range, rejection dari EMA20, continuation candle selepas pullback) - elak entry kalau harga sudah jauh terkeluar (extended) drpd EMA20.
6. Beri confidence yang JUJUR.

Berikan jawapan HANYA dalam format JSON tepat:
{{
  "decision": "{expected_decision}" atau "HOLD",
  "confidence": <nombor 0-100>,
  "reason": "<penjelasan ringkas 1-2 ayat Bahasa Melayu>",
  "key_level": "<micro support/resistance terdekat>",
  "sl_pips": <nombor pip untuk SL, atau null jika HOLD>
}}

**INGAT: Arah telah dipilih {expected_decision}, jadi jawapan decision mestilah "{expected_decision}" ATAU "HOLD". Jangan jawab arah bertentangan.**"""
    return prompt

def call_groq(prompt):
    try:
        resp = requests.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {GROQ_API_KEY}",
                "Content-Type": "application/json"
            },
            json={
                "model": "llama-3.3-70b-versatile",
                "max_tokens": 300,
                "messages": [{"role": "user", "content": prompt}]
            },
            timeout=30
        )
        data = resp.json()
        if "choices" not in data:
            log.error(f"Groq API error: {data}")
            return None
        text = data["choices"][0]["message"]["content"]
        return parse_ai_json(text, "Groq (Llama 3.3)")
    except Exception as e:
        log.error(f"Groq API error: {e}")
        return None

def parse_ai_json(text, source_name):
    try:
        clean = text.strip()
        if clean.startswith("```"):
            clean = clean.split("```")[1]
            if clean.startswith("json"):
                clean = clean[4:]
        clean = clean.strip()
        parsed = json.loads(clean)
        parsed["source"] = source_name
        return parsed
    except Exception as e:
        log.error(f"Failed to parse {source_name} response: {e} | raw: {text[:200]}")
        return None

# ─────────────────────────────────────────────
# 8. TELEGRAM NOTIFICATION (dengan statistik)
# ─────────────────────────────────────────────

def send_telegram_message(text):
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        resp = requests.post(url, json={
            "chat_id": TELEGRAM_CHAT_ID,
            "text": text,
            "parse_mode": "Markdown"
        }, timeout=15)
        if not resp.ok:
            log.error(f"Telegram send failed: {resp.text}")
    except Exception as e:
        log.error(f"Telegram error: {e}")

def format_notification(result, score_result, indicators=None):
    decision = result.get("decision", "").upper()
    emoji = {"BUY": "🟢", "SELL": "🔴"}.get(decision, "⚪")

    lines = [
        f"{emoji} *XAUUSD 10-Pip Scalp Signal (M1) — {decision} NOW*",
        f"Entry: `{result.get('entry')}` (harga semasa - market order)",
        f"Confidence: {result.get('confidence', '?')}%",
        f"Skor: BUY={score_result['bull_score']:.1f} | SELL={score_result['bear_score']:.1f} | Margin={score_result['margin']:.1f}",
        f"Threshold: {score_result['threshold']} (base {score_result['base_threshold']} + adj {score_result['atr_adjustment']})",
        f"Trend M15: {score_result['m15_dir']} | M5: {score_result['m5_dir']}",
        f"Struktur M1: {score_result['struct']}",
    ]

    if indicators:
        lines.append(
            f"M1: EMA20 {indicators.get('ema20')} | ATR14 {indicators.get('atr14')} | ADX14 {indicators.get('adx14')}"
        )

    lines.append("")
    lines.append(f"_{result.get('reason', '')}_")

    if result.get("key_level"):
        lines.append(f"Key level: {result['key_level']}")

    if result.get("entry_condition"):
        lines.append("")
        lines.append(f"⚠️ *Syarat Entry:* {result['entry_condition']}")

    lines.append("")
    lines.append("*Trade Levels:*")
    lines.append(f"SL: `{result.get('sl')}`")
    lines.append(f"TP: `{result.get('tp')}`")

    try:
        risk = abs(result["entry"] - result["sl"])
        reward = abs(result["tp"] - result["entry"])
        if risk > 0:
            lines.append(f"Risk:Reward ≈ 1:{round(reward / risk, 2)}")
    except (TypeError, KeyError):
        pass

    # Tambah statistik ringkas
    stats_text = format_stats()
    lines.append("")
    lines.append("*📊 Statistik Bot:*")
    lines.append(stats_text)

    lines.append("")
    lines.append(f"🕐 {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    lines.append("\n⚠️ Ini analysis sahaja. Buat keputusan buy/sell sendiri dalam MT5.")

    return "\n".join(lines)

# ─────────────────────────────────────────────
# 9. MAIN JOB (SAMA)
# ─────────────────────────────────────────────

def run_analysis():
    log.info("Running scheduled analysis...")

    m1_candles = fetch_candles("1min", M1_OUTPUTSIZE)
    if not m1_candles or len(m1_candles) < 60:
        log.error("Data M1 tidak cukup - skip check ini.")
        return

    current_price = m1_candles[-1]["close"]

    m5_candles = aggregate_candles(m1_candles, 5)
    m15_candles = aggregate_candles(m1_candles, 15)

    news_events = get_today_news()
    minutes_to_news = minutes_until_next_high_impact(news_events)
    if minutes_to_news is not None and minutes_to_news <= NEWS_BLACKOUT_MINUTES:
        log.info(f"High-impact USD news dalam ~{round(minutes_to_news)} minit - HOLD.")
        return

    m1_closes = [c["close"] for c in m1_candles]
    ema20_series = ema_series(m1_closes, 20)
    ema50_series = ema_series(m1_closes, 50)
    atr = atr_latest(m1_candles, 14)
    atr_avg = atr_average(m1_candles, period=14, lookback=100)
    indicators = {
        "ema20": round(ema20_series[-1], 2) if ema20_series else None,
        "ema50": round(ema50_series[-1], 2) if ema50_series else None,
        "rsi14": rsi_latest(m1_closes, 14),
        "atr14": atr,
        "atr_avg": atr_avg,
        "adx14": adx_latest(m1_candles, 14),
    }

    if indicators["atr14"] is None or indicators["atr14"] < MIN_ATR_M1_TRADE:
        log.info(f"ATR14 M1 ({indicators['atr14']}) bawah minimum - skip.")
        return
    if indicators["adx14"] is None or indicators["adx14"] < MIN_ADX_M1:
        log.info(f"ADX14 M1 ({indicators['adx14']}) bawah minimum - skip.")
        return

    ema20 = indicators["ema20"]
    if ema20 is not None and indicators["atr14"] is not None:
        distance = abs(current_price - ema20)
        max_dist = indicators["atr14"] * MAX_DISTANCE_FROM_EMA_FACTOR
        if distance > max_dist:
            log.info(f"Harga terlalu jauh dari EMA20 (jarak={distance:.2f}, max={max_dist:.2f}) - skip.")
            return

    plus_di, minus_di = get_di(m1_candles, 14)

    score_result = compute_scoring(m15_candles, m5_candles, m1_candles, indicators, current_price, plus_di, minus_di)
    direction = score_result["direction"]
    log.info(f"Scoring: direction={direction}, BUY={score_result['bull_score']:.1f}, SELL={score_result['bear_score']:.1f}, margin={score_result['margin']:.1f}, threshold={score_result['threshold']}")

    if direction == "NEUTRAL":
        log.info("Skor tidak mencukupi - HOLD.")
        return

    expected_decision = "BUY" if direction == "BULLISH" else "SELL"

    last_dir, last_time_str = get_cooldown()
    now = datetime.now(timezone.utc)
    if last_dir is not None and last_dir == expected_decision and last_time_str is not None:
        try:
            last_time = datetime.fromisoformat(last_time_str)
            elapsed = (now - last_time).total_seconds() / 60
            if elapsed < COOLDOWN_MINUTES:
                log.info(f"Cooldown: signal {expected_decision} baru {elapsed:.1f} minit lalu, skip.")
                return
        except ValueError:
            pass

    if is_duplicate(expected_decision, current_price, DUPLICATE_WINDOW_MINUTES):
        log.info(f"Duplicate entry {current_price} untuk {expected_decision} dalam {DUPLICATE_WINDOW_MINUTES} minit - skip.")
        return

    prompt = build_prompt(current_price, m1_candles, indicators, score_result, news_events, expected_decision)
    log.info("Semua gate lepas - Calling Groq API...")
    result = call_groq(prompt)
    if result is None:
        log.error("Groq gagal respond - skip.")
        return

    decision = result.get("decision", "").upper()
    confidence = result.get("confidence", 0)
    log.info(f"Analysis: {decision} @ {confidence}% confidence")

    if decision not in ("BUY", "SELL"):
        log.info("AI bagi HOLD - senyap.")
        return
    if confidence < CONFIDENCE_THRESHOLD:
        log.info(f"Confidence {confidence}% bawah threshold - skip.")
        return
    if decision != expected_decision:
        log.warning(f"AI jawab {decision} sedangkan scoring arah {expected_decision} - skip.")
        return

    sl_pips = result.get("sl_pips") or DEFAULT_SL_PIPS
    try:
        sl_pips = float(sl_pips)
    except (TypeError, ValueError):
        sl_pips = DEFAULT_SL_PIPS

    entry = current_price
    if decision == "BUY":
        tp = round(entry + TP_PIPS * PIP_SIZE, 2)
        sl = round(entry - sl_pips * PIP_SIZE, 2)
    else:
        tp = round(entry - TP_PIPS * PIP_SIZE, 2)
        sl = round(entry + sl_pips * PIP_SIZE, 2)

    result["entry"] = entry
    result["sl"] = sl
    result["tp"] = tp

    max_drift = round(MAX_ENTRY_DRIFT_PIPS * PIP_SIZE, 2)
    if decision == "BUY":
        invalid_beyond = round(entry + max_drift, 2)
        result["entry_condition"] = (
            f"Sah HANYA jika harga masih > EMA20 ({indicators['ema20']}) "
            f"DAN belum melepasi {invalid_beyond}. Kalau dah lajak, SKIP."
        )
    else:
        invalid_beyond = round(entry - max_drift, 2)
        result["entry_condition"] = (
            f"Sah HANYA jika harga masih < EMA20 ({indicators['ema20']}) "
            f"DAN belum jatuh bawah {invalid_beyond}. Kalau dah lajak, SKIP."
        )

    message = format_notification(result, score_result, indicators)
    send_telegram_message(message)
    log.info(f"SIGNAL SENT - {decision} @ {confidence}% | Entry {entry} SL {sl} TP {tp}")

    set_cooldown(decision, now)
    save_signal(
        direction=decision,
        entry=entry,
        sl=sl,
        tp=tp,
        confidence=confidence,
        bull_score=score_result['bull_score'],
        bear_score=score_result['bear_score'],
        atr=indicators['atr14'],
        adx=indicators['adx14']
    )

# ─────────────────────────────────────────────
# 10. KEEP-ALIVE SERVER + Scheduler (SATU SAHAJA)
# ─────────────────────────────────────────────

app = Flask(__name__)

@app.route("/")
def home():
    return "XAUUSD Signal Bot (Scoring v6 FINAL+) is running."

@app.route("/health", methods=["GET", "HEAD"])
def health():
    return "OK", 200

@app.route("/run-now")
def trigger_manual():
    Thread(target=run_analysis).start()
    return "Analysis triggered, check Telegram/Logs in a few seconds.", 200

def run_scheduler():
    # Jadualkan analysis setiap CHECK_INTERVAL_MINUTES
    schedule.every(CHECK_INTERVAL_MINUTES).minutes.do(run_analysis)
    # Jadualkan outcome checker setiap 5 minit
    schedule.every(5).minutes.do(check_outcomes)
    log.info(f"Scheduler started: analysis {CHECK_INTERVAL_MINUTES}min, outcome 5min.")
    # Jalankan sekali pada permulaan
    run_analysis()
    while True:
        schedule.run_pending()
        time.sleep(15)

if __name__ == "__main__":
    init_db()
    # Satu thread untuk scheduler (bukan dua)
    scheduler_thread = Thread(target=run_scheduler, daemon=True)
    scheduler_thread.start()
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
