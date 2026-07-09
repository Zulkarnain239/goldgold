"""
XAUUSD AI Signal Bot - Multi-Timeframe Scalp Edition (Enhanced v6.1)
====================================================================
Penambahbaikan:
- Komen signal_candle_datetime dibetulkan (candle semasa, bukan selepas entry).
- Migrasi DB: tambah kolum baru tanpa memusnahkan data sedia ada.
"""

import os
import json
import time
import logging
import threading
import re
import sqlite3
from datetime import datetime, timezone, timedelta
from flask import Flask, request
import requests
import schedule

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")
GROQ_API_KEY = os.environ.get("GROQ_API_KEY")
TWELVEDATA_API_KEY = os.environ.get("TWELVEDATA_API_KEY")
WEBHOOK_URL = os.environ.get("WEBHOOK_URL", "https://your-app.onrender.com")

SYMBOL = "XAU/USD"

CHECK_INTERVAL_MINUTES = 3
H1_CACHE_MINUTES = 20
CONFIDENCE_THRESHOLD = 75
NEWS_BLACKOUT_MINUTES = 60

PIP_SIZE = 0.1
TP_PIPS = 10
DEFAULT_SL_PIPS = 8

M1_OUTPUTSIZE = 900
H1_OUTPUTSIZE = 80

EMA_TREND_PERIOD = 50
EMA_SLOPE_LOOKBACK = 3
MIN_ATR_H1 = 1.5
MIN_ATR_M15 = 0.6
MIN_ATR_M5 = 0.3

MIN_ATR_M1_TRADE = 0.40
MIN_ADX_M1 = 20
MAX_ENTRY_DRIFT_PIPS = 3

COOLDOWN_MINUTES = 20
MIN_PRICE_DIFF = 0.30
RESULT_CHECK_DELAY_MINUTES = 30
SCORING_THRESHOLD = 60          # initial; stored in DB
SIGNAL_VALID_MINUTES = 8

LEARNING_INTERVAL = 100
MIN_SCORE_THRESHOLD = 40
MAX_SCORE_THRESHOLD = 80

RULE_ENGINE_SCORE_THRESHOLD = 50

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger("xauusd-bot")

# ─────────────────────────────────────────────
# DATABASE LAYER (SQLite) with optimizations & migration
# ─────────────────────────────────────────────
DB_PATH = "bot_data.db"
_db_lock = threading.Lock()
_db_initialized = False

def get_db_conn():
    """Return a new connection (no PRAGMA here, done once in init_db)."""
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def column_exists(conn, table, column):
    """Check if a column exists in a table."""
    cursor = conn.execute(f"PRAGMA table_info({table})")
    columns = [row[1] for row in cursor.fetchall()]
    return column in columns

def init_db():
    global _db_initialized
    with _db_lock:
        if _db_initialized:
            return
        conn = get_db_conn()
        # Optimizations - done only once
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute('''
            CREATE TABLE IF NOT EXISTS session (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        ''')
        conn.execute('''
            CREATE TABLE IF NOT EXISTS signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                direction TEXT,
                entry REAL,
                tp REAL,
                sl REAL,
                timestamp TEXT,
                result TEXT,
                checked INTEGER DEFAULT 0,
                confidence INTEGER,
                trend_summary TEXT,
                score INTEGER
            )
        ''')
        conn.execute('''
            CREATE TABLE IF NOT EXISTS api_counters (
                service TEXT PRIMARY KEY,
                count INTEGER DEFAULT 0,
                reset_date TEXT
            )
        ''')
        conn.execute('''
            CREATE TABLE IF NOT EXISTS state (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        ''')

        # ─── MIGRATIONS: Add new columns if they don't exist ───
        # For signals table
        if not column_exists(conn, 'signals', 'signal_candle_datetime'):
            conn.execute('ALTER TABLE signals ADD COLUMN signal_candle_datetime TEXT')
            log.info("Migration: Added column signal_candle_datetime to signals table.")
        if not column_exists(conn, 'signals', 'entry_indicators'):
            conn.execute('ALTER TABLE signals ADD COLUMN entry_indicators TEXT')
            log.info("Migration: Added column entry_indicators to signals table.")

        # Ensure counters exist
        for svc in ['twelvedata', 'groq', 'forexfactory', 'telegram']:
            conn.execute(
                'INSERT OR IGNORE INTO api_counters (service, count, reset_date) VALUES (?, 0, ?)',
                (svc, datetime.now(timezone.utc).date().isoformat())
            )
        conn.execute(
            'INSERT OR IGNORE INTO state (key, value) VALUES ("score_threshold", ?)',
            (str(SCORING_THRESHOLD),)
        )
        conn.commit()
        conn.close()
        _db_initialized = True

# ─── Helper DB functions with write lock ───

def _write_operation(func):
    def wrapper(*args, **kwargs):
        with _db_lock:
            conn = get_db_conn()
            try:
                result = func(conn, *args, **kwargs)
                conn.commit()
                return result
            finally:
                conn.close()
    return wrapper

def _read_operation(func):
    def wrapper(*args, **kwargs):
        conn = get_db_conn()
        try:
            return func(conn, *args, **kwargs)
        finally:
            conn.close()
    return wrapper

# ─── Session ───

@_read_operation
def get_session_value(conn, key):
    row = conn.execute('SELECT value FROM session WHERE key = ?', (key,)).fetchone()
    return row['value'] if row else None

@_write_operation
def set_session_value(conn, key, value):
    conn.execute('REPLACE INTO session (key, value) VALUES (?, ?)', (key, value))

def get_session_state():
    active = get_session_value('session_active') == '1'
    expiry = get_session_value('session_expiry')
    if expiry:
        expiry = datetime.fromisoformat(expiry)
    started = get_session_value('session_started')
    if started:
        started = datetime.fromisoformat(started)
    return active, expiry, started

def set_session_state(active, expiry=None, started=None):
    set_session_value('session_active', '1' if active else '0')
    if expiry:
        set_session_value('session_expiry', expiry.isoformat())
    else:
        set_session_value('session_expiry', '')
    if started:
        set_session_value('session_started', started.isoformat())
    else:
        set_session_value('session_started', '')

# ─── Signals ───

@_read_operation
def get_last_signal(conn):
    """Return last signal entry, timestamp, direction, and trend_summary."""
    row = conn.execute(
        'SELECT entry, timestamp, direction, trend_summary FROM signals ORDER BY id DESC LIMIT 1'
    ).fetchone()
    if row:
        return dict(row)
    return None

@_write_operation
def add_signal(conn, direction, entry, tp, sl, confidence, trend_summary, score, signal_candle_dt, entry_indicators):
    cur = conn.execute(
        '''INSERT INTO signals 
           (direction, entry, tp, sl, timestamp, confidence, trend_summary, checked, result, score,
            signal_candle_datetime, entry_indicators)
           VALUES (?, ?, ?, ?, ?, ?, ?, 0, NULL, ?, ?, ?)''',
        (direction, entry, tp, sl,
         datetime.now(timezone.utc).isoformat(),
         confidence, json.dumps(trend_summary), score,
         signal_candle_dt.isoformat() if signal_candle_dt else None,
         json.dumps(entry_indicators))
    )
    return cur.lastrowid

@_write_operation
def update_signal_result(conn, signal_id, result):
    conn.execute('UPDATE signals SET result = ?, checked = 1 WHERE id = ?', (result, signal_id))

@_read_operation
def get_pending_signals(conn):
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=RESULT_CHECK_DELAY_MINUTES)
    rows = conn.execute(
        '''SELECT id, direction, entry, tp, sl, timestamp, signal_candle_datetime
           FROM signals 
           WHERE checked = 0 AND timestamp <= ?''',
        (cutoff.isoformat(),)
    ).fetchall()
    return [dict(row) for row in rows]

@_read_operation
def get_stats(conn):
    total = conn.execute('SELECT COUNT(*) FROM signals').fetchone()[0]
    wins = conn.execute('SELECT COUNT(*) FROM signals WHERE result = "WIN"').fetchone()[0]
    losses = conn.execute('SELECT COUNT(*) FROM signals WHERE result = "LOSS"').fetchone()[0]
    be = conn.execute('SELECT COUNT(*) FROM signals WHERE result = "BE"').fetchone()[0]
    timeout = conn.execute('SELECT COUNT(*) FROM signals WHERE result = "TIMEOUT"').fetchone()[0]

    rows_profit = conn.execute(
        '''SELECT direction, entry, tp, sl, result FROM signals WHERE result IN ("WIN", "LOSS")'''
    ).fetchall()
    gross_profit = 0.0
    gross_loss = 0.0
    for r in rows_profit:
        if r['result'] == 'WIN':
            if r['direction'] == 'BUY':
                gross_profit += (r['tp'] - r['entry'])
            else:
                gross_profit += (r['entry'] - r['tp'])
        else:
            if r['direction'] == 'BUY':
                gross_loss += (r['entry'] - r['sl'])
            else:
                gross_loss += (r['sl'] - r['entry'])
    profit_factor = round(gross_profit / gross_loss, 2) if gross_loss > 0 else 0

    rows = conn.execute(
        '''SELECT direction, result, entry, tp, sl FROM signals ORDER BY id DESC LIMIT 30'''
    ).fetchall()
    last30 = [dict(row) for row in rows]
    return {
        'total': total,
        'wins': wins,
        'losses': losses,
        'be': be,
        'timeout': timeout,
        'profit_factor': profit_factor,
        'last30': last30
    }

# ─── API counters ───

@_read_operation
def get_api_counter(conn, service):
    row = conn.execute('SELECT count, reset_date FROM api_counters WHERE service = ?', (service,)).fetchone()
    if row:
        today = datetime.now(timezone.utc).date().isoformat()
        if row['reset_date'] != today:
            with _db_lock:
                conn2 = get_db_conn()
                conn2.execute('UPDATE api_counters SET count = 0, reset_date = ? WHERE service = ?', (today, service))
                conn2.commit()
                conn2.close()
            return 0
        return row['count']
    return 0

@_write_operation
def increment_api_counter(conn, service):
    conn.execute('UPDATE api_counters SET count = count + 1 WHERE service = ?', (service,))

# ─── State ───

@_write_operation
def set_state(conn, key, value):
    conn.execute('REPLACE INTO state (key, value) VALUES (?, ?)', (key, str(value)))

@_read_operation
def get_state(conn, key):
    row = conn.execute('SELECT value FROM state WHERE key = ?', (key,)).fetchone()
    return row['value'] if row else None

def get_score_threshold():
    val = get_state('score_threshold')
    if val:
        return int(val)
    return SCORING_THRESHOLD

def set_score_threshold(value):
    set_state('score_threshold', value)

# ─────────────────────────────────────────────
# API RETRY HELPER (with 429 handling)
# ─────────────────────────────────────────────

def api_request_with_retry(url, method='GET', params=None, json_data=None, headers=None, timeout=15, retries=3):
    for attempt in range(retries):
        try:
            if method.upper() == 'GET':
                resp = requests.get(url, params=params, headers=headers, timeout=timeout)
            else:
                resp = requests.post(url, params=params, json=json_data, headers=headers, timeout=timeout)
            
            # Success
            if resp.status_code in (200, 201):
                return resp
            
            # Handle 429 Too Many Requests
            if resp.status_code == 429:
                wait = (attempt + 1) * 5  # 5, 10, 15 seconds
                log.warning(f"API {url} returned 429, waiting {wait}s before retry ({attempt+1}/{retries})")
                time.sleep(wait)
                continue
            
            # Handle 5xx server errors
            if 500 <= resp.status_code < 600:
                log.warning(f"API {url} returned {resp.status_code}, retrying ({attempt+1}/{retries})")
                time.sleep((attempt+1) * 2)
                continue
            
            # Other errors return as is
            return resp
        
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            log.warning(f"API {url} error: {e}, retrying ({attempt+1}/{retries})")
            time.sleep((attempt+1) * 2)
            continue
    
    return None  # all retries failed

# ─────────────────────────────────────────────
# DATA COLLECTION (with retry)
# ─────────────────────────────────────────────

def fetch_candles(interval, outputsize):
    increment_api_counter('twelvedata')
    url = "https://api.twelvedata.com/time_series"
    params = {
        "symbol": SYMBOL,
        "interval": interval,
        "outputsize": outputsize,
        "apikey": TWELVEDATA_API_KEY
    }
    resp = api_request_with_retry(url, method='GET', params=params, timeout=15, retries=3)
    if resp is None:
        log.error(f"TwelveData failed after retries ({interval})")
        return None
    try:
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

_h1_cache = {"data": None, "timestamp": None}

def get_h1_trend_data():
    now = datetime.now(timezone.utc)
    if _h1_cache["data"] and _h1_cache["timestamp"]:
        age_min = (now - _h1_cache["timestamp"]).total_seconds() / 60
        if age_min < H1_CACHE_MINUTES:
            return _h1_cache["data"]
    h1_candles = fetch_candles("1h", H1_OUTPUTSIZE)
    if h1_candles:
        _h1_cache["data"] = h1_candles
        _h1_cache["timestamp"] = now
    return h1_candles

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
    increment_api_counter('forexfactory')
    url = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"
    resp = api_request_with_retry(url, method='GET', timeout=15, retries=3)
    if resp is None:
        log.error("ForexFactory failed after retries")
        return []
    try:
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
# INDICATORS (unchanged)
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

def atr_latest(candles, period=14):
    if len(candles) < period + 1:
        return None
    trs = []
    for i in range(1, len(candles)):
        h, l, pc = candles[i]["high"], candles[i]["low"], candles[i - 1]["close"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    window = trs[-period:] if len(trs) >= period else trs
    return round(sum(window) / len(window), 3) if window else None

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

def trend_bias(candles, fast=10, slow=20, trend_ma=EMA_TREND_PERIOD,
               slope_lookback=EMA_SLOPE_LOOKBACK, min_atr=None, atr_period=14):
    closes = [c["close"] for c in candles]
    min_len = max(slow, trend_ma) + slope_lookback
    if len(closes) < min_len:
        return "NEUTRAL"
    ema_fast_series = ema_series(closes, fast)
    ema_slow_series = ema_series(closes, slow)
    ema_trend_series = ema_series(closes, trend_ma)
    if not ema_fast_series or not ema_slow_series or not ema_trend_series:
        return "NEUTRAL"
    if len(ema_slow_series) <= slope_lookback:
        return "NEUTRAL"
    price = closes[-1]
    ema_fast_now = ema_fast_series[-1]
    ema_slow_now = ema_slow_series[-1]
    ema_trend_now = ema_trend_series[-1]
    ema_slow_slope = ema_slow_now - ema_slow_series[-1 - slope_lookback]

    if min_atr is not None:
        atr = atr_latest(candles, atr_period)
        if atr is None or atr < min_atr:
            return "NEUTRAL"

    # Higher High/Lower Low: 3 daripada 5 candle terakhir
    if len(candles) >= 6:
        highs = [c['high'] for c in candles[-6:]]
        lows = [c['low'] for c in candles[-6:]]
        higher_high_count = sum(1 for i in range(1, len(highs)) if highs[i] > highs[i-1])
        higher_low_count = sum(1 for i in range(1, len(lows)) if lows[i] > lows[i-1])
        bullish_hl = (higher_high_count >= 3 and higher_low_count >= 3)

        lower_high_count = sum(1 for i in range(1, len(highs)) if highs[i] < highs[i-1])
        lower_low_count = sum(1 for i in range(1, len(lows)) if lows[i] < lows[i-1])
        bearish_hl = (lower_high_count >= 3 and lower_low_count >= 3)
    else:
        bullish_hl = bearish_hl = False

    is_bullish = (ema_fast_now > ema_slow_now and price > ema_trend_now and ema_slow_slope > 0 and bullish_hl)
    is_bearish = (ema_fast_now < ema_slow_now and price < ema_trend_now and ema_slow_slope < 0 and bearish_hl)

    if is_bullish:
        return "BULLISH"
    elif is_bearish:
        return "BEARISH"
    return "NEUTRAL"

# ─────────────────────────────────────────────
# SCORING ENGINE (unchanged)
# ─────────────────────────────────────────────

def compute_score(indicators, trend_summary, current_price, m1_candles):
    score = 0
    trend_score = 0
    if trend_summary['h1'] == trend_summary['m15'] == trend_summary['m5']:
        if trend_summary['h1'] != 'NEUTRAL':
            trend_score = 25
        else:
            trend_score = 8
    elif trend_summary['h1'] == trend_summary['m15'] and trend_summary['h1'] != 'NEUTRAL':
        trend_score = 18
    elif trend_summary['m15'] == trend_summary['m5'] and trend_summary['m15'] != 'NEUTRAL':
        trend_score = 12
    else:
        trend_score = 5
    score += trend_score

    adx = indicators.get('adx14', 0)
    if adx is not None:
        if adx >= 40:
            adx_score = 20
        elif adx >= 30:
            adx_score = 15
        elif adx >= 20:
            adx_score = 8
        else:
            adx_score = 0
        score += adx_score
    else:
        score += 4

    atr = indicators.get('atr14', 0)
    if atr is not None:
        if atr >= 1.0:
            atr_score = 15
        elif atr >= 0.6:
            atr_score = 10
        elif atr >= 0.4:
            atr_score = 5
        else:
            atr_score = 0
        score += atr_score
    else:
        score += 3

    rsi = indicators.get('rsi14', 50)
    if rsi is not None:
        if 30 <= rsi <= 70:
            rsi_score = 15
        elif 20 <= rsi <= 80:
            rsi_score = 8
        else:
            rsi_score = 0
        score += rsi_score
    else:
        score += 5

    ema20 = indicators.get('ema20')
    if ema20:
        diff_pct = abs(current_price - ema20) / ema20 * 100
        if diff_pct < 0.3:
            ema_score = 15
        elif diff_pct < 0.8:
            ema_score = 10
        elif diff_pct < 1.5:
            ema_score = 5
        else:
            ema_score = 0
        score += ema_score
    else:
        score += 5

    if len(m1_candles) >= 2:
        last = m1_candles[-1]
        body = abs(last['close'] - last['open'])
        range_ = last['high'] - last['low']
        if range_ > 0:
            body_ratio = body / range_
            if body_ratio > 0.7:
                body_score = 10
            elif body_ratio > 0.5:
                body_score = 7
            elif body_ratio > 0.3:
                body_score = 4
            else:
                body_score = 0
        else:
            body_score = 0
        score += body_score
    else:
        score += 2

    return min(100, max(0, score))

# ─────────────────────────────────────────────
# RULE ENGINE (Enhanced with last candle direction)
# ─────────────────────────────────────────────

def rule_engine_decision(indicators, trend_summary, m1_candles):
    """Rule-based fallback with EMA20>EMA50 and last candle direction."""
    if trend_summary['h1'] != trend_summary['m15'] or trend_summary['m15'] != trend_summary['m5']:
        return "HOLD", 0
    if trend_summary['h1'] == "NEUTRAL":
        return "HOLD", 0

    adx = indicators.get('adx14', 0)
    atr = indicators.get('atr14', 0)
    if adx is None or adx < 25 or atr is None or atr < 0.4:
        return "HOLD", 0

    rsi = indicators.get('rsi14', 50)
    if rsi is None:
        rsi = 50

    ema20 = indicators.get('ema20')
    ema50 = indicators.get('ema50')
    if ema20 is None or ema50 is None:
        return "HOLD", 0

    # Check last candle direction (bullish = close > open, bearish = close < open)
    if len(m1_candles) < 2:
        return "HOLD", 0
    last = m1_candles[-1]
    bullish_candle = last['close'] > last['open']
    bearish_candle = last['close'] < last['open']

    # BUY if: trend bullish, ema20>ema50, rsi<70, and last candle bullish
    if trend_summary['h1'] == "BULLISH" and ema20 > ema50 and rsi < 70 and bullish_candle:
        return "BUY", 70
    # SELL if: trend bearish, ema20<ema50, rsi>30, and last candle bearish
    elif trend_summary['h1'] == "BEARISH" and ema20 < ema50 and rsi > 30 and bearish_candle:
        return "SELL", 70
    else:
        return "HOLD", 0

# ─────────────────────────────────────────────
# AI ANALYSIS LAYER (unchanged)
# ─────────────────────────────────────────────

def build_prompt(current_price, m1_candles, indicators, trend_summary, news_events, score, duplicate_status, stats):
    candles_summary = "\n".join([
        f"  {c['datetime']}: O={c['open']} H={c['high']} L={c['low']} C={c['close']}"
        for c in m1_candles[-20:]
    ])
    news_summary = "\n".join([
        f"  {e.get('date', '')} {e.get('title', '')} (impact: {e.get('impact', '')})"
        for e in news_events
    ]) or "  Tiada high/medium impact USD news hari ini."

    threshold = get_score_threshold()
    total = stats['total']
    wins = stats['wins']
    losses = stats['losses']
    winrate = round(wins/(wins+losses)*100, 1) if (wins+losses) > 0 else 0
    signals_today = get_signals_today()

    trend_strength = "Strong" if (indicators.get('adx14', 0) or 0) > 35 else "Moderate"

    prompt = f"""Kau seorang SCALPER XAUUSD berpengalaman di timeframe M1. Sistem sudah melakukan penapisan awal:
- Score: {score}/100 (ambang {threshold})
- Trend Strength: {trend_strength}
- Duplicate Signal: {duplicate_status}
- Average Win Rate: {winrate}% (from {total} signals)
- Signals Today: {signals_today}

TREND MULTI-TIMEFRAME (disahkan selaras):
- H1 Trend: {trend_summary['h1']}
- M15 Trend: {trend_summary['m15']}
- M5 Momentum: {trend_summary['m5']}

INDIKATOR M1 SEMASA:
- EMA20: {indicators['ema20']}
- EMA50: {indicators['ema50']}
- RSI14: {indicators['rsi14']}
- ATR14: {indicators['atr14']}
- ADX14: {indicators['adx14']} (>= {MIN_ADX_M1} bermakna trending)

HARGA SEMASA: {current_price}

20 CANDLE M1 TERKINI (dari terbaru ke lama):
{candles_summary}

ECONOMIC CALENDAR HARI INI (USD, High/Medium impact):
{news_summary}

PERATURAN KETAT:
1. ENTRY MESTI harga semasa ({current_price}) - market order.
2. TP sentiasa TEPAT 10 pips.
3. Cadangkan sl_pips (6-10 pips) berdasarkan ATR14 dan struktur candle.
4. Jika RSI overbought (>70) untuk BUY, atau oversold (<30) untuk SELL - HOLD.
5. Cari titik entry M1 yang bersih (breakout, rejection, continuation).
6. Beri confidence yang JUJUR.

Berikan jawapan HANYA dalam format JSON:
{{
  "decision": "BUY" atau "SELL" atau "HOLD",
  "confidence": <0-100>,
  "reason": "<penjelasan ringkas BM>",
  "key_level": "<micro support/resistance>",
  "sl_pips": <nombor pip>
}}"""
    return prompt

def call_groq(prompt):
    increment_api_counter('groq')
    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}",
        "Content-Type": "application/json"
    }
    json_data = {
        "model": "llama-3.3-70b-versatile",
        "max_tokens": 300,
        "messages": [{"role": "user", "content": prompt}]
    }
    resp = api_request_with_retry(url, method='POST', json_data=json_data, headers=headers, timeout=30, retries=3)
    if resp is None:
        log.error("Groq failed after retries")
        return None
    try:
        data = resp.json()
        if "choices" not in data:
            log.error(f"Groq API error: {data}")
            return None
        text = data["choices"][0]["message"]["content"]
        return parse_ai_json(text)
    except Exception as e:
        log.error(f"Groq API error: {e}")
        return None

def parse_ai_json(text):
    try:
        clean = text.strip()
        if clean.startswith("```"):
            clean = clean.split("```")[1]
            if clean.startswith("json"):
                clean = clean[4:]
        clean = clean.strip()
        return json.loads(clean)
    except Exception as e:
        log.error(f"Failed to parse AI response: {e} | raw: {text[:200]}")
        return None

# ─────────────────────────────────────────────
# TELEGRAM (Webhook) + auto-fix every 6 hours
# ─────────────────────────────────────────────

def send_telegram_message(text, chat_id=None):
    if chat_id is None:
        chat_id = TELEGRAM_CHAT_ID
    increment_api_counter('telegram')
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        resp = requests.post(url, json={
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "Markdown"
        }, timeout=15)
        if not resp.ok:
            log.error(f"Telegram send failed: {resp.text}")
    except Exception as e:
        log.error(f"Telegram error: {e}")

def set_webhook():
    if not WEBHOOK_URL:
        log.warning("WEBHOOK_URL not set. Cannot set webhook.")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/setWebhook"
    payload = {"url": f"{WEBHOOK_URL}/webhook"}
    try:
        resp = requests.post(url, json=payload, timeout=10)
        if resp.ok:
            log.info(f"Webhook set to {WEBHOOK_URL}/webhook")
        else:
            log.error(f"Failed to set webhook: {resp.text}")
    except Exception as e:
        log.error(f"Webhook set error: {e}")

def check_and_fix_webhook():
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getWebhookInfo"
        resp = requests.get(url, timeout=10)
        if resp.ok:
            data = resp.json()
            if data.get('ok'):
                current_url = data['result'].get('url')
                expected = f"{WEBHOOK_URL}/webhook"
                if current_url != expected:
                    log.warning(f"Webhook mismatch: {current_url} vs {expected}. Setting correct webhook.")
                    set_webhook()
                else:
                    log.info("Webhook already correctly set.")
            else:
                log.error(f"getWebhookInfo error: {data}")
        else:
            log.error(f"Failed to get webhook info: {resp.text}")
    except Exception as e:
        log.error(f"Webhook check error: {e}")

def webhook_maintenance_thread():
    """Run check_and_fix_webhook every 6 hours."""
    while True:
        check_and_fix_webhook()
        time.sleep(6 * 3600)  # 6 hours

# ─── Flask App (Single instance) ───
app = Flask(__name__)

@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        update = request.get_json()
        if not update:
            return "OK", 200
        process_telegram_update(update)
        return "OK", 200
    except Exception as e:
        log.error(f"Webhook error: {e}")
        return "OK", 200

@app.route("/")
def home():
    return "XAUUSD Signal Bot is running."

@app.route("/health", methods=["GET", "HEAD"])
def health():
    return "OK", 200

@app.route("/run-now")
def trigger_manual():
    threading.Thread(target=run_analysis).start()
    return "Analysis triggered, check Telegram/Logs in a few seconds.", 200

def process_telegram_update(update):
    if "message" not in update:
        return
    msg = update["message"]
    chat = msg.get("chat")
    if not chat or chat.get("id") != int(TELEGRAM_CHAT_ID):
        return
    text = msg.get("text")
    if not text or not text.startswith("/"):
        return
    parts = text.split(maxsplit=1)
    cmd = parts[0].lower()
    arg = parts[1] if len(parts) > 1 else None
    log.info(f"Received command: {cmd}")
    if cmd == "/start":
        cmd_start(arg, chat['id'])
    elif cmd == "/endsession":
        cmd_endsession(chat['id'])
    elif cmd == "/status":
        cmd_status(chat['id'])
    elif cmd == "/stats":
        cmd_stats(chat['id'])
    elif cmd == "/run":
        cmd_run(chat['id'])
    elif cmd == "/ping":
        cmd_ping(chat['id'])
    else:
        send_telegram_message("Perintah tidak dikenali. Gunakan: /start, /endsession, /status, /stats, /run, /ping", chat['id'])

# ─── Command Handlers ───

def cmd_start(arg, chat_id):
    active, expiry, started = get_session_state()
    if active:
        send_telegram_message("Sesi sudah aktif. Gunakan /endsession untuk matikan.", chat_id)
        return
    duration_seconds = None
    if arg:
        match = re.match(r"^(\d+\.?\d*)([hm])$", arg.strip())
        if match:
            val = float(match.group(1))
            unit = match.group(2)
            if unit == "h":
                duration_seconds = int(val * 3600)
            else:
                duration_seconds = int(val * 60)
            if duration_seconds <= 0:
                duration_seconds = None
    now = datetime.now(timezone.utc)
    expiry_time = now + timedelta(seconds=duration_seconds) if duration_seconds else None
    set_session_state(True, expiry_time, now)
    msg = "✅ *Trading Session Started*\n\n"
    msg += f"Status: ACTIVE\n"
    msg += f"Analysis: Every {CHECK_INTERVAL_MINUTES} minutes\n"
    msg += f"Signals: Enabled\n"
    if expiry_time:
        msg += f"\n⏳ Session will expire at: {expiry_time.strftime('%Y-%m-%d %H:%M UTC')}"
    send_telegram_message(msg, chat_id)

def cmd_endsession(chat_id):
    active, _, _ = get_session_state()
    if not active:
        send_telegram_message("Sesi sudah tidak aktif.", chat_id)
        return
    set_session_state(False, None, None)
    send_telegram_message("🛑 *Trading Session Ended*\n\nAnalysis paused.\nNo API requests will be made until /start.", chat_id)

def cmd_status(chat_id):
    active, expiry, started = get_session_state()
    active_str = "✅ ACTIVE" if active else "⏸ PAUSED"
    price = get_state('last_price') or 'N/A'
    trend = get_state('last_trend') or 'N/A'
    atr = get_state('last_atr') or 'N/A'
    adx = get_state('last_adx') or 'N/A'
    rsi = get_state('last_rsi') or 'N/A'
    score = get_state('last_score') or 'N/A'
    threshold = get_score_threshold()

    td = get_api_counter('twelvedata')
    groq = get_api_counter('groq')
    ff = get_api_counter('forexfactory')
    tel = get_api_counter('telegram')
    signals_today = get_signals_today()

    expiry_str = expiry.strftime("%Y-%m-%d %H:%M UTC") if expiry else "N/A"
    last_run = get_session_value('last_run_time')
    if last_run:
        last_run_dt = datetime.fromisoformat(last_run)
        last_run_str = last_run_dt.strftime("%Y-%m-%d %H:%M UTC")
    else:
        last_run_str = "Never"

    msg = f"*Bot Status*\n\n"
    msg += f"Trading : {active_str}\n"
    msg += f"Last Check : {last_run_str}\n"
    msg += f"Current Price : {price}\n"
    msg += f"Trend (H1) : {trend}\n"
    msg += f"ATR (M1) : {atr}\n"
    msg += f"ADX (M1) : {adx}\n"
    msg += f"RSI (M1) : {rsi}\n"
    msg += f"Score : {score}\n"
    msg += f"Threshold : {threshold}\n"
    msg += f"Session Expires : {expiry_str}\n"
    msg += f"Signals Today : {signals_today}\n"
    msg += f"API Calls (TD/Groq/FF/TG): {td}/{groq}/{ff}/{tel}\n"
    msg += f"Next check interval: {CHECK_INTERVAL_MINUTES} min"
    send_telegram_message(msg, chat_id)

def cmd_stats(chat_id):
    stats = get_stats()
    total = stats['total']
    wins = stats['wins']
    losses = stats['losses']
    be = stats['be']
    timeout = stats['timeout']
    winrate = round(wins/(wins+losses)*100, 1) if (wins+losses) > 0 else 0
    profit_factor = stats['profit_factor']

    last30 = stats['last30']
    rr_list = []
    for s in last30:
        if s['result'] in ('WIN', 'LOSS'):
            risk = abs(s['entry'] - s['sl'])
            reward = abs(s['tp'] - s['entry'])
            if risk > 0:
                rr_list.append(reward / risk)
    avg_rr = round(sum(rr_list)/len(rr_list), 2) if rr_list else 0

    with _db_lock:
        conn = get_db_conn()
        rows = conn.execute(
            "SELECT result FROM signals WHERE result IS NOT NULL ORDER BY id DESC LIMIT 100"
        ).fetchall()
        conn.close()
    results = [r['result'] for r in rows]
    longest_win_streak = 0
    longest_loss_streak = 0
    current_win = 0
    current_loss = 0
    for res in results:
        if res == 'WIN':
            current_win += 1
            current_loss = 0
            longest_win_streak = max(longest_win_streak, current_win)
        elif res == 'LOSS':
            current_loss += 1
            current_win = 0
            longest_loss_streak = max(longest_loss_streak, current_loss)
        else:
            current_win = 0
            current_loss = 0

    today_str = datetime.now(timezone.utc).date().isoformat()
    with _db_lock:
        conn = get_db_conn()
        row = conn.execute(
            "SELECT COUNT(*) FROM signals WHERE date(timestamp) = ? AND result IS NOT NULL",
            (today_str,)
        ).fetchone()
        total_today = row[0] if row else 0
        row2 = conn.execute(
            "SELECT COUNT(*) FROM signals WHERE date(timestamp) = ? AND result = 'WIN'",
            (today_str,)
        ).fetchone()
        wins_today = row2[0] if row2 else 0
        conn.close()
    winrate_today = round(wins_today/total_today*100, 1) if total_today > 0 else 0

    last30_buys = sum(1 for s in last30 if s['direction'] == 'BUY')
    last30_sells = sum(1 for s in last30 if s['direction'] == 'SELL')
    last30_wins = sum(1 for s in last30 if s['result'] == 'WIN')
    last30_losses = sum(1 for s in last30 if s['result'] == 'LOSS')
    last30_be = sum(1 for s in last30 if s['result'] == 'BE')
    last30_timeout = sum(1 for s in last30 if s['result'] == 'TIMEOUT')

    msg = f"*Signal Statistics*\n\n"
    msg += f"Total Signals : {total}\n"
    msg += f"Wins : {wins}\n"
    msg += f"Losses : {losses}\n"
    msg += f"Break-Even : {be}\n"
    msg += f"Timeout : {timeout}\n"
    msg += f"Win Rate (All) : {winrate}%\n"
    msg += f"Win Rate (Today) : {winrate_today}%\n"
    msg += f"Profit Factor : {profit_factor}\n"
    msg += f"Avg RR : {avg_rr}\n"
    msg += f"Longest Win Streak : {longest_win_streak}\n"
    msg += f"Longest Loss Streak : {longest_loss_streak}\n\n"
    msg += f"*Last 30 Signals:*\n"
    msg += f"BUY: {last30_buys}  SELL: {last30_sells}\n"
    msg += f"W: {last30_wins}  L: {last30_losses}  BE: {last30_be}  TO: {last30_timeout}"
    send_telegram_message(msg, chat_id)

def cmd_run(chat_id):
    active, _, _ = get_session_state()
    if not active:
        send_telegram_message("Session is paused. Use /start to activate.", chat_id)
        return
    send_telegram_message("🔄 Manual analysis triggered. Check Telegram/Logs in a few seconds.", chat_id)
    threading.Thread(target=run_analysis, daemon=True).start()

def cmd_ping(chat_id):
    send_telegram_message("✅ Bot Online", chat_id)

def get_signals_today():
    today_str = datetime.now(timezone.utc).date().isoformat()
    with _db_lock:
        conn = get_db_conn()
        row = conn.execute(
            "SELECT COUNT(*) FROM signals WHERE date(timestamp) = ?",
            (today_str,)
        ).fetchone()
        conn.close()
    return row[0] if row else 0

# ─────────────────────────────────────────────
# FORMAT NOTIFICATION
# ─────────────────────────────────────────────

def format_notification(result, trend_summary, indicators=None):
    decision = result.get("decision", "").upper()
    emoji = {"BUY": "🟢", "SELL": "🔴"}.get(decision, "⚪")
    lines = [
        f"{emoji} *XAUUSD 10-Pip Scalp Signal (M1) — {decision} NOW*",
        f"Entry: `{result.get('entry')}` (harga semasa - market order)",
        f"Confidence: {result.get('confidence', '?')}%",
        f"Trend: H1 {trend_summary['h1']} | M15 {trend_summary['m15']} | M5 {trend_summary['m5']}",
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
    lines.append(f"\n⏳ Signal valid for {SIGNAL_VALID_MINUTES} minutes from entry.")
    lines.append("")
    lines.append(f"🕐 {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    lines.append("\n⚠️ Ini analysis sahaja. Buat keputusan buy/sell sendiri dalam MT5.")
    return "\n".join(lines)

# ─────────────────────────────────────────────
# AUTO-LEARNING (Improved with 5-point buckets)
# ─────────────────────────────────────────────

def adjust_threshold():
    """Analyze win rate per 5-point bucket and choose threshold with best performance."""
    with _db_lock:
        conn = get_db_conn()
        rows = conn.execute(
            '''SELECT score, result FROM signals 
               WHERE result IS NOT NULL 
               ORDER BY id DESC LIMIT 500''',
        ).fetchall()
        conn.close()
    if len(rows) < 30:
        return

    # Group by 5-point buckets (40-44, 45-49, 50-54, ...)
    buckets = {}
    for r in rows:
        bucket = (r['score'] // 5) * 5
        buckets.setdefault(bucket, {'wins': 0, 'losses': 0})
        if r['result'] == 'WIN':
            buckets[bucket]['wins'] += 1
        elif r['result'] == 'LOSS':
            buckets[bucket]['losses'] += 1

    best_bucket = None
    best_winrate = 0
    for bucket, counts in sorted(buckets.items()):
        total = counts['wins'] + counts['losses']
        if total < 5:
            continue
        winrate = counts['wins'] / total
        if winrate > best_winrate:
            best_winrate = winrate
            best_bucket = bucket

    if best_bucket is None:
        return

    # New threshold = best bucket + 3 (middle of the 5-point bucket)
    new_threshold = best_bucket + 3
    new_threshold = max(MIN_SCORE_THRESHOLD, min(MAX_SCORE_THRESHOLD, new_threshold))
    current = get_score_threshold()
    if new_threshold != current:
        set_score_threshold(new_threshold)
        log.info(f"Auto-learning: best winrate {best_winrate:.2f} at bucket {best_bucket}, threshold set to {new_threshold}")

# ─────────────────────────────────────────────
# MAIN ANALYSIS JOB
# ─────────────────────────────────────────────

def run_analysis():
    log.info("Running scheduled analysis...")

    active, _, _ = get_session_state()
    if not active:
        log.info("Session not active - skipping.")
        return

    set_session_value('last_run_time', datetime.now(timezone.utc).isoformat())

    m1_candles = fetch_candles("1min", M1_OUTPUTSIZE)
    if not m1_candles or len(m1_candles) < 60:
        log.error("Data M1 tidak cukup - skip.")
        return

    current_price = m1_candles[-1]["close"]

    # Duplicate protection - init variables
    same_direction = False
    same_trend = False
    elapsed = 9999
    price_diff = 9999

    last_signal = get_last_signal()
    if last_signal:
        last_time = datetime.fromisoformat(last_signal['timestamp'])
        elapsed = (datetime.now(timezone.utc) - last_time).total_seconds() / 60
        price_diff = abs(current_price - last_signal['entry'])
        last_direction = last_signal['direction']
        last_trend_summary = json.loads(last_signal['trend_summary']) if last_signal.get('trend_summary') else {}
    else:
        last_direction = None
        last_trend_summary = {}

    m5_candles = aggregate_candles(m1_candles, 5)
    m15_candles = aggregate_candles(m1_candles, 15)
    h1_candles = get_h1_trend_data()

    h1_dir = trend_bias(h1_candles, min_atr=MIN_ATR_H1) if h1_candles else "NEUTRAL"
    m15_dir = trend_bias(m15_candles, min_atr=MIN_ATR_M15)
    m5_dir = trend_bias(m5_candles, min_atr=MIN_ATR_M5)
    log.info(f"Trend: H1={h1_dir}, M15={m15_dir}, M5={m5_dir}")
    aligned_bullish = h1_dir == "BULLISH" and m15_dir == "BULLISH" and m5_dir == "BULLISH"
    aligned_bearish = h1_dir == "BEARISH" and m15_dir == "BEARISH" and m5_dir == "BEARISH"
    if not (aligned_bullish or aligned_bearish):
        log.info("Trend not aligned - HOLD.")
        return

    current_trend_summary = {"h1": h1_dir, "m15": m15_dir, "m5": m5_dir}
    current_direction = "BUY" if aligned_bullish else "SELL"

    # Check duplicate with direction and trend
    if last_signal:
        same_direction = (last_direction == current_direction)
        same_trend = (last_trend_summary.get('h1') == h1_dir and
                      last_trend_summary.get('m15') == m15_dir and
                      last_trend_summary.get('m5') == m5_dir)
        if (elapsed < COOLDOWN_MINUTES and price_diff < MIN_PRICE_DIFF and same_direction and same_trend):
            log.info(f"Duplicate protection: same direction/trend, last signal {elapsed:.1f} min ago, price diff {price_diff:.2f} - skip.")
            return

    news_events = get_today_news()
    minutes_to_news = minutes_until_next_high_impact(news_events)
    if minutes_to_news is not None and minutes_to_news <= NEWS_BLACKOUT_MINUTES:
        log.info(f"News in {round(minutes_to_news)} min - HOLD.")
        return

    m1_closes = [c["close"] for c in m1_candles]
    ema20_series = ema_series(m1_closes, 20)
    ema50_series = ema_series(m1_closes, 50)
    indicators = {
        "ema20": round(ema20_series[-1], 2) if ema20_series else None,
        "ema50": round(ema50_series[-1], 2) if ema50_series else None,
        "rsi14": rsi_latest(m1_closes, 14),
        "atr14": atr_latest(m1_candles, 14),
        "adx14": adx_latest(m1_candles, 14),
    }

    if indicators["atr14"] is None or indicators["atr14"] < MIN_ATR_M1_TRADE:
        log.info(f"ATR14 {indicators['atr14']} < {MIN_ATR_M1_TRADE} - skip.")
        return
    if indicators["adx14"] is None or indicators["adx14"] < MIN_ADX_M1:
        log.info(f"ADX14 {indicators['adx14']} < {MIN_ADX_M1} - skip.")
        return

    trend_summary = {"h1": h1_dir, "m15": m15_dir, "m5": m5_dir}

    set_state('last_price', current_price)
    set_state('last_trend', h1_dir)
    set_state('last_atr', indicators['atr14'])
    set_state('last_adx', indicators['adx14'])
    set_state('last_rsi', indicators['rsi14'])

    score = compute_score(indicators, trend_summary, current_price, m1_candles)
    set_state('last_score', score)
    threshold = get_score_threshold()
    log.info(f"Scoring: {score} (threshold {threshold})")

    if score < threshold:
        log.info(f"Score {score} < {threshold} - skip AI.")
        return

    stats = get_stats()
    dup_status = "No" if (not last_signal or elapsed >= COOLDOWN_MINUTES or price_diff >= MIN_PRICE_DIFF or not same_direction or not same_trend) else "Yes (blocked)"
    prompt = build_prompt(current_price, m1_candles, indicators, trend_summary, news_events, score, dup_status, stats)
    log.info("Calling Groq...")
    result = call_groq(prompt)

    if result is None:
        log.warning("Groq unavailable. Falling back to Rule Engine.")
        decision, confidence = rule_engine_decision(indicators, trend_summary, m1_candles)
        if decision == "HOLD":
            log.info("Rule engine says HOLD.")
            return
        result = {
            "decision": decision,
            "confidence": confidence,
            "reason": "Rule Engine fallback due to Groq unavailability.",
            "key_level": "N/A",
            "sl_pips": DEFAULT_SL_PIPS
        }
    else:
        decision = result.get("decision", "").upper()
        confidence = result.get("confidence", 0)
        if decision not in ("BUY", "SELL"):
            log.info("AI HOLD - skip.")
            return
        if confidence < CONFIDENCE_THRESHOLD:
            log.info(f"Confidence {confidence} < {CONFIDENCE_THRESHOLD} - skip.")
            return

    sl_pips = result.get("sl_pips") or DEFAULT_SL_PIPS
    try:
        sl_pips = float(sl_pips)
    except:
        sl_pips = DEFAULT_SL_PIPS

    entry = current_price
    if decision == "BUY":
        tp = round(entry + TP_PIPS * PIP_SIZE, 2)
        sl = round(entry - sl_pips * PIP_SIZE, 2)
    else:
        tp = round(entry - TP_PIPS * PIP_SIZE, 2)
        sl = round(entry + sl_pips * PIP_SIZE, 2)

    max_drift = round(MAX_ENTRY_DRIFT_PIPS * PIP_SIZE, 2)
    if decision == "BUY":
        invalid_beyond = round(entry + max_drift, 2)
        entry_condition = (f"Sah HANYA jika harga masih > EMA20 ({indicators['ema20']}) "
                           f"DAN belum melepasi {invalid_beyond}. Kalau dah lajak drpd tu, SKIP.")
    else:
        invalid_beyond = round(entry - max_drift, 2)
        entry_condition = (f"Sah HANYA jika harga masih < EMA20 ({indicators['ema20']}) "
                           f"DAN belum jatuh bawah {invalid_beyond}. Kalau dah lajak drpd tu, SKIP.")

    result["entry"] = entry
    result["sl"] = sl
    result["tp"] = tp
    result["entry_condition"] = entry_condition

    # signal_candle_datetime is the datetime of the candle that contains the entry price.
    # This is the most recent candle (m1_candles[-1]).
    signal_candle_dt = _parse_dt(m1_candles[-1]["datetime"])

    # Prepare entry_indicators for DB
    entry_indicators = {
        "atr": indicators.get("atr14"),
        "adx": indicators.get("adx14"),
        "rsi": indicators.get("rsi14"),
        "ema20": indicators.get("ema20"),
        "ema50": indicators.get("ema50"),
        "score": score,
        "confidence": confidence,
        "ai_reason": result.get("reason", "")
    }

    signal_id = add_signal(decision, entry, tp, sl, confidence, trend_summary, score, signal_candle_dt, entry_indicators)
    log.info(f"Signal saved id={signal_id}")

    message = format_notification(result, trend_summary, indicators)
    send_telegram_message(message)

    total_signals = get_stats()['total']
    if total_signals % LEARNING_INTERVAL == 0:
        adjust_threshold()

    log.info(f"SIGNAL SENT - {decision} @ {confidence}% | Entry {entry} SL {sl} TP {tp}")

# ─────────────────────────────────────────────
# SIGNAL RESULT CHECKER (Stable with exact candle timestamp)
# ─────────────────────────────────────────────

def check_signal_results():
    pending = get_pending_signals()
    if not pending:
        return
    log.info(f"Checking {len(pending)} pending signals...")
    for sig in pending:
        # Use signal_candle_datetime if available, else fallback to signal timestamp
        if sig.get('signal_candle_datetime'):
            try:
                signal_candle_time = datetime.fromisoformat(sig['signal_candle_datetime'])
            except:
                signal_candle_time = datetime.fromisoformat(sig['timestamp'])
        else:
            signal_candle_time = datetime.fromisoformat(sig['timestamp'])

        now = datetime.now(timezone.utc)
        duration_min = (now - signal_candle_time).total_seconds() / 60
        if duration_min < 1:
            continue

        # Fetch enough candles from signal_candle_time onward
        output = int(duration_min) + 20
        if output > 500:
            output = 500
        candles = fetch_candles("1min", output)
        if not candles:
            continue

        # Filter candles with datetime >= signal_candle_time
        filtered = []
        for c in candles:
            dt = _parse_dt(c["datetime"])
            if dt >= signal_candle_time:
                filtered.append(c)

        if not filtered:
            log.warning(f"No candles found after signal time {signal_candle_time} for signal {sig['id']}")
            continue

        direction = sig['direction']
        tp = sig['tp']
        sl = sig['sl']
        result = None
        for c in filtered:
            hit_tp = False
            hit_sl = False
            if direction == 'BUY':
                if c['high'] >= tp:
                    hit_tp = True
                if c['low'] <= sl:
                    hit_sl = True
            else:
                if c['low'] <= tp:
                    hit_tp = True
                if c['high'] >= sl:
                    hit_sl = True
            if hit_tp and hit_sl:
                result = 'LOSS'
                break
            if hit_tp:
                result = 'WIN'
                break
            if hit_sl:
                result = 'LOSS'
                break
        if result is None:
            result = 'TIMEOUT'
        update_signal_result(sig['id'], result)
        log.info(f"Signal {sig['id']} result: {result}")

def result_checker_thread():
    while True:
        check_signal_results()
        time.sleep(60)

# ─────────────────────────────────────────────
# SCHEDULER
# ─────────────────────────────────────────────

def run_scheduler():
    schedule.every(CHECK_INTERVAL_MINUTES).minutes.do(run_analysis)
    log.info(f"Scheduler started, running every {CHECK_INTERVAL_MINUTES} minutes.")
    run_analysis()
    while True:
        schedule.run_pending()
        time.sleep(15)

# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

if __name__ == "__main__":
    init_db()
    if TELEGRAM_BOT_TOKEN and WEBHOOK_URL:
        # Initial webhook check
        check_and_fix_webhook()
        # Start webhook maintenance thread (every 6 hours)
        threading.Thread(target=webhook_maintenance_thread, daemon=True).start()
    else:
        log.warning("Webhook not set - missing token or WEBHOOK_URL.")
    threading.Thread(target=result_checker_thread, daemon=True).start()
    scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
    scheduler_thread.start()
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
