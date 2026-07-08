"""
XAUUSD AI Signal Bot v9.0 – Stable & Adaptive
==============================================
Improvements over v8.0:
1. Fixed potential SQLite deadlock in check_outcomes() by using a single connection
   for all updates within the transaction.
2. H1 ATR filter now uses dynamic threshold (70% of 30‑period average ATR).
3. H1 bearish filter uses explicit `h1_ema20_falling` flag instead of negating rising.
4. Market regime detection remains Python‑based (can be extended with ADX/Bollinger).
5. Outcome checker still limited by M5 resolution (intrabar ambiguity → UNKNOWN).
6. Confidence calibration still basic (score‑based) – ready for multi‑context learning in future.
"""

import os
import re
import json
import time
import logging
import sqlite3
import threading
import requests
from datetime import datetime, timezone, timedelta
from threading import Thread
from flask import Flask
import schedule

# ──────────────────────────── CONFIG ────────────────────────────
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID   = os.environ.get("TELEGRAM_CHAT_ID")
GROQ_API_KEY       = os.environ.get("GROQ_API_KEY")
TWELVEDATA_API_KEY = os.environ.get("TWELVEDATA_API_KEY")

SYMBOL = "XAU/USD"
CHECK_INTERVAL      = 15          # minutes
COOLDOWN_MINUTES    = 45
MIN_CONFIDENCE_SCORE = 65

# Session times (UTC)
ASIAN   = (0, 8)
LONDON  = (8, 16)
NY      = (13, 21)
OVERLAP = (13, 16)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger("xauusd-bot-v9")

analysis_lock = threading.Lock()

# ──────────────────────────── GLOBAL CACHE ────────────────────────────
cache = {
    "daily":     {"data": None, "ts": datetime.min.replace(tzinfo=timezone.utc)},
    "weekly":    {"data": None, "ts": datetime.min.replace(tzinfo=timezone.utc)},
    "monthly":   {"data": None, "ts": datetime.min.replace(tzinfo=timezone.utc)},
    "news":      {"data": [], "ts": datetime.min.replace(tzinfo=timezone.utc)},
    "ai":        {"last_m5_ts": None, "response": None},
    "ai_fail_ts": datetime.min.replace(tzinfo=timezone.utc),
}

# ──────────────────────────── UTILS ────────────────────────────
def fetch_with_retry(url, params=None, headers=None, timeout=15, max_retries=3, backoff=2):
    for attempt in range(max_retries):
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=timeout)
            if resp.status_code == 200:
                return resp.json()
            else:
                log.warning(f"HTTP {resp.status_code} attempt {attempt+1}")
        except Exception as e:
            log.warning(f"Request error attempt {attempt+1}: {e}")
        time.sleep(backoff ** attempt)
    return None

def escape_html(text):
    return (str(text)
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace('"', "&quot;")
            .replace("'", "&#x27;"))

# ──────────────────────────── DATA LAYER (with cache) ────────────────────────────
def fetch_twelvedata(interval, outputsize):
    url = "https://api.twelvedata.com/time_series"
    params = {
        "symbol": SYMBOL,
        "interval": interval,
        "outputsize": outputsize,
        "apikey": TWELVEDATA_API_KEY
    }
    data = fetch_with_retry(url, params=params)
    if data and "values" in data:
        return data["values"]
    return []

def get_daily_levels():
    now = datetime.now(timezone.utc)
    if cache["daily"]["data"] and (now - cache["daily"]["ts"]) < timedelta(hours=1):
        return cache["daily"]["data"]
    url = "https://api.twelvedata.com/time_series"
    params = {
        "symbol": SYMBOL,
        "interval": "1day",
        "outputsize": 2,
        "apikey": TWELVEDATA_API_KEY
    }
    data = fetch_with_retry(url, params=params)
    if data and "values" in data and len(data["values"]) >= 2:
        yesterday = data["values"][1]
        levels = {"pdh": float(yesterday["high"]), "pdl": float(yesterday["low"])}
        cache["daily"] = {"data": levels, "ts": now}
        return levels
    return None

def get_weekly_levels():
    now = datetime.now(timezone.utc)
    if cache["weekly"]["data"] and (now - cache["weekly"]["ts"]) < timedelta(hours=6):
        return cache["weekly"]["data"]
    url = "https://api.twelvedata.com/time_series"
    params = {
        "symbol": SYMBOL,
        "interval": "1week",
        "outputsize": 2,
        "apikey": TWELVEDATA_API_KEY
    }
    data = fetch_with_retry(url, params=params)
    if data and "values" in data and len(data["values"]) >= 2:
        last_week = data["values"][1]
        levels = {"pwh": float(last_week["high"]), "pwl": float(last_week["low"])}
        cache["weekly"] = {"data": levels, "ts": now}
        return levels
    return None

def get_monthly_levels():
    now = datetime.now(timezone.utc)
    if cache["monthly"]["data"] and (now - cache["monthly"]["ts"]) < timedelta(hours=12):
        return cache["monthly"]["data"]
    url = "https://api.twelvedata.com/time_series"
    params = {
        "symbol": SYMBOL,
        "interval": "1month",
        "outputsize": 2,
        "apikey": TWELVEDATA_API_KEY
    }
    data = fetch_with_retry(url, params=params)
    if data and "values" in data and len(data["values"]) >= 2:
        last_month = data["values"][1]
        levels = {"pmh": float(last_month["high"]), "pml": float(last_month["low"])}
        cache["monthly"] = {"data": levels, "ts": now}
        return levels
    return None

def get_economic_calendar():
    now = datetime.now(timezone.utc)
    if cache["news"]["data"] and (now - cache["news"]["ts"]) < timedelta(minutes=15):
        return cache["news"]["data"]
    try:
        url = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"
        events = fetch_with_retry(url)
        if not events:
            return cache["news"]["data"]
        today = now.strftime("%Y-%m-%d")
        relevant = []
        for e in events:
            if e.get("country") == "USD" and e.get("impact") in ("High", "Medium"):
                date_str = e.get("date", "")
                if date_str.startswith(today):
                    try:
                        et = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                        minutes_left = int((et - now).total_seconds() / 60)
                    except:
                        minutes_left = 999
                    relevant.append({
                        "title": e.get("title", ""),
                        "impact": e.get("impact", ""),
                        "minutes": minutes_left
                    })
        news = sorted(relevant, key=lambda x: x["minutes"])
        cache["news"] = {"data": news, "ts": now}
        return news
    except Exception as e:
        log.error(f"Calendar error: {e}")
        return cache["news"]["data"]

def get_session():
    h = datetime.now(timezone.utc).hour
    if OVERLAP[0] <= h < OVERLAP[1]:
        return ("LONDON-NY OVERLAP", "HIGH")
    elif LONDON[0] <= h < LONDON[1]:
        return ("LONDON", "HIGH")
    elif NY[0] <= h < NY[1]:
        return ("NEW YORK", "HIGH")
    elif ASIAN[0] <= h < ASIAN[1]:
        return ("ASIAN", "LOW")
    else:
        return ("TRANSITION", "MEDIUM")

# ──────────────────────────── DATA VALIDATION ────────────────────────────
def validate_data(m5, m15, h1=None):
    if not m5 or len(m5) < 50:
        return False, "M5 insufficient candles"
    if not m15 or len(m15) < 20:
        return False, "M15 insufficient candles"
    if h1 and len(h1) < 20:
        return False, "H1 insufficient candles"
    def check_sequential(candles, interval_min):
        for i in range(len(candles)-1):
            try:
                t1 = datetime.fromisoformat(candles[i]["datetime"].replace("Z", "+00:00"))
                t2 = datetime.fromisoformat(candles[i+1]["datetime"].replace("Z", "+00:00"))
                if (t1 - t2).total_seconds() > interval_min * 60 * 2:
                    return False
            except:
                return False
        return True
    if not check_sequential(m5, 5):
        return False, "M5 timestamps not sequential"
    if not check_sequential(m15, 15):
        return False, "M15 timestamps not sequential"
    for c in m5+m15:
        try:
            o, h, l, cl = float(c["open"]), float(c["high"]), float(c["low"]), float(c["close"])
            if not (l <= o <= h and l <= cl <= h) or o <= 0 or h <= 0:
                return False, f"OHLC invalid: {c['datetime']}"
        except:
            return False, "Non-numeric OHLC"
    m5_times = [c["datetime"] for c in m5]
    if len(set(m5_times)) != len(m5_times):
        return False, "Duplicate M5 candles"
    return True, "OK"

# ──────────────────────────── FEATURE ENGINEERING ────────────────────────────
def ema(series, period):
    if len(series) < period:
        return None
    k = 2 / (period + 1)
    ema_val = series[0]
    for price in series[1:]:
        ema_val = price * k + ema_val * (1 - k)
    return ema_val

def atr_wilder(candles, period=14):
    if len(candles) < period + 1:
        return 0
    tr = []
    for i in range(len(candles)-1):
        h = float(candles[i]["high"])
        l = float(candles[i]["low"])
        c_prev = float(candles[i+1]["close"])
        tr.append(max(h-l, abs(h-c_prev), abs(l-c_prev)))
    if len(tr) < period:
        return 0
    atr_val = sum(tr[:period]) / period
    for i in range(period, len(tr)):
        atr_val = (atr_val * (period - 1) + tr[i]) / period
    return atr_val

def average_true_range(candles, period=30):
    tr_list = []
    for i in range(min(len(candles)-1, period)):
        h = float(candles[i]["high"])
        l = float(candles[i]["low"])
        c_prev = float(candles[i+1]["close"])
        tr = max(h-l, abs(h-c_prev), abs(l-c_prev))
        tr_list.append(tr)
    return sum(tr_list)/len(tr_list) if tr_list else 0

def asian_range(m5):
    candles = []
    for c in m5:
        dt = datetime.fromisoformat(c["datetime"].replace("Z", "+00:00"))
        if 0 <= dt.hour < 8:
            candles.append(c)
    if not candles:
        return None, None
    return max(float(c["high"]) for c in candles), min(float(c["low"]) for c in candles)

def calculate_features(m5, m15, h1=None):
    # M5
    closes_m5 = [float(c["close"]) for c in m5[::-1]]
    current = closes_m5[-1]
    ema20_m5 = ema(closes_m5, 20)
    ema50_m5 = ema(closes_m5, 50) if len(closes_m5) >= 50 else None
    atr_m5 = atr_wilder(m5, 14)
    atr_avg_30 = average_true_range(m5, 30) if len(m5) >= 31 else None
    # M15
    closes_m15 = [float(c["close"]) for c in m15[::-1]]
    ema20_m15 = ema(closes_m15, 20) if len(closes_m15) >= 20 else None
    ema50_m15 = ema(closes_m15, 50) if len(closes_m15) >= 50 else None
    atr_m15 = atr_wilder(m15, 14)
    # H1
    h1_bias = None
    h1_ema20_rising = False
    h1_ema20_falling = False
    h1_price_above_ema20 = False
    h1_atr_ok = True
    atr_h1 = None
    atr_h1_avg = None
    if h1 and len(h1) >= 50:
        closes_h1 = [float(c["close"]) for c in h1[::-1]]
        ema20_h1 = ema(closes_h1, 20)
        ema50_h1 = ema(closes_h1, 50) if len(closes_h1) >= 50 else None
        if ema20_h1 and ema50_h1:
            if ema20_h1 > ema50_h1:
                h1_bias = "BULLISH"
            elif ema20_h1 < ema50_h1:
                h1_bias = "BEARISH"
            else:
                h1_bias = "NEUTRAL"
            # EMA direction
            prev_ema20_h1 = ema(closes_h1[:-1], 20) if len(closes_h1) > 20 else None
            if prev_ema20_h1:
                if ema20_h1 > prev_ema20_h1:
                    h1_ema20_rising = True
                elif ema20_h1 < prev_ema20_h1:
                    h1_ema20_falling = True
            h1_price_above_ema20 = closes_h1[-1] > ema20_h1
            # ATR H1 dynamic threshold
            atr_h1 = atr_wilder(h1, 14)
            atr_h1_avg = average_true_range(h1, 30) if len(h1) >= 31 else None
            if atr_h1 and atr_h1_avg:
                h1_atr_ok = (atr_h1 >= atr_h1_avg * 0.7)  # must be at least 70% of average
            else:
                h1_atr_ok = True  # not enough data, allow
    asian_high, asian_low = asian_range(m5)
    daily = get_daily_levels()
    weekly = get_weekly_levels()
    monthly = get_monthly_levels()
    return {
        "current_price": current,
        "ema20_m5": ema20_m5,
        "ema50_m5": ema50_m5,
        "atr_m5": atr_m5,
        "atr_m15": atr_m15,
        "atr_avg_30": atr_avg_30,
        "asian_high": asian_high,
        "asian_low": asian_low,
        "pdh": daily["pdh"] if daily else None,
        "pdl": daily["pdl"] if daily else None,
        "pwh": weekly["pwh"] if weekly else None,
        "pwl": weekly["pwl"] if weekly else None,
        "pmh": monthly["pmh"] if monthly else None,
        "pml": monthly["pml"] if monthly else None,
        "h1_bias": h1_bias,
        "h1_ema20_rising": h1_ema20_rising,
        "h1_ema20_falling": h1_ema20_falling,
        "h1_price_above_ema20": h1_price_above_ema20,
        "h1_atr_ok": h1_atr_ok,
        "m5_candles": m5,
        "m15_candles": m15
    }

# ──────────────────────────── MARKET REGIME DETECTION ────────────────────────────
def detect_market_regime(features):
    atr5 = features["atr_m5"]
    ema20 = features["ema20_m5"]
    ema50 = features["ema50_m5"]
    price = features["current_price"]
    if atr5 >= 0.60:
        return "HIGH_VOL"
    elif atr5 <= 0.20:
        return "LOW_VOL"
    if ema20 and ema50:
        if ema20 > ema50 and price > ema20:
            return "TRENDING_UP"
        elif ema20 < ema50 and price < ema20:
            return "TRENDING_DOWN"
    return "RANGING"

# ──────────────────────────── SWING DETECTION (SCORING) ────────────────────────────
def find_swing_points(candles, lookback=3):
    highs = [(float(c["high"]), c["datetime"]) for c in candles]
    lows = [(float(c["low"]), c["datetime"]) for c in candles]
    swing_highs = []
    swing_lows = []
    if len(highs) < 2*lookback+1:
        return swing_highs, swing_lows
    for i in range(lookback, len(highs)-lookback):
        if highs[i][0] == max([h[0] for h in highs[i-lookback:i+lookback+1]]):
            swing_highs.append(highs[i])
        if lows[i][0] == min([l[0] for l in lows[i-lookback:i+lookback+1]]):
            swing_lows.append(lows[i])
    return swing_highs, swing_lows

def trend_direction_from_swings(features):
    m5 = features["m5_candles"]
    if not m5 or len(m5) < 20:
        return None
    swing_highs, swing_lows = find_swing_points(m5, lookback=3)
    if len(swing_highs) < 3 or len(swing_lows) < 3:
        return None
    sh = sorted(swing_highs, key=lambda x: x[1])
    sl = sorted(swing_lows, key=lambda x: x[1])
    score = 0
    for i in range(1, len(sh)):
        if sh[i][0] > sh[i-1][0]:
            score += 1  # HH
        else:
            score -= 1  # LH
    for i in range(1, len(sl)):
        if sl[i][0] > sl[i-1][0]:
            score += 1  # HL
        else:
            score -= 1  # LL
    if score >= 2:
        return "BUY"
    elif score <= -2:
        return "SELL"
    return None

# ──────────────────────────── AI STRUCTURE (with fail cache) ────────────────────────────
ALLOWED_TREND = ["BULLISH", "BEARISH", "NEUTRAL"]
ALLOWED_STATE = ["TRENDING", "RANGING", "BREAKOUT", "REVERSAL", "HIGH_VOL", "LOW_VOL"]
ALLOWED_STRUCT = ["HH_HL", "LL_LH", "DOUBLE_TOP", "DOUBLE_BOTTOM", "FLAG", "PENNANT", "CHOPPY", "UNCLEAR"]

def call_ai_structure(m5, m15, features):
    now = datetime.now(timezone.utc)
    if (now - cache["ai_fail_ts"]).total_seconds() < 300:
        return None
    latest_ts = m5[0]["datetime"]
    if cache["ai"]["last_m5_ts"] == latest_ts and cache["ai"]["response"]:
        return cache["ai"]["response"]
    m5_text = "\n".join(
        f"{c['datetime']}: O={c['open']} H={c['high']} L={c['low']} C={c['close']}"
        for c in m5[:20]
    )
    m15_text = "\n".join(
        f"{c['datetime']}: O={c['open']} H={c['high']} L={c['low']} C={c['close']}"
        for c in m15[:15]
    )
    prompt = f"""Anda penganalisis pasaran XAUUSD. Berdasarkan data candle M5 dan M15, berikan analisis STRUKTUR pasaran sahaja, tanpa cadangan trade.

M5 (20 candle):
{m5_text}

M15 (15 candle):
{m15_text}

Harga semasa: {features['current_price']}
EMA20 M5: {features.get('ema20_m5')}
EMA50 M5: {features.get('ema50_m5')}
ATR M5: {features['atr_m5']}

Jawab HANYA JSON:
{{
  "market_state": "TRENDING" / "RANGING" / "BREAKOUT" / "REVERSAL" / "HIGH_VOL" / "LOW_VOL",
  "trend": "BULLISH" / "BEARISH" / "NEUTRAL",
  "structure": "HH_HL" / "LL_LH" / "DOUBLE_TOP" / "DOUBLE_BOTTOM" / "FLAG" / "PENNANT" / "CHOPPY" / "UNCLEAR",
  "confidence_structure": <0-100>,
  "reason": "Penjelasan ringkas BM 1-2 ayat."
}}"""
    try:
        resp = requests.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {GROQ_API_KEY}",
                "Content-Type": "application/json"
            },
            json={
                "model": "llama-3.3-70b-versatile",
                "max_tokens": 200,
                "temperature": 0.2,
                "messages": [{"role": "user", "content": prompt}]
            },
            timeout=30
        )
        data = resp.json()
        content = data["choices"][0]["message"]["content"]
        clean = content.strip()
        if clean.startswith("```"):
            clean = clean.split("```")[1]
            if clean.startswith("json"):
                clean = clean[4:]
        ai_json = json.loads(clean)
        for key in ["trend", "market_state", "structure"]:
            if key in ai_json:
                ai_json[key] = ai_json[key].strip()
        validated = validate_ai_response(ai_json)
        if validated:
            cache["ai"] = {"last_m5_ts": latest_ts, "response": validated}
            cache["ai_fail_ts"] = datetime.min.replace(tzinfo=timezone.utc)
        return validated
    except Exception as e:
        log.error(f"AI call error: {e}")
        cache["ai_fail_ts"] = datetime.now(timezone.utc)
        return None

def validate_ai_response(ai):
    if not ai:
        return None
    if ai.get("trend", "").upper() not in ALLOWED_TREND:
        return None
    if ai.get("market_state", "").upper() not in ALLOWED_STATE:
        return None
    if ai.get("structure", "") not in ALLOWED_STRUCT:
        return None
    conf = ai.get("confidence_structure", 0)
    if not isinstance(conf, (int, float)) or not (0 <= conf <= 100):
        return None
    return ai

# ──────────────────────────── RULE ENGINE FILTERS ────────────────────────────
def filter_h1_trend(features, direction):
    bias = features.get("h1_bias")
    if not bias or bias == "NEUTRAL":
        return False
    if direction == "BUY":
        if bias != "BULLISH":
            return False
        if not features.get("h1_price_above_ema20", False):
            return False
        if not features.get("h1_ema20_rising", False):
            return False
        if not features.get("h1_atr_ok", True):
            return False
        return True
    else:  # SELL
        if bias != "BEARISH":
            return False
        if features.get("h1_price_above_ema20", True):  # should be below
            return False
        if not features.get("h1_ema20_falling", False):  # must be explicitly falling
            return False
        if not features.get("h1_atr_ok", True):
            return False
        return True

def filter_trend(python_direction, ai):
    if not python_direction:
        return False
    ai_trend = ai.get("trend", "").upper()
    if ai_trend == "BULLISH":
        ai_dir = "BUY"
    elif ai_trend == "BEARISH":
        ai_dir = "SELL"
    else:
        return False
    return ai_dir == python_direction

def filter_ema(features, direction):
    ema20 = features["ema20_m5"]
    ema50 = features["ema50_m5"]
    if not ema20 or not ema50:
        return False
    if direction == "BUY" and ema20 > ema50:
        return True
    if direction == "SELL" and ema20 < ema50:
        return True
    return False

def filter_session(session_name):
    return True

def filter_news(news_list):
    for n in news_list:
        if n["impact"] == "High" and 0 <= n["minutes"] <= 30:
            return False
    return True

def filter_liquidity(features):
    price = features["current_price"]
    buf = features["atr_m5"] * 0.3
    ath, atl = features["asian_high"], features["asian_low"]
    if ath and price >= ath - buf: return True
    if atl and price <= atl + buf: return True
    if features["pdh"] and price >= features["pdh"] - buf: return True
    if features["pdl"] and price <= features["pdl"] + buf: return True
    if features["pwh"] and price >= features["pwh"] - buf: return True
    if features["pwl"] and price <= features["pwl"] + buf: return True
    if features["pmh"] and price >= features["pmh"] - buf: return True
    if features["pml"] and price <= features["pml"] + buf: return True
    m15 = features.get("m15_candles")
    if m15:
        sh, sl = find_swing_points(m15, lookback=5)
        for s in sh[:2]:
            if price >= s[0] - buf: return True
        for s in sl[:2]:
            if price <= s[0] + buf: return True
    return False

def filter_cooldown(last_trade, direction):
    if not last_trade or last_trade["direction"] != direction:
        return True
    elapsed = (datetime.now(timezone.utc) - last_trade["time"]).total_seconds() / 60
    return elapsed >= COOLDOWN_MINUTES

def filter_atr_dynamic(features):
    curr_atr = features["atr_m5"]
    avg_atr = features["atr_avg_30"]
    if avg_atr and curr_atr < avg_atr * 0.6:
        return False
    return True

def filter_safety(features):
    if features["current_price"] is None or features["ema20_m5"] is None:
        return False
    return True

# ──────────────────────────── RISK MANAGER (DB functions accept optional conn) ────────────────────────────
def risk_manager_update(conn=None):
    own = conn is None
    if own:
        conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT outcome FROM signals ORDER BY id DESC LIMIT 3")
    recent = [row[0] for row in c.fetchall()]
    if recent == ["LOSS", "LOSS", "LOSS"]:
        now = datetime.now(timezone.utc).isoformat()
        c.execute("DELETE FROM risk_state")
        c.execute("INSERT INTO risk_state (id, state) VALUES (1, 'cooldown')")
        c.execute("DELETE FROM risk_cooldown")
        c.execute("INSERT INTO risk_cooldown (id, time) VALUES (1, ?)", (now,))
        if own:
            conn.commit()
            conn.close()
        return True
    if own:
        conn.close()
    return False

def risk_manager_allow_trade():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT state FROM risk_state WHERE id=1")
    row = c.fetchone()
    state = row[0] if row else "normal"
    if state == "cooldown":
        c.execute("SELECT time FROM risk_cooldown WHERE id=1")
        row2 = c.fetchone()
        if row2:
            cooldown_time = datetime.fromisoformat(row2[0])
            if (datetime.now(timezone.utc) - cooldown_time).total_seconds() < 7200:
                conn.close()
                return False
            else:
                c.execute("DELETE FROM risk_state")
                c.execute("INSERT INTO risk_state (id, state) VALUES (1, 'first_trade')")
                conn.commit()
                conn.close()
                return True
        else:
            conn.close()
            return True
    elif state == "first_trade":
        conn.close()
        return True
    conn.close()
    return True

def get_risk_state():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT state FROM risk_state WHERE id=1")
    row = c.fetchone()
    conn.close()
    return row[0] if row else "normal"

def set_risk_state(state, conn=None):
    own = conn is None
    if own:
        conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("DELETE FROM risk_state")
    c.execute("INSERT INTO risk_state (id, state) VALUES (1, ?)", (state,))
    if own:
        conn.commit()
        conn.close()

# ──────────────────────────── CONFIDENCE CALIBRATION (accepts optional conn) ────────────────────────────
def update_confidence_calibration(score, outcome, conn=None):
    if outcome not in ("WIN", "LOSS"):
        return
    own = conn is None
    if own:
        conn = sqlite3.connect(DB_FILE)
    bucket = (score // 5) * 5
    if bucket > 100: bucket = 100
    score_range = f"{bucket}-{bucket+4}" if bucket < 100 else "100"
    c = conn.cursor()
    c.execute("SELECT total, wins FROM confidence_calibration WHERE score_range=?", (score_range,))
    row = c.fetchone()
    if row:
        total, wins = row
    else:
        total, wins = 0, 0
    total += 1
    if outcome == "WIN":
        wins += 1
    c.execute("INSERT OR REPLACE INTO confidence_calibration (score_range, total, wins) VALUES (?,?,?)",
              (score_range, total, wins))
    if own:
        conn.commit()
        conn.close()

def get_calibrated_threshold():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT score_range, total, wins FROM confidence_calibration")
    rows = c.fetchall()
    conn.close()
    if not rows:
        return MIN_CONFIDENCE_SCORE
    best_threshold = 65
    for score_range, total, wins in rows:
        if total < 20:
            continue
        win_rate = wins / total
        if win_rate >= 0.55:
            low = int(score_range.split('-')[0])
            if low < best_threshold:
                best_threshold = low
    return max(60, best_threshold)

# ──────────────────────────── SESSION STATS (accepts optional conn) ────────────────────────────
def update_session_stats(session_name, outcome, conn=None):
    if outcome not in ("WIN", "LOSS"):
        return
    own = conn is None
    if own:
        conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT wins, losses FROM session_stats WHERE session=?", (session_name,))
    row = c.fetchone()
    if row:
        wins, losses = row
    else:
        wins, losses = 0, 0
    if outcome == "WIN":
        wins += 1
    else:
        losses += 1
    c.execute("INSERT OR REPLACE INTO session_stats (session, wins, losses) VALUES (?,?,?)",
              (session_name, wins, losses))
    if own:
        conn.commit()
        conn.close()

def get_session_win_rate(session_name):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT wins, losses FROM session_stats WHERE session=?", (session_name,))
    row = c.fetchone()
    conn.close()
    if row and (row[0]+row[1]) > 0:
        return row[0]/(row[0]+row[1]) * 100
    return 50

# ──────────────────────────── DECISION AGGREGATOR ────────────────────────────
def decision_aggregator(features, ai, session, news, last_trade):
    python_direction = trend_direction_from_swings(features)
    if not python_direction:
        return {"decision": "HOLD", "reason": "Tiada trend jelas (swing)", "score": 0}

    if not filter_h1_trend(features, python_direction):
        return {"decision": "HOLD", "reason": "H1 tidak selari", "score": 0}

    checks = {
        "h1": True,
        "trend": filter_trend(python_direction, ai),
        "ema": filter_ema(features, python_direction),
        "session": filter_session(session[0]),
        "news": filter_news(news),
        "liquidity": filter_liquidity(features),
        "cooldown": filter_cooldown(last_trade, python_direction),
        "atr_dynamic": filter_atr_dynamic(features),
        "safety": filter_safety(features)
    }

    ai_agree = (ai.get("trend", "").upper() == "BULLISH" and python_direction == "BUY") or \
               (ai.get("trend", "").upper() == "BEARISH" and python_direction == "SELL")

    score = 0
    if checks["h1"]: score += 20
    if checks["trend"]: score += 15
    if checks["ema"]: score += 15
    if checks["session"]: score += 5
    if checks["news"]: score += 15
    if checks["liquidity"]: score += 10
    if checks["cooldown"]: score += 5
    if checks["atr_dynamic"]: score += 5
    if checks["safety"]: score += 0
    if ai_agree: score += 10

    session_winrate = get_session_win_rate(session[0])
    if session_winrate > 60:
        score += 5
    elif session_winrate < 40:
        score -= 10

    score = max(0, min(100, score))

    threshold = get_calibrated_threshold()
    if score >= 80:
        conf_level = "HIGH"
    elif score >= threshold:
        conf_level = "MODERATE"
    else:
        return {"decision": "HOLD", "reason": f"Score {score} < {threshold}", "score": score}

    entry = features["current_price"]
    atr_val = features["atr_m5"]
    sl, tp = compute_sl_tp(python_direction, entry, atr_val)
    rr = abs(tp - entry) / abs(entry - sl) if abs(entry - sl) > 0 else 0

    return {
        "decision": python_direction,
        "confidence_score": score,
        "confidence_level": conf_level,
        "entry": round(entry, 2),
        "sl": sl,
        "tp": tp,
        "rr": round(rr, 2),
        "reason": ai.get("reason", ""),
        "checks": checks,
        "ai_agree": ai_agree
    }

def compute_sl_tp(direction, entry, atr):
    sl_mult = 1.2
    tp_mult = 2.0
    if direction == "BUY":
        sl = round(entry - atr * sl_mult, 2)
        tp = round(entry + atr * tp_mult, 2)
    else:
        sl = round(entry + atr * sl_mult, 2)
        tp = round(entry - atr * tp_mult, 2)
    return sl, tp

# ──────────────────────────── SQLite DB ────────────────────────────
DB_FILE = "signals.db"

def init_db():
    conn = sqlite3.connect(DB_FILE)
    conn.execute("PRAGMA journal_mode=WAL;")
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS signals (
                 id INTEGER PRIMARY KEY AUTOINCREMENT,
                 timestamp TEXT,
                 trade_id TEXT,
                 session TEXT,
                 price REAL,
                 decision TEXT,
                 confidence_score INTEGER,
                 entry REAL,
                 sl REAL,
                 tp REAL,
                 rr REAL,
                 outcome TEXT DEFAULT 'PENDING',
                 profit_pips REAL,
                 checked INTEGER DEFAULT 0)''')
    c.execute('''CREATE TABLE IF NOT EXISTS market_snapshot (
                 id INTEGER PRIMARY KEY AUTOINCREMENT,
                 timestamp TEXT,
                 current_price REAL,
                 ema20_m5 REAL,
                 ema50_m5 REAL,
                 atr_m5 REAL,
                 asian_high REAL,
                 asian_low REAL,
                 pdh REAL,
                 pdl REAL,
                 session TEXT,
                 regime TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS last_trade (
                 id INTEGER PRIMARY KEY,
                 direction TEXT,
                 time TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS risk_state (
                 id INTEGER PRIMARY KEY,
                 state TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS risk_cooldown (
                 id INTEGER PRIMARY KEY,
                 time TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS session_stats (
                 session TEXT PRIMARY KEY,
                 wins INTEGER DEFAULT 0,
                 losses INTEGER DEFAULT 0)''')
    c.execute('''CREATE TABLE IF NOT EXISTS confidence_calibration (
                 score_range TEXT PRIMARY KEY,
                 total INTEGER DEFAULT 0,
                 wins INTEGER DEFAULT 0)''')
    conn.commit()
    conn.close()
    create_indexes()

def create_indexes():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("CREATE INDEX IF NOT EXISTS idx_signals_timestamp ON signals(timestamp)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_signals_outcome ON signals(outcome)")
    conn.commit()
    conn.close()

def log_signal(signal, features, regime, session_name):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('''INSERT INTO signals (timestamp, session, price, decision, confidence_score, entry, sl, tp, rr, outcome)
                 VALUES (?,?,?,?,?,?,?,?,?,?)''',
              (datetime.now(timezone.utc).isoformat(),
               session_name,
               features["current_price"],
               signal["decision"],
               signal["confidence_score"],
               signal["entry"],
               signal["sl"],
               signal["tp"],
               signal["rr"],
               "PENDING"))
    trade_id = f"{signal['decision']} #{c.lastrowid}"
    c.execute("UPDATE signals SET trade_id=? WHERE id=?", (trade_id, c.lastrowid))
    if signal["decision"] in ("BUY", "SELL"):
        c.execute("DELETE FROM last_trade")
        c.execute("INSERT INTO last_trade (direction, time) VALUES (?,?)",
                  (signal["decision"], datetime.now(timezone.utc).isoformat()))
    c.execute('''INSERT INTO market_snapshot (timestamp, current_price, ema20_m5, ema50_m5, atr_m5, asian_high, asian_low, pdh, pdl, session, regime)
                 VALUES (?,?,?,?,?,?,?,?,?,?,?)''',
              (datetime.now(timezone.utc).isoformat(),
               features["current_price"],
               features["ema20_m5"],
               features["ema50_m5"],
               features["atr_m5"],
               features["asian_high"],
               features["asian_low"],
               features["pdh"],
               features["pdl"],
               session_name,
               regime))
    conn.commit()
    conn.close()
    return trade_id

def get_last_trade():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT direction, time FROM last_trade ORDER BY id DESC LIMIT 1")
    row = c.fetchone()
    conn.close()
    if row:
        return {"direction": row[0], "time": datetime.fromisoformat(row[1])}
    return None

# ──────────────────────────── OUTCOME CHECKER (single transaction) ────────────────────────────
def check_outcomes():
    conn = sqlite3.connect(DB_FILE)
    conn.execute("BEGIN IMMEDIATE")
    c = conn.cursor()
    c.execute("SELECT id, entry, sl, tp, decision, timestamp, session, confidence_score FROM signals WHERE outcome='PENDING'")
    pending = c.fetchall()
    now = datetime.now(timezone.utc)
    for sig_id, entry, sl, tp, dec, ts_str, session_name, score in pending:
        sig_time = datetime.fromisoformat(ts_str)
        if (now - sig_time).total_seconds() < 60:
            continue
        url = "https://api.twelvedata.com/time_series"
        params = {
            "symbol": SYMBOL,
            "interval": "5min",
            "start_date": sig_time.strftime("%Y-%m-%d %H:%M:%S"),
            "end_date": now.strftime("%Y-%m-%d %H:%M:%S"),
            "apikey": TWELVEDATA_API_KEY
        }
        data = fetch_with_retry(url, params=params)
        if not data or "values" not in data:
            continue
        candles = data["values"]
        if not candles:
            continue
        outcome = None
        profit = 0
        for c in reversed(candles):
            high = float(c["high"])
            low = float(c["low"])
            if dec == "BUY":
                if high >= tp and low <= sl:
                    outcome = "UNKNOWN"
                    profit = 0
                    break
                elif high >= tp:
                    outcome = "WIN"
                    profit = round(tp - entry, 2)
                    break
                elif low <= sl:
                    outcome = "LOSS"
                    profit = round(entry - sl, 2)
                    break
            else:
                if low <= tp and high >= sl:
                    outcome = "UNKNOWN"
                    profit = 0
                    break
                elif low <= tp:
                    outcome = "WIN"
                    profit = round(entry - tp, 2)
                    break
                elif high >= sl:
                    outcome = "LOSS"
                    profit = round(sl - entry, 2)
                    break
        if outcome:
            c.execute("UPDATE signals SET outcome=?, profit_pips=?, checked=1 WHERE id=?",
                      (outcome, profit, sig_id))
            if outcome in ("WIN", "LOSS"):
                # All updates use the same connection
                update_session_stats(session_name, outcome, conn)
                update_confidence_calibration(score, outcome, conn)
                if outcome == "LOSS":
                    risk_manager_update(conn)
                elif outcome == "WIN" and get_risk_state() == "first_trade":
                    set_risk_state("normal", conn)
    conn.commit()
    conn.close()

# ──────────────────────────── WEEKLY REPORT ────────────────────────────
def weekly_report():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    week_start = (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%d")
    c.execute("SELECT COUNT(*), SUM(CASE WHEN outcome='WIN' THEN 1 ELSE 0 END), SUM(CASE WHEN outcome='LOSS' THEN 1 ELSE 0 END), SUM(profit_pips) FROM signals WHERE timestamp >= ? AND outcome IN ('WIN','LOSS')", (week_start,))
    total, wins, losses, net = c.fetchone()
    if not total:
        conn.close()
        return
    win_rate = (wins/total*100) if total else 0
    c.execute("SELECT SUM(CASE WHEN outcome='WIN' THEN profit_pips ELSE 0 END), SUM(CASE WHEN outcome='LOSS' THEN ABS(profit_pips) ELSE 0 END) FROM signals WHERE timestamp >= ? AND outcome IN ('WIN','LOSS')", (week_start,))
    gross_profit, gross_loss = c.fetchone()
    profit_factor = gross_profit / gross_loss if gross_loss and gross_loss > 0 else 0
    c.execute("SELECT AVG(profit_pips), AVG(rr) FROM signals WHERE timestamp >= ? AND outcome IN ('WIN','LOSS')", (week_start,))
    avg_profit, avg_rr = c.fetchone()
    c.execute("SELECT session, COUNT(*), SUM(CASE WHEN outcome='WIN' THEN 1 ELSE 0 END) FROM signals WHERE timestamp >= ? AND outcome IN ('WIN','LOSS') GROUP BY session", (week_start,))
    session_rows = c.fetchall()
    session_str = "\n".join([f"{s}: {w}/{t} ({w*100//t if t else 0}%)" for s,t,w in session_rows])
    c.execute("SELECT decision, COUNT(*), SUM(CASE WHEN outcome='WIN' THEN 1 ELSE 0 END) FROM signals WHERE timestamp >= ? AND outcome IN ('WIN','LOSS') GROUP BY decision", (week_start,))
    dir_rows = c.fetchall()
    dir_str = "\n".join([f"{d}: {w}/{t} ({w*100//t if t else 0}%)" for d,t,w in dir_rows])
    report = (
        f"📊 <b>Weekly Performance</b>\n"
        f"📅 {week_start} → {datetime.now(timezone.utc).strftime('%Y-%m-%d')}\n"
        f"✅ Wins: {wins} | ❌ Losses: {losses} | 🏆 Win Rate: {win_rate:.1f}%\n"
        f"💰 Net Pips: {net:+.1f}\n"
        f"📈 Profit Factor: {profit_factor:.2f} | Expectancy: {avg_profit:.2f} pips/trade | Avg R: {avg_rr:.2f}\n"
        f"\n<b>By Session</b>:\n{escape_html(session_str)}\n"
        f"\n<b>By Direction</b>:\n{escape_html(dir_str)}\n"
        f"🔧 Calibrated Threshold: {get_calibrated_threshold()}"
    )
    conn.close()
    send_telegram(report)

# ──────────────────────────── TELEGRAM (HTML) ────────────────────────────
def send_telegram(text):
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": text,
            "parse_mode": "HTML"
        }
        resp = requests.post(url, json=payload, timeout=15)
        if not resp.ok:
            log.error(f"Telegram failed: {resp.text}")
    except Exception as e:
        log.error(f"Telegram error: {e}")

def format_signal_message(trade_id, signal, regime, session_name, half_risk=False):
    emoji = "🟢" if signal["decision"] == "BUY" else "🔴"
    risk_note = "⚠️ HALF RISK " if half_risk else ""
    msg = f"{emoji} <b>{escape_html(trade_id)}</b> {risk_note}\n"
    msg += f"Confidence: {signal['confidence_score']}% ({escape_html(signal['confidence_level'])})\n"
    msg += f"Entry: {signal['entry']}\n"
    msg += f"SL: {signal['sl']} | TP: {signal['tp']}\n"
    msg += f"RR: 1:{signal['rr']}\n"
    msg += f"Regime: {escape_html(regime)}\n"
    msg += f"Session: {escape_html(session_name)}\n"
    msg += f"Alasan: {escape_html(signal['reason'])}\n"
    msg += "⚠️ Trade at own risk."
    return msg

# ──────────────────────────── MAIN JOB ────────────────────────────
def run_analysis():
    if not analysis_lock.acquire(blocking=False):
        log.info("Analysis already running, skipping.")
        return
    try:
        log.info("=== Analysis start ===")
        if not risk_manager_allow_trade():
            send_telegram("⚠️ <b>Risk Manager:</b> Trading dihentikan 2 jam (3 kerugian berturut).")
            return

        m5 = fetch_twelvedata("5min", 200)
        m15 = fetch_twelvedata("15min", 100)
        h1  = fetch_twelvedata("1h", 100)
        valid, err = validate_data(m5, m15, h1)
        if not valid:
            send_telegram(f"⚠️ Data validation failed: {escape_html(err)}")
            return

        news = get_economic_calendar()
        features = calculate_features(m5, m15, h1)
        regime = detect_market_regime(features)

        ai = call_ai_structure(m5, m15, features)
        if not ai:
            send_telegram("⚪ HOLD (AI tidak tersedia)")
            return

        session = get_session()
        last_trade = get_last_trade()
        signal = decision_aggregator(features, ai, session, news, last_trade)

        half_risk = (get_risk_state() == "first_trade")

        if signal["decision"] == "HOLD":
            send_telegram(f"⚪ HOLD (Score {signal['score']})\n{escape_html(signal['reason'])}")
            conn = sqlite3.connect(DB_FILE)
            c = conn.cursor()
            c.execute('''INSERT INTO market_snapshot (timestamp, current_price, ema20_m5, ema50_m5, atr_m5, asian_high, asian_low, pdh, pdl, session, regime)
                         VALUES (?,?,?,?,?,?,?,?,?,?,?)''',
                      (datetime.now(timezone.utc).isoformat(),
                       features["current_price"], features["ema20_m5"], features["ema50_m5"],
                       features["atr_m5"], features["asian_high"], features["asian_low"],
                       features["pdh"], features["pdl"], session[0], regime))
            conn.commit()
            conn.close()
            return

        trade_id = log_signal(signal, features, regime, session[0])
        msg = format_signal_message(trade_id, signal, regime, session[0], half_risk)
        send_telegram(msg)
        log.info(f"Signal sent: {trade_id}")

    except Exception as e:
        log.error(f"Analysis error: {e}")
        send_telegram(f"❌ Ralat sistem: {escape_html(str(e)[:200])}")
    finally:
        analysis_lock.release()

# ──────────────────────────── FLASK + SCHEDULER ────────────────────────────
app = Flask(__name__)

@app.route("/")
def home():
    return "XAUUSD Bot v9.0 running."

@app.route("/health")
def health():
    return "OK", 200

@app.route("/run-now")
def run_now():
    Thread(target=run_analysis).start()
    return "Triggered."

def scheduler_loop():
    schedule.every(CHECK_INTERVAL).minutes.do(run_analysis)
    schedule.every(5).minutes.do(check_outcomes)
    schedule.every().sunday.at("00:05").do(weekly_report)
    log.info("Scheduler started.")
    run_analysis()
    while True:
        schedule.run_pending()
        time.sleep(30)

if __name__ == "__main__":
    init_db()
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("INSERT OR IGNORE INTO risk_state (id, state) VALUES (1, 'normal')")
    conn.commit()
    conn.close()
    Thread(target=scheduler_loop, daemon=True).start()
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
