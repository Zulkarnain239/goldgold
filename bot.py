"""
XAUUSD AI Signal Bot - Multi-Timeframe Scalp Edition
=====================================================
Fasa 2: Analysis-only bot dengan multi-timeframe trend gating.

Flow:
  Fetch M1 (300 candle, 1 API call) -> aggregate M5 + M15 sendiri
  H1 di-cache (fetch berasingan setiap ~20 minit sahaja, jimat quota)
  -> GATE 1: Trend H1/M15/M5 kena SELARAS (semua bullish / semua bearish)
  -> GATE 2: Tiada High-impact USD news dalam 60 minit akan datang
  -> Kira indicator M1 (EMA20, EMA50, RSI14, ATR14)
  -> Hantar ke Groq untuk cari titik entry M1
  -> GATE 3: Confidence >= 75%
  -> Entry = harga semasa (market order), TP tetap 10 pips, SL ikut ATR/struktur
  -> Notify Telegram

PENTING: Bot ini TIDAK execute trade. Kau yang buat keputusan buy/sell
sendiri dalam MT5 berdasarkan signal yang dihantar.
"""

import os
import json
import time
import logging
import requests
from datetime import datetime, timezone
from threading import Thread
from flask import Flask
import schedule

# ─────────────────────────────────────────────
# CONFIG — semua dari Environment Variables (set dalam Render.com dashboard)
# ─────────────────────────────────────────────
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")
GROQ_API_KEY = os.environ.get("GROQ_API_KEY")
TWELVEDATA_API_KEY = os.environ.get("TWELVEDATA_API_KEY")

SYMBOL = "XAU/USD"

# Check interval - 3 minit adalah paling laju yang SELAMAT untuk TwelveData free tier
# (800 request/hari). 1 minit akan exceed limit dalam beberapa jam sahaja.
CHECK_INTERVAL_MINUTES = 3
H1_CACHE_MINUTES = 20          # H1 trend di-cache, jarang berubah drastik dalam minit
CONFIDENCE_THRESHOLD = 75      # hanya notify bila confidence >= ni
NEWS_BLACKOUT_MINUTES = 60     # HOLD jika High impact USD news dalam tempoh ni

PIP_SIZE = 0.1                 # 1 pip = 0.10 pergerakan harga XAUUSD (10 pips = 1.00)
TP_PIPS = 10                   # target tetap - TIDAK berubah
DEFAULT_SL_PIPS = 8            # fallback jika AI tak bagi sl_pips yang munasabah

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger("xauusd-bot")

# Cache mudah untuk H1 data (dalam memory sahaja, reset bila service restart)
_h1_cache = {"data": None, "timestamp": None}


# ─────────────────────────────────────────────
# 1. DATA COLLECTION
# ─────────────────────────────────────────────

def fetch_candles(interval, outputsize):
    """Fetch candle dari TwelveData, return dalam urutan KRONOLOGI (lama -> baru)."""
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

        # TwelveData bagi newest-first; reverse supaya kronologi (lama -> baru)
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


def get_h1_trend_data():
    """H1 jarang berubah drastik dalam beberapa minit - cache untuk jimat quota TwelveData."""
    now = datetime.now(timezone.utc)
    if _h1_cache["data"] and _h1_cache["timestamp"]:
        age_min = (now - _h1_cache["timestamp"]).total_seconds() / 60
        if age_min < H1_CACHE_MINUTES:
            return _h1_cache["data"]

    h1_candles = fetch_candles("1h", 30)
    if h1_candles:
        _h1_cache["data"] = h1_candles
        _h1_cache["timestamp"] = now
    return h1_candles


def aggregate_candles(m1_candles, group_size):
    """Bina candle timeframe lebih tinggi (M5/M15) dari data M1 - jimat API call."""
    aggregated = []
    for i in range(0, len(m1_candles) - group_size + 1, group_size):
        chunk = m1_candles[i:i + group_size]
        aggregated.append({
            "datetime": chunk[0]["datetime"],
            "open": chunk[0]["open"],
            "high": max(c["high"] for c in chunk),
            "low": min(c["low"] for c in chunk),
            "close": chunk[-1]["close"],
        })
    return aggregated


def get_today_news():
    """Tarik economic calendar dari Forex Factory, filter USD High/Medium impact hari ini."""
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
    """Return minit sehingga event High-impact USD akan datang, atau None jika tiada dalam skop relevan."""
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
        if -5 <= delta_min <= 180:  # dalam skop relevan (baru lepas hingga 3 jam akan datang)
            if soonest is None or delta_min < soonest:
                soonest = delta_min
    return soonest


# ─────────────────────────────────────────────
# 2. INDICATORS (pure Python, tiada dependency tambahan)
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


def trend_bias(candles, fast=10, slow=20):
    """Arah trend berdasarkan EMA cepat vs perlahan. Return BULLISH/BEARISH/NEUTRAL."""
    closes = [c["close"] for c in candles]
    if len(closes) < slow:
        return "NEUTRAL"
    ema_fast = ema_series(closes, fast)
    ema_slow = ema_series(closes, slow)
    if not ema_fast or not ema_slow:
        return "NEUTRAL"
    if ema_fast[-1] > ema_slow[-1]:
        return "BULLISH"
    elif ema_fast[-1] < ema_slow[-1]:
        return "BEARISH"
    return "NEUTRAL"


# ─────────────────────────────────────────────
# 3. AI ANALYSIS LAYER
# ─────────────────────────────────────────────

def build_prompt(current_price, m1_candles, indicators, trend_summary, news_events):
    candles_summary = "\n".join([
        f"  {c['datetime']}: O={c['open']} H={c['high']} L={c['low']} C={c['close']}"
        for c in m1_candles[-20:]
    ])

    news_summary = "\n".join([
        f"  {e.get('date', '')} {e.get('title', '')} (impact: {e.get('impact', '')})"
        for e in news_events
    ]) or "  Tiada high/medium impact USD news hari ini."

    prompt = f"""Kau seorang SCALPER XAUUSD berpengalaman di timeframe M1. Sistem sudah CONFIRM trend H1/M15/M5 SELARAS sebelum minta analisa kau - tugas kau HANYA cari titik ENTRY M1 yang tepat mengikut arah trend ni, target TEPAT 10 pips.

TREND MULTI-TIMEFRAME (sudah dikira dan disahkan selaras, PERCAYA info ni):
- H1 Trend: {trend_summary['h1']}
- M15 Trend: {trend_summary['m15']}
- M5 Momentum: {trend_summary['m5']}

INDIKATOR M1 SEMASA:
- EMA20: {indicators['ema20']}
- EMA50: {indicators['ema50']}
- RSI14: {indicators['rsi14']}
- ATR14: {indicators['atr14']} (ukuran volatiliti - guna untuk cadangan SL yang munasabah)

HARGA SEMASA: {current_price}

20 CANDLE M1 TERKINI (dari terbaru ke lama):
{candles_summary}

ECONOMIC CALENDAR HARI INI (USD, High/Medium impact):
{news_summary}

PERATURAN KETAT:
1. ENTRY MESTI harga semasa ({current_price}) - kau trading market order SEKARANG, jangan cadang entry pada harga lain.
2. TP sentiasa TEPAT 10 pips dari entry, ke arah trend di atas.
3. Cadangkan sl_pips (nombor pip sahaja, biasanya 6-10 pips) berdasarkan ATR14 dan struktur candle M1 terkini.
4. Kalau RSI overbought (>70) untuk BUY, atau oversold (<30) untuk SELL - risiko reversal tinggi, bagi HOLD.
5. Cari titik masuk M1 yang confirm arah trend (breakout micro-range, rejection dari EMA20, continuation candle selepas pullback) - jangan asal ada pergerakan kecil terus bagi signal.
6. Beri confidence yang JUJUR berdasarkan kekuatan bukti - sistem akan tapis dan hanya proceed signal dengan confidence tinggi.

Berikan jawapan HANYA dalam format JSON tepat, tiada teks lain:
{{
  "decision": "BUY" atau "SELL" atau "HOLD",
  "confidence": <nombor 0-100, jujur>,
  "reason": "<penjelasan ringkas 1-2 ayat Bahasa Melayu - sebut bukti M1 dan macam mana ia selari dengan trend>",
  "key_level": "<micro support/resistance terdekat>",
  "sl_pips": <nombor pip untuk SL, contoh 8, atau null jika HOLD>
}}"""
    return prompt


def call_groq(prompt):
    """Groq - free tier, laju. Guna model Llama 3.3 70B."""
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
            log.error(f"Groq API returned no 'choices'. Status: {resp.status_code}. Full response: {data}")
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
# 4. TELEGRAM NOTIFICATION
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


def format_notification(result, trend_summary):
    decision = result.get("decision", "").upper()
    emoji = {"BUY": "🟢", "SELL": "🔴"}.get(decision, "⚪")

    lines = [
        f"{emoji} *XAUUSD 10-Pip Scalp Signal (M1) — {decision} NOW*",
        f"Entry: `{result.get('entry')}` (harga semasa - market order)",
        f"Confidence: {result.get('confidence', '?')}%",
        f"Trend: H1 {trend_summary['h1']} | M15 {trend_summary['m15']} | M5 {trend_summary['m5']}",
        "",
        f"_{result.get('reason', '')}_",
    ]

    if result.get("key_level"):
        lines.append(f"Key level: {result['key_level']}")

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

    lines.append("")
    lines.append(f"🕐 {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    lines.append("\n⚠️ Ini analysis sahaja. Buat keputusan buy/sell sendiri dalam MT5.")

    return "\n".join(lines)


# ─────────────────────────────────────────────
# 5. MAIN JOB
# ─────────────────────────────────────────────

def run_analysis():
    log.info("Running scheduled analysis...")

    m1_candles = fetch_candles("1min", 300)
    if not m1_candles or len(m1_candles) < 60:
        log.error("Data M1 tidak cukup - skip check ini.")
        return

    current_price = m1_candles[-1]["close"]

    # Aggregate M5/M15 dari data M1 yang sama - jimat API call
    m5_candles = aggregate_candles(m1_candles, 5)
    m15_candles = aggregate_candles(m1_candles, 15)
    h1_candles = get_h1_trend_data()

    h1_dir = trend_bias(h1_candles) if h1_candles else "NEUTRAL"
    m15_dir = trend_bias(m15_candles)
    m5_dir = trend_bias(m5_candles)

    log.info(f"Trend check - H1: {h1_dir}, M15: {m15_dir}, M5: {m5_dir}")

    # GATE 1: Multi-timeframe alignment - kalau tak selaras, skip terus (jimat Groq quota)
    aligned_bullish = h1_dir == "BULLISH" and m15_dir == "BULLISH" and m5_dir == "BULLISH"
    aligned_bearish = h1_dir == "BEARISH" and m15_dir == "BEARISH" and m5_dir == "BEARISH"

    if not (aligned_bullish or aligned_bearish):
        log.info("Trend TIDAK selaras merentasi H1/M15/M5 - HOLD (senyap, Groq tak dipanggil).")
        return

    # GATE 2: News blackout
    news_events = get_today_news()
    minutes_to_news = minutes_until_next_high_impact(news_events)
    if minutes_to_news is not None and minutes_to_news <= NEWS_BLACKOUT_MINUTES:
        log.info(f"High-impact USD news dalam ~{round(minutes_to_news)} minit - HOLD (elak volatility, Groq tak dipanggil).")
        return

    # Kira indicator M1
    m1_closes = [c["close"] for c in m1_candles]
    ema20_series = ema_series(m1_closes, 20)
    ema50_series = ema_series(m1_closes, 50)
    indicators = {
        "ema20": round(ema20_series[-1], 2) if ema20_series else None,
        "ema50": round(ema50_series[-1], 2) if ema50_series else None,
        "rsi14": rsi_latest(m1_closes, 14),
        "atr14": atr_latest(m1_candles, 14),
    }

    trend_summary = {"h1": h1_dir, "m15": m15_dir, "m5": m5_dir}
    prompt = build_prompt(current_price, m1_candles, indicators, trend_summary, news_events)

    log.info("Trend selaras + tiada news blackout - Calling Groq API...")
    result = call_groq(prompt)
    if result is None:
        log.error("Groq gagal respond - skip check ini.")
        return

    decision = result.get("decision", "").upper()
    confidence = result.get("confidence", 0)
    log.info(f"Analysis: {decision} @ {confidence}% confidence")

    # GATE 3: Decision mesti BUY/SELL dan confidence lepas threshold
    if decision not in ("BUY", "SELL"):
        log.info("AI bagi HOLD - notification di-skip (senyap).")
        return

    if confidence < CONFIDENCE_THRESHOLD:
        log.info(f"Confidence {confidence}% bawah threshold {CONFIDENCE_THRESHOLD}% - notification di-skip.")
        return

    # Entry = harga semasa (BUKAN AI pilih), TP tetap 10 pips, SL ikut cadangan AI
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

    message = format_notification(result, trend_summary)
    send_telegram_message(message)
    log.info(f"SIGNAL SENT - {decision} @ {confidence}% | Entry {entry} SL {sl} TP {tp}")


# ─────────────────────────────────────────────
# 6. KEEP-ALIVE SERVER (untuk Render.com free tier + UptimeRobot)
# ─────────────────────────────────────────────

app = Flask(__name__)


@app.route("/")
def home():
    return "XAUUSD Signal Bot is running."


@app.route("/health", methods=["GET", "HEAD"])
def health():
    return "OK", 200


@app.route("/run-now")
def trigger_manual():
    Thread(target=run_analysis).start()
    return "Analysis triggered, check Telegram/Logs in a few seconds.", 200


def run_scheduler():
    schedule.every(CHECK_INTERVAL_MINUTES).minutes.do(run_analysis)
    log.info(f"Scheduler started, running every {CHECK_INTERVAL_MINUTES} minutes.")
    run_analysis()
    while True:
        schedule.run_pending()
        time.sleep(15)


if __name__ == "__main__":
    scheduler_thread = Thread(target=run_scheduler, daemon=True)
    scheduler_thread.start()
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
