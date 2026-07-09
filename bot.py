"""
XAUUSD AI Signal Bot - Multi-Timeframe Scalp Edition
=====================================================
Fasa 2: Analysis-only bot dengan multi-timeframe trend gating.
... (komen asal kekal) ...
"""

import os
import json
import time
import logging
import requests
import threading
from datetime import datetime, timezone
from threading import Thread
from flask import Flask
import schedule

# ── CONFIG ──
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")
GROQ_API_KEY = os.environ.get("GROQ_API_KEY")
TWELVEDATA_API_KEY = os.environ.get("TWELVEDATA_API_KEY")

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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger("xauusd-bot")

_h1_cache = {"data": None, "timestamp": None}

# ── TAMBAHAN: Kawalan Sesi ──
session_active = False
session_lock = threading.Lock()

def set_session(state: bool):
    """Tukar status sesi dengan thread-safe."""
    global session_active
    with session_lock:
        session_active = state
    status = "AKTIF" if state else "BERHENTI"
    log.info(f"Session status: {status}")
    # Beritahu pengguna melalui Telegram
    msg = f"✅ Sesi *{status}*.\n" + ("Bot sedang menganalisis setiap {} minit.".format(CHECK_INTERVAL_MINUTES) if state else "Semua panggilan API dihentikan sehingga /startsession.")
    send_telegram_message(msg)

def is_session_active():
    with session_lock:
        return session_active

# ── 1. DATA COLLECTION (fungsi asal tidak berubah) ──
def fetch_candles(interval, outputsize):
    # ... (kod asal) ...

def get_h1_trend_data():
    # ... (kod asal) ...

def _parse_dt(dt_str):
    # ... (kod asal) ...

def aggregate_candles(m1_candles, group_size):
    # ... (kod asal) ...

def get_today_news():
    # ... (kod asal) ...

def minutes_until_next_high_impact(events):
    # ... (kod asal) ...

# ── 2. INDICATORS (tiada perubahan) ──
def ema_series(closes, period):
    # ... (kod asal) ...

def rsi_latest(closes, period=14):
    # ... (kod asal) ...

def atr_latest(candles, period=14):
    # ... (kod asal) ...

def adx_latest(candles, period=14):
    # ... (kod asal) ...

def trend_bias(candles, fast=10, slow=20, trend_ma=EMA_TREND_PERIOD,
                slope_lookback=EMA_SLOPE_LOOKBACK, min_atr=None, atr_period=14):
    # ... (kod asal) ...

# ── 3. AI ANALYSIS LAYER ──
def build_prompt(current_price, m1_candles, indicators, trend_summary, news_events):
    # ... (kod asal) ...

def call_groq(prompt):
    # ... (kod asal) ...

def parse_ai_json(text, source_name):
    # ... (kod asal) ...

# ── 4. TELEGRAM NOTIFICATION ──
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

def format_notification(result, trend_summary, indicators=None):
    # ... (kod asal) ...

# ── 5. MAIN JOB (ditambah kawalan sesi) ──
def run_analysis():
    # ** TAMBAHAN: Periksa sesi sebelum teruskan **
    if not is_session_active():
        log.info("Sesi tidak aktif - analisis diabaikan.")
        return

    log.info("Running scheduled analysis...")

    m1_candles = fetch_candles("1min", M1_OUTPUTSIZE)
    if not m1_candles or len(m1_candles) < 60:
        log.error("Data M1 tidak cukup - skip check ini.")
        return

    current_price = m1_candles[-1]["close"]
    m5_candles = aggregate_candles(m1_candles, 5)
    m15_candles = aggregate_candles(m1_candles, 15)
    h1_candles = get_h1_trend_data()

    h1_dir = trend_bias(h1_candles, min_atr=MIN_ATR_H1) if h1_candles else "NEUTRAL"
    m15_dir = trend_bias(m15_candles, min_atr=MIN_ATR_M15)
    m5_dir = trend_bias(m5_candles, min_atr=MIN_ATR_M5)

    log.info(f"Trend check - H1: {h1_dir}, M15: {m15_dir}, M5: {m5_dir}")

    aligned_bullish = h1_dir == "BULLISH" and m15_dir == "BULLISH" and m5_dir == "BULLISH"
    aligned_bearish = h1_dir == "BEARISH" and m15_dir == "BEARISH" and m5_dir == "BEARISH"

    if not (aligned_bullish or aligned_bearish):
        log.info("Trend TIDAK selaras merentasi H1/M15/M5 - HOLD (senyap, Groq tak dipanggil).")
        return

    news_events = get_today_news()
    minutes_to_news = minutes_until_next_high_impact(news_events)
    if minutes_to_news is not None and minutes_to_news <= NEWS_BLACKOUT_MINUTES:
        log.info(f"High-impact USD news dalam ~{round(minutes_to_news)} minit - HOLD (elak volatility, Groq tak dipanggil).")
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
        log.info(f"ATR14 M1 ({indicators['atr14']}) bawah minimum {MIN_ATR_M1_TRADE} - volatiliti tak cukup utk TP 10 pip. Skip.")
        return

    if indicators["adx14"] is None or indicators["adx14"] < MIN_ADX_M1:
        log.info(f"ADX14 M1 ({indicators['adx14']}) bawah minimum {MIN_ADX_M1} - market mendatar/choppy. Skip.")
        return

    trend_summary = {"h1": h1_dir, "m15": m15_dir, "m5": m5_dir}
    prompt = build_prompt(current_price, m1_candles, indicators, trend_summary, news_events)

    log.info("Semua gate lepas (trend + news + ATR/ADX) - Calling Groq API...")
    result = call_groq(prompt)
    if result is None:
        log.error("Groq gagal respond - skip check ini.")
        return

    decision = result.get("decision", "").upper()
    confidence = result.get("confidence", 0)
    log.info(f"Analysis: {decision} @ {confidence}% confidence")

    if decision not in ("BUY", "SELL"):
        log.info("AI bagi HOLD - notification di-skip (senyap).")
        return

    if confidence < CONFIDENCE_THRESHOLD:
        log.info(f"Confidence {confidence}% bawah threshold {CONFIDENCE_THRESHOLD}% - notification di-skip.")
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
            f"DAN belum melepasi {invalid_beyond}. Kalau dah lajak drpd tu, SKIP trade ni."
        )
    else:
        invalid_beyond = round(entry - max_drift, 2)
        result["entry_condition"] = (
            f"Sah HANYA jika harga masih < EMA20 ({indicators['ema20']}) "
            f"DAN belum jatuh bawah {invalid_beyond}. Kalau dah lajak drpd tu, SKIP trade ni."
        )

    message = format_notification(result, trend_summary, indicators)
    send_telegram_message(message)
    log.info(f"SIGNAL SENT - {decision} @ {confidence}% | Entry {entry} SL {sl} TP {tp}")

# ── TAMBAHAN: Telegram Command Listener (long polling) ──
def telegram_polling():
    """Thread berasingan untuk mendengar arahan /startsession & /endsession."""
    offset = None
    log.info("Telegram polling started...")
    while True:
        try:
            url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates"
            params = {"timeout": 30, "offset": offset}
            resp = requests.get(url, params=params, timeout=35)
            data = resp.json()
            if not data.get("ok"):
                log.error(f"Polling error: {data}")
                time.sleep(5)
                continue

            for update in data["result"]:
                offset = update["update_id"] + 1
                message = update.get("message")
                if not message:
                    continue
                text = message.get("text", "").strip()
                chat_id = message["chat"]["id"]

                # Hanya layan arahan dari chat ID yang dibenarkan (supaya selamat)
                if str(chat_id) != str(TELEGRAM_CHAT_ID):
                    continue

                if text == "/startsession":
                    if not is_session_active():
                        set_session(True)
                    else:
                        send_telegram_message("ℹ️ Sesi sudah pun *AKTIF*.")
                elif text == "/endsession":
                    if is_session_active():
                        set_session(False)
                    else:
                        send_telegram_message("ℹ️ Sesi sudah pun *BERHENTI*.")
                # Boleh tambah /status untuk semak status
                elif text == "/status":
                    status = "AKTIF" if is_session_active() else "BERHENTI"
                    send_telegram_message(f"📊 Status sesi: *{status}*.")
        except Exception as e:
            log.error(f"Polling loop error: {e}")
            time.sleep(10)

# ── 6. KEEP-ALIVE SERVER ──
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
    # Jalankan analisis pertama (hanya jika sesi aktif)
    run_analysis()
    while True:
        schedule.run_pending()
        time.sleep(15)

if __name__ == "__main__":
    # Mulakan thread pendengar Telegram
    polling_thread = Thread(target=telegram_polling, daemon=True)
    polling_thread.start()

    # Mulakan scheduler (analisis hanya jalan bila sesi AKTIF)
    scheduler_thread = Thread(target=run_scheduler, daemon=True)
    scheduler_thread.start()

    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
