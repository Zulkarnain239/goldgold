"""
XAUUSD AI Signal Bot
=====================
Fasa 1: Analysis-only bot.
- Tarik data harga XAUUSD (TwelveData) + economic calendar (Forex Factory)
- Hantar data ke Groq (Llama 3.3 70B) untuk analysis
- Hantar notification result ke Telegram
- Jalan setiap 30 minit (guna scheduler dalam script + keep-alive server untuk Render.com)

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
TELEGRAM_BOT_TOKEN = os.environ.get("8612217468:AAERSdXbHAl0YZ1skgVuFZ-wGbc4EkR_boY")
TELEGRAM_CHAT_ID = os.environ.get("7513297859")

GROQ_API_KEY = os.environ.get("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")

TWELVEDATA_API_KEY = os.environ.get("0bc48a0fe324496e889d740e29e4ba43")  # free tier: twelvedata.com

SYMBOL = "XAU/USD"
CHECK_INTERVAL_MINUTES = 30

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger("xauusd-bot")


# ─────────────────────────────────────────────
# 1. DATA COLLECTION
# ─────────────────────────────────────────────

def get_price_data():
    """Tarik harga terkini + candle data (H1) dari TwelveData."""
    try:
        url = "https://api.twelvedata.com/time_series"
        params = {
            "symbol": SYMBOL,
            "interval": "1h",
            "outputsize": 20,
            "apikey": TWELVEDATA_API_KEY
        }
        resp = requests.get(url, params=params, timeout=15)
        data = resp.json()

        if "values" not in data:
            log.error(f"TwelveData error: {data}")
            return None

        candles = data["values"]
        latest = candles[0]

        return {
            "current_price": float(latest["close"]),
            "recent_candles": candles[:10],  # 10 candle terkini untuk context
        }
    except Exception as e:
        log.error(f"Error fetching price data: {e}")
        return None


def get_economic_calendar():
    """Tarik economic calendar dari Forex Factory (JSON feed, free)."""
    try:
        url = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"
        resp = requests.get(url, timeout=15)
        events = resp.json()

        # Filter untuk USD & high/medium impact je (relevan untuk gold)
        relevant = [
            e for e in events
            if e.get("country") == "USD" and e.get("impact") in ("High", "Medium")
        ]
        return relevant[:8]  # limit supaya prompt tak terlalu panjang
    except Exception as e:
        log.error(f"Error fetching calendar: {e}")
        return []


# ─────────────────────────────────────────────
# 2. AI ANALYSIS LAYER
# ─────────────────────────────────────────────

def build_prompt(price_data, calendar):
    """Bina prompt untuk dihantar ke Groq."""
    candles_summary = "\n".join([
        f"  {c['datetime']}: O={c['open']} H={c['high']} L={c['low']} C={c['close']}"
        for c in price_data["recent_candles"]
    ])

    calendar_summary = "\n".join([
        f"  {e.get('date', '')} {e.get('title', '')} (impact: {e.get('impact', '')})"
        for e in calendar
    ]) or "  Tiada high/medium impact USD news dalam minggu ini."

    prompt = f"""Kau adalah analyst forex/gold berpengalaman. Analisa XAUUSD (Gold/USD) berdasarkan data berikut dan berikan cadangan trading.

HARGA SEMASA: {price_data['current_price']}

10 CANDLE H1 TERKINI (dari terbaru ke lama):
{candles_summary}

ECONOMIC CALENDAR MINGGU INI (USD, High/Medium impact):
{calendar_summary}

Berikan jawapan HANYA dalam format JSON tepat seperti ini, tiada teks lain:
{{
  "decision": "BUY" atau "SELL" atau "HOLD",
  "confidence": <nombor 0-100>,
  "reason": "<penjelasan ringkas 1-2 ayat dalam Bahasa Melayu>",
  "key_level": "<support/resistance penting yang perlu diperhatikan>"
}}"""
    return prompt


def call_groq(prompt):
    """Groq - free tier, laju. Guna model Llama 3.3 70B (kualiti bagus, free)."""
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
        text = data["choices"][0]["message"]["content"]
        return parse_ai_json(text, "Groq (Llama 3.3)")
    except Exception as e:
        log.error(f"Groq API error: {e}")
        return None


def parse_ai_json(text, source_name):
    """AI kadang bagi markdown code fence, kena bersihkan dulu sebelum parse JSON."""
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
# 3. TELEGRAM NOTIFICATION
# ─────────────────────────────────────────────

def send_telegram_message(text):
    try:
        url = f"https://api.telegram.org/bot{8612217468:AAERSdXbHAl0YZ1skgVuFZ-wGbc4EkR_boY}/sendMessage"
        resp = requests.post(url, json={
            "chat_id": TELEGRAM_CHAT_ID,
            "text": text,
            "parse_mode": "Markdown"
        }, timeout=15)
        if not resp.ok:
            log.error(f"Telegram send failed: {resp.text}")
    except Exception as e:
        log.error(f"Telegram error: {e}")


def format_notification(result, price_data):
    if result is None:
        return "⚠️ *XAUUSD Signal Bot*\nGroq gagal respond. Check API key / logs."

    decision_emoji = {"BUY": "🟢", "SELL": "🔴", "HOLD": "⚪"}
    emoji = decision_emoji.get(result.get("decision", "").upper(), "⚪")

    lines = [
        f"{emoji} *XAUUSD Signal — {result.get('decision', '?')}*",
        f"Harga semasa: `{price_data['current_price']}`",
        f"Confidence: {result.get('confidence', '?')}%",
        "",
        f"_{result.get('reason', '')}_",
    ]

    if result.get("key_level"):
        lines.append(f"Key level: {result['key_level']}")

    lines.append("")
    lines.append(f"🕐 {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    lines.append("\n⚠️ Ini analysis sahaja. Buat keputusan buy/sell sendiri dalam MT5.")

    return "\n".join(lines)


# ─────────────────────────────────────────────
# 4. MAIN JOB
# ─────────────────────────────────────────────

def run_analysis():
    log.info("Running scheduled analysis...")

    price_data = get_price_data()
    if price_data is None:
        send_telegram_message("⚠️ *XAUUSD Signal Bot*\nGagal tarik data harga. Check TwelveData API key/limit.")
        return

    calendar = get_economic_calendar()
    prompt = build_prompt(price_data, calendar)

    log.info("Calling Groq API...")
    result = call_groq(prompt)

    message = format_notification(result, price_data)

    send_telegram_message(message)
    log.info("Notification sent.")


# ─────────────────────────────────────────────
# 5. KEEP-ALIVE SERVER (untuk Render.com free tier + UptimeRobot)
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
    """Endpoint untuk trigger analysis manual bila-bila masa (test dari browser)."""
    Thread(target=run_analysis).start()
    return "Analysis triggered, check Telegram in a few seconds.", 200


def run_scheduler():
    schedule.every(CHECK_INTERVAL_MINUTES).minutes.do(run_analysis)
    log.info(f"Scheduler started, running every {CHECK_INTERVAL_MINUTES} minutes.")

    # Run once on startup so kau boleh test terus
    run_analysis()

    while True:
        schedule.run_pending()
        time.sleep(30)


if __name__ == "__main__":
    # Scheduler jalan dalam background thread, Flask server jalan di foreground
    # (Flask server perlu untuk keep Render.com service alive + UptimeRobot ping)
    scheduler_thread = Thread(target=run_scheduler, daemon=True)
    scheduler_thread.start()

    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
