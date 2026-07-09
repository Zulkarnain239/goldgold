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
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

GROQ_API_KEY = os.environ.get("GROQ_API_KEY")

TWELVEDATA_API_KEY = os.environ.get("TWELVEDATA_API_KEY")  # free tier: twelvedata.com

SYMBOL = "XAU/USD"
CHECK_INTERVAL_MINUTES = 2  # check kerap secara senyap; notification hantar bila AI jumpa peluang jelas

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger("xauusd-bot")


# ─────────────────────────────────────────────
# 1. DATA COLLECTION
# ─────────────────────────────────────────────

def get_price_data():
    """Tarik harga terkini + candle data (M1) dari TwelveData - untuk scalping."""
    try:
        url = "https://api.twelvedata.com/time_series"
        params = {
            "symbol": SYMBOL,
            "interval": "1min",
            "outputsize": 30,
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
            "recent_candles": candles[:20],  # 20 candle M1 terkini untuk context scalping
        }
    except Exception as e:
        log.error(f"Error fetching price data: {e}")
        return None


def get_economic_calendar():
    """Tarik economic calendar dari Forex Factory (JSON feed, free).
    Filter untuk HARI INI sahaja - relevan untuk scalping jangka pendek."""
    try:
        url = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"
        resp = requests.get(url, timeout=15)
        events = resp.json()

        today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        # Filter untuk USD & high/medium impact, dan tarikh hari ini sahaja
        relevant = [
            e for e in events
            if e.get("country") == "USD"
            and e.get("impact") in ("High", "Medium")
            and str(e.get("date", "")).startswith(today_str)
        ]
        return relevant[:8]  # limit supaya prompt tak terlalu panjang
    except Exception as e:
        log.error(f"Error fetching calendar: {e}")
        return []


# ─────────────────────────────────────────────
# 2. AI ANALYSIS LAYER
# ─────────────────────────────────────────────

def build_prompt(price_data, calendar):
    """Bina prompt untuk dihantar ke Groq - fokus scalping M1."""
    candles_summary = "\n".join([
        f"  {c['datetime']}: O={c['open']} H={c['high']} L={c['low']} C={c['close']}"
        for c in price_data["recent_candles"]
    ])

    calendar_summary = "\n".join([
        f"  {e.get('date', '')} {e.get('title', '')} (impact: {e.get('impact', '')})"
        for e in calendar
    ]) or "  Tiada high/medium impact USD news hari ini."

    prompt = f"""Kau adalah seorang SCALPER XAUUSD (Gold/USD) berpengalaman, sedang aktif memerhati market di timeframe M1 (1 minit) sekarang. Kau mencari SATU jenis peluang sahaja: pergerakan pantas sebanyak TEPAT 10 PIPS - target ini KECIL dan SELALU berlaku dalam beberapa minit di market gold, ia bukan peluang rare yang jarang muncul.

MINDSET PENTING: 10 pips adalah target kecil dan realistik - kau TIDAK perlu tunggu "perfect setup" atau pergerakan besar untuk capai ini. Kebanyakan micro-trend/momentum burst yang biasa berlaku dalam candle M1 sudah cukup untuk capai 10 pips. Jangan terlalu berhati-hati sampai terlepas peluang yang sebenarnya cukup baik - tapi jangan juga asal ada pergerakan kecil terus bagi signal tanpa asas yang jelas.

CHECKLIST - bagi signal BUY/SELL jika SEKURANG-KURANGNYA 2 daripada 4 kriteria ini jelas kelihatan dalam 20 candle M1:
1. **Momentum arah jelas** - 3-5 candle terakhir bergerak konsisten ke satu arah (higher highs/higher lows untuk BUY, lower highs/lower lows untuk SELL)
2. **Breakout micro-range** - harga baru sahaja pecah keluar dari range kecil (consolidation) yang terbentuk dalam beberapa candle sebelum ni
3. **Rejection jelas** - candle menunjukkan penolakan kuat (wick panjang) dari satu level, diikuti pergerakan ke arah bertentangan
4. **Continuation pattern** - selepas pullback kecil, harga sambung semula arah trend asal dengan candle body yang kukuh

Jika TIADA satu pun kriteria di atas kelihatan jelas (market benar-benar choppy/flat/tiada struktur), bagi HOLD.

Jika ada news high-impact dalam beberapa jam akan datang hari ini yang boleh sebabkan spike/whipsaw tiba-tiba, pertimbangkan risiko ini - HOLD jika terlalu berisiko walaupun kriteria di atas dipenuhi.

HARGA SEMASA: {price_data['current_price']}

20 CANDLE M1 TERKINI (dari terbaru ke lama - FOKUS UTAMA kau untuk baca momentum/order flow semasa):
{candles_summary}

ECONOMIC CALENDAR HARI INI SAHAJA (USD, High/Medium impact):
{calendar_summary}

Jika decision BUY atau SELL:
- TP = entry ± 10 pips (TEPAT, contoh 1.00 pergerakan harga XAUUSD = 10 pips)
- SL logik berdasarkan struktur candle terdekat (biasanya 6-10 pips)

Berikan jawapan HANYA dalam format JSON tepat seperti ini, tiada teks lain:
{{
  "decision": "BUY" atau "SELL" atau "HOLD",
  "confidence": <nombor 0-100, sekadar rujukan>,
  "reason": "<penjelasan ringkas 1-2 ayat dalam Bahasa Melayu - sebut kriteria checklist mana yang dipenuhi>",
  "key_level": "<micro support/resistance terdekat dalam beberapa minit ni>",
  "entry": <nombor harga entry, atau null jika HOLD>,
  "sl": <nombor harga stop loss (~6-10 pips), atau null jika HOLD>,
  "tp": <nombor harga take profit (TEPAT 10 pips dari entry), atau null jika HOLD>
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

        if "choices" not in data:
            # Log full response body supaya kita nampak sebab sebenar (rate limit, invalid model, auth, dll)
            log.error(f"Groq API returned no 'choices'. Status: {resp.status_code}. Full response: {data}")
            return None

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


def format_notification(result, price_data):
    if result is None:
        return "⚠️ *XAUUSD Signal Bot*\nGroq gagal respond. Check API key / logs."

    decision_emoji = {"BUY": "🟢", "SELL": "🔴", "HOLD": "⚪"}
    decision = result.get("decision", "").upper()
    emoji = decision_emoji.get(decision, "⚪")

    lines = [
        f"{emoji} *XAUUSD 10-Pip Scalp Signal (M1) — {result.get('decision', '?')}*",
        f"Harga semasa: `{price_data['current_price']}`",
        f"Confidence: {result.get('confidence', '?')}%",
        "",
        f"_{result.get('reason', '')}_",
    ]

    if result.get("key_level"):
        lines.append(f"Key level: {result['key_level']}")

    # SL/TP hanya relevan kalau decision BUY atau SELL, bukan HOLD
    if decision in ("BUY", "SELL") and result.get("entry") is not None:
        lines.append("")
        lines.append("*Trade Levels:*")
        lines.append(f"Entry: `{result.get('entry', '?')}`")
        lines.append(f"SL: `{result.get('sl', '?')}`")
        lines.append(f"TP: `{result.get('tp', '?')}`")

        # Kira risk-reward ratio kalau semua nombor ada, untuk quick reference
        try:
            entry = float(result["entry"])
            sl = float(result["sl"])
            tp = float(result["tp"])
            risk = abs(entry - sl)
            reward = abs(tp - entry)
            if risk > 0:
                rr = round(reward / risk, 2)
                lines.append(f"Risk:Reward ≈ 1:{rr}")
        except (TypeError, ValueError, KeyError):
            pass

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
        log.error("Gagal tarik data harga - skip check ini (senyap, tak spam Telegram tiap kali fail).")
        return

    calendar = get_economic_calendar()
    prompt = build_prompt(price_data, calendar)

    log.info("Calling Groq API...")
    result = call_groq(prompt)

    if result is None:
        log.error("Groq gagal respond - skip check ini.")
        return

    decision = result.get("decision", "").upper()
    confidence = result.get("confidence", 0)

    log.info(f"Analysis: {decision} @ {confidence}% confidence")

    # Notify Telegram bila AI kenal pasti signal (bukan HOLD) - tiada gating confidence,
    # kriteria kelayakan signal dikawal dalam prompt itu sendiri
    if decision in ("BUY", "SELL"):
        message = format_notification(result, price_data)
        send_telegram_message(message)
        log.info(f"SIGNAL DETECTED - Notification sent ({decision} @ {confidence}%).")
    else:
        log.info("HOLD - tiada peluang jelas, notification di-skip (senyap).")


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
