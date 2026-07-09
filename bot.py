"""
XAUUSD AI Signal Bot - Multi-Timeframe Scalp Edition
=====================================================
Fasa 3: Analysis-only bot dengan trend SCORING (bukan hard multi-timeframe gate).

Kenapa H1 dibuang terus (Fasa 2 -> Fasa 3):
  Scalping M1 sasaran 10 pip bukan swing trading. Setup paling biasa untuk scalp
  ialah: H1 masih Bearish (belum sempat flip) tapi M15/M5/M1 dah pullback/reversal
  bullish yang kuat. EMA50 pada H1 amat perlahan (perlukan ~50 jam data utk flip) -
  bot lama akan HOLD walaupun M1 dah naik 30-50 pip, sebab syarat lama paksa
  H1 == M15 == M5 (semua wajib sama). Ini terlalu ketat utk scalping dan banyak
  terlepas trade sah (rujuk kes sebenar: H1 BEARISH, M15/M5 BULLISH, M1 naik kuat -
  bot lama senyap terus, Groq tak dipanggil).

Flow (Fasa 3):
  Fetch M1 (900 candle, 1 API call) -> aggregate M5 + M15 sendiri
    (aggregation diselaraskan dgn sempadan jam sebenar :00/:05/:15..., bukan blok tetap)
  -> Kira M15 trend & M5 trend (guna trend_bias(): EMA cross + harga vs EMA50 +
     slope EMA20 + ATR minimum ikut timeframe)
  -> Kira indicator M1 (EMA20, EMA50, RSI14, ATR14, ADX14)
  -> GATE 1 (SISTEM MARKAH, gantikan "semua timeframe wajib sama"):
       M15 sehala        = +4   (trend utama)
       M5 sehala         = +5   (momentum, plg dekat dgn M1)
       EMA20 > EMA50 M1  = +2   (struktur M1)
       Harga > EMA20 M1  = +2   (struktur M1)
     Markah >= 11/13 diperlukan (secara praktikal ni bermakna M15 & M5 KEDUA-DUA
     mesti sehala [4+5=9] + sekurang-kurangnya SATU struktur M1 turut sehala.
     H1 langsung tiada kaitan dalam pengiraan ni.)
  -> GATE 1b: ATR14 M1 >= minimum (TP 10 pip perlu volatiliti cukup) DAN ADX14 M1
     >= minimum (elak market mendatar/choppy) - ni kekal gate KERAS (bukan markah)
     sebab ia soal "boleh trade ke tidak", bukan "arah mana".
  -> GATE 2: Tiada High-impact USD news dalam 60 minit akan datang
  -> Hantar ke Groq untuk cari titik entry M1
  -> GATE 3: Confidence >= 75%
  -> Entry = harga semasa (market order), TP tetap 10 pips, SL ikut ATR/struktur
  -> Notification turut sertakan SYARAT KESAHIHAN ENTRY (EMA20 + had drift harga)
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

# Check interval - 3 minit. Setiap run kini HANYA 1 TwelveData API call (M1 fetch;
# H1 dah dibuang terus jadi tiada lagi fetch berasingan). Pada 3 minit = ~480
# call/hari, bawah had 800/hari free tier - ada ruang kalau nak pendekkan
# ke 2 minit (~720/hari) kalau kau nak reaksi lebih laju drpd momentum M1.
CHECK_INTERVAL_MINUTES = 3
CONFIDENCE_THRESHOLD = 75      # hanya notify bila confidence >= ni
NEWS_BLACKOUT_MINUTES = 60     # HOLD jika High impact USD news dalam tempoh ni

PIP_SIZE = 0.1                 # 1 pip = 0.10 pergerakan harga XAUUSD (10 pips = 1.00)
TP_PIPS = 10                   # target tetap - TIDAK berubah
DEFAULT_SL_PIPS = 8            # fallback jika AI tak bagi sl_pips yang munasabah

# TwelveData caj mengikut BILANGAN CALL, bukan saiz output - naikkan outputsize
# di sini TIDAK menambah kos quota harian.
M1_OUTPUTSIZE = 900             # ~15 jam data M1 - cukup utk EMA50 pada M15 & M5 lepas aggregate

# --- Trend filter M15/M5 - EMA cross sahaja TAK CUKUP, tambah syarat (trend_bias()) ---
EMA_TREND_PERIOD = 50           # harga mesti > EMA ni utk BULLISH (< utk BEARISH)
EMA_SLOPE_LOOKBACK = 3          # bilangan candle ke belakang utk kira slope EMA-slow
MIN_ATR_M15 = 0.6               # unit harga (USD) - trend M15 diabaikan jika ATR14 M15 bawah ni
MIN_ATR_M5 = 0.3
# NOTA: nilai ATR minimum di atas anggaran/starting-point sahaja. Volatiliti XAUUSD dalam
# unit harga berubah ikut tahap harga semasa gold. Backtest & laraskan ikut broker kau.

# --- GATE 1 (BARU): Sistem markah gantikan "H1==M15==M5 wajib sama" ---
SCORE_M15 = 4                   # M15 = trend utama utk scalp
SCORE_M5 = 5                    # M5 = momentum, plg dekat dgn M1 (bobot plg tinggi)
SCORE_EMA_STACK = 2              # EMA20 vs EMA50 pada M1 selari dgn bias
SCORE_PRICE_VS_EMA20 = 2         # harga M1 di atas/bawah EMA20 selari dgn bias
MAX_TREND_SCORE = SCORE_M15 + SCORE_M5 + SCORE_EMA_STACK + SCORE_PRICE_VS_EMA20  # 13
MIN_TREND_SCORE = 11             # perlu M15 & M5 KEDUA-DUA sehala (9) + >=1 struktur M1 (2)

# --- Momentum + volatiliti filter (Gate 1b, gate KERAS - sebelum panggil AI) ---
MIN_ATR_M1_TRADE = 0.40         # ATR14 M1 (unit harga) - bawah ni, market terlalu senyap utk TP 10 pip
MIN_ADX_M1 = 25                 # ADX14 M1 - bawah ni dianggap market mendatar (choppy), skip

# --- Entry safety - had drift harga sebelum entry dianggap tak sah lagi ---
MAX_ENTRY_DRIFT_PIPS = 3        # amaran dlm notifikasi jika harga dah bergerak > ni drpd entry asal

# --- SESSION CONTROL (Telegram commands) ---
# Default: sesi aktif supaya bot berfungsi serta-merta selepas deploy.
# Gunakan /endsession untuk hentikan semua panggilan API (analisis dihentikan sepenuhnya).
# Gunakan /startsession untuk sambung semula.
session_active = True

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger("xauusd-bot")


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


def _parse_dt(dt_str):
    """Parse datetime string dari TwelveData (format 'YYYY-MM-DD HH:MM:SS')."""
    return datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")


def aggregate_candles(m1_candles, group_size):
    """Bina candle timeframe lebih tinggi (M5/M15) dari data M1 - jimat API call.

    Candle diselaraskan ikut SEMPADAN MASA SEBENAR (contoh utk M5: :00-:04,
    :05-:09, :10-:14...), bukan blok tetap 5/15 dari index 0 dataset. Bucket
    separuh di hujung awal/akhir dataset dibuang supaya OHLC setiap bar tepat.
    """
    if not m1_candles:
        return []

    buckets = {}
    for c in m1_candles:
        dt = _parse_dt(c["datetime"])
        bucket_minute = (dt.minute // group_size) * group_size
        bucket_start = dt.replace(minute=bucket_minute, second=0, microsecond=0)
        buckets.setdefault(bucket_start, []).append(c)

    keys = list(buckets.keys())  # dict Python 3.7+ kekalkan urutan insertion (kronologi)
    aggregated = []
    for idx, key in enumerate(keys):
        chunk = buckets[key]
        is_edge = (idx == 0 or idx == len(keys) - 1)
        if is_edge and len(chunk) < group_size:
            continue  # buang bucket separuh di hujung dataset
        aggregated.append({
            "datetime": key.strftime("%Y-%m-%d %H:%M:%S"),
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


def adx_latest(candles, period=14):
    """ADX (Average Directional Index) - Wilder smoothing, pure Python.
    Tapis market yang sedang mendatar (ranging) - ADX rendah = jangan percaya
    "trend" yang wujud dari EMA cross sahaja.
    """
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
    """Tentukan arah trend - BUKAN sekadar EMA cross.

    Syarat BULLISH (kesemua mesti benar):
      1. EMA-fast > EMA-slow (cross asas)
      2. Harga semasa > EMA-trend (cth EMA50) - confirm bukan cuma cross sekejap
      3. EMA-slow SEDANG slope MENAIK - bukan sekadar posisi bersilang
      4. ATR >= min_atr (jika diberi) - elak "trend" palsu masa market sideways/senyap
    BEARISH ialah kebalikan kesemua syarat di atas. Selain itu -> NEUTRAL.
    """
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

    is_bullish = (ema_fast_now > ema_slow_now and price > ema_trend_now and ema_slow_slope > 0)
    is_bearish = (ema_fast_now < ema_slow_now and price < ema_trend_now and ema_slow_slope < 0)

    if is_bullish:
        return "BULLISH"
    elif is_bearish:
        return "BEARISH"
    return "NEUTRAL"


def compute_trend_score(m15_dir, m5_dir, ema20, ema50, price):
    """Sistem MARKAH - gantikan syarat keras 'H1==M15==M5 wajib sama'.

    M15 = trend utama, M5 = momentum (bobot plg tinggi sbb plg dekat dgn M1 entry),
    struktur EMA20/50 & harga M1 jadi pengesah tambahan. H1 SENGAJA tak diguna
    langsung dalam markah ni - EMA50 pada H1 terlalu perlahan utk scalping 10 pip.

    Return (bullish_score, bearish_score, breakdown_text) - breakdown utk log &
    utk bagi konteks kat Groq/Telegram supaya telus komponen mana yang menyumbang.
    """
    bullish, bearish = 0, 0
    parts = []

    if m15_dir == "BULLISH":
        bullish += SCORE_M15
        parts.append(f"M15 BULLISH(+{SCORE_M15})")
    elif m15_dir == "BEARISH":
        bearish += SCORE_M15
        parts.append(f"M15 BEARISH(+{SCORE_M15})")

    if m5_dir == "BULLISH":
        bullish += SCORE_M5
        parts.append(f"M5 BULLISH(+{SCORE_M5})")
    elif m5_dir == "BEARISH":
        bearish += SCORE_M5
        parts.append(f"M5 BEARISH(+{SCORE_M5})")

    if ema20 is not None and ema50 is not None:
        if ema20 > ema50:
            bullish += SCORE_EMA_STACK
            parts.append(f"EMA20>EMA50(+{SCORE_EMA_STACK})")
        elif ema20 < ema50:
            bearish += SCORE_EMA_STACK
            parts.append(f"EMA20<EMA50(+{SCORE_EMA_STACK})")

    if ema20 is not None and price is not None:
        if price > ema20:
            bullish += SCORE_PRICE_VS_EMA20
            parts.append(f"Harga>EMA20(+{SCORE_PRICE_VS_EMA20})")
        elif price < ema20:
            bearish += SCORE_PRICE_VS_EMA20
            parts.append(f"Harga<EMA20(+{SCORE_PRICE_VS_EMA20})")

    breakdown = ", ".join(parts) if parts else "tiada komponen selaras"
    return bullish, bearish, breakdown


# ─────────────────────────────────────────────
# 3. AI ANALYSIS LAYER
# ─────────────────────────────────────────────

def build_prompt(current_price, m1_candles, indicators, trend_summary, news_events, score_breakdown):
    candles_summary = "\n".join([
        f"  {c['datetime']}: O={c['open']} H={c['high']} L={c['low']} C={c['close']}"
        for c in m1_candles[-20:]
    ])

    news_summary = "\n".join([
        f"  {e.get('date', '')} {e.get('title', '')} (impact: {e.get('impact', '')})"
        for e in news_events
    ]) or "  Tiada high/medium impact USD news hari ini."

    prompt = f"""Kau seorang SCALPER XAUUSD berpengalaman di timeframe M1. Sistem sudah CONFIRM BIAS trend dari markah M15+M5+struktur M1 (H1 sengaja TIDAK diguna - terlalu perlahan utk scalping 10 pip), dan ADX14/ATR14 M1 sudah cukup kuat (market trending, bukan choppy) sebelum minta analisa kau - tugas kau HANYA cari titik ENTRY M1 yang tepat mengikut arah bias ni, target TEPAT 10 pips.

BIAS TREND (dari sistem MARKAH, markah {trend_summary['score']}/{trend_summary['max_score']}, minimum {MIN_TREND_SCORE} - PERCAYA info ni):
- Bias: {trend_summary['bias']}
- Komponen markah: {score_breakdown}
- M15 Trend: {trend_summary['m15']}
- M5 Momentum: {trend_summary['m5']}

INDIKATOR M1 SEMASA:
- EMA20: {indicators['ema20']}
- EMA50: {indicators['ema50']}
- RSI14: {indicators['rsi14']}
- ATR14: {indicators['atr14']} (ukuran volatiliti - guna untuk cadangan SL yang munasabah)
- ADX14: {indicators['adx14']} (>= {MIN_ADX_M1} bermakna market sedang trending, dah ditapis dari choppy)

HARGA SEMASA: {current_price}

20 CANDLE M1 TERKINI (dari terbaru ke lama):
{candles_summary}

ECONOMIC CALENDAR HARI INI (USD, High/Medium impact):
{news_summary}

PERATURAN KETAT:
1. ENTRY MESTI harga semasa ({current_price}) - kau trading market order SEKARANG, jangan cadang entry pada harga lain.
2. TP sentiasa TEPAT 10 pips dari entry, ke arah BIAS di atas.
3. Cadangkan sl_pips (nombor pip sahaja, biasanya 6-10 pips) berdasarkan ATR14 dan struktur candle M1 terkini.
4. Kalau RSI overbought (>70) untuk BUY, atau oversold (<30) untuk SELL - risiko reversal tinggi, bagi HOLD.
5. Cari titik masuk M1 yang confirm arah BIAS (breakout micro-range, rejection dari EMA20, continuation candle selepas pullback) - jangan asal ada pergerakan kecil terus bagi signal. Elak entry kalau harga sudah jauh terkeluar (extended) drpd EMA20 - ini tanda "chasing", bukan entry yang bersih.
6. Beri confidence yang JUJUR berdasarkan kekuatan bukti - sistem akan tapis dan hanya proceed signal dengan confidence tinggi.

Berikan jawapan HANYA dalam format JSON tepat, tiada teks lain:
{{
  "decision": "BUY" atau "SELL" atau "HOLD",
  "confidence": <nombor 0-100, jujur>,
  "reason": "<penjelasan ringkas 1-2 ayat Bahasa Melayu - sebut bukti M1 dan macam mana ia selari dengan bias>",
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


def format_notification(result, trend_summary, indicators=None):
    decision = result.get("decision", "").upper()
    emoji = {"BUY": "🟢", "SELL": "🔴"}.get(decision, "⚪")

    lines = [
        f"{emoji} *XAUUSD 10-Pip Scalp Signal (M1) — {decision} NOW*",
        f"Entry: `{result.get('entry')}` (harga semasa - market order)",
        f"Confidence: {result.get('confidence', '?')}%",
        f"Bias: {trend_summary['bias']} (markah {trend_summary['score']}/{trend_summary['max_score']}) | M15 {trend_summary['m15']} | M5 {trend_summary['m5']}",
    ]

    if indicators:
        lines.append(
            f"M1: EMA20 {indicators.get('ema20')} | ATR14 {indicators.get('atr14')} | ADX14 {indicators.get('adx14')}"
        )

    lines.append("")
    lines.append(f"_{result.get('reason', '')}_")

    if result.get("key_level"):
        lines.append(f"Key level: {result['key_level']}")

    # Syarat kesahihan entry - elak "chase" harga yg dah bergerak jauh drpd setup
    # asal masa notification sampai/dibaca di telefon.
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

    lines.append("")
    lines.append(f"🕐 {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    lines.append("\n⚠️ Ini analysis sahaja. Buat keputusan buy/sell sendiri dalam MT5.")

    return "\n".join(lines)


# ─────────────────────────────────────────────
# 5. MAIN JOB
# ─────────────────────────────────────────────

def run_analysis():
    # --- SESSION CHECK ---
    if not session_active:
        log.info("Session inactive, skipping analysis (gunakan /startsession untuk aktifkan semula).")
        return
    # --- END SESSION CHECK ---

    log.info("Running scheduled analysis...")

    m1_candles = fetch_candles("1min", M1_OUTPUTSIZE)
    if not m1_candles or len(m1_candles) < 60:
        log.error("Data M1 tidak cukup - skip check ini.")
        return

    current_price = m1_candles[-1]["close"]

    # Aggregate M5/M15 dari data M1 yang sama - jimat API call
    # (diselaraskan dgn sempadan masa sebenar :00/:05/:15... - rujuk aggregate_candles())
    m5_candles = aggregate_candles(m1_candles, 5)
    m15_candles = aggregate_candles(m1_candles, 15)

    m15_dir = trend_bias(m15_candles, min_atr=MIN_ATR_M15)
    m5_dir = trend_bias(m5_candles, min_atr=MIN_ATR_M5)

    # Kira indicator M1 (diperlukan awal sbb turut jadi input sistem markah)
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

    # GATE 1 (SISTEM MARKAH - gantikan "H1==M15==M5 wajib sama")
    bullish_score, bearish_score, breakdown = compute_trend_score(
        m15_dir, m5_dir, indicators["ema20"], indicators["ema50"], current_price
    )
    log.info(
        f"Trend score - M15:{m15_dir} M5:{m5_dir} | Bullish {bullish_score}/{MAX_TREND_SCORE}, "
        f"Bearish {bearish_score}/{MAX_TREND_SCORE} ({breakdown})"
    )

    if bullish_score >= MIN_TREND_SCORE and bullish_score > bearish_score:
        bias = "BULLISH"
        bias_score = bullish_score
    elif bearish_score >= MIN_TREND_SCORE and bearish_score > bullish_score:
        bias = "BEARISH"
        bias_score = bearish_score
    else:
        log.info(f"Markah trend bawah threshold {MIN_TREND_SCORE} (atau seri) - HOLD (senyap, Groq tak dipanggil).")
        return

    # GATE 1b: ATR & ADX M1 - gate KERAS (soal "boleh trade ke tidak", bukan "arah mana")
    if indicators["atr14"] is None or indicators["atr14"] < MIN_ATR_M1_TRADE:
        log.info(f"ATR14 M1 ({indicators['atr14']}) bawah minimum {MIN_ATR_M1_TRADE} - volatiliti tak cukup utk TP 10 pip. Skip.")
        return

    if indicators["adx14"] is None or indicators["adx14"] < MIN_ADX_M1:
        log.info(f"ADX14 M1 ({indicators['adx14']}) bawah minimum {MIN_ADX_M1} - market mendatar/choppy. Skip.")
        return

    # GATE 2: News blackout
    news_events = get_today_news()
    minutes_to_news = minutes_until_next_high_impact(news_events)
    if minutes_to_news is not None and minutes_to_news <= NEWS_BLACKOUT_MINUTES:
        log.info(f"High-impact USD news dalam ~{round(minutes_to_news)} minit - HOLD (elak volatility, Groq tak dipanggil).")
        return

    trend_summary = {"m15": m15_dir, "m5": m5_dir, "bias": bias, "score": bias_score, "max_score": MAX_TREND_SCORE}
    prompt = build_prompt(current_price, m1_candles, indicators, trend_summary, news_events, breakdown)

    log.info("Semua gate lepas (markah trend + ATR/ADX + news) - Calling Groq API...")
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

    # Syarat kesahihan entry - elak "chase" harga yang dah bergerak jauh drpd setup
    # asal masa notification sampai ke telefon / masa kau sempat buka MT5.
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


# ─────────────────────────────────────────────
# 6. TELEGRAM POLLING UNTUK KAWALAN SESI
#    (/startsession & /endsession)
# ─────────────────────────────────────────────

def poll_telegram_commands():
    """
    Thread berasingan: pantau arahan dari chat Telegram.
    /startsession  → aktifkan semula analisis (set session_active = True)
    /endsession    → hentikan semua panggilan API (set session_active = False)
    """
    global session_active
    last_update_id = 0
    base_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"

    log.info("Telegram command listener started (polling setiap 5 saat).")
    while True:
        try:
            url = f"{base_url}/getUpdates"
            params = {"offset": last_update_id + 1, "timeout": 10, "allowed_updates": ["message"]}
            resp = requests.get(url, params=params, timeout=20)
            data = resp.json()

            if not data.get("ok"):
                log.error(f"getUpdates error: {data}")
                time.sleep(5)
                continue

            for update in data.get("result", []):
                update_id = update["update_id"]
                last_update_id = update_id  # sentiasa kemaskini

                msg = update.get("message")
                if not msg:
                    continue
                text = msg.get("text", "").strip()
                chat_id = msg.get("chat", {}).get("id")

                if text.startswith("/startsession"):
                    session_active = True
                    log.info(f"Session diaktifkan oleh chat_id {chat_id}.")
                    requests.post(f"{base_url}/sendMessage", json={
                        "chat_id": chat_id,
                        "text": "✅ Sesi analisis **AKTIF**. Bot akan mula hantar signal semula."
                    }, timeout=10)
                elif text.startswith("/endsession"):
                    session_active = False
                    log.info(f"Session dihentikan oleh chat_id {chat_id}.")
                    requests.post(f"{base_url}/sendMessage", json={
                        "chat_id": chat_id,
                        "text": "⏸️ Sesi analisis **DIHENTIKAN**. Bot tidak akan buat sebarang panggilan API sehingga /startsession."
                    }, timeout=10)
                # abaikan mesej lain

        except Exception as e:
            log.error(f"Polling Telegram error: {e}")
            time.sleep(10)


# ─────────────────────────────────────────────
# 7. KEEP-ALIVE SERVER (untuk Render.com free tier + UptimeRobot)
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
    # Start Telegram polling untuk commands
    polling_thread = Thread(target=poll_telegram_commands, daemon=True)
    polling_thread.start()

    scheduler_thread = Thread(target=run_scheduler, daemon=True)
    scheduler_thread.start()

    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
