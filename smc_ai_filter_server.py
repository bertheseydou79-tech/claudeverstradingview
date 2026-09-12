"""
TradingView Webhook Relay
=========================
TradingView -> Render -> Telegram

Le serveur ne prend aucune décision de trading.
TradingView est responsable des filtres et décisions.
"""

import os
import json
import hmac
import logging

import httpx
from fastapi import FastAPI, Request, HTTPException


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("tradingview-relay")


# ============================================================
# OUTILS
# ============================================================

def clean(value):
    if value is None:
        return ""
    return str(value).strip().strip('"').strip("'")


def get_value(payload, *names):
    for name in names:
        if name in payload and payload[name] not in ("", None):
            return payload[name]
    return None


def format_number(value):
    if value is None:
        return "-"

    try:
        number = float(value)

        if number.is_integer():
            return str(int(number))

        return f"{number:.8f}".rstrip("0").rstrip(".")

    except (ValueError, TypeError):
        return str(value)


def normalize_direction(value):
    if value is None:
        return "SIGNAL"

    direction = str(value).upper().strip()

    if direction in ("LONG", "BUY", "ACHAT"):
        return "BUY"

    if direction in ("SHORT", "SELL", "VENTE"):
        return "SELL"

    return direction


# ============================================================
# VARIABLES D'ENVIRONNEMENT
# ============================================================

WEBHOOK_SECRET = clean(
    os.environ.get("WEBHOOK_SECRET")
)

TELEGRAM_BOT_TOKEN = clean(
    os.environ.get("TELEGRAM_BOT_TOKEN")
)

TELEGRAM_CHAT_ID = clean(
    os.environ.get("TELEGRAM_CHAT_ID")
)


# ============================================================
# MESSAGE TELEGRAM
# ============================================================

def build_telegram_message(payload):

    direction_raw = get_value(
        payload,
        "dir",
        "direction",
        "signal",
        "side"
    )

    direction = normalize_direction(direction_raw)

    symbol = get_value(
        payload,
        "sym",
        "symbol",
        "ticker"
    ) or "?"

    score = get_value(
        payload,
        "score"
    )

    entry = get_value(
        payload,
        "entry",
        "pe",
        "price"
    )

    sl = get_value(
        payload,
        "sl",
        "stop"
    )

    tp1 = get_value(
        payload,
        "tp1"
    )

    tp2 = get_value(
        payload,
        "tp2"
    )

    tp3 = get_value(
        payload,
        "tp3"
    )

    risk_pct = get_value(
        payload,
        "risk_pct",
        "riskPct"
    )

    if direction == "BUY":
        icon = "🟢"
    elif direction == "SELL":
        icon = "🔴"
    else:
        icon = "⚪"

    message = (
        f"{icon} {direction} {symbol}\n"
        f"\n"
        f"Score: {format_number(score)}\n"
        f"Entry: {format_number(entry)}\n"
        f"SL: {format_number(sl)}\n"
        f"\n"
        f"TP1: {format_number(tp1)}\n"
        f"TP2: {format_number(tp2)}\n"
        f"TP3: {format_number(tp3)}\n"
        f"\n"
        f"Risk: {format_number(risk_pct)}%"
    )

    return message


# ============================================================
# TELEGRAM
# ============================================================

async def send_telegram(text):

    if not TELEGRAM_BOT_TOKEN:
        log.error("TELEGRAM_BOT_TOKEN manquant")

        raise HTTPException(
            status_code=500,
            detail="TELEGRAM_BOT_TOKEN manquant"
        )

    if not TELEGRAM_CHAT_ID:
        log.error("TELEGRAM_CHAT_ID manquant")

        raise HTTPException(
            status_code=500,
            detail="TELEGRAM_CHAT_ID manquant"
        )

    url = (
        "https://api.telegram.org/"
        f"bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    )

    telegram_payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "disable_web_page_preview": True
    }

    try:

        async with httpx.AsyncClient(
            timeout=15
        ) as client:

            response = await client.post(
                url,
                json=telegram_payload
            )

        log.info(
            "Telegram HTTP %s",
            response.status_code
        )

        log.info(
            "Telegram réponse: %s",
            response.text
        )

        if response.status_code != 200:

            raise HTTPException(
                status_code=502,
                detail=response.text
            )

        result = response.json()

        if not result.get("ok", False):

            raise HTTPException(
                status_code=502,
                detail=response.text
            )

        return result

    except HTTPException:
        raise

    except Exception as e:

        log.exception(
            "Erreur connexion Telegram"
        )

        raise HTTPException(
            status_code=502,
            detail=f"Erreur connexion Telegram: {str(e)}"
        )


# ============================================================
# APPLICATION FASTAPI
# ============================================================

app = FastAPI(
    title="TradingView Telegram Relay"
)


# ============================================================
# HEALTH CHECK
# ============================================================

@app.get("/")
async def health():

    return {
        "status": "ok",
        "mode": "relay_only"
    }


# ============================================================
# TEST TELEGRAM
# ============================================================

@app.get("/test-telegram")
async def test_telegram():

    message = (
        "🟢 TEST TELEGRAM\n"
        "\n"
        "Le relais Render fonctionne.\n"
        "\n"
        "Service: claudeverstradingview\n"
        "Status: OK"
    )

    result = await send_telegram(message)

    return {
        "ok": True,
        "telegram_test": True,
        "telegram_response": result
    }


# ============================================================
# WEBHOOK TRADINGVIEW
# ============================================================

@app.post("/webhook")
async def webhook(request: Request):

    raw = await request.body()

    try:

        payload = json.loads(raw)

    except json.JSONDecodeError:

        raise HTTPException(
            status_code=400,
            detail="JSON invalide"
        )

    if not isinstance(payload, dict):

        raise HTTPException(
            status_code=400,
            detail="Le JSON doit être un objet"
        )

    received_secret = str(
        payload.pop("secret", "")
    )

    if not WEBHOOK_SECRET:

        log.error(
            "WEBHOOK_SECRET manquant"
        )

        raise HTTPException(
            status_code=500,
            detail="WEBHOOK_SECRET manquant"
        )

    if not hmac.compare_digest(
        received_secret,
        WEBHOOK_SECRET
    ):

        log.warning(
            "Secret webhook invalide"
        )

        raise HTTPException(
            status_code=401,
            detail="Secret invalide"
        )

    message = build_telegram_message(
        payload
    )

    result = await send_telegram(
        message
    )

    log.info(
        "Signal envoyé : %s %s",
        payload.get("dir"),
        payload.get("sym")
    )

    return {
        "ok": True,
        "sent": True,
        "telegram_response": result
    }
