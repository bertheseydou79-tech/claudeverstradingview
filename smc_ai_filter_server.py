"""
TradingView Webhook Relay — MULTI-CANAUX
========================================
TradingView -> Render -> Telegram (plusieurs canaux)

Le serveur ne prend aucune décision de trading.
TradingView est responsable des filtres et décisions.

NOUVEAU : envoie le même message à PLUSIEURS canaux Telegram.
Configure la variable d'environnement TELEGRAM_CHAT_IDS sur Render :
   TELEGRAM_CHAT_IDS = -1001111111111,-1002222222222,-1003333333333
(plusieurs chat_id séparés par des virgules)

Rétro-compatible : si TELEGRAM_CHAT_IDS est vide, on retombe sur
l'ancienne variable TELEGRAM_CHAT_ID (un seul canal).
"""

import os
import json
import hmac
import logging

import httpx
from fastapi import FastAPI, Request, HTTPException


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


def parse_chat_ids(raw):
    """Transforme '-100111, -100222' en ['-100111', '-100222']."""
    if not raw:
        return []
    ids = []
    for part in raw.replace(";", ",").split(","):
        cid = clean(part)
        if cid:
            ids.append(cid)
    return ids


# ============================================================
# VARIABLES D'ENVIRONNEMENT
# ============================================================

WEBHOOK_SECRET = clean(
    os.environ.get("WEBHOOK_SECRET")
)

TELEGRAM_BOT_TOKEN = clean(
    os.environ.get("TELEGRAM_BOT_TOKEN")
)

# NOUVEAU : liste de canaux (séparés par des virgules)
TELEGRAM_CHAT_IDS = parse_chat_ids(
    os.environ.get("TELEGRAM_CHAT_IDS")
)

# Rétro-compat : ancien canal unique en secours
_LEGACY_CHAT_ID = clean(
    os.environ.get("TELEGRAM_CHAT_ID")
)
if not TELEGRAM_CHAT_IDS and _LEGACY_CHAT_ID:
    TELEGRAM_CHAT_IDS = [_LEGACY_CHAT_ID]


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

    tp1 = get_value(payload, "tp1")
    tp2 = get_value(payload, "tp2")
    tp3 = get_value(payload, "tp3")

    risk_pct = get_value(
        payload,
        "risk_pct",
        "riskPct"
    )

    # Lot envoyé par TradingView
    lot = get_value(
        payload,
        "lot",
        "lots",
        "position_size"
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
        f"Risk: {format_number(risk_pct)}%\n"
        f"Lot: {format_number(lot)}"
    )

    return message


# ============================================================
# TELEGRAM (envoi vers UN canal)
# ============================================================

async def send_to_chat(client, chat_id, text):
    """Envoie le message à un seul canal. Retourne (ok, detail)."""
    url = (
        "https://api.telegram.org/"
        f"bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    )

    telegram_payload = {
        "chat_id": chat_id,
        "text": text,
        "disable_web_page_preview": True
    }

    try:
        response = await client.post(url, json=telegram_payload)

        log.info(
            "Telegram %s -> HTTP %s | %s",
            chat_id,
            response.status_code,
            response.text
        )

        if response.status_code != 200:
            return False, response.text

        result = response.json()

        if not result.get("ok", False):
            return False, response.text

        return True, "ok"

    except Exception as e:
        log.exception("Erreur connexion Telegram (%s)", chat_id)
        return False, str(e)


# ============================================================
# TELEGRAM (diffusion MULTI-CANAUX)
# ============================================================

async def broadcast_telegram(text):

    if not TELEGRAM_BOT_TOKEN:
        log.error("TELEGRAM_BOT_TOKEN manquant")
        raise HTTPException(
            status_code=500,
            detail="TELEGRAM_BOT_TOKEN manquant"
        )

    if not TELEGRAM_CHAT_IDS:
        log.error("Aucun chat_id configuré (TELEGRAM_CHAT_IDS)")
        raise HTTPException(
            status_code=500,
            detail="Aucun canal configuré (TELEGRAM_CHAT_IDS)"
        )

    results = []
    sent = 0
    failed = 0

    async with httpx.AsyncClient(timeout=15) as client:
        for chat_id in TELEGRAM_CHAT_IDS:
            ok, detail = await send_to_chat(client, chat_id, text)
            results.append({
                "chat_id": chat_id,
                "ok": ok,
                "detail": detail
            })
            if ok:
                sent += 1
            else:
                failed += 1

    # Si AUCUN canal n'a reçu, on remonte une erreur.
    if sent == 0:
        raise HTTPException(
            status_code=502,
            detail={"message": "Aucun envoi réussi", "results": results}
        )

    return {"sent": sent, "failed": failed, "results": results}


# ============================================================
# APPLICATION FASTAPI
# ============================================================

app = FastAPI(
    title="TradingView Telegram Relay (multi-canaux)"
)


# ============================================================
# HEALTH CHECK
# ============================================================

@app.get("/")
async def health():
    return {
        "status": "ok",
        "mode": "relay_only",
        "channels": len(TELEGRAM_CHAT_IDS)
    }


# ============================================================
# TEST TELEGRAM (diffuse à tous les canaux)
# ============================================================

@app.get("/test-telegram")
async def test_telegram():

    message = (
        "🟢 TEST TELEGRAM\n"
        "\n"
        "Le relais Render fonctionne.\n"
        f"Canaux configurés: {len(TELEGRAM_CHAT_IDS)}\n"
        "Status: OK"
    )

    result = await broadcast_telegram(message)

    return {
        "ok": True,
        "telegram_test": True,
        "broadcast": result
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

    received_secret = str(payload.pop("secret", ""))

    if not WEBHOOK_SECRET:
        log.error("WEBHOOK_SECRET manquant")
        raise HTTPException(
            status_code=500,
            detail="WEBHOOK_SECRET manquant"
        )

    if not hmac.compare_digest(received_secret, WEBHOOK_SECRET):
        log.warning("Secret webhook invalide")
        raise HTTPException(
            status_code=401,
            detail="Secret invalide"
        )

    message = build_telegram_message(payload)

    result = await broadcast_telegram(message)

    log.info(
        "Signal diffusé : %s %s | lot=%s | %s/%s canaux OK",
        payload.get("dir"),
        payload.get("sym"),
        payload.get("lot"),
        result["sent"],
        result["sent"] + result["failed"]
    )

    return {
        "ok": True,
        "sent": True,
        "broadcast": result
    }
