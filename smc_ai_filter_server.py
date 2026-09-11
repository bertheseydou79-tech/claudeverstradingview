```python
"""
TradingView Webhook Relay
=========================
TradingView -> Render -> Telegram

Le serveur NE PREND AUCUNE décision de trading.
L'indicateur TradingView est responsable de tous les filtres et décisions.

Le serveur :
1. reçoit le JSON TradingView
2. vérifie le secret
3. formate le signal
4. envoie le message à Telegram
"""

import os
import json
import hmac
import logging

import httpx
from fastapi import FastAPI, Request, HTTPException

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("tradingview-relay")


# ----------------------------------------------------------------------------
# 1) CONFIGURATION — variables d'environnement Render
# ----------------------------------------------------------------------------

def _clean(v: str) -> str:
    return (v or "").strip().strip('"').strip("'").strip()


WEBHOOK_SECRET = _clean(os.environ["WEBHOOK_SECRET"])
TELEGRAM_BOT_TOKEN = _clean(os.environ["TELEGRAM_BOT_TOKEN"])
TELEGRAM_CHAT_ID = _clean(os.environ["TELEGRAM_CHAT_ID"])


# ----------------------------------------------------------------------------
# 2) UTILITAIRES
# ----------------------------------------------------------------------------

def get_value(payload: dict, *names):
    """Retourne la première valeur disponible."""
    for name in names:
        if name in payload and payload[name] not in ("", None):
            return payload[name]
    return None


def format_number(value):
    """Formate proprement les nombres sans modifier leur valeur."""
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
    """Convertit LONG/SHORT en BUY/SELL pour Telegram."""
    if value is None:
        return "SIGNAL"

    direction = str(value).upper().strip()

    if direction in ("LONG", "BUY", "ACHAT"):
        return "BUY"

    if direction in ("SHORT", "SELL", "VENTE"):
        return "SELL"

    return direction


# ----------------------------------------------------------------------------
# 3) CONSTRUCTION DU MESSAGE TELEGRAM
# ----------------------------------------------------------------------------

def build_telegram_message(payload: dict) -> str:

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

    score = get_value(payload, "score")
    entry = get_value(payload, "entry", "pe", "price")
    sl = get_value(payload, "sl", "stop")
    tp1 = get_value(payload, "tp1")
    tp2 = get_value(payload, "tp2")
    tp3 = get_value(payload, "tp3")
    risk_pct = get_value(payload, "risk_pct", "riskPct")

    # Icônes uniquement pour l'affichage.
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


# ----------------------------------------------------------------------------
# 4) ENVOI TELEGRAM
# ----------------------------------------------------------------------------

async def send_telegram(text: str):

    url = (
        f"https://api.telegram.org/"
        f"bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    )

    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "disable_web_page_preview": True,
    }

    async with httpx.AsyncClient(timeout=15) as client:

        response = await client.post(
            url,
            json=payload
        )

        if response.status_code != 200:

            log.error(
                "Erreur Telegram %s : %s",
                response.status_code,
                response.text
            )

            raise HTTPException(
                status_code=502,
                detail="Erreur Telegram"
            )


# ----------------------------------------------------------------------------
# 5) APPLICATION FASTAPI
# ----------------------------------------------------------------------------

app = FastAPI(
    title="TradingView Telegram Relay"
)


# ----------------------------------------------------------------------------
# 6) HEALTH CHECK
# ----------------------------------------------------------------------------

@app.get("/")
async def health():

    return {
        "status": "ok",
        "mode": "relay_only"
    }


# ----------------------------------------------------------------------------
# 7) WEBHOOK TRADINGVIEW
# ----------------------------------------------------------------------------

@app.post("/webhook")
async def webhook(request: Request):

    # Lire le corps de la requête
    raw = await request.body()

    # Décoder le JSON
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

    # ------------------------------------------------------------------------
    # Vérification du secret
    # ------------------------------------------------------------------------

    secret = str(
        payload.pop("secret", "")
    )

    if not hmac.compare_digest(
        secret,
        WEBHOOK_SECRET
    ):

        raise HTTPException(
            status_code=401,
            detail="Secret invalide"
        )

    # ------------------------------------------------------------------------
    # Construire le message
    # ------------------------------------------------------------------------

    message = build_telegram_message(payload)

    # ------------------------------------------------------------------------
    # Envoyer Telegram
    # ------------------------------------------------------------------------

    await send_telegram(message)

    # ------------------------------------------------------------------------
    # Log
    # ------------------------------------------------------------------------

    log.info(
        "Signal envoyé : %s %s",
        payload.get("dir"),
        payload.get("sym")
    )

    return {
        "ok": True,
        "sent": True
    }
```
