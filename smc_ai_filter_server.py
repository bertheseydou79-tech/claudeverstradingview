"""
TradingView Webhook Relay — MULTI-CANAUX + POSTGRESQL + MT5
=============================================================

TradingView
    -> Render /webhook
        -> PostgreSQL
        -> Telegram
        -> MT5 via /next-signal

Le serveur ne prend aucune décision de trading.
TradingView reste responsable des signaux.

PostgreSQL conserve les signaux même après redémarrage de Render.
"""

import os
import json
import hmac
import uuid
import logging
from datetime import datetime, timezone, timedelta

import httpx
import psycopg2
from psycopg2.extras import RealDictCursor

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


def parse_chat_ids(raw):

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

TELEGRAM_CHAT_IDS = parse_chat_ids(
    os.environ.get("TELEGRAM_CHAT_IDS")
)

_LEGACY_CHAT_ID = clean(
    os.environ.get("TELEGRAM_CHAT_ID")
)

if not TELEGRAM_CHAT_IDS and _LEGACY_CHAT_ID:
    TELEGRAM_CHAT_IDS = [_LEGACY_CHAT_ID]


# ------------------------------------------------------------
# PostgreSQL
# ------------------------------------------------------------

DATABASE_URL = clean(
    os.environ.get("DATABASE_URL")
)


# ------------------------------------------------------------
# Clé API réservée à MT5
# ------------------------------------------------------------

MT5_API_KEY = clean(
    os.environ.get("MT5_API_KEY")
)


# ============================================================
# BASE DE DONNÉES
# ============================================================

def get_db():

    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL manquant")

    return psycopg2.connect(
        DATABASE_URL,
        sslmode="require"
    )


def init_db():

    conn = get_db()

    try:

        with conn.cursor() as cur:

            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS signals (

                    id BIGSERIAL PRIMARY KEY,

                    signal_id TEXT UNIQUE NOT NULL,

                    direction TEXT NOT NULL,

                    symbol TEXT NOT NULL,

                    score DOUBLE PRECISION,

                    entry DOUBLE PRECISION,

                    sl DOUBLE PRECISION,

                    tp1 DOUBLE PRECISION,

                    tp2 DOUBLE PRECISION,

                    tp3 DOUBLE PRECISION,

                    risk_pct DOUBLE PRECISION,

                    lot DOUBLE PRECISION,

                    status TEXT NOT NULL DEFAULT 'NEW',

                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

                    claimed_at TIMESTAMPTZ,

                    executed_at TIMESTAMPTZ,

                    rejected_at TIMESTAMPTZ,

                    error_message TEXT
                )
                """
            )

            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_signals_status
                ON signals(status)
                """
            )

            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_signals_created
                ON signals(created_at)
                """
            )

        conn.commit()

        log.info("PostgreSQL initialisée.")

    finally:

        conn.close()


# ============================================================
# AUTHENTIFICATION MT5
# ============================================================

def check_mt5_api(request: Request):

    if not MT5_API_KEY:
        log.error("MT5_API_KEY manquant")

        raise HTTPException(
            status_code=500,
            detail="MT5_API_KEY manquant"
        )

    received_key = clean(
        request.headers.get("X-MT5-API-KEY")
    )

    if not received_key:
        raise HTTPException(
            status_code=401,
            detail="Clé MT5 manquante"
        )

    if not hmac.compare_digest(
        received_key,
        MT5_API_KEY
    ):
        log.warning("Tentative MT5 avec clé invalide")

        raise HTTPException(
            status_code=401,
            detail="Clé MT5 invalide"
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

    direction = normalize_direction(
        direction_raw
    )

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
# TELEGRAM
# ============================================================

async def send_to_chat(client, chat_id, text):

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

        response = await client.post(
            url,
            json=telegram_payload
        )

        log.info(
            "Telegram %s -> HTTP %s",
            chat_id,
            response.status_code
        )

        if response.status_code != 200:
            return False, response.text

        result = response.json()

        if not result.get("ok", False):
            return False, response.text

        return True, "ok"

    except Exception as e:

        log.exception(
            "Erreur Telegram (%s)",
            chat_id
        )

        return False, str(e)


async def broadcast_telegram(text):

    if not TELEGRAM_BOT_TOKEN:

        raise HTTPException(
            status_code=500,
            detail="TELEGRAM_BOT_TOKEN manquant"
        )

    if not TELEGRAM_CHAT_IDS:

        raise HTTPException(
            status_code=500,
            detail="Aucun canal Telegram configuré"
        )

    results = []
    sent = 0
    failed = 0

    async with httpx.AsyncClient(
        timeout=15
    ) as client:

        for chat_id in TELEGRAM_CHAT_IDS:

            ok, detail = await send_to_chat(
                client,
                chat_id,
                text
            )

            results.append({
                "chat_id": chat_id,
                "ok": ok,
                "detail": detail
            })

            if ok:
                sent += 1
            else:
                failed += 1

    if sent == 0:

        raise HTTPException(
            status_code=502,
            detail={
                "message": "Aucun envoi Telegram réussi",
                "results": results
            }
        )

    return {
        "sent": sent,
        "failed": failed,
        "results": results
    }


# ============================================================
# APPLICATION
# ============================================================

app = FastAPI(
    title="TradingView Telegram Relay + MT5"
)


# ============================================================
# STARTUP
# ============================================================

@app.on_event("startup")
async def startup():

    try:

        init_db()

    except Exception:

        log.exception(
            "Impossible d'initialiser PostgreSQL"
        )


# ============================================================
# HEALTH CHECK
# ============================================================

@app.get("/")
async def health():

    return {
        "status": "ok",
        "mode": "tradingview-relay-mt5",
        "telegram_channels": len(
            TELEGRAM_CHAT_IDS
        ),
        "database": bool(DATABASE_URL),
        "mt5_api": bool(MT5_API_KEY)
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
        f"Canaux configurés: {len(TELEGRAM_CHAT_IDS)}\n"
        "PostgreSQL: active\n"
        "MT5 API: active\n"
        "Status: OK"
    )

    result = await broadcast_telegram(
        message
    )

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

    # --------------------------------------------------------
    # Vérification secret TradingView
    # --------------------------------------------------------

    received_secret = str(
        payload.pop("secret", "")
    )

    if not WEBHOOK_SECRET:

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

    # --------------------------------------------------------
    # Extraction
    # --------------------------------------------------------

    direction = normalize_direction(
        get_value(
            payload,
            "dir",
            "direction",
            "signal",
            "side"
        )
    )

    symbol = clean(
        get_value(
            payload,
            "sym",
            "symbol",
            "ticker"
        )
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

    score = get_value(
        payload,
        "score"
    )

    risk_pct = get_value(
        payload,
        "risk_pct",
        "riskPct"
    )

    lot = get_value(
        payload,
        "lot",
        "lots",
        "position_size"
    )

    # --------------------------------------------------------
    # Validation minimale
    # --------------------------------------------------------

    if direction not in ("BUY", "SELL"):

        raise HTTPException(
            status_code=400,
            detail="Direction invalide"
        )

    if not symbol:

        raise HTTPException(
            status_code=400,
            detail="Symbole manquant"
        )

    if entry in (None, ""):

        raise HTTPException(
            status_code=400,
            detail="Entry manquant"
        )

    if sl in (None, ""):

        raise HTTPException(
            status_code=400,
            detail="SL manquant"
        )

    # --------------------------------------------------------
    # ID UNIQUE
    # --------------------------------------------------------

    signal_id = (
        datetime.now(timezone.utc)
        .strftime("%Y%m%d%H%M%S")
        + "-"
        + uuid.uuid4().hex[:8]
    )

    # --------------------------------------------------------
    # STOCKAGE POSTGRESQL
    # --------------------------------------------------------

    conn = get_db()

    try:

        with conn.cursor() as cur:

            cur.execute(
                """
                INSERT INTO signals (
                    signal_id,
                    direction,
                    symbol,
                    score,
                    entry,
                    sl,
                    tp1,
                    tp2,
                    tp3,
                    risk_pct,
                    lot,
                    status
                )

                VALUES (
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'NEW'
                )

                RETURNING id
                """,

                (
                    signal_id,
                    direction,
                    symbol,
                    float(score) if score not in (None, "") else None,
                    float(entry),
                    float(sl),
                    float(tp1) if tp1 not in (None, "") else None,
                    float(tp2) if tp2 not in (None, "") else None,
                    float(tp3) if tp3 not in (None, "") else None,
                    float(risk_pct) if risk_pct not in (None, "") else None,
                    float(lot) if lot not in (None, "") else None
                )
            )

            db_id = cur.fetchone()[0]

        conn.commit()

    finally:

        conn.close()

    log.info(
        "Signal enregistré : %s | %s %s",
        signal_id,
        direction,
        symbol
    )

    # --------------------------------------------------------
    # TELEGRAM
    # --------------------------------------------------------

    message = build_telegram_message(
        payload
    )

    telegram_result = await broadcast_telegram(
        message
    )

    # --------------------------------------------------------
    # RÉPONSE
    # --------------------------------------------------------

    return {

        "ok": True,

        "signal_id": signal_id,

        "database_id": db_id,

        "status": "NEW",

        "telegram": telegram_result
    }


# ============================================================
# MT5 : PROCHAIN SIGNAL
# ============================================================

@app.get("/next-signal")
async def next_signal(
    request: Request
):

    check_mt5_api(request)

    conn = get_db()

    try:

        with conn.cursor(
            cursor_factory=RealDictCursor
        ) as cur:

            # ------------------------------------------------
            # Signal NEW
            # OU signal CLAIMED depuis plus de 5 minutes
            # ------------------------------------------------

            cur.execute(
                """
                SELECT *
                FROM signals

                WHERE
                    status = 'NEW'

                    OR

                    (
                        status = 'CLAIMED'
                        AND claimed_at <
                            NOW() - INTERVAL '5 minutes'
                    )

                ORDER BY created_at ASC

                LIMIT 1

                FOR UPDATE SKIP LOCKED
                """
            )

            signal = cur.fetchone()

            if not signal:

                conn.commit()

                return {
                    "ok": True,
                    "signal": None
                }

            # ------------------------------------------------
            # CLAIM
            # ------------------------------------------------

            cur.execute(
                """
                UPDATE signals

                SET
                    status = 'CLAIMED',
                    claimed_at = NOW()

                WHERE signal_id = %s
                """,

                (
                    signal["signal_id"],
                )
            )

        conn.commit()

        return {
            "ok": True,
            "signal": dict(signal)
        }

    finally:

        conn.close()


# ============================================================
# MT5 : SIGNAL EXÉCUTÉ
# ============================================================

@app.post("/signal-executed")
async def signal_executed(
    request: Request
):

    check_mt5_api(request)

    try:

        data = await request.json()

    except Exception:

        raise HTTPException(
            status_code=400,
            detail="JSON invalide"
        )

    signal_id = clean(
        data.get("signal_id")
    )

    if not signal_id:

        raise HTTPException(
            status_code=400,
            detail="signal_id manquant"
        )

    conn = get_db()

    try:

        with conn.cursor() as cur:

            cur.execute(
                """
                UPDATE signals

                SET
                    status = 'EXECUTED',
                    executed_at = NOW()

                WHERE
                    signal_id = %s
                    AND status = 'CLAIMED'

                RETURNING signal_id
                """,

                (
                    signal_id,
                )
            )

            result = cur.fetchone()

        conn.commit()

    finally:

        conn.close()

    if not result:

        raise HTTPException(
            status_code=409,
            detail="Signal introuvable ou déjà traité"
        )

    log.info(
        "Signal exécuté : %s",
        signal_id
    )

    return {
        "ok": True,
        "signal_id": signal_id,
        "status": "EXECUTED"
    }


# ============================================================
# MT5 : SIGNAL REJETÉ
# ============================================================

@app.post("/signal-rejected")
async def signal_rejected(
    request: Request
):

    check_mt5_api(request)

    try:

        data = await request.json()

    except Exception:

        raise HTTPException(
            status_code=400,
            detail="JSON invalide"
        )

    signal_id = clean(
        data.get("signal_id")
    )

    error_message = clean(
        data.get("error")
    )

    if not signal_id:

        raise HTTPException(
            status_code=400,
            detail="signal_id manquant"
        )

    conn = get_db()

    try:

        with conn.cursor() as cur:

            cur.execute(
                """
                UPDATE signals

                SET
                    status = 'REJECTED',
                    rejected_at = NOW(),
                    error_message = %s

                WHERE
                    signal_id = %s
                    AND status = 'CLAIMED'

                RETURNING signal_id
                """,

                (
                    error_message,
                    signal_id
                )
            )

            result = cur.fetchone()

        conn.commit()

    finally:

        conn.close()

    if not result:

        raise HTTPException(
            status_code=409,
            detail="Signal introuvable ou déjà traité"
        )

    return {
        "ok": True,
        "signal_id": signal_id,
        "status": "REJECTED"
    }


# ============================================================
# LISTE DES SIGNAUX — ADMIN / DEBUG
# ============================================================

@app.get("/signals")
async def signals(
    request: Request
):

    check_mt5_api(request)

    conn = get_db()

    try:

        with conn.cursor(
            cursor_factory=RealDictCursor
        ) as cur:

            cur.execute(
                """
                SELECT
                    signal_id,
                    direction,
                    symbol,
                    entry,
                    sl,
                    tp1,
                    tp2,
                    tp3,
                    risk_pct,
                    lot,
                    status,
                    created_at,
                    claimed_at,
                    executed_at,
                    rejected_at,
                    error_message

                FROM signals

                ORDER BY created_at DESC

                LIMIT 50
                """
            )

            rows = cur.fetchall()

        return {
            "ok": True,
            "count": len(rows),
            "signals": [
                dict(row)
                for row in rows
            ]
        }

    finally:

        conn.close()
