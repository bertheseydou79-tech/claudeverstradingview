"""
TradingView Webhook Relay — MULTI-CANAUX + POSTGRESQL + MT5 + FILTRE NEWS
=========================================================================

TradingView
    -> Render /webhook
        -> PostgreSQL
        -> Telegram
        -> MT5 via /next-signal   (avec FILTRE NEWS)

Le serveur ne prend aucune decision de trading.
TradingView reste responsable des signaux.

NOUVEAU : filtre news live (calendrier ForexFactory / faireconomy).
Le serveur telecharge le calendrier ~1x/heure et REFUSE de servir un signal
si une annonce a fort impact est dans la fenetre configuree (avant/apres).
Le robot MT5 n'a PAS besoin d'etre modifie.

PostgreSQL conserve les signaux meme apres redemarrage de Render.
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
# Cle API reservee a MT5
# ------------------------------------------------------------

MT5_API_KEY = clean(
    os.environ.get("MT5_API_KEY")
)


# ------------------------------------------------------------
# FILTRE NEWS (calendrier economique live)
# ------------------------------------------------------------

def _list_env(name, default):
    raw = clean(os.environ.get(name)) or default
    return [x.strip().upper() for x in raw.replace(";", ",").split(",") if x.strip()]


def _int_env(name, default):
    try:
        return int(clean(os.environ.get(name)) or default)
    except ValueError:
        return default


# Active/desactive le filtre (mets NEWS_FILTER_ENABLED=0 pour couper)
NEWS_FILTER_ENABLED = clean(
    os.environ.get("NEWS_FILTER_ENABLED", "1")
) not in ("0", "false", "False", "")

# Source du calendrier (ForexFactory via faireconomy, gratuit, sans cle)
NEWS_CALENDAR_URL = clean(
    os.environ.get("NEWS_CALENDAR_URL")
) or "https://nfs.faireconomy.media/ff_calendar_thisweek.json"

# Niveaux d'impact qui bloquent (par defaut : High seulement)
NEWS_IMPACTS = _list_env("NEWS_IMPACTS", "High")

# Devises concernees (l'or = USD ; ajoute EUR,GBP... si tu veux)
NEWS_CURRENCIES = _list_env("NEWS_CURRENCIES", "USD")

# Fenetre de blocage autour de l'annonce (minutes)
NEWS_BEFORE_MIN = _int_env("NEWS_BEFORE_MIN", 30)
NEWS_AFTER_MIN = _int_env("NEWS_AFTER_MIN", 30)

# Rafraichissement du calendrier (minutes). >= 30 conseille (limite ForexFactory)
NEWS_REFRESH_MIN = _int_env("NEWS_REFRESH_MIN", 60)

# En cas d'echec, delai minimum avant de reessayer (minutes) -> evite de
# marteler ForexFactory (limite 2 telechargements / 5 min)
NEWS_RETRY_MIN = _int_env("NEWS_RETRY_MIN", 5)

# Cache en memoire
_news_events = []        # [{title, country, impact, dt(UTC)}]
_news_fetched_at = None  # datetime UTC du dernier chargement REUSSI
_news_last_attempt = None  # datetime UTC de la derniere TENTATIVE (reussie ou non)


# ============================================================
# BASE DE DONNEES
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

        log.info("PostgreSQL initialisee.")

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
            detail="Cle MT5 manquante"
        )

    if not hmac.compare_digest(
        received_key,
        MT5_API_KEY
    ):
        log.warning("Tentative MT5 avec cle invalide")

        raise HTTPException(
            status_code=401,
            detail="Cle MT5 invalide"
        )


# ============================================================
# FILTRE NEWS — calendrier live (ForexFactory / faireconomy)
# ============================================================

async def refresh_news_calendar():
    """Telecharge et met en cache les evenements filtres. Ne casse jamais le service."""
    global _news_events, _news_fetched_at

    try:
        async with httpx.AsyncClient(timeout=20) as client:
            r = await client.get(
                NEWS_CALENDAR_URL,
                headers={"User-Agent": "Mozilla/5.0"}
            )

        text = r.text.strip()

        # Si limite depassee, ForexFactory renvoie du HTML "Request Denied"
        if r.status_code != 200 or not text.startswith("["):
            log.warning(
                "News: reponse inattendue (HTTP %s) - on garde le cache",
                r.status_code
            )
            return

        raw = json.loads(text)

    except Exception:
        log.exception("News: echec de recuperation du calendrier")
        return

    events = []

    for e in raw:
        try:
            impact = str(e.get("impact", "")).upper()
            country = str(e.get("country", "")).upper()

            if impact not in NEWS_IMPACTS:
                continue
            if country not in NEWS_CURRENCIES:
                continue

            dt = datetime.fromisoformat(str(e.get("date")))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)

            events.append({
                "title": str(e.get("title", "")),
                "country": country,
                "impact": impact,
                "dt": dt.astimezone(timezone.utc),
            })
        except Exception:
            continue

    _news_events = events
    _news_fetched_at = datetime.now(timezone.utc)

    log.info(
        "News: %d evenements charges (devises=%s impacts=%s)",
        len(events), NEWS_CURRENCIES, NEWS_IMPACTS
    )


async def maybe_refresh_news():
    """Rafraichit le calendrier si perime, avec back-off en cas d'echec
    (respecte la limite ForexFactory : 2 telechargements / 5 min)."""
    global _news_last_attempt

    if not NEWS_FILTER_ENABLED:
        return

    now = datetime.now(timezone.utc)

    # Cache encore frais -> rien a faire
    if _news_fetched_at is not None:
        if (now - _news_fetched_at).total_seconds() < NEWS_REFRESH_MIN * 60:
            return

    # Back-off : ne pas reessayer plus d'une fois par NEWS_RETRY_MIN,
    # meme si le dernier essai a echoue (evite de marteler ForexFactory)
    if _news_last_attempt is not None:
        if (now - _news_last_attempt).total_seconds() < NEWS_RETRY_MIN * 60:
            return

    _news_last_attempt = now
    await refresh_news_calendar()


def news_blackout(now):
    """Retourne (bloque: bool, raison: str) si 'now' est dans une fenetre news."""
    if not NEWS_FILTER_ENABLED:
        return (False, "")

    before = timedelta(minutes=NEWS_BEFORE_MIN)
    after = timedelta(minutes=NEWS_AFTER_MIN)

    for e in _news_events:
        start = e["dt"] - before
        end = e["dt"] + after
        if start <= now <= end:
            return (
                True,
                "%s (%s) a %s" % (
                    e["title"],
                    e["country"],
                    e["dt"].strftime("%Y-%m-%d %H:%M UTC"),
                ),
            )

    return (False, "")


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
            detail="Aucun canal Telegram configure"
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
                "message": "Aucun envoi Telegram reussi",
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
    title="TradingView Telegram Relay + MT5 + News"
)


# ============================================================
# STARTUP
# ============================================================

@app.on_event("startup")
async def startup():

    try:
        init_db()
    except Exception:
        log.exception("Impossible d'initialiser PostgreSQL")

    try:
        await refresh_news_calendar()
    except Exception:
        log.exception("Impossible de charger le calendrier news")


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
        "mt5_api": bool(MT5_API_KEY),
        "news_filter": NEWS_FILTER_ENABLED,
        "news_events_loaded": len(_news_events),
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
        f"Canaux configures: {len(TELEGRAM_CHAT_IDS)}\n"
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
# ETAT DU FILTRE NEWS (debug / verification)
# ============================================================

@app.get("/news")
async def news(request: Request):

    check_mt5_api(request)

    await maybe_refresh_news()

    now = datetime.now(timezone.utc)
    blocked, reason = news_blackout(now)

    upcoming = [
        {
            "title": e["title"],
            "country": e["country"],
            "impact": e["impact"],
            "when_utc": e["dt"].strftime("%Y-%m-%d %H:%M"),
        }
        for e in sorted(_news_events, key=lambda x: x["dt"])
        if e["dt"] >= now - timedelta(minutes=NEWS_AFTER_MIN)
    ][:20]

    return {
        "ok": True,
        "enabled": NEWS_FILTER_ENABLED,
        "blocked_now": blocked,
        "reason": reason,
        "window_before_min": NEWS_BEFORE_MIN,
        "window_after_min": NEWS_AFTER_MIN,
        "currencies": NEWS_CURRENCIES,
        "impacts": NEWS_IMPACTS,
        "events_loaded": len(_news_events),
        "last_fetch_utc": (
            _news_fetched_at.strftime("%Y-%m-%d %H:%M")
            if _news_fetched_at else None
        ),
        "upcoming": upcoming,
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
            detail="Le JSON doit etre un objet"
        )

    # --------------------------------------------------------
    # Verification secret TradingView
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
        "Signal enregistre : %s | %s %s",
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
    # REPONSE
    # --------------------------------------------------------

    return {

        "ok": True,

        "signal_id": signal_id,

        "database_id": db_id,

        "status": "NEW",

        "telegram": telegram_result
    }


# ============================================================
# MT5 : PROCHAIN SIGNAL   (avec FILTRE NEWS)
# ============================================================

@app.get("/next-signal")
async def next_signal(
    request: Request
):

    check_mt5_api(request)

    # Rafraichit le calendrier si perime (au plus ~1x/heure)
    await maybe_refresh_news()

    # Symbole demande par l'EA (ex: /next-signal?symbol=GOLD)
    # Chaque robot ne recoit QUE les signaux de son symbole.
    want_symbol = clean(request.query_params.get("symbol"))

    conn = get_db()

    try:

        with conn.cursor(
            cursor_factory=RealDictCursor
        ) as cur:

            # ------------------------------------------------
            # Signal NEW (ou CLAIMED depuis > 5 min),
            # filtre par symbole si l'EA en precise un.
            # ------------------------------------------------

            if want_symbol:

                cur.execute(
                    """
                    SELECT *
                    FROM signals

                    WHERE
                        (
                            status = 'NEW'
                            OR
                            (
                                status = 'CLAIMED'
                                AND claimed_at <
                                    NOW() - INTERVAL '5 minutes'
                            )
                        )

                        AND UPPER(symbol) = UPPER(%s)

                    ORDER BY created_at ASC

                    LIMIT 1

                    FOR UPDATE SKIP LOCKED
                    """,
                    (want_symbol,)
                )

            else:

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
            # FILTRE NEWS : ne pas trader autour d'une annonce
            # ------------------------------------------------

            blocked, reason = news_blackout(
                datetime.now(timezone.utc)
            )

            if blocked:

                cur.execute(
                    """
                    UPDATE signals

                    SET
                        status = 'REJECTED',
                        rejected_at = NOW(),
                        error_message = %s

                    WHERE signal_id = %s
                    """,

                    (
                        ("news: " + reason)[:500],
                        signal["signal_id"],
                    )
                )

                conn.commit()

                log.info(
                    "News blackout -> signal %s rejete (%s)",
                    signal["signal_id"],
                    reason
                )

                return {
                    "ok": True,
                    "signal": None,
                    "news_blocked": True,
                    "reason": reason
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
# MT5 : SIGNAL EXECUTE
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
            detail="Signal introuvable ou deja traite"
        )

    log.info(
        "Signal execute : %s",
        signal_id
    )

    return {
        "ok": True,
        "signal_id": signal_id,
        "status": "EXECUTED"
    }


# ============================================================
# MT5 : SIGNAL REJETE
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
            detail="Signal introuvable ou deja traite"
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
