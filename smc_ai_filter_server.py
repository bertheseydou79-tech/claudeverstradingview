"""
SMC AI Filter — pont TradingView -> Claude -> Telegram
=======================================================
Recoit l'alerte JSON de SMC PRO v5.1, verifie tes regles, envoie une carte sur Telegram.

- L'indicateur a DEJA calcule le trade et applique ses filtres (moteur v5.1) AVANT
  d'emettre l'alerte. Le serveur ne re-rejette donc PAS sur HTF/PD/grade : il les
  affiche en contexte. Rejet DUR uniquement : TP1 < 1.5R, score < seuil, donnees
  incompletes. Flags STRICT_HTF / STRICT_PD / STRICT_GRADE pour durcir individuellement.
- Aucune execution d'ordre ici. Cle API Claude sur le serveur, jamais dans TradingView.

Lancer :  uvicorn smc_ai_filter_server:app --host 0.0.0.0 --port 8000
"""

import os
import json
import hmac
import logging

import httpx
from fastapi import FastAPI, Request, BackgroundTasks, HTTPException

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("smc-filter")

# ----------------------------------------------------------------------------
# 1) CONFIG (variables d'environnement — voir .env.example)
# ----------------------------------------------------------------------------
def _clean(v: str) -> str:
    # Supprime espaces, tabulations et sauts de ligne parasites (copier-coller)
    return (v or "").strip().strip('"').strip("'").strip()

WEBHOOK_SECRET     = _clean(os.environ["WEBHOOK_SECRET"])
ANTHROPIC_API_KEY  = _clean(os.environ["ANTHROPIC_API_KEY"])
ANTHROPIC_MODEL    = _clean(os.environ.get("ANTHROPIC_MODEL", "claude-sonnet-5"))
TELEGRAM_BOT_TOKEN = _clean(os.environ["TELEGRAM_BOT_TOKEN"])
TELEGRAM_CHAT_ID   = _clean(os.environ["TELEGRAM_CHAT_ID"])

MIN_SCORE   = float(os.environ.get("MIN_SCORE", "70"))
MIN_TP1_R   = float(os.environ.get("MIN_TP1_R", "1.5"))
STRICT_HTF   = os.environ.get("STRICT_HTF",   "0") == "1"  # HTF hors zone (4H+1H) -> rejet dur
STRICT_PD    = os.environ.get("STRICT_PD",    "0") == "1"  # P/D a contre-sens     -> rejet dur
STRICT_GRADE = os.environ.get("STRICT_GRADE", "0") == "1"  # grade < B             -> rejet dur
GRADES_TRADABLES = {"A+", "A"}
GRADES_ALERTE    = {"B"}

# ----------------------------------------------------------------------------
# 2) MAPPING — aligne sur le JSON reel de ton v5.1 (dir/sym/tf/...)
# ----------------------------------------------------------------------------
FIELD_MAP = {
    "symbol":    ["sym", "symbol", "ticker"],
    "timeframe": ["tf", "timeframe", "interval"],
    "direction": ["dir", "signal", "direction", "sens", "side"],
    "score":     ["score", "scoreLong", "scoreShort"],
    "grade":     ["grade", "categorie"],
    "entry":     ["entry", "entree", "pe", "price"],
    "sl":        ["sl", "stop"],
    "tp1":       ["tp1"],
    "tp2":       ["tp2"],
    "tp3":       ["tp3"],
    "lot":       ["lot", "size", "lots"],
    "zone4h":    ["zone4h", "z4", "htf4h"],
    "zone1h":    ["zone1h", "z1", "htf1h"],
    "pd":        ["pd", "premium_discount", "marketZone", "zone"],
    "sweep":     ["sweep"],
    "choch":     ["choch", "chochLink", "sweepChochLink"],
    "risk_pct":  ["risk_pct", "riskPct"],
}


def pick(payload: dict, key: str):
    for name in FIELD_MAP.get(key, [key]):
        if name in payload and payload[name] not in ("", None):
            return payload[name]
    return None


def to_float(v):
    if v is None:
        return None
    if isinstance(v, bool):
        return None
    try:
        return float(str(v).replace(",", ".").strip())
    except ValueError:
        return None


def norm_dir(v):
    if v is None:
        return None
    s = str(v).upper()
    if s in ("LONG", "BUY", "ACHAT"):
        return "LONG"
    if s in ("SHORT", "SELL", "VENTE"):
        return "SHORT"
    return s


# ----------------------------------------------------------------------------
# 3) CONTROLES DETERMINISTES (arithmetique cote serveur, pas cote IA)
# ----------------------------------------------------------------------------
def run_checks(p: dict) -> dict:
    direction = norm_dir(pick(p, "direction"))
    entry = to_float(pick(p, "entry"))
    sl    = to_float(pick(p, "sl"))
    tp1   = to_float(pick(p, "tp1"))
    score = to_float(pick(p, "score"))
    grade = str(pick(p, "grade") or "").upper().strip()
    z4    = str(pick(p, "zone4h") or "").lower()
    z1    = str(pick(p, "zone1h") or "").lower()
    pd    = str(pick(p, "pd") or "").upper()

    c = {}

    # RR de TP1 (regle firme : >= 1.5R)  -- BLOQUANT
    rr_tp1 = None
    if entry is not None and sl is not None and tp1 is not None and entry != sl:
        rr_tp1 = round(abs(tp1 - entry) / abs(entry - sl), 2)
    c["rr_tp1"] = rr_tp1
    c["tp1_r_ok"] = (rr_tp1 is not None and rr_tp1 >= MIN_TP1_R)

    # Score minimum  -- BLOQUANT
    c["score"] = score
    c["score_ok"] = (score is not None and score >= MIN_SCORE)

    # Donnees minimales presentes  -- BLOQUANT
    c["donnees_completes"] = all(x is not None for x in (direction, entry, sl, tp1, score))

    # --- CONTEXTE (non bloquant sauf flag STRICT_* correspondant) ---
    c["grade"] = grade
    c["grade_tradable"] = grade in GRADES_TRADABLES
    c["grade_alerte"]   = grade in GRADES_ALERTE

    hors4 = ("hors" in z4) or z4 in ("", "none", "no")
    hors1 = ("hors" in z1) or z1 in ("", "none", "no")
    c["htf_ok"] = not (hors4 and hors1)

    if direction == "LONG":
        c["pd_ok"] = ("DISCOUNT" in pd)
    elif direction == "SHORT":
        c["pd_ok"] = ("PREMIUM" in pd)
    else:
        c["pd_ok"] = None
    c["pd"] = pd or "?"

    c["direction"] = direction
    c["strict"] = {"htf": STRICT_HTF, "pd": STRICT_PD, "grade": STRICT_GRADE}

    # Synthese des rejets DURS
    fails = []
    if not c["donnees_completes"]:
        fails.append("donnees_incompletes")
    if not c["tp1_r_ok"]:
        fails.append(f"TP1 < {MIN_TP1_R}R")
    if not c["score_ok"]:
        fails.append(f"score < {MIN_SCORE}")
    if STRICT_HTF and not c["htf_ok"]:
        fails.append("HTF hors zone (4H+1H)")
    if STRICT_PD and c["pd_ok"] is False:
        fails.append("P/D a contre-sens")
    if STRICT_GRADE and not (c["grade_tradable"] or c["grade_alerte"]):
        fails.append("grade < B")
    c["blocking_fails"] = fails
    c["blocking_ok"] = (len(fails) == 0)
    return c


# ----------------------------------------------------------------------------
# 4) FILTRE CLAUDE (verdict + mise en forme ; ne calcule aucun chiffre)
# ----------------------------------------------------------------------------
SYSTEM_PROMPT = """Tu es un FILTRE de validation de trades SMC/ICT pour un compte prop firm.

L'indicateur SMC PRO v5.1 a DEJA calcule le setup (direction, score, entree, SL, TP1-3, lot)
et a DEJA applique ses filtres avant d'emettre ce signal. Tu ne generes PAS de trade et tu
n'inventes AUCUN chiffre. Ton role : rendre un verdict et un message Telegram clair.

On te fournit "controles_deterministes" (calcules cote serveur, fais-leur confiance) :
- blocking_ok / blocking_fails : rejets DURS. Si blocking_ok = false, le verdict DOIT etre REJETE.
- htf_ok, pd_ok, grade : CONTEXTE de qualite. Ne bloquent pas (sauf s'ils sont dans blocking_fails),
  mais tu DOIS les signaler comme AVERTISSEMENT dans les raisons quand ils sont defavorables
  (HTF hors zone, P/D a contre-sens, grade faible).

Verdicts possibles :
- "VALIDE"        : blocking_ok = true ET contexte propre.
- "VALIDE_AVEC_RESERVE" : blocking_ok = true mais au moins un avertissement de contexte.
- "REJETE"        : blocking_ok = false.
- "DONNEES_INCOMPLETES" : donnees manquantes.

Reponds UNIQUEMENT avec un objet JSON, sans texte autour, sans balises Markdown :
{
  "verdict": "...",
  "raisons": ["raison courte", "..."],
  "message_telegram": "message court, texte simple (pas de HTML), pret a envoyer"
}
"""


async def ask_claude(payload: dict, checks: dict) -> dict:
    user_content = json.dumps(
        {"signal": payload, "controles_deterministes": checks},
        ensure_ascii=False, indent=2,
    )
    body = {
        "model": ANTHROPIC_MODEL,
        "max_tokens": 1024,
        "system": SYSTEM_PROMPT,
        "messages": [{"role": "user", "content": user_content}],
    }
    headers = {
        "x-api-key": ANTHROPIC_API_KEY,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json",
    }
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.post("https://api.anthropic.com/v1/messages",
                              json=body, headers=headers)
        r.raise_for_status()
        data = r.json()

    text = "".join(b.get("text", "") for b in data.get("content", [])
                   if b.get("type") == "text")
    clean = text.replace("```json", "").replace("```", "").strip()
    try:
        return json.loads(clean)
    except json.JSONDecodeError:
        return {"verdict": "ERREUR_PARSING",
                "raisons": ["Reponse Claude non-JSON"],
                "message_telegram": text[:800] or "Reponse vide"}


# ----------------------------------------------------------------------------
# 5) TELEGRAM (texte simple -> pas de 400 sur caracteres speciaux)
# ----------------------------------------------------------------------------
async def send_telegram(text: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.post(url, json={
            "chat_id": TELEGRAM_CHAT_ID,
            "text": text,
            "disable_web_page_preview": True,
        })
        if resp.status_code != 200:
            log.error("Telegram error %s : %s", resp.status_code, resp.text)


# ----------------------------------------------------------------------------
# 6) TRAITEMENT (tache de fond -> le webhook repond en < 3 s)
# ----------------------------------------------------------------------------
async def process(payload: dict):
    try:
        checks = run_checks(payload)
        result = await ask_claude(payload, checks)

        verdict = result.get("verdict", "?")
        msg = result.get("message_telegram") or "(pas de message)"
        emoji = {"VALIDE": "🟢", "VALIDE_AVEC_RESERVE": "🟡",
                 "REJETE": "🔴", "DONNEES_INCOMPLETES": "⚪"}.get(verdict, "❔")

        sym = pick(payload, "symbol") or "?"
        tf  = pick(payload, "timeframe") or "?"

        full = (
            f"{emoji} {verdict} — {sym} {tf}\n"
            f"RR TP1: {checks.get('rr_tp1')} | Score: {checks.get('score')} "
            f"| Grade: {checks.get('grade') or '-'} | P/D: {checks.get('pd')}\n"
            f"HTF ok: {checks.get('htf_ok')}\n"
            f"------------\n{msg}"
        )
        await send_telegram(full)
        log.info("Traite : %s %s -> %s", sym, tf, verdict)
    except Exception as e:
        log.exception("Echec traitement")
        try:
            await send_telegram(f"⚠️ Erreur serveur SMC filter : {e}")
        except Exception:
            pass


# ----------------------------------------------------------------------------
# 7) API
# ----------------------------------------------------------------------------
app = FastAPI(title="SMC AI Filter")


@app.get("/")
async def health():
    return {"status": "ok",
            "strict": {"htf": STRICT_HTF, "pd": STRICT_PD, "grade": STRICT_GRADE}}


@app.post("/webhook")
async def webhook(request: Request, background: BackgroundTasks):
    raw = await request.body()
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="JSON invalide")

    secret = str(payload.pop("secret", ""))          # secret dans le CORPS, jamais l'URL
    if not hmac.compare_digest(secret, WEBHOOK_SECRET):
        raise HTTPException(status_code=401, detail="Secret invalide")

    background.add_task(process, payload)             # rend la main tout de suite
    return {"ok": True}
