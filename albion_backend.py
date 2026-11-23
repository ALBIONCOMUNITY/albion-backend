#!/usr/bin/env python3
"""
ALBION Backend v2 - Real market analysis API + daily allowance tracking

Reads live prices from Yahoo Finance, computes technical indicators
(RSI, MACD, Moving Averages) across 4 timeframes, and returns
unanimous signals as JSON.

v2 changes:
  * Per-device daily allowance (default 3 analyses/day, London date),
    enforced server-side and logged to SQLite (device, date, market, op, ts).
  * POST /api/unlock {device} -> marks a device as unlimited (called when
    the member confirms payment inside the app).
  * GET /api/usage?device=... -> live counter for the UI.
  * Parallel Yahoo fetching (8 workers) so a full scan is fast again.
  * ThreadingHTTPServer for concurrent requests.
  * Fixed news feeds (Reuters public RSS is dead).

Endpoints:
  GET  /api/health
  GET  /api/markets
  GET  /api/analyse?market=forex&op=daytrade&device=dev_xxx
  GET  /api/news?market=forex
  GET  /api/usage?device=dev_xxx
  POST /api/unlock   body: {"device":"dev_xxx"}

Run:  python3 albion_backend_v2.py
Env:  PORT (default 8000) · ALBION_DB (default albion.db)
      ALBION_DAILY_LIMIT (default 3)

Member binding (Telegram member ID):
  * Optional member=ID on /api/analyse, /api/usage and in the /api/unlock body.
    A known member shares ONE daily quota across all devices; anonymous
    visitors keep per-device keying. Premium = premium device OR premium member.
  GET  /api/member?member=ID  -> binding, premium state and usage of one member
  GET  /api/export.csv        -> one row per bound member (paste into Google Sheets)

Telegram bot (webhook, same process):
  POST /tg/webhook      Telegram updates (header X-Telegram-Bot-Api-Secret-Token = TG_WEBHOOK_SECRET)
                        * chat_join_request on the community group -> approve + DM the personal app link
                        * /start in private -> DM the personal app link
  GET  /go?m=ID         logs the click, then 302 to APP_URL?member=ID
  GET  /api/funnel.csv  one row per Telegram member: campaign, joined, DM sent, clicks, analyses
  Env: TELEGRAM_BOT_TOKEN, TG_WEBHOOK_SECRET, TG_COMMUNITY_CHAT (default -1003897608428),
       APP_URL (default https://invite.albioncomunity.co.uk/), PUBLIC_URL (this backend's https URL),
       ALBION_ADMIN_KEY (optional: if set, /api/member and the CSVs need ?key=...)

DEPLOY: on Railway attach a Volume mounted at /data and set ALBION_DB=/data/albion.db so the SQLite file survives restarts; without it the allowance counter resets on each deploy.
"""
import json, os, re, time, threading, sqlite3, datetime, csv, io
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
import urllib.request, urllib.error
from concurrent.futures import ThreadPoolExecutor

# ── CONFIG ──────────────────────────────────────────────────────
PORT = int(os.environ.get("PORT", 8000))
CACHE_TTL = 300  # 5 minutes
DAILY_LIMIT = int(os.environ.get("ALBION_DAILY_LIMIT", "3"))
DB_PATH = os.environ.get("ALBION_DB", "albion.db")
DEVICE_RE = re.compile(r"^[A-Za-z0-9_\-]{1,64}$")
MEMBER_RE = re.compile(r"^[A-Za-z0-9_-]{1,64}$")
TG_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
TG_SECRET = os.environ.get("TG_WEBHOOK_SECRET", "").strip()
TG_API = os.environ.get("TG_API_BASE", "https://api.telegram.org").rstrip("/")
TG_COMMUNITY = os.environ.get("TG_COMMUNITY_CHAT", "-1003897608428").strip()
APP_URL = os.environ.get("APP_URL", "https://invite.albioncomunity.co.uk/").strip()
PUBLIC_URL = os.environ.get("PUBLIC_URL", "").strip().rstrip("/")
ADMIN_KEY = os.environ.get("ALBION_ADMIN_KEY", "").strip()

# Markets and their instruments (identical to v1)
MARKETS = {
    "forex": {
        "label": "Forex", "unit": "pairs", "count": 16,
        "instruments": [
            {"symbol":"GBPCHF=X","name":"GBP/CHF","type":"forex"},
            {"symbol":"EURCHF=X","name":"EUR/CHF","type":"forex"},
            {"symbol":"USDJPY=X","name":"USD/JPY","type":"forex"},
            {"symbol":"EURUSD=X","name":"EUR/USD","type":"forex"},
            {"symbol":"GBPUSD=X","name":"GBP/USD","type":"forex"},
            {"symbol":"AUDUSD=X","name":"AUD/USD","type":"forex"},
            {"symbol":"USDCAD=X","name":"USD/CAD","type":"forex"},
            {"symbol":"NZDUSD=X","name":"NZD/USD","type":"forex"},
            {"symbol":"EURGBP=X","name":"EUR/GBP","type":"forex"},
            {"symbol":"AUDJPY=X","name":"AUD/JPY","type":"forex"},
            {"symbol":"EURAUD=X","name":"EUR/AUD","type":"forex"},
            {"symbol":"GBPAUD=X","name":"GBP/AUD","type":"forex"},
            {"symbol":"USDCHF=X","name":"USD/CHF","type":"forex"},
            {"symbol":"EURJPY=X","name":"EUR/JPY","type":"forex"},
            {"symbol":"GBPJPY=X","name":"GBP/JPY","type":"forex"},
            {"symbol":"CADJPY=X","name":"CAD/JPY","type":"forex"},
        ]
    },
    "commodities": {
        "label": "Commodities", "unit": "instruments", "count": 12,
        "instruments": [
            {"symbol":"GC=F","name":"Gold (XAU/USD)","type":"commodity"},
            {"symbol":"SI=F","name":"Silver (XAG/USD)","type":"commodity"},
            {"symbol":"HG=F","name":"Copper (HGLD)","type":"commodity"},
            {"symbol":"PL=F","name":"Platinum (XPT/USD)","type":"commodity"},
            {"symbol":"PA=F","name":"Palladium (XPD/USD)","type":"commodity"},
            {"symbol":"ZC=F","name":"Corn","type":"commodity"},
            {"symbol":"ZW=F","name":"Wheat","type":"commodity"},
            {"symbol":"ZS=F","name":"Soybeans","type":"commodity"},
            {"symbol":"KC=F","name":"Coffee C","type":"commodity"},
            {"symbol":"CT=F","name":"Cotton No. 2","type":"commodity"},
            {"symbol":"LB=F","name":"Live Cattle","type":"commodity"},
            {"symbol":"LE=F","name":"Lean Hogs","type":"commodity"},
        ]
    },
    "indices": {
        "label": "Indices", "unit": "indices", "count": 14,
        "instruments": [
            {"symbol":"^NDX","name":"Nasdaq 100","type":"index"},
            {"symbol":"^GSPC","name":"S&P 500","type":"index"},
            {"symbol":"^DJI","name":"Dow Jones","type":"index"},
            {"symbol":"^FTSE","name":"FTSE 100","type":"index"},
            {"symbol":"^GDAXI","name":"DAX 40","type":"index"},
            {"symbol":"^FCHI","name":"CAC 40","type":"index"},
            {"symbol":"^STOXX50E","name":"Euro Stoxx 50","type":"index"},
            {"symbol":"^N225","name":"Nikkei 225","type":"index"},
            {"symbol":"^HSI","name":"Hang Seng","type":"index"},
            {"symbol":"^AXJO","name":"ASX 200","type":"index"},
            {"symbol":"^TSX","name":"TSX Composite","type":"index"},
            {"symbol":"^IXIC","name":"Nasdaq Composite","type":"index"},
            {"symbol":"^RUT","name":"Russell 2000","type":"index"},
            {"symbol":"^VIX","name":"VIX","type":"index"},
        ]
    },
    "futures": {
        "label": "Futures", "unit": "contracts", "count": 10,
        "instruments": [
            {"symbol":"CL=F","name":"Crude Oil WTI","type":"future"},
            {"symbol":"BZ=F","name":"Brent Crude","type":"future"},
            {"symbol":"NG=F","name":"Natural Gas","type":"future"},
            {"symbol":"RB=F","name":"RBOB Gasoline","type":"future"},
            {"symbol":"ZB=F","name":"US 10Y Bond","type":"future"},
            {"symbol":"ZN=F","name":"US 5Y Bond","type":"future"},
            {"symbol":"ZT=F","name":"US 2Y Bond","type":"future"},
            {"symbol":"6E=F","name":"Euro FX Futures","type":"future"},
            {"symbol":"6B=F","name":"British Pound Futures","type":"future"},
            {"symbol":"JY=F","name":"Japanese Yen Futures","type":"future"},
        ]
    },
    "stocks": {
        "label": "Stocks", "unit": "tickers", "count": 20,
        "instruments": [
            {"symbol":"AAPL","name":"Apple (AAPL)","type":"stock"},
            {"symbol":"MSFT","name":"Microsoft (MSFT)","type":"stock"},
            {"symbol":"GOOGL","name":"Alphabet (GOOGL)","type":"stock"},
            {"symbol":"AMZN","name":"Amazon (AMZN)","type":"stock"},
            {"symbol":"NVDA","name":"Nvidia (NVDA)","type":"stock"},
            {"symbol":"META","name":"Meta (META)","type":"stock"},
            {"symbol":"TSLA","name":"Tesla (TSLA)","type":"stock"},
            {"symbol":"BRK-B","name":"Berkshire Hathaway","type":"stock"},
            {"symbol":"JPM","name":"JPMorgan (JPM)","type":"stock"},
            {"symbol":"V","name":"Visa (V)","type":"stock"},
            {"symbol":"UNH","name":"UnitedHealth (UNH)","type":"stock"},
            {"symbol":"XOM","name":"Exxon Mobil (XOM)","type":"stock"},
            {"symbol":"LLY","name":"Eli Lilly (LLY)","type":"stock"},
            {"symbol":"AVGO","name":"Broadcom (AVGO)","type":"stock"},
            {"symbol":"MA","name":"Mastercard (MA)","type":"stock"},
            {"symbol":"COST","name":"Costco (COST)","type":"stock"},
            {"symbol":"HD","name":"Home Depot (HD)","type":"stock"},
            {"symbol":"PG","name":"Procter & Gamble","type":"stock"},
            {"symbol":"RR.L","name":"Rolls-Royce (RR.L)","type":"stock"},
            {"symbol":"SHEL.L","name":"Shell (SHEL.L)","type":"stock"},
        ]
    },
}

# Operation type -> timeframes to check (identical to v1)
OP_TIMEFRAMES = {
    "daytrade": [
        ("5m",  "5 Min",  "1d"),
        ("15m", "15 Min", "5d"),
        ("1h",  "1 Hour", "1mo"),
        ("4h",  "4 Hour", "3mo"),
    ],
    "buyhold": [
        ("1d",  "Daily",     "3mo"),
        ("1wk", "Weekly",    "1y"),
        ("1mo", "Monthly",   "2y"),
        ("3mo", "Quarterly", "5y"),
    ],
    "binary": [
        ("1m",  "1 Min",  "5d"),
        ("5m",  "5 Min",  "1mo"),
        ("15m", "15 Min", "3mo"),
        ("30m", "30 Min", "6mo"),
    ],
}

# ── DATA FETCHING ───────────────────────────────────────────────
_cache = {}
_cache_lock = threading.Lock()

def fetch_yahoo(symbol, interval, range_):
    """Fetch OHLC data from Yahoo Finance chart API."""
    key = f"{symbol}:{interval}:{range_}"
    with _cache_lock:
        if key in _cache:
            ts, data = _cache[key]
            if time.time() - ts < CACHE_TTL:
                return data
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{urllib.request.quote(symbol)}?range={range_}&interval={interval}"
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=20) as r:
        raw = json.loads(r.read())
    result = raw["chart"]["result"][0]
    quote = result["indicators"]["quote"][0]
    closes = [c for c in quote["close"] if c is not None]
    highs  = [h for h in quote["high"]  if h is not None]
    lows   = [l for l in quote["low"]   if l is not None]
    price  = result["meta"].get("regularMarketPrice")
    prev   = result["meta"].get("chartPreviousClose")
    data = {"closes": closes, "highs": highs, "lows": lows, "price": price, "prev": prev}
    with _cache_lock:
        _cache[key] = (time.time(), data)
    return data

# ── TECHNICAL INDICATORS (identical to v1) ──────────────────────
def sma(values, period):
    if len(values) < period:
        return None
    return sum(values[-period:]) / period

def ema(values, period):
    if len(values) < period:
        return None
    k = 2 / (period + 1)
    e = sum(values[:period]) / period
    for v in values[period:]:
        e = v * k + e * (1 - k)
    return e

def rsi(closes, period=14):
    if len(closes) < period + 1:
        return None
    gains, losses = [], []
    for i in range(1, len(closes)):
        d = closes[i] - closes[i-1]
        gains.append(max(d, 0))
        losses.append(max(-d, 0))
    avg_g = sum(gains[:period]) / period
    avg_l = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        avg_g = (avg_g * (period-1) + gains[i]) / period
        avg_l = (avg_l * (period-1) + losses[i]) / period
    if avg_l == 0:
        return 100.0
    rs = avg_g / avg_l
    return 100 - (100 / (1 + rs))

def macd(closes, fast=12, slow=26, signal=9):
    if len(closes) < slow + signal:
        return None, None
    def ema_series(vals, p):
        if len(vals) < p: return []
        k = 2/(p+1)
        e = sum(vals[:p])/p
        out = [e]
        for v in vals[p:]:
            e = v*k + e*(1-k)
            out.append(e)
        return out
    ef = ema_series(closes, fast)
    es = ema_series(closes, slow)
    if len(ef) < len(es) or len(es) < signal:
        return None, None
    offset = len(ef) - len(es)
    ef_aligned = ef[offset:]
    macd_line = [a - b for a, b in zip(ef_aligned, es)]
    sig_line = ema_series(macd_line, signal)
    if not sig_line:
        return None, None
    return macd_line[-1], sig_line[-1]

def analyse_timeframe(closes, highs, lows):
    """Return a signal dict for one timeframe."""
    if len(closes) < 20:
        return {"signal": "neutral", "confidence": 0, "details": {}}

    price = closes[-1]
    ma20 = sma(closes, 20)
    ma50 = sma(closes, 50) if len(closes) >= 50 else sma(closes, min(50, len(closes)))
    r = rsi(closes, 14)
    m, s = macd(closes)

    score = 0
    details = {}

    if ma20 and price > ma20:
        score += 1
        details["ma20"] = "above"
    elif ma20:
        score -= 1
        details["ma20"] = "below"

    if ma20 and ma50:
        if ma20 > ma50:
            score += 1
            details["ma_cross"] = "bullish"
        else:
            score -= 1
            details["ma_cross"] = "bearish"

    if r is not None:
        details["rsi"] = round(r, 1)
        if r < 30:
            score += 1
        elif r > 70:
            score -= 1
        elif r > 50:
            score += 0.5
        else:
            score -= 0.5

    if m is not None and s is not None:
        details["macd"] = "bullish" if m > s else "bearish"
        if m > s:
            score += 1
        else:
            score -= 1

    if len(closes) >= 6:
        mom = (closes[-1] - closes[-6]) / closes[-6] * 100
        details["momentum_5"] = round(mom, 2)
        if mom > 0.5:
            score += 0.5
        elif mom < -0.5:
            score -= 0.5

    max_score = 5
    ratio = score / max_score
    if ratio >= 0.6:
        sig = "strong_buy"
    elif ratio >= 0.2:
        sig = "buy"
    elif ratio <= -0.6:
        sig = "strong_sell"
    elif ratio <= -0.2:
        sig = "sell"
    else:
        sig = "neutral"

    return {"signal": sig, "score": round(score, 1), "details": details}

# ── ALLOWANCE DB (SQLite) ───────────────────────────────────────
_conn_lock = threading.Lock()

def _db():
    c = sqlite3.connect(DB_PATH, timeout=10)
    c.execute("CREATE TABLE IF NOT EXISTS analyses(id INTEGER PRIMARY KEY AUTOINCREMENT,"
              " device TEXT NOT NULL, d TEXT NOT NULL, market TEXT, op TEXT, ts REAL)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_analyses_dev_date ON analyses(device, d)")
    c.execute("CREATE TABLE IF NOT EXISTS premium_devices(device TEXT PRIMARY KEY, ts REAL)")
    return c

def london_date():
    try:
        from zoneinfo import ZoneInfo
        return datetime.datetime.now(ZoneInfo("Europe/London")).strftime("%Y-%m-%d")
    except Exception:
        return time.strftime("%Y-%m-%d", time.gmtime())

def migrate_db():
    """Idempotent schema migration for member binding; safe to run on every start."""
    with _conn_lock, _db() as c:
        for col in ("member", "user_key"):
            try:
                c.execute(f"ALTER TABLE analyses ADD COLUMN {col} TEXT")
            except sqlite3.OperationalError:
                pass  # column already exists
        c.execute("UPDATE analyses SET user_key = COALESCE(NULLIF(member,''), device) WHERE user_key IS NULL")
        c.execute("CREATE INDEX IF NOT EXISTS idx_analyses_key_date ON analyses(user_key, d)")
        c.execute("CREATE TABLE IF NOT EXISTS member_bindings(member TEXT PRIMARY KEY, device TEXT, ts REAL)")
        c.execute("CREATE TABLE IF NOT EXISTS premium_members(member TEXT PRIMARY KEY, ts REAL)")
        c.execute("CREATE TABLE IF NOT EXISTS tg_members(member TEXT PRIMARY KEY, username TEXT,"
                  " first_name TEXT, campaign TEXT, joined_at REAL, dm_sent INTEGER DEFAULT 0,"
                  " dm_error TEXT, via TEXT)")
        c.execute("CREATE TABLE IF NOT EXISTS app_clicks(id INTEGER PRIMARY KEY AUTOINCREMENT,"
                  " member TEXT NOT NULL, ts REAL)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_app_clicks_member ON app_clicks(member)")

def clean_member(raw):
    raw = (raw or "").strip()
    return raw if MEMBER_RE.match(raw) else ""

def quota_key(member, device):
    return clean_member(member) or clean_device(device)

def count_today(key):
    d = london_date()
    with _conn_lock, _db() as c:
        row = c.execute("SELECT COUNT(*) FROM analyses WHERE user_key=? AND d=?", (key, d)).fetchone()
    return int(row[0])

def is_premium(dev, member=""):
    with _conn_lock, _db() as c:
        if c.execute("SELECT 1 FROM premium_devices WHERE device=?", (dev,)).fetchone():
            return True
        return bool(member) and c.execute(
            "SELECT 1 FROM premium_members WHERE member=?", (member,)).fetchone() is not None

def log_analysis(dev, market, op, member=""):
    now = time.time()
    with _conn_lock, _db() as c:
        c.execute("INSERT INTO analyses(device,d,market,op,ts,member,user_key) VALUES(?,?,?,?,?,?,?)",
                  (dev, london_date(), market, op, now, member or None, member or dev))
        if member:
            c.execute("INSERT INTO member_bindings(member, device, ts) VALUES(?,?,?) "
                      "ON CONFLICT(member) DO UPDATE SET device=excluded.device, ts=excluded.ts",
                      (member, dev, now))

def _iso(ts):
    return datetime.datetime.fromtimestamp(ts, datetime.timezone.utc).isoformat() if ts else None

def member_info(member):
    d = london_date()
    with _conn_lock, _db() as c:
        b = c.execute("SELECT device, ts FROM member_bindings WHERE member=?", (member,)).fetchone()
        today = c.execute("SELECT COUNT(*) FROM analyses WHERE user_key=? AND d=?", (member, d)).fetchone()[0]
        total, last = c.execute("SELECT COUNT(*), MAX(ts) FROM analyses WHERE user_key=?", (member,)).fetchone()
    device = b[0] if b else None
    return {"member": member, "device": device, "bound_at": _iso(b[1]) if b else None,
            "is_premium": is_premium(device or "", member),
            "today_count": int(today), "total_count": int(total), "last_seen": _iso(last)}

def export_csv():
    with _conn_lock, _db() as c:
        members = [r[0] for r in c.execute("SELECT member FROM member_bindings ORDER BY ts DESC")]
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(["member_id", "device", "is_premium", "today_analyses", "total_analyses", "last_seen"])
    for m in members:
        i = member_info(m)
        w.writerow([m, i["device"] or "", "true" if i["is_premium"] else "false",
                    i["today_count"], i["total_count"], i["last_seen"] or ""])
    return buf.getvalue()

# ── TELEGRAM BOT (webhook) ──────────────────────────────────────
def tg(method, **params):
    """Calls the Bot API. The token is never logged."""
    if not TG_TOKEN:
        return {"ok": False, "description": "TELEGRAM_BOT_TOKEN not set"}
    req = urllib.request.Request(f"{TG_API}/bot{TG_TOKEN}/{method}",
                                 data=json.dumps(params).encode(),
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            return json.load(r)
    except urllib.error.HTTPError as e:
        try:
            return json.loads(e.read().decode() or "{}")
        except Exception:
            return {"ok": False, "description": f"HTTP {e.code}"}
    except Exception as e:
        return {"ok": False, "description": type(e).__name__}

def app_link(member):
    """Personal link sent by the bot. Through /go when PUBLIC_URL is set, so the click is logged."""
    if PUBLIC_URL:
        return f"{PUBLIC_URL}/go?m={member}"
    return APP_URL + ("&" if "?" in APP_URL else "?") + "member=" + member

def topic_link(thread_id):
    chat = TG_COMMUNITY[4:] if TG_COMMUNITY.startswith("-100") else TG_COMMUNITY.lstrip("-")
    return f"https://t.me/c/{chat}/{thread_id}"

def _esc(s):
    return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def welcome_text(first_name, returning=False):
    hi = "Welcome back" if returning else "Welcome to ALBION"
    name = f", {_esc(first_name)}" if first_name else ""
    return (f"<b>{hi}{name}</b> 👋\n"
            + ("" if returning else "You're in the community.\n")
            + "\n🆓 <b>Your free analysis app</b>\n"
              "3 analyses a day, linked to your Telegram account, so your allowance "
              "follows you on any device. Use the button below to open it.\n\n"
              "🔹 Start with quick-context (daily, 8am London) and read 🪶 mindset before posting.\n"
              "<i>Reads are conditional; the decision is always yours.</i>")

def send_app_link(chat_id, member, first_name, returning=False):
    return tg("sendMessage", chat_id=chat_id, text=welcome_text(first_name, returning),
              parse_mode="HTML", disable_web_page_preview=True,
              reply_markup={"inline_keyboard": [
                  [{"text": "📊 Open the ALBION app", "url": app_link(member)}],
                  [{"text": "👋 Start here", "url": topic_link(9)}],
              ]})

def record_tg_member(member, user, campaign, via, dm):
    with _conn_lock, _db() as c:
        c.execute("INSERT INTO tg_members(member, username, first_name, campaign, joined_at, dm_sent, dm_error, via)"
                  " VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(member) DO UPDATE SET"
                  " username=excluded.username, first_name=excluded.first_name,"
                  " campaign=COALESCE(tg_members.campaign, excluded.campaign),"
                  " dm_sent=MAX(tg_members.dm_sent, excluded.dm_sent),"
                  " dm_error=CASE WHEN excluded.dm_sent=1 THEN NULL ELSE excluded.dm_error END",
                  (member, user.get("username"), user.get("first_name"), campaign, time.time(),
                   1 if dm.get("ok") else 0, None if dm.get("ok") else str(dm.get("description", ""))[:200], via))

def handle_update(upd):
    jr = upd.get("chat_join_request")
    if jr and str(jr.get("chat", {}).get("id")) == TG_COMMUNITY:
        user = jr.get("from", {})
        member = clean_member(str(user.get("id", "")))
        if not member or user.get("is_bot"):
            return
        tg("approveChatJoinRequest", chat_id=jr["chat"]["id"], user_id=user["id"])
        campaign = (jr.get("invite_link") or {}).get("name") or None
        dm = send_app_link(jr.get("user_chat_id") or user["id"], member, user.get("first_name"))
        record_tg_member(member, user, campaign, "join_request", dm)
        print(f"[tg] join approved member={member} campaign={campaign} dm={dm.get('ok')}", flush=True)
        return
    msg = upd.get("message") or {}
    if msg.get("chat", {}).get("type") == "private" and (msg.get("text") or "").startswith("/start"):
        user = msg.get("from", {})
        member = clean_member(str(user.get("id", "")))
        if not member:
            return
        arg = (msg["text"].split(maxsplit=1) + [""])[1].strip()
        campaign = arg if arg and MEMBER_RE.match(arg) else None
        dm = send_app_link(msg["chat"]["id"], member, user.get("first_name"), returning=True)
        record_tg_member(member, user, campaign, "start", dm)
        print(f"[tg] /start member={member} dm={dm.get('ok')}", flush=True)

def log_click(member):
    with _conn_lock, _db() as c:
        c.execute("INSERT INTO app_clicks(member, ts) VALUES(?,?)", (member, time.time()))

def _cell(v):
    v = v or ""
    return "'" + v if v[:1] in ("=", "+", "-", "@") else v  # no formulas in Sheets

def funnel_csv():
    with _conn_lock, _db() as c:
        rows = c.execute(
            "SELECT t.member, t.username, t.first_name, t.campaign, t.via, t.joined_at, t.dm_sent,"
            " (SELECT COUNT(*) FROM app_clicks k WHERE k.member=t.member),"
            " (SELECT MIN(ts) FROM app_clicks k WHERE k.member=t.member),"
            " (SELECT MAX(ts) FROM app_clicks k WHERE k.member=t.member),"
            " (SELECT COUNT(*) FROM analyses a WHERE a.user_key=t.member),"
            " (SELECT MAX(ts) FROM analyses a WHERE a.user_key=t.member),"
            " EXISTS(SELECT 1 FROM premium_members p WHERE p.member=t.member)"
            " FROM tg_members t ORDER BY t.joined_at DESC").fetchall()
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(["member_id", "username", "first_name", "campaign", "via", "joined_at", "dm_sent",
                "app_clicks", "first_click", "last_click", "total_analyses", "last_analysis", "is_premium"])
    for r in rows:
        w.writerow([r[0], _cell(r[1]), _cell(r[2]), _cell(r[3]), r[4] or "", _iso(r[5]) or "",
                    "true" if r[6] else "false", r[7], _iso(r[8]) or "", _iso(r[9]) or "",
                    r[10], _iso(r[11]) or "", "true" if r[12] else "false"])
    return buf.getvalue()

# ── MAIN ANALYSIS (parallel fetches) ────────────────────────────
def analyse_market(market_key, op_type):
    market = MARKETS[market_key]
    timeframes = OP_TIMEFRAMES[op_type]
    results = []
    errors = []

    with ThreadPoolExecutor(max_workers=8) as ex:
        fut = {}
        for inst in market["instruments"]:
            for interval, label, range_ in timeframes:
                fut[(inst["symbol"], interval)] = ex.submit(fetch_yahoo, inst["symbol"], interval, range_)

        for inst in market["instruments"]:
            sym = inst["symbol"]
            tf_results = []
            ok = True
            for interval, label, range_ in timeframes:
                try:
                    d = fut[(sym, interval)].result(timeout=60)
                    tf_res = analyse_timeframe(d["closes"], d["highs"], d["lows"])
                    tf_res["current_price"] = d.get("price")
                    tf_res["prev_close"] = d.get("prev")
                    tf_results.append({"label": label, **tf_res})
                except Exception as e:
                    errors.append(f"{sym}/{interval}: {e}")
                    ok = False
                    tf_results.append({"label": label, "signal": "error", "score": 0, "details": {}})

            if not ok:
                continue

            signals = [t["signal"] for t in tf_results]
            buy_signals = {"buy", "strong_buy"}
            sell_signals = {"sell", "strong_sell"}

            all_buy = all(s in buy_signals for s in signals)
            all_sell = all(s in sell_signals for s in signals)

            if all_buy:
                overall = "STRONG BUY" if all(s == "strong_buy" for s in signals) else "BUY"
                direction = "buy"
            elif all_sell:
                overall = "STRONG SELL" if all(s == "strong_sell" for s in signals) else "SELL"
                direction = "sell"
            else:
                overall = "MIXED"
                direction = "neutral"

            price = tf_results[0].get("current_price")
            prev = tf_results[0].get("prev_close")
            change_pct = ((price - prev) / prev * 100) if (price and prev) else 0

            results.append({
                "symbol": sym,
                "name": inst["name"],
                "price": round(price, 4) if price else None,
                "change_pct": round(change_pct, 2),
                "overall": overall,
                "direction": direction,
                "timeframes": tf_results,
                "aligned": direction != "neutral",
            })

    results.sort(key=lambda x: (not x["aligned"], -abs(x["timeframes"][0].get("score", 0))))

    return {
        "market": market_key,
        "market_label": market["label"],
        "operation": op_type,
        "timeframes_checked": [t[1] for t in timeframes],
        "total_instruments": market["count"],
        "signals_found": sum(1 for r in results if r["aligned"]),
        "results": results[:10],
        "errors": errors[:5],
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime()),
    }

# ── NEWS (working feeds; Reuters public RSS is dead) ───────────
def get_news(market_key):
    feeds = [
        ("MarketWatch", "https://feeds.content.dowjones.io/public/rss/mw_topstories"),
        ("CNBC", "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=100003114"),
        ("Investing.com", "https://www.investing.com/rss/news.rss"),
    ]
    articles = []
    for source, url in feeds:
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=10) as r:
                content = r.read().decode("utf-8", errors="ignore")
            items = re.findall(r"<item>(.*?)</item>", content, re.DOTALL)
            for item in items[:3]:
                title_m = re.search(r"<title>(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?</title>", item, re.DOTALL)
                link_m = re.search(r"<link>(.*?)</link>", item)
                desc_m = re.search(r"<description>(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?</description>", item, re.DOTALL)
                if title_m:
                    articles.append({
                        "source": source,
                        "title": title_m.group(1).strip()[:120],
                        "url": link_m.group(1).strip() if link_m else "#",
                        "excerpt": re.sub(r"<[^>]+>", "", desc_m.group(1)).strip()[:200] if desc_m else "",
                    })
        except Exception:
            pass
    return articles[:6]

def clean_device(raw):
    raw = (raw or "").strip()
    return raw if DEVICE_RE.match(raw) else "anon"

# ── HTTP HANDLER ────────────────────────────────────────────────
class Handler(BaseHTTPRequestHandler):
    def _json(self, code, data):
        body = json.dumps(data, indent=2).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.end_headers()

    @staticmethod
    def _safe_update(upd):
        try:
            handle_update(upd)
        except Exception as e:
            print(f"[tg] update failed: {type(e).__name__}: {e}", flush=True)

    def _admin_ok(self, params):
        if not ADMIN_KEY:
            return True
        if params.get("key", [""])[0] == ADMIN_KEY:
            return True
        self._json(403, {"error": "forbidden"})
        return False

    def _csv(self, text, filename):
        body = text.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/csv; charset=utf-8")
        self.send_header("Content-Disposition", f'attachment; filename="{filename}"')
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/")
        length = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(length) if length else b"{}"
        try:
            payload = json.loads(raw.decode("utf-8") or "{}")
        except Exception:
            payload = {}

        if path == "/tg/webhook":
            if not TG_SECRET or self.headers.get("X-Telegram-Bot-Api-Secret-Token", "") != TG_SECRET:
                self._json(403, {"error": "forbidden"})
                return
            self._json(200, {"ok": True})  # answer Telegram at once, work in the background
            threading.Thread(target=self._safe_update, args=(payload,), daemon=True).start()
        elif path == "/api/unlock":
            dev = clean_device(str(payload.get("device", "")))
            member = clean_member(str(payload.get("member", "") or ""))
            with _conn_lock, _db() as c:
                c.execute("INSERT OR IGNORE INTO premium_devices(device, ts) VALUES(?,?)", (dev, time.time()))
                if member:
                    c.execute("INSERT OR IGNORE INTO premium_members(member, ts) VALUES(?,?)",
                              (member, time.time()))
            self._json(200, {"status": "ok", "device": dev, "unlimited": True,
                             "member": member or None})
        else:
            self._json(404, {"error": f"Not found: {path}"})

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/")
        params = parse_qs(parsed.query)

        if path == "/api/health":
            self._json(200, {"status": "ok", "service": "ALBION Backend v2",
                             "time": time.strftime("%H:%M:%S UTC", time.gmtime()),
                             "daily_limit": DAILY_LIMIT})

        elif path == "/api/markets":
            self._json(200, {k: {"label": v["label"], "count": v["count"],
                                 "instruments": len(v["instruments"])} for k, v in MARKETS.items()})

        elif path == "/api/usage":
            dev = clean_device(params.get("device", ["anon"])[0])
            member = clean_member(params.get("member", [""])[0])
            cnt = count_today(quota_key(member, dev))
            prem = is_premium(dev, member)
            self._json(200, {"device": dev, "date": london_date(), "limit": DAILY_LIMIT,
                             "today_count": cnt,
                             "remaining": DAILY_LIMIT - cnt if not prem else DAILY_LIMIT,
                             "is_premium": prem})

        elif path == "/api/analyse":
            market = params.get("market", ["forex"])[0]
            op = params.get("op", ["daytrade"])[0]
            if market not in MARKETS:
                self._json(400, {"error": f"Unknown market: {market}. Use: {list(MARKETS.keys())}"})
                return
            if op not in OP_TIMEFRAMES:
                self._json(400, {"error": f"Unknown operation: {op}. Use: {list(OP_TIMEFRAMES.keys())}"})
                return
            dev = clean_device(params.get("device", ["anon"])[0])
            member = clean_member(params.get("member", [""])[0])
            qkey = quota_key(member, dev)

            prem = is_premium(dev, member)
            cnt = count_today(qkey)
            if not prem and cnt >= DAILY_LIMIT:
                self._json(429, {"error": "daily_limit", "limit": DAILY_LIMIT,
                                 "today_count": cnt, "date": london_date()})
                return

            try:
                t0 = time.time()
                result = analyse_market(market, op)
                log_analysis(dev, market, op, member)
                cnt2 = cnt + 1
                result["news"] = get_news(market)
                result["elapsed_s"] = round(time.time() - t0, 1)
                result["today_count"] = cnt2
                result["limit"] = DAILY_LIMIT
                result["remaining"] = max(0, DAILY_LIMIT - cnt2) if not prem else DAILY_LIMIT
                result["is_premium"] = prem
                result["member"] = member or None
                result["quota_key"] = qkey
                self._json(200, result)
            except Exception as e:
                self._json(500, {"error": str(e)})

        elif path == "/api/news":
            market = params.get("market", ["forex"])[0]
            self._json(200, {"news": get_news(market)})

        elif path == "/api/member":
            if not self._admin_ok(params):
                return
            member = clean_member(params.get("member", [""])[0])
            if not member:
                self._json(400, {"error": "missing_or_invalid_member"})
                return
            self._json(200, member_info(member))

        elif path == "/api/export.csv":
            if self._admin_ok(params):
                self._csv(export_csv(), "albion_members.csv")

        elif path == "/api/funnel.csv":
            if self._admin_ok(params):
                self._csv(funnel_csv(), "albion_funnel.csv")

        elif path == "/go":
            member = clean_member(params.get("m", [""])[0])
            if member:
                log_click(member)
                target = APP_URL + ("&" if "?" in APP_URL else "?") + "member=" + member
            else:
                target = APP_URL
            self.send_response(302)
            self.send_header("Location", target)
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", "0")
            self.end_headers()

        else:
            self._json(404, {"error": "Not found. Use /api/health, /api/markets, "
                                      "/api/analyse?market=X&op=Y&device=Z, /api/news, "
                                      "/api/usage?device=Z, or POST /api/unlock"})

    def log_message(self, fmt, *args):
        print(f"[{time.strftime('%H:%M:%S')}] {fmt % args}")

if __name__ == "__main__":
    print(f"ALBION Backend v2 starting on port {PORT}")
    print(f"  Health:  http://localhost:{PORT}/api/health")
    print(f"  Markets: http://localhost:{PORT}/api/markets")
    print(f"  Usage:   http://localhost:{PORT}/api/usage?device=dev_xxx")
    print(f"  Analyse: http://localhost:{PORT}/api/analyse?market=forex&op=daytrade&device=dev_xxx")
    print(f"  Limit:   {DAILY_LIMIT} analyses/day/device (London date) · db: {DB_PATH}")
    migrate_db()
    server = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    server.serve_forever()
