import os
import json
import sqlite3
import logging
from datetime import datetime, timezone
from typing import Optional

from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("dxy-gold-paper")

TV_WEBHOOK_SECRET = os.environ.get("TV_WEBHOOK_SECRET", "")
START_BALANCE = float(os.environ.get("START_BALANCE", "10000"))
RISK_PCT = float(os.environ.get("RISK_PCT", "1.0"))
DB_PATH = os.environ.get("DB_PATH", "/data/paper.db")

VALID_TIMEFRAMES = {"15m", "30m", "1H", "4H"}
CONTRACT_SIZE_OZ_PER_LOT = 100.0  # standard XAUUSD lot

app = FastAPI(title="DXY Gold Paper Trading Engine")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)


def get_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True) if os.path.dirname(DB_PATH) else None
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_db()
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS account (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            balance REAL NOT NULL
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS agents (
            timeframe TEXT PRIMARY KEY,
            status TEXT NOT NULL DEFAULT 'flat',
            side TEXT,
            entry_price REAL,
            entry_dxy REAL,
            stop_price REAL,
            size_oz REAL,
            opened_at TEXT,
            current_gold REAL,
            current_dxy REAL,
            last_signal TEXT,
            last_update TEXT
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timeframe TEXT,
            event TEXT,
            side TEXT,
            reason TEXT,
            gold_price REAL,
            dxy_price REAL,
            pnl REAL,
            balance_after REAL,
            raw TEXT,
            created_at TEXT
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS processed_keys (
            dedup_key TEXT PRIMARY KEY,
            created_at TEXT
        )
    """)
    c.execute("SELECT COUNT(*) as n FROM account")
    if c.fetchone()["n"] == 0:
        c.execute("INSERT INTO account (id, balance) VALUES (1, ?)", (START_BALANCE,))
    for tf in VALID_TIMEFRAMES:
        c.execute("INSERT OR IGNORE INTO agents (timeframe, status) VALUES (?, 'flat')", (tf,))
    conn.commit()
    conn.close()


init_db()


class WebhookPayload(BaseModel):
    secret: str
    source: Optional[str] = None
    event: str
    timeframe: str
    symbol: Optional[str] = None
    dxy_symbol: Optional[str] = None
    gold_price: float
    dxy_price: float
    gold_atr: Optional[float] = None
    dxy_impulse_atr: Optional[float] = None
    gold_move_atr: Optional[float] = None
    confirmations: Optional[str] = None
    side: Optional[str] = None
    reason: Optional[str] = None
    tv_position: Optional[str] = None
    bar_time: str


def now_iso():
    return datetime.now(timezone.utc).isoformat()


@app.get("/")
def root():
    return {"service": "dxy-gold-paper-trading-engine", "status": "ok", "real_trading": False}


@app.get("/health")
def health():
    return {"status": "ok", "time": now_iso()}


@app.get("/state")
def get_state():
    conn = get_db()
    c = conn.cursor()
    balance = c.execute("SELECT balance FROM account WHERE id = 1").fetchone()["balance"]

    agents = []
    unrealized_total = 0.0
    last_gold, last_dxy = None, None
    for row in c.execute("SELECT * FROM agents ORDER BY timeframe"):
        row = dict(row)
        unrealized = None
        if row["status"] == "open" and row["current_gold"] is not None and row["entry_price"] is not None:
            direction = 1 if row["side"] == "long" else -1
            unrealized = (row["current_gold"] - row["entry_price"]) * direction * (row["size_oz"] or 0)
            unrealized_total += unrealized
        if row["current_gold"] is not None:
            last_gold = row["current_gold"]
        if row["current_dxy"] is not None:
            last_dxy = row["current_dxy"]
        agents.append({
            "timeframe": row["timeframe"],
            "status": row["status"],
            "side": row["side"],
            "entry_price": row["entry_price"],
            "current_price": row["current_gold"],
            "stop": row["stop_price"],
            "size_oz": row["size_oz"],
            "unrealized_pnl": unrealized,
            "last_signal": row["last_signal"],
            "last_update": row["last_update"],
        })

    events = [dict(r) for r in c.execute(
        "SELECT * FROM events ORDER BY id DESC LIMIT 50"
    )]
    conn.close()

    return {
        "status": "ok",
        "real_trading": False,
        "balance": balance,
        "equity": balance + unrealized_total,
        "dxy_price": last_dxy,
        "gold_price": last_gold,
        "agents": agents,
        "recent_events": events,
    }


@app.post("/paper/reset")
def paper_reset():
    conn = get_db()
    c = conn.cursor()
    c.execute("UPDATE account SET balance = ? WHERE id = 1", (START_BALANCE,))
    c.execute("DELETE FROM agents")
    for tf in VALID_TIMEFRAMES:
        c.execute("INSERT INTO agents (timeframe, status) VALUES (?, 'flat')", (tf,))
    c.execute("DELETE FROM events")
    c.execute("DELETE FROM processed_keys")
    conn.commit()
    conn.close()
    log.info("Paper account reset to %s", START_BALANCE)
    return {"status": "ok", "balance": START_BALANCE}


def log_event(c, timeframe, event, side, reason, gold_price, dxy_price, pnl, balance_after, raw):
    c.execute(
        """INSERT INTO events
           (timeframe, event, side, reason, gold_price, dxy_price, pnl, balance_after, raw, created_at)
           VALUES (?,?,?,?,?,?,?,?,?,?)""",
        (timeframe, event, side, reason, gold_price, dxy_price, pnl, balance_after, raw, now_iso()),
    )


@app.post("/webhook/tradingview")
async def tradingview_webhook(request: Request):
    body = await request.json()
    try:
        payload = WebhookPayload(**body)
    except Exception as e:
        log.warning("Bad webhook payload: %s", e)
        raise HTTPException(status_code=400, detail=f"Invalid payload: {e}")

    if not TV_WEBHOOK_SECRET or payload.secret != TV_WEBHOOK_SECRET:
        log.warning("Webhook secret mismatch for timeframe=%s", payload.timeframe)
        raise HTTPException(status_code=401, detail="Invalid webhook secret")

    if payload.timeframe not in VALID_TIMEFRAMES:
        raise HTTPException(status_code=400, detail=f"Unknown timeframe {payload.timeframe}")

    dedup_key = f"{payload.timeframe}:{payload.event}:{payload.bar_time}:{payload.side or ''}"
    conn = get_db()
    c = conn.cursor()
    try:
        c.execute("INSERT INTO processed_keys (dedup_key, created_at) VALUES (?, ?)", (dedup_key, now_iso()))
    except sqlite3.IntegrityError:
        conn.close()
        log.info("Duplicate webhook ignored: %s", dedup_key)
        return {"status": "ok", "duplicate": True}

    agent = dict(c.execute("SELECT * FROM agents WHERE timeframe = ?", (payload.timeframe,)).fetchone())
    balance = c.execute("SELECT balance FROM account WHERE id = 1").fetchone()["balance"]

    if payload.event == "bar":
        c.execute(
            "UPDATE agents SET current_gold=?, current_dxy=?, last_update=? WHERE timeframe=?",
            (payload.gold_price, payload.dxy_price, now_iso(), payload.timeframe),
        )
        log_event(c, payload.timeframe, "bar", None, None, payload.gold_price, payload.dxy_price, None, balance, json.dumps(body))

    elif payload.event == "entry":
        if agent["status"] == "open":
            log.warning("Entry received but agent %s already open, ignoring", payload.timeframe)
        else:
            stop_distance = None
            if payload.gold_atr:
                stop_distance = payload.gold_atr * 1.5
            risk_amount = balance * (RISK_PCT / 100.0)
            size_oz = (risk_amount / stop_distance) if stop_distance and stop_distance > 0 else 0.0
            stop_price = (
                payload.gold_price - stop_distance if payload.side == "long"
                else payload.gold_price + stop_distance
            ) if stop_distance else None

            c.execute(
                """UPDATE agents SET status='open', side=?, entry_price=?, entry_dxy=?, stop_price=?,
                   size_oz=?, opened_at=?, current_gold=?, current_dxy=?, last_signal=?, last_update=?
                   WHERE timeframe=?""",
                (payload.side, payload.gold_price, payload.dxy_price, stop_price, size_oz,
                 payload.bar_time, payload.gold_price, payload.dxy_price,
                 f"entry_{payload.side}", now_iso(), payload.timeframe),
            )
            log_event(c, payload.timeframe, "entry", payload.side, payload.reason,
                      payload.gold_price, payload.dxy_price, None, balance, json.dumps(body))

    elif payload.event == "exit":
        if agent["status"] != "open":
            log.warning("Exit received but agent %s not open, ignoring", payload.timeframe)
        else:
            direction = 1 if agent["side"] == "long" else -1
            pnl = (payload.gold_price - agent["entry_price"]) * direction * (agent["size_oz"] or 0)
            new_balance = balance + pnl
            c.execute("UPDATE account SET balance=? WHERE id=1", (new_balance,))
            c.execute(
                """UPDATE agents SET status='flat', side=NULL, entry_price=NULL, entry_dxy=NULL,
                   stop_price=NULL, size_oz=NULL, opened_at=NULL, current_gold=?, current_dxy=?,
                   last_signal=?, last_update=? WHERE timeframe=?""",
                (payload.gold_price, payload.dxy_price, f"exit_{payload.reason}", now_iso(), payload.timeframe),
            )
            log_event(c, payload.timeframe, "exit", agent["side"], payload.reason,
                      payload.gold_price, payload.dxy_price, pnl, new_balance, json.dumps(body))
    else:
        conn.close()
        raise HTTPException(status_code=400, detail=f"Unknown event type {payload.event}")

    conn.commit()
    conn.close()
    return {"status": "ok"}
