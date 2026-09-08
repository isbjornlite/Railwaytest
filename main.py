import os
import time
from collections import defaultdict, deque
from datetime import datetime, timezone
from threading import Lock

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware

START_BALANCE = float(os.getenv("START_BALANCE", "10000"))
WEBHOOK_SECRET = os.getenv("TV_WEBHOOK_SECRET", "CHANGE_ME")
RISK_PCT = float(os.getenv("RISK_PCT", "1.0"))
MAX_POSITIONS = int(os.getenv("MAX_POSITIONS", "4"))

TIMEFRAMES = ["15m", "30m", "1H", "4H"]
AGENT_NAMES = {"15m": "Momentum", "30m": "Intraday", "1H": "Swing", "4H": "Macro"}

app = FastAPI(title="DXY Gold TradingView Paper Engine", version="1.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=False, allow_methods=["*"], allow_headers=["*"])

lock = Lock()
events = deque(maxlen=150)
agents = {}
for tf in TIMEFRAMES:
    agents[tf] = {
        "timeframe": tf, "name": AGENT_NAMES[tf], "balance": START_BALANCE,
        "equity": START_BALANCE, "pnl": 0.0, "position": "FLAT",
        "qty": 0.0, "entry": None, "stop": None, "last_price": None,
        "last_dxy": None, "last_update": None, "trades": 0, "wins": 0, "losses": 0,
        "last_signal": None
    }

market = {
    "gold": None, "dxy": None, "correlation": None, "source": "TradingView",
    "last_update": None, "feed_status": "WAITING_FOR_TRADINGVIEW"
}

def add_event(msg, payload=None):
    item = {"time": datetime.now(timezone.utc).isoformat(), "message": msg, "payload": payload}
    with lock:
        events.appendleft(item)
    print(f"[TV-PAPER] {msg}", flush=True)

def reset_all():
    with lock:
        for tf in TIMEFRAMES:
            a = agents[tf]
            a.update({
                "balance": START_BALANCE, "equity": START_BALANCE, "pnl": 0.0,
                "position": "FLAT", "qty": 0.0, "entry": None, "stop": None,
                "last_price": None, "last_dxy": None, "last_update": None,
                "trades": 0, "wins": 0, "losses": 0, "last_signal": None
            })

@app.get("/")
def root():
    return {"ok": True, "service": "DXY Gold TradingView Paper Engine", "timeframes": TIMEFRAMES, "paper_only": True}

@app.get("/health")
def health():
    return {"ok": True, "source": "TradingView", "feed_status": market["feed_status"]}

@app.get("/state")
def state():
    with lock:
        return {
            "ok": True,
            "paper_only": True,
            "engine": {"status": "RUNNING", "source": "TradingView"},
            "market": dict(market),
            "agents": [dict(agents[tf]) for tf in TIMEFRAMES],
            "events": list(events)
        }

@app.post("/paper/reset")
def paper_reset():
    reset_all()
    add_event("All paper accounts reset.")
    return {"ok": True}

def update_mark_to_market(a, price):
    if a["position"] == "LONG":
        unreal = (price - a["entry"]) * a["qty"]
    elif a["position"] == "SHORT":
        unreal = (a["entry"] - price) * a["qty"]
    else:
        unreal = 0.0
    a["equity"] = a["balance"] + unreal
    a["pnl"] = a["equity"] - START_BALANCE

def close_position(a, price, reason):
    if a["position"] == "LONG":
        realized = (price - a["entry"]) * a["qty"]
    else:
        realized = (a["entry"] - price) * a["qty"]
    a["balance"] += realized
    a["equity"] = a["balance"]
    a["pnl"] = a["balance"] - START_BALANCE
    a["trades"] += 1
    if realized >= 0:
        a["wins"] += 1
    else:
        a["losses"] += 1
    old = a["position"]
    a.update({"position": "FLAT", "qty": 0.0, "entry": None, "stop": None, "last_signal": f"EXIT {old} / {reason}"})
    add_event(f"{a['timeframe']} {old} EXIT {reason} @ {price:.4f}, realized {realized:+.2f}")

def open_position(a, side, price, atr, reason):
    if a["position"] != "FLAT":
        return False
    if sum(1 for x in agents.values() if x["position"] != "FLAT") >= MAX_POSITIONS:
        return False
    stop_dist = max(float(atr or 0) * 1.5, price * 0.001)
    risk_cash = max(a["equity"], 0) * (RISK_PCT / 100.0)
    qty = risk_cash / stop_dist if stop_dist > 0 else 0
    if qty <= 0:
        return False
    stop = price - stop_dist if side == "LONG" else price + stop_dist
    a.update({"position": side, "qty": qty, "entry": price, "stop": stop, "last_signal": f"ENTRY {side} / {reason}"})
    add_event(f"{a['timeframe']} {side} ENTRY @ {price:.4f}, qty {qty:.4f}, stop {stop:.4f}")
    return True

@app.post("/webhook/tradingview")
async def tradingview_webhook(request: Request):
    try:
        data = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Webhook body must be JSON")

    if data.get("secret") != WEBHOOK_SECRET:
        raise HTTPException(status_code=401, detail="Invalid webhook secret")

    tf = str(data.get("timeframe", ""))
    if tf not in agents:
        raise HTTPException(status_code=400, detail=f"Unsupported timeframe: {tf}")

    event = str(data.get("event", "bar"))
    price = float(data.get("gold_price") or 0)
    dxy = float(data.get("dxy_price") or 0)
    atr = float(data.get("gold_atr") or 0)
    side = str(data.get("side", "FLAT"))

    with lock:
        a = agents[tf]
        if price <= 0:
            raise HTTPException(status_code=400, detail="Missing gold_price")
        market["gold"] = price
        market["dxy"] = dxy or market["dxy"]
        market["last_update"] = datetime.now(timezone.utc).isoformat()
        market["feed_status"] = "LIVE_TRADINGVIEW"
        a["last_price"] = price
        a["last_dxy"] = dxy or a["last_dxy"]
        a["last_update"] = market["last_update"]

        # Server-side protective stop as an additional safety layer.
        if a["position"] == "LONG" and a["stop"] and price <= a["stop"]:
            close_position(a, price, "server_stop")
        elif a["position"] == "SHORT" and a["stop"] and price >= a["stop"]:
            close_position(a, price, "server_stop")

        if event == "entry":
            if side in ("LONG", "SHORT"):
                open_position(a, side, price, atr, str(data.get("reason", "TradingView signal")))
        elif event == "exit":
            if a["position"] != "FLAT":
                close_position(a, price, str(data.get("reason", "TradingView exit")))

        update_mark_to_market(a, price)

    return {"ok": True, "received": event, "timeframe": tf}

@app.post("/webhook/test")
async def webhook_test(request: Request):
    data = await request.json()
    if data.get("secret") != WEBHOOK_SECRET:
        raise HTTPException(status_code=401, detail="Invalid webhook secret")
    return {"ok": True, "message": "Webhook authentication works."}
