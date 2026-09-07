import os, asyncio, json, time
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import websockets

API_KEY = os.getenv("TWELVE_DATA_API_KEY", "").strip()
START_BALANCE = float(os.getenv("START_BALANCE", "10000"))

GOLD_SYMBOL = "XAU/USD"
UUP_SYMBOL = "UUP"
FX_SYMBOLS = ["EUR/USD", "USD/JPY", "GBP/USD", "USD/CAD", "USD/SEK", "USD/CHF"]
ALL_SYMBOLS = [GOLD_SYMBOL, UUP_SYMBOL] + FX_SYMBOLS

DXY_COEFF = {
    "EUR/USD": -0.576,
    "USD/JPY": 0.136,
    "GBP/USD": -0.119,
    "USD/CAD": 0.091,
    "USD/SEK": 0.042,
    "USD/CHF": 0.036,
}
DXY_BASE = 50.14348112

agents = {
    tf: {
        "name": name, "timeframe": tf, "balance": START_BALANCE,
        "pnl": 0.0, "position": None, "last_signal": None,
    }
    for name, tf in [
        ("Scalper", "3m"), ("Momentum", "15m"), ("Intraday", "30m"),
        ("Swing", "1H"), ("Macro", "4H"),
    ]
}

market = {
    "dxy": None,
    "gold": None,
    "uup": None,
    "updated": None,
    "connected": False,
    "dxy_source": "Waiting for DXY / UUP proxy",
    "components": {},
    "subscriptions": {},
}
events = []
running = True


def add_event(msg):
    events.insert(0, {"ts": time.strftime("%H:%M:%S"), "msg": msg})
    del events[100:]


def calculate_dxy(c):
    if not all(s in c and c[s] > 0 for s in FX_SYMBOLS):
        return None
    value = DXY_BASE
    for symbol, exponent in DXY_COEFF.items():
        value *= c[symbol] ** exponent
    return value


def uup_to_dxy(uup):
    # UUP is an ETF proxy, not the ICE DXY. This is display-only.
    return uup * 3.58 if uup and uup > 0 else None


def extract_status_details(msg):
    """Pull useful symbol/status fields from Twelve Data status messages without exposing secrets."""
    found = {}

    def walk(obj, path=""):
        if isinstance(obj, dict):
            for k, v in obj.items():
                key = str(k).lower()
                p = f"{path}.{k}" if path else str(k)
                if key in {"symbol", "symbols", "success", "failed", "subscribed", "unsubscribed", "status", "message", "code", "reason"}:
                    found[p] = v
                if isinstance(v, (dict, list)):
                    walk(v, p)
        elif isinstance(obj, list):
            for i, v in enumerate(obj):
                if isinstance(v, (dict, list)):
                    walk(v, f"{path}[{i}]")

    walk(msg)
    return found


def classify_subscription(symbol, status_msg):
    """Best-effort classification. Raw status is also retained in diagnostics."""
    text = json.dumps(status_msg, ensure_ascii=False).lower()
    if symbol.lower() in text:
        bad_words = ["failed", "error", "invalid", "not found", "unauthorized", "denied", "unsupported"]
        if any(w in text for w in bad_words):
            return "failed"
        good_words = ["success", "subscribed", "ok"]
        if any(w in text for w in good_words):
            return "subscribed"
    return "warning"


async def subscribe_one(ws, symbol):
    await ws.send(json.dumps({
        "action": "subscribe",
        "params": {"symbols": symbol}
    }))
    add_event(f"Subscription requested: {symbol}")


async def engine():
    global running
    if not API_KEY:
        add_event("TWELVE_DATA_API_KEY is missing.")
        return

    url = f"wss://ws.twelvedata.com/v1/quotes/price?apikey={API_KEY}"

    while running:
        try:
            # Start a clean diagnostics state on each reconnect.
            market["subscriptions"] = {s: "pending" for s in ALL_SYMBOLS}
            market["components"] = {}
            market["uup"] = None
            market["dxy"] = None
            market["dxy_source"] = "Waiting for DXY / UUP proxy"

            async with websockets.connect(
                url, ping_interval=20, ping_timeout=20, close_timeout=10
            ) as ws:
                market["connected"] = True
                add_event("WebSocket connected to Twelve Data.")

                # Subscribe one symbol at a time so we can see exactly which
                # symbols the current account accepts.
                for symbol in ALL_SYMBOLS:
                    try:
                        await subscribe_one(ws, symbol)
                        # Small pause lets status responses arrive separately.
                        await asyncio.sleep(0.20)
                    except Exception as e:
                        market["subscriptions"][symbol] = "request_error"
                        add_event(f"Subscription request error {symbol}: {type(e).__name__}")

                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                    except Exception:
                        continue

                    event = msg.get("event")
                    if event in ("subscribe-status", "error"):
                        details = extract_status_details(msg)
                        compact = json.dumps(details, ensure_ascii=False, separators=(",", ":"))
                        add_event(f"Twelve Data {event}: {compact[:500]}")

                        # Update per-symbol diagnostics when the message names one.
                        text = json.dumps(msg, ensure_ascii=False).lower()
                        for symbol in ALL_SYMBOLS:
                            if symbol.lower() in text:
                                market["subscriptions"][symbol] = classify_subscription(symbol, msg)
                        continue

                    if event != "price":
                        continue

                    symbol = str(msg.get("symbol", ""))
                    try:
                        price = float(msg.get("price"))
                    except (TypeError, ValueError):
                        continue
                    if price <= 0:
                        continue

                    upper = symbol.upper()
                    if upper == GOLD_SYMBOL.upper():
                        market["gold"] = price
                        market["subscriptions"][GOLD_SYMBOL] = "subscribed"
                    elif upper == UUP_SYMBOL.upper():
                        market["uup"] = price
                        market["subscriptions"][UUP_SYMBOL] = "subscribed"
                    else:
                        matched = next((s for s in FX_SYMBOLS if s.upper() == upper), None)
                        if matched:
                            market["components"][matched] = price
                            market["subscriptions"][matched] = "subscribed"

                    dxy = calculate_dxy(market["components"])
                    if dxy is not None:
                        market["dxy"] = dxy
                        market["dxy_source"] = "Reconstructed from 6 FX components"
                    elif market["uup"] is not None:
                        market["dxy"] = uup_to_dxy(market["uup"])
                        market["dxy_source"] = "UUP ETF proxy (scaled for display)"

                    market["updated"] = time.time()

        except Exception as e:
            market["connected"] = False
            add_event(f"WebSocket error: {type(e).__name__}; retrying in 5s.")
            await asyncio.sleep(5)


@asynccontextmanager
async def lifespan(app):
    task = asyncio.create_task(engine())
    yield
    task.cancel()


app = FastAPI(title="DXY Gold Paper Traders", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_credentials=False,
    allow_methods=["*"], allow_headers=["*"]
)


@app.get("/")
def root():
    return {
        "ok": True,
        "mode": "PAPER_ONLY",
        "message": "DXY Gold paper-trading backend is online.",
        "dxy_source": market["dxy_source"],
    }


@app.get("/health")
def health():
    return {"ok": True, "connected": market["connected"], "market": market}


@app.get("/state")
def state():
    return {
        "mode": "PAPER_ONLY",
        "market": market,
        "agents": list(agents.values()),
        "events": events[:40],
    }


@app.post("/paper/reset")
def reset():
    for a in agents.values():
        a["balance"] = START_BALANCE
        a["pnl"] = 0.0
        a["position"] = None
        a["last_signal"] = None
    add_event("All paper accounts reset.")
    return {"ok": True, "start_balance": START_BALANCE}
