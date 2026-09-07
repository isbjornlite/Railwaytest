import os, asyncio, json, time, math
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import websockets

API_KEY = os.getenv("TWELVE_DATA_API_KEY", "").strip()
START_BALANCE = float(os.getenv("START_BALANCE", "10000"))

GOLD_SYMBOL = "XAU/USD"
UUP_SYMBOL = "UUP"
FX_SYMBOLS = ["EUR/USD","USD/JPY","GBP/USD","USD/CAD","USD/SEK","USD/CHF"]

# If all six FX pairs stream, build the ICE-style DXY.
DXY_COEFF = {
    "EUR/USD": -0.576,
    "USD/JPY": 0.136,
    "GBP/USD": -0.119,
    "USD/CAD": 0.091,
    "USD/SEK": 0.042,
    "USD/CHF": 0.036,
}
DXY_BASE = 50.14348112

# UUP is Twelve Data's commonly used DXY ETF proxy. We keep its raw value
# separately and use it as a fallback for dollar-strength direction.
TD_SYMBOLS = os.getenv("TD_SYMBOLS", "XAU/USD,UUP")

agents = {
    tf: {
        "name": name, "timeframe": tf, "balance": START_BALANCE,
        "pnl": 0.0, "position": None, "last_signal": None,
    }
    for name, tf in [
        ("Scalper","3m"), ("Momentum","15m"), ("Intraday","30m"),
        ("Swing","1H"), ("Macro","4H"),
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
    # UUP is an ETF proxy, not the ICE index. This scale is only for
    # a familiar DXY-like display; strategy direction uses UUP returns.
    return uup * 3.58 if uup and uup > 0 else None

async def subscribe(ws, symbols):
    await ws.send(json.dumps({
        "action": "subscribe",
        "params": {"symbols": ",".join(symbols)}
    }))

async def engine():
    global running
    if not API_KEY:
        add_event("TWELVE_DATA_API_KEY is missing.")
        return

    url = f"wss://ws.twelvedata.com/v1/quotes/price?apikey={API_KEY}"

    while running:
        try:
            async with websockets.connect(
                url, ping_interval=20, ping_timeout=20, close_timeout=10
            ) as ws:
                # Subscribe to a small reliable set first. UUP gives a fallback
                # dollar-strength stream even if one of the FX pairs is absent.
                await subscribe(ws, [GOLD_SYMBOL, UUP_SYMBOL])
                add_event(f"Subscribed to: {GOLD_SYMBOL}, {UUP_SYMBOL}")

                # Try the six FX components separately; an unavailable symbol
                # should not prevent gold/UUP from streaming.
                try:
                    await subscribe(ws, FX_SYMBOLS)
                    add_event("Requested six FX components for reconstructed DXY.")
                except Exception:
                    add_event("FX component subscription failed; using UUP proxy.")

                market["connected"] = True

                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                    except Exception:
                        continue

                    event = msg.get("event")
                    if event in ("subscribe-status","error"):
                        text = msg.get("message") or msg.get("status") or "unknown"
                        add_event(f"Twelve Data {event}: {text}")
                        continue
                    if event != "price":
                        continue

                    symbol = str(msg.get("symbol","")).upper()
                    try:
                        price = float(msg.get("price"))
                    except (TypeError, ValueError):
                        continue
                    if price <= 0:
                        continue

                    if symbol == GOLD_SYMBOL.upper():
                        market["gold"] = price
                    elif symbol == UUP_SYMBOL.upper():
                        market["uup"] = price
                    else:
                        matched = next((s for s in FX_SYMBOLS if s.upper() == symbol), None)
                        if matched:
                            market["components"][matched] = price

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
        "ok": True, "mode": "PAPER_ONLY",
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
        "events": events[:30],
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
