import os, asyncio, json, time, math
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import websockets

API_KEY = os.getenv("TWELVE_DATA_API_KEY", "").strip()
START_BALANCE = float(os.getenv("START_BALANCE", "10000"))

# Twelve Data does not expose the ICE DXY as the literal symbol "DXY"
# on every plan. We therefore reconstruct the official DXY formula from
# its six currency components, which are normal FX symbols.
FX_SYMBOLS = [
    "EUR/USD", "USD/JPY", "GBP/USD", "USD/CAD", "USD/SEK", "USD/CHF"
]
GOLD_SYMBOL = "XAU/USD"
TD_SYMBOLS = os.getenv("TD_SYMBOLS", ",".join([GOLD_SYMBOL] + FX_SYMBOLS))

# ICE DXY formula coefficients.
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
        "name": name,
        "timeframe": tf,
        "balance": START_BALANCE,
        "pnl": 0.0,
        "position": None,
        "last_signal": None,
    }
    for name, tf in [
        ("Scalper", "3m"),
        ("Momentum", "15m"),
        ("Intraday", "30m"),
        ("Swing", "1H"),
        ("Macro", "4H"),
    ]
}

market = {
    "dxy": None,
    "gold": None,
    "updated": None,
    "connected": False,
    "dxy_source": "ICE DXY reconstructed from 6 FX components",
    "components": {},
}
events = []
running = True


def add_event(msg):
    events.insert(0, {"ts": time.strftime("%H:%M:%S"), "msg": msg})
    del events[100:]


def calculate_dxy(components):
    if not all(s in components and components[s] > 0 for s in FX_SYMBOLS):
        return None
    value = DXY_BASE
    for symbol, exponent in DXY_COEFF.items():
        value *= components[symbol] ** exponent
    return value


async def engine():
    global running
    if not API_KEY:
        add_event("TWELVE_DATA_API_KEY is missing. Add it in Railway Variables.")
        return

    url = f"wss://ws.twelvedata.com/v1/quotes/price?apikey={API_KEY}"
    subscribe_symbols = TD_SYMBOLS
    add_event(f"Subscribing to: {subscribe_symbols}")

    while running:
        try:
            async with websockets.connect(
                url, ping_interval=20, ping_timeout=20, close_timeout=10
            ) as ws:
                await ws.send(json.dumps({
                    "action": "subscribe",
                    "params": {"symbols": subscribe_symbols},
                }))
                market["connected"] = True
                add_event("Connected to Twelve Data. Building DXY from FX components.")

                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                    except Exception:
                        continue

                    # Twelve Data can send subscribe/error messages as well as price events.
                    if msg.get("event") == "subscribe-status":
                        add_event(f"Subscription status: {msg.get('status', 'unknown')}")
                        continue
                    if msg.get("event") == "error":
                        add_event(f"Twelve Data error: {msg.get('message', 'unknown')}")
                        continue

                    if msg.get("event") != "price":
                        continue

                    symbol = str(msg.get("symbol", "")).upper()
                    try:
                        price = float(msg.get("price"))
                    except (TypeError, ValueError):
                        continue
                    if price <= 0:
                        continue

                    # Normalize symbol case.
                    matched = next((s for s in [GOLD_SYMBOL] + FX_SYMBOLS if s.upper() == symbol), None)
                    if not matched:
                        continue

                    if matched == GOLD_SYMBOL:
                        market["gold"] = price
                    else:
                        market["components"][matched] = price
                        dxy = calculate_dxy(market["components"])
                        if dxy is not None:
                            market["dxy"] = dxy

                    market["updated"] = time.time()

        except Exception as e:
            market["connected"] = False
            add_event(f"Twelve Data connection error: {type(e).__name__}; retrying.")
            await asyncio.sleep(5)


@asynccontextmanager
async def lifespan(app):
    task = asyncio.create_task(engine())
    yield
    task.cancel()


app = FastAPI(title="DXY Gold Paper Traders", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
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
