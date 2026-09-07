import os, asyncio, json, time, math, urllib.parse, urllib.request
from contextlib import asynccontextmanager
from datetime import datetime
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import websockets

# ============================================================
# DXY x GOLD — PAPER STRATEGY ENGINE / DIAGNOSTIC BUILD
# ============================================================
# PAPER ONLY. No broker and no real orders.

API_KEY = os.getenv("TWELVE_DATA_API_KEY", "").strip()
START_BALANCE = float(os.getenv("START_BALANCE", "10000"))
RISK_PCT = float(os.getenv("RISK_PCT", "0.01"))
MAX_POSITION_NOTIONAL = float(os.getenv("MAX_POSITION_NOTIONAL", "25000"))
EXIT_REVERSAL_PCT = float(os.getenv("EXIT_REVERSAL_PERCENT", "0.30")) / 100.0
IMPULSE_BARS = int(os.getenv("IMPULSE_BARS", "10"))
MIN_IMPULSE_ATR = float(os.getenv("MIN_DXY_IMPULSE_ATR", "2.0"))
MAX_GOLD_MOVE_ATR = float(os.getenv("MAX_GOLD_MOVE_ATR", "0.75"))
RETRACE_PCT = float(os.getenv("DXY_RETRACE_PERCENT", "50")) / 100.0
MAX_SETUP_AGE = int(os.getenv("MAX_SETUP_AGE_BARS", "30"))
CONFIRMATIONS_NEEDED = int(os.getenv("CONFIRMATIONS_NEEDED", "2"))
USE_CORRELATION_FILTER = os.getenv("USE_CORRELATION_FILTER", "false").lower() == "true"
MIN_CORRELATION = float(os.getenv("MIN_CORRELATION", "-0.20"))
EMA_FAST = int(os.getenv("EMA_FAST", "20"))
EMA_SLOW = int(os.getenv("EMA_SLOW", "50"))
RSI_LEN = int(os.getenv("RSI_LEN", "14"))
ATR_LEN = int(os.getenv("ATR_LEN", "14"))
HISTORY_OUTPUTSIZE = int(os.getenv("HISTORY_OUTPUTSIZE", "100"))
REST_DELAY_SECONDS = float(os.getenv("REST_DELAY_SECONDS", "7.6"))

GOLD_SYMBOL = "XAU/USD"
FX_SYMBOLS = ["EUR/USD", "USD/JPY", "GBP/USD", "USD/CAD", "USD/SEK", "USD/CHF"]
ALL_SYMBOLS = [GOLD_SYMBOL] + FX_SYMBOLS

# If an old TD_SYMBOLS contains DXY/UUP, ignore it and use exactly what the
# strategy needs. Otherwise honor an explicit complete list.
raw_symbols = [x.strip() for x in os.getenv("TD_SYMBOLS", "").split(",") if x.strip()]
if not raw_symbols or any(x.upper() in {"DXY", "UUP"} for x in raw_symbols):
    WS_SYMBOLS = ALL_SYMBOLS
else:
    WS_SYMBOLS = raw_symbols

DXY_COEFF = {
    "EUR/USD": -0.576,
    "USD/JPY": 0.136,
    "GBP/USD": -0.119,
    "USD/CAD": 0.091,
    "USD/SEK": 0.042,
    "USD/CHF": 0.036,
}
DXY_BASE = 50.14348112
TIMEFRAMES = [
    ("3m", "3min", "Scalper", 3),
    ("15m", "15min", "Momentum", 15),
    ("30m", "30min", "Intraday", 30),
    ("1H", "1h", "Swing", 60),
    ("4H", "4h", "Macro", 240),
]


def now_ts():
    return time.time()


def add_event(msg):
    line = str(msg)[:500]
    events.insert(0, {"ts": time.strftime("%H:%M:%S"), "msg": line})
    del events[100:]
    print(f"[DXY-GOLD] {line}", flush=True)


def parse_time(value):
    try:
        if isinstance(value, (int, float)):
            return float(value)
        s = str(value).replace("Z", "+00:00")
        return datetime.fromisoformat(s).timestamp()
    except Exception:
        return now_ts()


def calculate_dxy(prices):
    if not all(s in prices and prices[s] > 0 for s in FX_SYMBOLS):
        return None
    value = DXY_BASE
    try:
        for symbol, exponent in DXY_COEFF.items():
            value *= prices[symbol] ** exponent
        return float(value)
    except Exception:
        return None


def sma(values, n):
    return sum(values[-n:]) / n if len(values) >= n else None


def ema(values, n):
    if len(values) < n:
        return None
    k = 2 / (n + 1)
    e = sum(values[:n]) / n
    for x in values[n:]:
        e = x * k + e * (1 - k)
    return e


def atr(bars, n=14):
    if len(bars) < n + 1:
        return None
    trs = []
    for i in range(1, len(bars)):
        b, p = bars[i], bars[i - 1]
        trs.append(max(b["h"] - b["l"], abs(b["h"] - p["c"]), abs(b["l"] - p["c"])))
    return sum(trs[-n:]) / n if len(trs) >= n else None


def rsi(values, n=14):
    if len(values) < n + 1:
        return None
    gains, losses = [], []
    for i in range(len(values) - n, len(values)):
        d = values[i] - values[i - 1]
        gains.append(max(d, 0))
        losses.append(max(-d, 0))
    avg_gain = sum(gains) / n
    avg_loss = sum(losses) / n
    if avg_loss == 0:
        return 100.0
    return 100 - 100 / (1 + avg_gain / avg_loss)


def corr_returns(a, b, n=50):
    if len(a) < n + 1 or len(b) < n + 1:
        return None
    ar = [(a[i] / a[i - 1] - 1) for i in range(len(a) - n, len(a))]
    br = [(b[i] / b[i - 1] - 1) for i in range(len(b) - n, len(b))]
    ma, mb = sum(ar) / n, sum(br) / n
    num = sum((x - ma) * (y - mb) for x, y in zip(ar, br))
    da = sum((x - ma) ** 2 for x in ar)
    db = sum((y - mb) ** 2 for y in br)
    return num / math.sqrt(da * db) if da and db else None


def rest_json(url):
    request = urllib.request.Request(
        url, headers={"User-Agent": "DXY-Gold-Paper-Traders/diagnostic"}
    )
    with urllib.request.urlopen(request, timeout=20) as response:
        return json.loads(response.read().decode())


def fetch_series(symbol, interval, outputsize):
    query = urllib.parse.urlencode(
        {
            "symbol": symbol,
            "interval": interval,
            "outputsize": outputsize,
            "apikey": API_KEY,
            "format": "JSON",
        }
    )
    data = rest_json("https://api.twelvedata.com/time_series?" + query)
    if data.get("status") == "error" or "values" not in data:
        raise RuntimeError(data.get("message", "Twelve Data history error"))
    return list(reversed([
        {
            "t": parse_time(x["datetime"]),
            "o": float(x["open"]),
            "h": float(x["high"]),
            "l": float(x["low"]),
            "c": float(x["close"]),
        }
        for x in data["values"]
    ]))


def update_bar(bars, ts, price, minutes):
    bucket = int(ts // (minutes * 60)) * (minutes * 60)
    if not bars or bars[-1]["t"] != bucket:
        bars.append({"t": bucket, "o": price, "h": price, "l": price, "c": price})
    else:
        b = bars[-1]
        b["h"] = max(b["h"], price)
        b["l"] = min(b["l"], price)
        b["c"] = price
    if len(bars) > 300:
        del bars[:-300]


agents = {
    tf: {
        "name": name,
        "timeframe": tf,
        "balance": START_BALANCE,
        "equity": START_BALANCE,
        "pnl": 0.0,
        "position": None,
        "trades": 0,
        "wins": 0,
        "losses": 0,
        "last_signal": None,
        "setup": None,
        "bars": 0,
        "correlation": None,
    }
    for tf, _, name, _ in TIMEFRAMES
}

market = {
    "dxy": None,
    "gold": None,
    "updated": None,
    "connected": False,
    "dxy_source": "ICE DXY reconstructed from 6 FX components",
    "components": {},
    "correlation": None,
    "feed_status": "starting",
    "subscription_status": None,
    "subscriptions": {s: {"requested": s in WS_SYMBOLS, "received": False, "last_price": None, "last_update": None} for s in ALL_SYMBOLS},
    "history_status": {tf: {"gold_bars": 0, "dxy_bars": 0, "status": "waiting"} for tf, _, _, _ in TIMEFRAMES},
}

events = []
histories = {tf: {"gold": [], "dxy": []} for tf, _, _, _ in TIMEFRAMES}


def signal_for(tf):
    a = agents[tf]
    gold = histories[tf]["gold"]
    dxy = histories[tf]["dxy"]
    minimum = max(60, IMPULSE_BARS + ATR_LEN + 5)
    if len(gold) < minimum or len(dxy) < minimum:
        return None

    gc = [x["c"] for x in gold]
    dc = [x["c"] for x in dxy]
    gold_atr = atr(gold, ATR_LEN)
    dxy_atr = atr(dxy, ATR_LEN)
    if not gold_atr or not dxy_atr:
        return None

    start = -IMPULSE_BARS - 1
    old_d = dc[start]
    new_d = dc[-1]
    impulse = new_d - old_d
    impulse_atr = abs(impulse) / dxy_atr
    gold_move = abs(gc[-1] - gc[start]) / gold_atr

    if impulse_atr < MIN_IMPULSE_ATR or gold_move > MAX_GOLD_MOVE_ATR:
        return None

    dxy_direction = 1 if impulse > 0 else -1
    expected_gold = -dxy_direction
    window = dc[start:]
    high, low = max(window), min(window)

    if dxy_direction > 0:
        denominator = high - old_d
        retrace = (high - dc[-1]) / denominator if denominator > 0 else 0
    else:
        denominator = old_d - low
        retrace = (dc[-1] - low) / denominator if denominator > 0 else 0

    if retrace < RETRACE_PCT:
        return None

    ef, es, rv = ema(gc, EMA_FAST), ema(gc, EMA_SLOW), rsi(gc, RSI_LEN)
    confirmations = 0
    reasons = []

    if expected_gold > 0 and ef is not None and es is not None and ef >= es:
        confirmations += 1; reasons.append("EMA bullish")
    if expected_gold < 0 and ef is not None and es is not None and ef <= es:
        confirmations += 1; reasons.append("EMA bearish")
    if rv is not None and ((expected_gold > 0 and rv >= 50) or (expected_gold < 0 and rv <= 50)):
        confirmations += 1; reasons.append("RSI aligned")
    if expected_gold > 0 and gc[-1] >= gold[-1]["o"]:
        confirmations += 1; reasons.append("bull candle")
    if expected_gold < 0 and gc[-1] <= gold[-1]["o"]:
        confirmations += 1; reasons.append("bear candle")

    correlation = corr_returns(dc, gc, 50)
    a["correlation"] = correlation
    if USE_CORRELATION_FILTER and (correlation is None or correlation > MIN_CORRELATION):
        return None
    if confirmations < CONFIRMATIONS_NEEDED:
        return None

    return {
        "side": "LONG" if expected_gold > 0 else "SHORT",
        "price": gc[-1],
        "dxy": dc[-1],
        "impulse_atr": round(impulse_atr, 2),
        "gold_move_atr": round(gold_move, 2),
        "retrace": round(retrace * 100, 1),
        "confirmations": confirmations,
        "reasons": reasons,
        "atr": gold_atr,
        "correlation": correlation,
    }


def open_trade(tf, signal):
    a = agents[tf]
    if a["position"] is not None:
        return
    risk_cash = max(a["balance"], 0) * RISK_PCT
    stop_distance = max(signal["atr"] * 1.5, 0.5)
    qty = min(risk_cash / stop_distance, MAX_POSITION_NOTIONAL / signal["price"])
    if qty <= 0:
        return

    a["position"] = {
        "side": signal["side"],
        "entry": signal["price"],
        "qty": qty,
        "entry_dxy": signal["dxy"],
        "stop": signal["price"] - stop_distance if signal["side"] == "LONG" else signal["price"] + stop_distance,
        "best_dxy": signal["dxy"],
        "opened": now_ts(),
        "risk": risk_cash,
    }
    a["last_signal"] = signal
    a["trades"] += 1
    add_event(f"{a['name']} {tf}: PAPER {signal['side']} XAU/USD @ {signal['price']:.2f} | DXY retrace {signal['retrace']:.1f}%")


def manage_trade(tf):
    a = agents[tf]
    p = a["position"]
    if not p or market["gold"] is None or market["dxy"] is None:
        return

    gold = market["gold"]
    dxy = market["dxy"]
    side = p["side"]
    p["best_dxy"] = min(p["best_dxy"], dxy) if side == "LONG" else max(p["best_dxy"], dxy)

    if side == "LONG":
        unrealized = (gold - p["entry"]) * p["qty"]
        stop_hit = gold <= p["stop"]
        reversal_hit = dxy >= p["best_dxy"] * (1 + EXIT_REVERSAL_PCT)
    else:
        unrealized = (p["entry"] - gold) * p["qty"]
        stop_hit = gold >= p["stop"]
        reversal_hit = dxy <= p["best_dxy"] * (1 - EXIT_REVERSAL_PCT)

    a["equity"] = a["balance"] + unrealized
    a["pnl"] = a["equity"] - START_BALANCE

    reason = "ATR stop" if stop_hit else ("DXY reversal" if reversal_hit else None)
    if reason:
        a["balance"] += unrealized
        a["equity"] = a["balance"]
        a["pnl"] = a["balance"] - START_BALANCE
        if unrealized >= 0:
            a["wins"] += 1
        else:
            a["losses"] += 1
        add_event(f"{a['name']} {tf}: EXIT {side} @ {gold:.2f} | {reason} | P/L {unrealized:+.2f}")
        a["position"] = None


def run_strategy():
    if market["gold"] is None or market["dxy"] is None:
        return
    for tf, _, _, _ in TIMEFRAMES:
        manage_trade(tf)
        if agents[tf]["position"] is None:
            sig = signal_for(tf)
            if sig:
                open_trade(tf, sig)
        agents[tf]["bars"] = len(histories[tf]["gold"])


async def load_history():
    if not API_KEY:
        add_event("ERROR: TWELVE_DATA_API_KEY is missing.")
        market["history_status"] = {tf: {"gold_bars": 0, "dxy_bars": 0, "status": "missing API key"} for tf, _, _, _ in TIMEFRAMES}
        return

    add_event(f"History loader started. {len(ALL_SYMBOLS)} symbols x {len(TIMEFRAMES)} timeframes. REST delay={REST_DELAY_SECONDS}s.")

    # Sequential requests are intentional: they reduce the chance of hitting the
    # user's Twelve Data per-minute credit limit.
    for tf, interval, _, _ in TIMEFRAMES:
        try:
            market["history_status"][tf]["status"] = "loading"
            gold = await asyncio.to_thread(fetch_series, GOLD_SYMBOL, interval, HISTORY_OUTPUTSIZE)
            histories[tf]["gold"] = gold
            add_event(f"History {tf}: Gold {len(gold)} bars loaded.")
            await asyncio.sleep(REST_DELAY_SECONDS)

            component_maps = []
            for symbol in FX_SYMBOLS:
                try:
                    values = await asyncio.to_thread(fetch_series, symbol, interval, HISTORY_OUTPUTSIZE)
                    component_maps.append((symbol, {x["t"]: x["c"] for x in values}))
                    add_event(f"History {tf}: {symbol} {len(values)} bars loaded.")
                except Exception as exc:
                    add_event(f"History {tf}: {symbol} FAILED: {type(exc).__name__}: {str(exc)[:150]}")
                    component_maps = []
                    break
                await asyncio.sleep(REST_DELAY_SECONDS)

            if len(component_maps) == len(FX_SYMBOLS):
                common = set(component_maps[0][1])
                for _, mapping in component_maps[1:]:
                    common &= set(mapping)
                dxy_bars = []
                for ts in sorted(common):
                    prices = {symbol: mapping[ts] for symbol, mapping in component_maps}
                    value = calculate_dxy(prices)
                    if value:
                        dxy_bars.append({"t": ts, "o": value, "h": value, "l": value, "c": value})
                histories[tf]["dxy"] = dxy_bars[-300:]
                market["history_status"][tf] = {"gold_bars": len(gold), "dxy_bars": len(dxy_bars), "status": "ready"}
                add_event(f"History {tf}: DXY reconstructed with {len(dxy_bars)} common bars.")
            else:
                market["history_status"][tf]["status"] = "DXY components incomplete"

        except Exception as exc:
            market["history_status"][tf]["status"] = "failed"
            add_event(f"History {tf} FAILED: {type(exc).__name__}: {str(exc)[:180]}")

    add_event("History loader finished.")


async def ws_engine():
    if not API_KEY:
        market["feed_status"] = "missing API key"
        return

    url = f"wss://ws.twelvedata.com/v1/quotes/price?apikey={API_KEY}"
    symbols = WS_SYMBOLS
    add_event(f"WebSocket target symbols ({len(symbols)}): {','.join(symbols)}")

    while True:
        try:
            market["feed_status"] = "connecting"
            async with websockets.connect(url, ping_interval=20, ping_timeout=20, close_timeout=10) as ws:
                await ws.send(json.dumps({"action": "subscribe", "params": {"symbols": symbols}}))
                market["connected"] = True
                market["feed_status"] = "connected / waiting for prices"
                add_event("Connected to Twelve Data WebSocket.")

                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                    except Exception:
                        continue

                    event = msg.get("event")
                    if event == "subscribe-status":
                        market["subscription_status"] = msg
                        add_event("SUBSCRIBE STATUS: " + json.dumps(msg, separators=(",", ":"))[:700])
                        continue
                    if event == "error":
                        add_event("TWELVE DATA ERROR: " + json.dumps(msg, separators=(",", ":"))[:700])
                        continue
                    if event == "heartbeat":
                        continue
                    if event != "price":
                        # Keep diagnostics for unexpected events.
                        if event:
                            add_event("WS EVENT: " + json.dumps(msg, separators=(",", ":"))[:500])
                        continue

                    symbol_raw = str(msg.get("symbol", "")).upper()
                    matched = next((s for s in ALL_SYMBOLS if s.upper() == symbol_raw), None)
                    if not matched:
                        continue
                    try:
                        price = float(msg.get("price"))
                    except Exception:
                        continue
                    if price <= 0:
                        continue

                    ts = now_ts()
                    market["updated"] = ts
                    market["subscriptions"][matched]["received"] = True
                    market["subscriptions"][matched]["last_price"] = price
                    market["subscriptions"][matched]["last_update"] = ts

                    if matched == GOLD_SYMBOL:
                        market["gold"] = price
                        for tf, _, _, minutes in TIMEFRAMES:
                            update_bar(histories[tf]["gold"], ts, price, minutes)
                    else:
                        market["components"][matched] = price
                        dxy = calculate_dxy(market["components"])
                        if dxy is not None:
                            market["dxy"] = dxy
                            market["feed_status"] = "live / DXY calculated"
                            for tf, _, _, minutes in TIMEFRAMES:
                                update_bar(histories[tf]["dxy"], ts, dxy, minutes)

                    if market["gold"] is not None and market["dxy"] is not None:
                        c = corr_returns(
                            [x["c"] for x in histories["1H"]["dxy"]],
                            [x["c"] for x in histories["1H"]["gold"]],
                            50,
                        )
                        market["correlation"] = c
                        run_strategy()

        except Exception as exc:
            market["connected"] = False
            market["feed_status"] = "disconnected / retrying"
            add_event(f"WebSocket ERROR: {type(exc).__name__}: {str(exc)[:180]}")
            await asyncio.sleep(5)


@asynccontextmanager
async def lifespan(app):
    history_task = asyncio.create_task(load_history())
    ws_task = asyncio.create_task(ws_engine())
    yield
    history_task.cancel()
    ws_task.cancel()


app = FastAPI(title="DXY Gold AI Paper Traders", lifespan=lifespan)
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
        "message": "DXY Gold paper-trading strategy backend is online.",
        "dxy_source": market["dxy_source"],
        "feed_status": market["feed_status"],
    }


@app.get("/health")
def health():
    return {"ok": True, "connected": market["connected"], "market": market}


@app.get("/state")
def state():
    return {
        "mode": "PAPER_ONLY",
        "strategy": {
            "impulse_bars": IMPULSE_BARS,
            "min_dxy_impulse_atr": MIN_IMPULSE_ATR,
            "max_gold_move_atr": MAX_GOLD_MOVE_ATR,
            "dxy_retrace_percent": RETRACE_PCT * 100,
            "exit_reversal_percent": EXIT_REVERSAL_PCT * 100,
            "confirmations_needed": CONFIRMATIONS_NEEDED,
            "risk_pct": RISK_PCT * 100,
            "correlation_filter": USE_CORRELATION_FILTER,
            "max_setup_age_bars": MAX_SETUP_AGE,
        },
        "market": market,
        "agents": list(agents.values()),
        "events": events[:30],
    }


@app.post("/paper/reset")
def reset():
    for a in agents.values():
        a.update(
            {
                "balance": START_BALANCE,
                "equity": START_BALANCE,
                "pnl": 0.0,
                "position": None,
                "trades": 0,
                "wins": 0,
                "losses": 0,
                "last_signal": None,
                "setup": None,
            }
        )
    add_event("All paper accounts reset.")
    return {"ok": True, "start_balance": START_BALANCE}