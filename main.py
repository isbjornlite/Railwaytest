import os, asyncio, json, time
from typing import Dict, Any
import requests
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import websockets

app = FastAPI(title="DXY Gold Paper Trading Backend", version="4.0-biquote")

app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True,
                   allow_methods=["*"], allow_headers=["*"])

TD_API_KEY = os.getenv("TWELVE_DATA_API_KEY", "")
TD_WS = f"wss://ws.twelvedata.com/v1/quotes/price?apikey={TD_API_KEY}"
START_BALANCE = float(os.getenv("START_BALANCE", "10000"))

market: Dict[str, Any] = {
    "dxy": None, "gold": None, "updated": None,
    "connected": False, "dxy_source": "BiQuote",
    "biquote_connected": False, "twelve_data_connected": False,
}
agents = [
    {"name":"Scalper","timeframe":"3m","balance":START_BALANCE,"pnl":0.0,"position":None,"last_signal":None},
    {"name":"Momentum","timeframe":"15m","balance":START_BALANCE,"pnl":0.0,"position":None,"last_signal":None},
    {"name":"Intraday","timeframe":"30m","balance":START_BALANCE,"pnl":0.0,"position":None,"last_signal":None},
    {"name":"Swing","timeframe":"1H","balance":START_BALANCE,"pnl":0.0,"position":None,"last_signal":None},
    {"name":"Macro","timeframe":"4H","balance":START_BALANCE,"pnl":0.0,"position":None,"last_signal":None},
]
events=[]

def log(msg):
    events.insert(0, {"ts":time.strftime("%H:%M:%S"), "msg":msg})
    del events[80:]

def reset_agents():
    for a in agents:
        a.update(balance=START_BALANCE,pnl=0.0,position=None,last_signal=None)

async def biquote_loop():
    url="https://biquote.io/hubs/tick"
    while True:
        try:
            async with websockets.connect(url,ping_interval=20,ping_timeout=20) as ws:
                market["biquote_connected"]=True
                log("Connected to BiQuote SignalR.")
                await ws.send(json.dumps({"protocol":"json","version":1})+"\x1e")
                await ws.send(json.dumps({"type":1,"target":"Subscribe","arguments":["DXY"]})+"\x1e")
                log("Subscribed to BiQuote DXY.")
                while True:
                    raw=await ws.recv()
                    for part in raw.split("\x1e"):
                        if not part: continue
                        try: msg=json.loads(part)
                        except: continue
                        if msg.get("target")=="ReceiveTick" and msg.get("arguments"):
                            tick=msg["arguments"][0]
                            if str(tick.get("symbol","")).upper()=="DXY" and tick.get("mid") is not None:
                                market["dxy"]=float(tick["mid"])
                                market["updated"]=time.time()
                                market["dxy_source"]="BiQuote DXY"
        except Exception as e:
            market["biquote_connected"]=False
            log(f"BiQuote WebSocket error: {type(e).__name__}; using REST fallback.")
            try:
                rr=requests.get("https://biquote.io/api/DXY",timeout=8)
                rr.raise_for_status()
                tick=rr.json()
                if tick.get("mid") is not None:
                    market["dxy"]=float(tick["mid"])
                    market["updated"]=time.time()
                    market["dxy_source"]="BiQuote DXY REST fallback"
                    log("BiQuote DXY REST fallback succeeded.")
            except Exception as re:
                log(f"BiQuote REST fallback error: {type(re).__name__}.")
            await asyncio.sleep(5)

async def twelve_data_loop():
    if not TD_API_KEY:
        log("TWELVE_DATA_API_KEY is missing.")
        return
    while True:
        try:
            async with websockets.connect(TD_WS,ping_interval=20,ping_timeout=20) as ws:
                market["twelve_data_connected"]=True
                market["connected"]=True
                log("Connected to Twelve Data.")
                await ws.send(json.dumps({"action":"subscribe","params":{"symbols":"XAU/USD"}}))
                log("Subscribed to Twelve Data XAU/USD.")
                while True:
                    raw=await ws.recv()
                    for part in raw.split("\x1e"):
                        if not part: continue
                        try: msg=json.loads(part)
                        except: continue
                        if msg.get("event")=="price":
                            price=msg.get("price")
                            if msg.get("symbol")=="XAU/USD" and price is not None:
                                market["gold"]=float(price)
                                market["updated"]=time.time()
        except Exception as e:
            market["twelve_data_connected"]=False
            market["connected"]=bool(market["biquote_connected"])
            log(f"Twelve Data WebSocket error: {type(e).__name__}; retrying in 5s.")
            await asyncio.sleep(5)

@app.on_event("startup")
async def startup():
    asyncio.create_task(biquote_loop())
    asyncio.create_task(twelve_data_loop())
    log("Backend started: BiQuote DXY + Twelve Data XAU/USD. PAPER ONLY.")

@app.get("/")
def root():
    return {"ok":True,"mode":"PAPER_ONLY","message":"BiQuote DXY + Twelve Data Gold backend is online."}

@app.get("/health")
def health():
    return {"ok":True,"mode":"PAPER_ONLY","biquote_connected":market["biquote_connected"],
            "twelve_data_connected":market["twelve_data_connected"],
            "dxy":market["dxy"],"gold":market["gold"],"dxy_source":market["dxy_source"]}

@app.get("/state")
def state():
    return {"mode":"PAPER_ONLY","market":market,"agents":agents,"events":events}

@app.post("/paper/reset")
def paper_reset():
    reset_agents(); log("Paper accounts reset to starting balance.")
    return {"ok":True,"mode":"PAPER_ONLY","start_balance":START_BALANCE}

@app.get("/test/biquote")
def test_biquote():
    try:
        rr=requests.get("https://biquote.io/api/DXY",timeout=8)
        rr.raise_for_status()
        return {"ok":True,"source":"BiQuote","dxy":rr.json()}
    except Exception as e:
        return {"ok":False,"source":"BiQuote","error":str(e)}
