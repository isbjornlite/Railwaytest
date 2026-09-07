
import os, asyncio, json, time
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import websockets

API_KEY=os.getenv("TWELVE_DATA_API_KEY","").strip()
SYMBOLS=os.getenv("TD_SYMBOLS","XAU/USD,DXY")
START_BALANCE=float(os.getenv("START_BALANCE","10000"))
agents={tf:{"name":name,"timeframe":tf,"balance":START_BALANCE,"pnl":0.0,"position":None,"last_signal":None}
        for name,tf in [("Scalper","3m"),("Momentum","15m"),("Intraday","30m"),("Swing","1H"),("Macro","4H")]}
market={"dxy":None,"gold":None,"updated":None,"connected":False}
events=[]
running=True

def add_event(msg):
    events.insert(0,{"ts":time.strftime("%H:%M:%S"),"msg":msg})
    del events[100:]

async def engine():
    global running
    if not API_KEY:
        add_event("TWELVE_DATA_API_KEY is missing. Add it in Railway Variables.")
        return
    url=f"wss://ws.twelvedata.com/v1/quotes/price?apikey={API_KEY}"
    while running:
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                await ws.send(json.dumps({"action":"subscribe","params":{"symbols":SYMBOLS}}))
                market["connected"]=True
                add_event("Connected to Twelve Data.")
                async for raw in ws:
                    msg=json.loads(raw)
                    if msg.get("event")=="price":
                        symbol=str(msg.get("symbol","")).upper()
                        price=float(msg.get("price",0))
                        if "XAU" in symbol: market["gold"]=price
                        elif "DXY" in symbol: market["dxy"]=price
                        market["updated"]=time.time()
                        # Paper-trading execution is intentionally conservative:
                        # this service records market data and leaves real orders disabled.
        except Exception as e:
            market["connected"]=False
            add_event(f"Twelve Data connection error: {type(e).__name__}; retrying.")
            await asyncio.sleep(5)

@asynccontextmanager
async def lifespan(app):
    task=asyncio.create_task(engine())
    yield
    task.cancel()

app=FastAPI(title="DXY Gold Paper Traders",lifespan=lifespan)
app.add_middleware(CORSMiddleware,allow_origins=["*"],allow_credentials=False,allow_methods=["*"],allow_headers=["*"])

@app.get("/")
def root():
    return {"ok":True,"mode":"PAPER_ONLY","message":"DXY Gold paper-trading backend is online."}

@app.get("/health")
def health():
    return {"ok":True,"connected":market["connected"],"market":market}

@app.get("/state")
def state():
    return {"mode":"PAPER_ONLY","market":market,"agents":list(agents.values()),"events":events[:30]}

@app.post("/paper/reset")
def reset():
    for a in agents.values():
        a["balance"]=START_BALANCE;a["pnl"]=0.0;a["position"]=None;a["last_signal"]=None
    add_event("All paper accounts reset.")
    return {"ok":True,"start_balance":START_BALANCE}
