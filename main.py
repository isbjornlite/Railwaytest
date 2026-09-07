import os, asyncio, json, time, math, urllib.parse, urllib.request
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import websockets

API_KEY = os.getenv('TWELVE_DATA_API_KEY', '').strip()
START_BALANCE = float(os.getenv('START_BALANCE', '10000'))
RISK_PCT = float(os.getenv('RISK_PCT', '0.01'))
MAX_POSITION_NOTIONAL = float(os.getenv('MAX_POSITION_NOTIONAL', '25000'))
EXIT_REVERSAL_PCT = float(os.getenv('EXIT_REVERSAL_PERCENT', '0.30')) / 100.0
IMPULSE_BARS = int(os.getenv('IMPULSE_BARS', '10'))
MIN_IMPULSE_ATR = float(os.getenv('MIN_DXY_IMPULSE_ATR', '2.0'))
MAX_GOLD_MOVE_ATR = float(os.getenv('MAX_GOLD_MOVE_ATR', '0.75'))
RETRACE_PCT = float(os.getenv('DXY_RETRACE_PERCENT', '50')) / 100.0
MAX_SETUP_AGE = int(os.getenv('MAX_SETUP_AGE_BARS', '30'))
CONFIRMATIONS_NEEDED = int(os.getenv('CONFIRMATIONS_NEEDED', '2'))
USE_CORRELATION_FILTER = os.getenv('USE_CORRELATION_FILTER', 'false').lower() == 'true'
MIN_CORRELATION = float(os.getenv('MIN_CORRELATION', '-0.20'))
EMA_FAST = int(os.getenv('EMA_FAST', '20'))
EMA_SLOW = int(os.getenv('EMA_SLOW', '50'))
RSI_LEN = int(os.getenv('RSI_LEN', '14'))
ATR_LEN = int(os.getenv('ATR_LEN', '14'))

GOLD_SYMBOL = 'XAU/USD'
FX_SYMBOLS = ['EUR/USD','USD/JPY','GBP/USD','USD/CAD','USD/SEK','USD/CHF']
DEFAULT_SYMBOLS = ','.join([GOLD_SYMBOL] + FX_SYMBOLS)
# Always subscribe to the symbols the strategy actually needs. This also prevents
# an old Railway TD_SYMBOLS value such as XAU/USD,UUP from silently disabling DXY.
_requested_symbols = [x.strip() for x in os.getenv('TD_SYMBOLS', '').split(',') if x.strip()]
TD_SYMBOLS = DEFAULT_SYMBOLS if not _requested_symbols or any(x.upper() in {'DXY','UUP'} for x in _requested_symbols) else ','.join(_requested_symbols)

DXY_COEFF = {'EUR/USD':-0.576,'USD/JPY':0.136,'GBP/USD':-0.119,'USD/CAD':0.091,'USD/SEK':0.042,'USD/CHF':0.036}
DXY_BASE = 50.14348112
TIMEFRAMES = [('3m','3min','Scalper'),('15m','15min','Momentum'),('30m','30min','Intraday'),('1H','1h','Swing'),('4H','4h','Macro')]


def now_ts(): return time.time()

def add_event(msg):
    events.insert(0, {'ts': time.strftime('%H:%M:%S'), 'msg': msg})
    del events[100:]

def calculate_dxy(c):
    if not all(s in c and c[s] > 0 for s in FX_SYMBOLS): return None
    v = DXY_BASE
    for s,e in DXY_COEFF.items(): v *= c[s] ** e
    return v

def sma(vals, n):
    return sum(vals[-n:])/n if len(vals) >= n else None

def ema(vals, n):
    if len(vals) < n: return None
    k=2/(n+1); e=sum(vals[:n])/n
    for x in vals[n:]: e=x*k+e*(1-k)
    return e

def atr(bars, n=14):
    if len(bars) < n+1: return None
    trs=[]
    for i in range(1,len(bars)):
        b,p=bars[i],bars[i-1]
        trs.append(max(b['h']-b['l'], abs(b['h']-p['c']), abs(b['l']-p['c'])))
    return sum(trs[-n:])/n if len(trs)>=n else None

def rsi(vals,n=14):
    if len(vals)<n+1:return None
    gains=[];losses=[]
    for i in range(len(vals)-n,len(vals)):
        d=vals[i]-vals[i-1]
        gains.append(max(d,0));losses.append(max(-d,0))
    ag=sum(gains)/n; al=sum(losses)/n
    if al==0:return 100.0
    return 100-100/(1+ag/al)

def corr(a,b,n=50):
    if len(a)<n or len(b)<n:return None
    x=a[-n:];y=b[-n:]
    mx=sum(x)/n;my=sum(y)/n
    num=sum((u-mx)*(v-my) for u,v in zip(x,y));dx=sum((u-mx)**2 for u in x);dy=sum((v-my)**2 for v in y)
    return num/math.sqrt(dx*dy) if dx and dy else None

def parse_time(s):
    try:
        if isinstance(s,(int,float)): return float(s)
        return datetime.fromisoformat(str(s).replace('Z','+00:00')).timestamp()
    except Exception:return now_ts()

def rest_json(url):
    req=urllib.request.Request(url,headers={'User-Agent':'DXY-Gold-Paper-Traders/1.0'})
    with urllib.request.urlopen(req,timeout=15) as r: return json.loads(r.read().decode())

def fetch_history(interval, outputsize=120):
    qs=urllib.parse.urlencode({'symbol':GOLD_SYMBOL,'interval':interval,'outputsize':outputsize,'apikey':API_KEY,'format':'JSON'})
    url='https://api.twelvedata.com/time_series?'+qs
    data=rest_json(url)
    if data.get('status')=='error' or 'values' not in data: raise RuntimeError(data.get('message','Twelve Data history error'))
    return list(reversed([{'t':parse_time(x['datetime']),'o':float(x['open']),'h':float(x['high']),'l':float(x['low']),'c':float(x['close'])} for x in data['values']]))

def fetch_fx_history(interval, symbol, outputsize=120):
    qs=urllib.parse.urlencode({'symbol':symbol,'interval':interval,'outputsize':outputsize,'apikey':API_KEY,'format':'JSON'})
    data=rest_json('https://api.twelvedata.com/time_series?'+qs)
    if data.get('status')=='error' or 'values' not in data: raise RuntimeError(data.get('message','Twelve Data history error'))
    return list(reversed([{'t':parse_time(x['datetime']),'c':float(x['close'])} for x in data['values']]))

agents={tf:{'name':name,'timeframe':tf,'balance':START_BALANCE,'equity':START_BALANCE,'pnl':0.0,'position':None,'trades':0,'wins':0,'losses':0,'last_signal':None,'setup':None,'bars':0,'correlation':None} for tf,_,name in TIMEFRAMES}
market={'dxy':None,'gold':None,'updated':None,'connected':False,'dxy_source':'ICE DXY reconstructed from 6 FX components','components':{},'correlation':None}
events=[]

histories={tf:{'gold':[],'dxy':[]} for tf,_,_ in TIMEFRAMES}
last_tick={}
engine_task=None


def update_bar(bars, ts, price, minutes):
    bucket=int(ts//(minutes*60))*(minutes*60)
    if not bars or bars[-1]['t']!=bucket:
        bars.append({'t':bucket,'o':price,'h':price,'l':price,'c':price})
    else:
        b=bars[-1];b['h']=max(b['h'],price);b['l']=min(b['l'],price);b['c']=price
    if len(bars)>250: del bars[:-250]

def signal_for(tf):
    a=agents[tf]; g=histories[tf]['gold']; d=histories[tf]['dxy']
    if len(g)<max(60,IMPULSE_BARS+ATR_LEN+5) or len(d)<max(60,IMPULSE_BARS+ATR_LEN+5): return None
    gc=[x['c'] for x in g]; dc=[x['c'] for x in d]
    ga=atr(g,ATR_LEN); da=atr(d,ATR_LEN)
    if not ga or not da:return None
    old_d=dc[-IMPULSE_BARS-1]; new_d=dc[-1]; impulse=new_d-old_d
    impulse_atr=abs(impulse)/da
    gold_move=abs(gc[-1]-gc[-IMPULSE_BARS-1])/ga
    if impulse_atr<MIN_IMPULSE_ATR or gold_move>MAX_GOLD_MOVE_ATR:return None
    direction=1 if impulse>0 else -1 # expected gold direction is opposite
    expected=-direction
    high=max(dc[-IMPULSE_BARS-1:]); low=min(dc[-IMPULSE_BARS-1:])
    if direction>0:
        retr=(high-dc[-1])/(high-old_d) if high>old_d else 0
    else:
        retr=(dc[-1]-low)/(old_d-low) if old_d>low else 0
    if retr<RETRACE_PCT:return None
    ef=ema(gc,EMA_FAST); es=ema(gc,EMA_SLOW); rv=rsi(gc,RSI_LEN)
    confirmations=0; reasons=[]
    if expected>0 and ef and es and ef>=es: confirmations+=1;reasons.append('EMA bullish')
    if expected<0 and ef and es and ef<=es: confirmations+=1;reasons.append('EMA bearish')
    if rv is not None and ((expected>0 and rv>=50) or (expected<0 and rv<=50)): confirmations+=1;reasons.append('RSI aligned')
    if expected>0 and gc[-1]>=g[-1]['o']: confirmations+=1;reasons.append('bull candle')
    if expected<0 and gc[-1]<=g[-1]['o']: confirmations+=1;reasons.append('bear candle')
    c=corr(dc, gc, 50); a['correlation']=c
    if USE_CORRELATION_FILTER and (c is None or c>MIN_CORRELATION): return None
    if confirmations<CONFIRMATIONS_NEEDED:return None
    return {'side':'LONG' if expected>0 else 'SHORT','price':gc[-1],'dxy':dc[-1],'impulse_atr':round(impulse_atr,2),'gold_move_atr':round(gold_move,2),'retrace':round(retr*100,1),'confirmations':confirmations,'reasons':reasons,'atr':ga,'correlation':c}

def open_trade(tf, sig):
    a=agents[tf]
    if a['position'] is not None:return
    risk_cash=max(a['balance'],0)*RISK_PCT
    stop_dist=max(sig['atr']*1.5,0.5)
    qty=min(risk_cash/stop_dist, MAX_POSITION_NOTIONAL/sig['price'])
    if qty<=0:return
    a['position']={'side':sig['side'],'entry':sig['price'],'qty':qty,'entry_dxy':sig['dxy'],'stop':sig['price']-stop_dist if sig['side']=='LONG' else sig['price']+stop_dist,'best_dxy':sig['dxy'],'opened':now_ts(),'risk':risk_cash}
    a['last_signal']=sig
    a['trades']+=1
    add_event(f"{a['name']} {tf}: PAPER {sig['side']} XAU/USD @ {sig['price']:.2f} | DXY retrace {sig['retrace']:.1f}%")

def manage_trade(tf):
    a=agents[tf];p=a['position']
    if not p:return
    gold=market['gold'];dxy=market['dxy']
    if not gold or not dxy:return
    side=p['side'];p['best_dxy']=min(p['best_dxy'],dxy) if side=='LONG' else max(p['best_dxy'],dxy)
    exit_reason=None
    if side=='LONG':
        if gold<=p['stop']:exit_reason='ATR stop'
        elif dxy>=p['best_dxy']*(1+EXIT_REVERSAL_PCT):exit_reason='DXY reversal'
        pnl=(gold-p['entry'])*p['qty']
    else:
        if gold>=p['stop']:exit_reason='ATR stop'
        elif dxy<=p['best_dxy']*(1-EXIT_REVERSAL_PCT):exit_reason='DXY reversal'
        pnl=(p['entry']-gold)*p['qty']
    a['pnl']=a['balance']-START_BALANCE+pnl;a['equity']=a['balance']+pnl
    if exit_reason:
        a['balance']+=pnl;a['pnl']=a['balance']-START_BALANCE;a['equity']=a['balance']
        if pnl>=0:a['wins']+=1
        else:a['losses']+=1
        add_event(f"{a['name']} {tf}: EXIT {side} @ {gold:.2f} | {exit_reason} | P/L {pnl:+.2f}")
        a['position']=None

def run_strategy():
    if market['gold'] is None or market['dxy'] is None:return
    for tf,interval,name in TIMEFRAMES:
        # manage existing trade first
        manage_trade(tf)
        if agents[tf]['position'] is None:
            sig=signal_for(tf)
            if sig: open_trade(tf,sig)
        agents[tf]['bars']=len(histories[tf]['gold'])
        agents[tf]['equity']=agents[tf]['balance']
        if agents[tf]['position']:
            p=agents[tf]['position'];g=market['gold']
            agents[tf]['equity']=agents[tf]['balance']+((g-p['entry'])*p['qty'] if p['side']=='LONG' else (p['entry']-g)*p['qty'])
            agents[tf]['pnl']=agents[tf]['equity']-START_BALANCE

async def load_history():
    # Gold history is fetched per timeframe. DXY is reconstructed from the six FX histories.
    for tf,interval,_ in TIMEFRAMES:
        try:
            g=fetch_history(interval,120)
            histories[tf]['gold']=g
            # Build DXY history by fetching the same interval for each component and aligning by timestamp.
            maps=[]
            for s in FX_SYMBOLS:
                vals=fetch_fx_history(interval,s,120);maps.append({x['t']:x['c'] for x in vals})
            common=sorted(set(maps[0]).intersection(*[set(m) for m in maps[1:]]))
            d=[]
            for t in common:
                c={s:maps[i][t] for i,s in enumerate(FX_SYMBOLS)};v=calculate_dxy(c)
                if v:d.append({'t':t,'o':v,'h':v,'l':v,'c':v})
            histories[tf]['dxy']=d[-120:]
            add_event(f"Loaded {len(g)} {tf} Gold bars + {len(d)} DXY bars.")
        except Exception as e:
            add_event(f"History {tf} failed: {type(e).__name__}: {str(e)[:120]}")

async def ws_engine():
    if not API_KEY:
        add_event('TWELVE_DATA_API_KEY is missing. Add it in Railway Variables.');return
    url=f'wss://ws.twelvedata.com/v1/quotes/price?apikey={API_KEY}'
    symbols=TD_SYMBOLS
    add_event(f'Subscribing live: {symbols}')
    while True:
        try:
            async with websockets.connect(url,ping_interval=20,ping_timeout=20,close_timeout=10) as ws:
                await ws.send(json.dumps({'action':'subscribe','params':{'symbols':symbols}}))
                market['connected']=True;add_event('Connected to Twelve Data live feed.')
                async for raw in ws:
                    try:msg=json.loads(raw)
                    except Exception:continue
                    ev=msg.get('event')
                    if ev=='subscribe-status': add_event(f"Subscription status: {msg.get('status','unknown')} {msg.get('message','')}");continue
                    if ev=='error': add_event(f"Twelve Data error: {msg.get('message','unknown')}");continue
                    if ev!='price':continue
                    sym=str(msg.get('symbol','')).upper()
                    try:price=float(msg.get('price'))
                    except:continue
                    if price<=0:continue
                    matched=next((s for s in [GOLD_SYMBOL]+FX_SYMBOLS if s.upper()==sym),None)
                    if not matched:continue
                    ts=now_ts();market['updated']=ts
                    if matched==GOLD_SYMBOL:
                        market['gold']=price
                        for tf,interval,_ in TIMEFRAMES:update_bar(histories[tf]['gold'],ts,price,int(interval.replace('min','')) if 'min' in interval else int(float(interval.replace('h',''))*60))
                    else:
                        market['components'][matched]=price
                        dxy=calculate_dxy(market['components'])
                        if dxy:
                            market['dxy']=dxy
                            for tf,interval,_ in TIMEFRAMES:update_bar(histories[tf]['dxy'],ts,dxy,int(interval.replace('min','')) if 'min' in interval else int(float(interval.replace('h',''))*60))
                    if market['gold'] and market['dxy']:
                        # Update rolling correlation on 1H as the dashboard headline.
                        c=corr([x['c'] for x in histories['1H']['dxy']],[x['c'] for x in histories['1H']['gold']],50)
                        market['correlation']=c
                        run_strategy()
        except Exception as e:
            market['connected']=False;add_event(f'Live connection error: {type(e).__name__}; retrying in 5s.');await asyncio.sleep(5)

@asynccontextmanager
async def lifespan(app):
    global engine_task
    await load_history()
    engine_task=asyncio.create_task(ws_engine())
    yield
    if engine_task:engine_task.cancel()

app=FastAPI(title='DXY Gold AI Paper Traders',lifespan=lifespan)
app.add_middleware(CORSMiddleware,allow_origins=['*'],allow_credentials=False,allow_methods=['*'],allow_headers=['*'])

@app.get('/')
def root():return {'ok':True,'mode':'PAPER_ONLY','message':'DXY Gold paper-trading strategy backend is online.','dxy_source':market['dxy_source']}

@app.get('/health')
def health():return {'ok':True,'connected':market['connected'],'market':market}

@app.get('/state')
def state():
    return {'mode':'PAPER_ONLY','strategy':{'impulse_bars':IMPULSE_BARS,'min_dxy_impulse_atr':MIN_IMPULSE_ATR,'max_gold_move_atr':MAX_GOLD_MOVE_ATR,'dxy_retrace_percent':RETRACE_PCT*100,'exit_reversal_percent':EXIT_REVERSAL_PCT*100,'confirmations_needed':CONFIRMATIONS_NEEDED,'risk_pct':RISK_PCT*100,'correlation_filter':USE_CORRELATION_FILTER},'market':market,'agents':list(agents.values()),'events':events[:30]}

@app.post('/paper/reset')
def reset():
    for a in agents.values():
        a.update({'balance':START_BALANCE,'equity':START_BALANCE,'pnl':0.0,'position':None,'trades':0,'wins':0,'losses':0,'last_signal':None,'setup':None})
    add_event('All paper accounts reset.')
    return {'ok':True,'start_balance':START_BALANCE}
