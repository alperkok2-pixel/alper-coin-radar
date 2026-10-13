"""ALPER Radar V8.6 BALANCED | paper-only Binance USD-M perpetuals.
Python 3.11+, websocket-client==1.9.0. No exchange keys or orders.
Run: python -u alper_radar.py ; test: python alper_radar.py --self-test
Env: TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, RADAR_DB=/var/data/alper_radar.sqlite3,
FOLLOW_COUNT=60, FOLLOW_REFRESH_SECONDS=60, MIN_24H_QUOTE=15000000.
Experimental heuristics; no validated profitability or advance warning guarantee.
"""
from __future__ import annotations
import collections, datetime as dt, fcntl, json, logging, math, os, random, signal, sqlite3, sys, threading, time, urllib.parse, urllib.request
from dataclasses import dataclass, field
from pathlib import Path
try: import websocket
except ImportError: websocket=None
LOG=logging.getLogger('ALPER')
REST='https://fapi.binance.com'
WS=os.getenv('BINANCE_WS_BASE','wss://fstream.binance.com/market/ws').rstrip('/')
DB=os.getenv('RADAR_DB','alper_radar.sqlite3')
TOKEN=os.getenv('TELEGRAM_TOKEN','').strip(); CHAT=os.getenv('TELEGRAM_CHAT_ID','').strip()
FOLLOW_COUNT=max(5,min(100,int(os.getenv('FOLLOW_COUNT','60'))))
REFRESH=max(30,int(os.getenv('FOLLOW_REFRESH_SECONDS','60')))
MIN_QUOTE=float(os.getenv('MIN_24H_QUOTE','15000000'))
MIN_TRADES=max(8,int(os.getenv('MIN_AGG_TRADES','20')))
MIN_FLOW_USDT=float(os.getenv('MIN_FLOW_USDT','12000'))
FLOW_RATIO=max(.65,min(.95,float(os.getenv('FLOW_RATIO','.72'))))
BURST_RATIO=max(1.3,float(os.getenv('BURST_RATIO','1.8')))
QUALITY_ENABLED=os.getenv('QUALITY_ENABLED','1')=='1'
QUALITY_FLOW_RATIO=float(os.getenv('QUALITY_FLOW_RATIO','.76'))
QUALITY_BURST_RATIO=float(os.getenv('QUALITY_BURST_RATIO','2.0'))
QUALITY_MIN_FLOW_USDT=float(os.getenv('QUALITY_MIN_FLOW_USDT','18000'))
QUALITY_MAX_MOVE_PCT=float(os.getenv('QUALITY_MAX_MOVE_PCT','.20'))
QUALITY_REQUIRE_5M=os.getenv('QUALITY_REQUIRE_5M','1')=='1'
TREND_REFRESH=max(300,int(os.getenv('TREND_REFRESH_SECONDS','900')))
REPORT_INTERVAL=max(1800,int(os.getenv('REPORT_INTERVAL_SECONDS','3600')))
FEE_BPS=max(0,float(os.getenv('ROUND_TRIP_FEE_BPS','10')))
SLIPPAGE_BPS=max(0,float(os.getenv('ROUND_TRIP_SLIPPAGE_BPS','4')))
COOLDOWN_SECONDS=3*3600; WATCH_COOLDOWN=20*60
STOP=threading.Event()
def now_ms(): return int(time.time()*1000)
def number(x,default=0.):
    try:
        v=float(x); return v if math.isfinite(v) else default
    except (ValueError,TypeError,OverflowError): return default
def api(path):
    req=urllib.request.Request(REST+path,headers={'User-Agent':'ALPER-Paper-Radar/8.6'})
    with urllib.request.urlopen(req,timeout=20) as resp: return json.load(resp)
def universe():
    return {x['symbol'] for x in api('/fapi/v1/exchangeInfo').get('symbols',[])
            if x.get('status')=='TRADING' and x.get('contractType')=='PERPETUAL' and x.get('quoteAsset')=='USDT'}
@dataclass
class PriceState:
    price:float=0.; quote:float=0.; updated:int=0
    history:collections.deque=field(default_factory=lambda:collections.deque(maxlen=180))
@dataclass
class TradeState:
    trades:collections.deque=field(default_factory=collections.deque)

def analyze(trades,price,updated,at=None,mode='confirm'):
    at=now_ms() if at is None else at
    if price<=0 or not 0<=at-updated<=4500:return None
    window=20000 if mode=='watch' else 45000
    minimum=max(7,MIN_TRADES//2) if mode=='watch' else MIN_TRADES
    selected=[x for x in trades if 0<=at-x[0]<=2*window]
    first=[x for x in selected if at-x[0]>window]
    last=[x for x in selected if at-x[0]<=window]
    if len(first)<minimum or len(last)<minimum:return None
    before=sum(x[2] for x in first); current=sum(x[2] for x in last)
    minflow=MIN_FLOW_USDT*(.65 if mode=='watch' else 1)
    minburst=max(1.3,BURST_RATIO*.85) if mode=='watch' else BURST_RATIO
    minratio=max(.65,FLOW_RATIO-.04) if mode=='watch' else FLOW_RATIO
    if before<minflow/4 or current<minflow:return None
    burst=current/max(before,1)
    if burst<minburst:return None
    buys=sum(x[2] for x in last if x[3]==1); frac=buys/current
    if frac>=minratio:direction,strength='LONG',frac
    elif 1-frac>=minratio:direction,strength='SHORT',1-frac
    else:return None
    drift=(price/first[-1][1]-1)*100
    move=drift*(1 if direction=='LONG' else -1)
    if not (.02 if mode=='watch' else .045)<=move<=(.18 if mode=='watch' else .24):return None
    prices=[x[1] for x in last]+[price]
    if (max(prices)-min(prices))/price*100>(.40 if mode=='watch' else .65):return None
    half=window//2
    halves=([x for x in last if at-x[0]>half],[x for x in last if at-x[0]<=half])
    for part in halves:
        vol=sum(x[2] for x in part)
        if len(part)<4 or vol<=0:return None
        buy=sum(x[2] for x in part if x[3]==1)
        same=buy/vol if direction=='LONG' else 1-buy/vol
        if same<.60:return None
    # V8.5 used min() (oldest recent trade); preserve intended reference explicitly.
    recent=min(halves[1],key=lambda x:x[0],default=None)
    if recent is None:return None
    lastmove=(price/recent[1]-1)*100*(1 if direction=='LONG' else -1)
    if lastmove<(.008 if mode=='watch' else .015):return None
    return dict(direction=direction,price=price,strength=strength,burst=burst,flow=current,move=move,age_ms=at-updated)

def trend_allows(direction,trend):
    return direction in ('LONG','SHORT') and all(trend.get(f)==direction for f in ('15m','1h'))
def quality_reason(val,trend):
    if not QUALITY_ENABLED:return None
    if QUALITY_REQUIRE_5M and trend.get('5m')!=val.get('direction'):return 'quality_5m_mismatch'
    metrics=[val.get(k) for k in ('strength','burst','flow','move')]
    if any(not isinstance(x,(int,float)) or not math.isfinite(x) for x in metrics):return 'quality_metrics_invalid'
    strength,burst,flow,move=metrics
    if strength<QUALITY_FLOW_RATIO:return 'quality_strength'
    if burst<QUALITY_BURST_RATIO:return 'quality_burst'
    if flow<QUALITY_MIN_FLOW_USDT:return 'quality_flow'
    if not .045<=move<=QUALITY_MAX_MOVE_PCT:return 'quality_move'
    return None
def quality_allows(val,trend):return quality_reason(val,trend) is None

class Database:
    def __init__(self,path=DB):
        self.lock=threading.RLock()
        if path!=':memory:':Path(path).parent.mkdir(parents=True,exist_ok=True)
        self.con=sqlite3.connect(path,check_same_thread=False,timeout=20)
        with self.lock:
            self.con.execute('PRAGMA journal_mode=WAL')
            self.con.execute('''CREATE TABLE IF NOT EXISTS signals(id INTEGER PRIMARY KEY,created INTEGER NOT NULL,symbol TEXT NOT NULL,direction TEXT NOT NULL,entry REAL NOT NULL,tp REAL NOT NULL,stop REAL NOT NULL,status TEXT NOT NULL DEFAULT 'OPEN',closed INTEGER,exit_price REAL,flow_ratio REAL,burst REAL)''')
            existing={r[1] for r in self.con.execute('PRAGMA table_info(signals)')}
            for col,typ in [('watch_delay_ms','INTEGER'),('telegram_delay_ms','INTEGER'),('trend_5m','TEXT'),('trend_15m','TEXT'),('trend_1h','TEXT'),('gross_pct','REAL'),('net_est_pct','REAL')]:
                if col not in existing:self.con.execute(f'ALTER TABLE signals ADD COLUMN {col} {typ}')
            self.con.execute('CREATE INDEX IF NOT EXISTS open_status ON signals(status,symbol)')
            self.con.execute('''CREATE TABLE IF NOT EXISTS watches(id INTEGER PRIMARY KEY,created INTEGER NOT NULL,symbol TEXT NOT NULL,direction TEXT NOT NULL,price REAL NOT NULL,status TEXT NOT NULL DEFAULT 'WATCH',resolved INTEGER)''')
            self.con.execute('''CREATE TABLE IF NOT EXISTS notification_outbox(id INTEGER PRIMARY KEY,signal_id INTEGER NOT NULL,kind TEXT NOT NULL,message TEXT NOT NULL,created INTEGER NOT NULL,delivered INTEGER,attempts INTEGER NOT NULL DEFAULT 0,UNIQUE(signal_id,kind))''')
            self.con.commit()
    def recent(self,table,sym,direction,ts,period):
        if table not in ('signals','watches'):raise ValueError('Invalid table')
        with self.lock:return self.con.execute(f'SELECT 1 FROM {table} WHERE symbol=? AND direction=? AND created>? LIMIT 1',(sym,direction,ts-period*1000)).fetchone() is not None
    def open_exists(self,sym):
        with self.lock:return self.con.execute("SELECT 1 FROM signals WHERE symbol=? AND status='OPEN' LIMIT 1",(sym,)).fetchone() is not None
    def open_symbols(self):
        with self.lock:return {r[0] for r in self.con.execute("SELECT DISTINCT symbol FROM signals WHERE status='OPEN'")}
    def watch(self,ts,sym,val):
        with self.lock:
            self.con.execute('INSERT INTO watches(created,symbol,direction,price) VALUES(?,?,?,?)',(ts,sym,val['direction'],val['price']));self.con.commit()
    def _outbox(self,id_,kind,msg,ts):
        self.con.execute('INSERT OR IGNORE INTO notification_outbox(signal_id,kind,message,created) VALUES(?,?,?,?)',(id_,kind,msg,ts))
    def confirmed_if_eligible(self,ts,sym,val,trend=None):
        trend=trend or {};direction=val['direction'];p=val['price'];long=direction=='LONG'
        if p<=0 or not trend_allows(direction,trend) or not quality_allows(val,trend):return None
        tp=p*(1.015 if long else .985);sl=p*(.9925 if long else 1.0075)
        with self.lock:
            if self.open_exists(sym) or self.recent('signals',sym,direction,ts,COOLDOWN_SECONDS):return None
            row=self.con.execute("SELECT created FROM watches WHERE symbol=? AND direction=? AND status='WATCH' AND created<? AND created>=? ORDER BY created DESC LIMIT 1",(sym,direction,ts,ts-600000)).fetchone()
            if not row:return None
            delay=ts-row[0]
            cur=self.con.execute('''INSERT INTO signals(created,symbol,direction,entry,tp,stop,status,flow_ratio,burst,watch_delay_ms,trend_5m,trend_15m,trend_1h) VALUES(?,?,?,?,?,?,'OPEN',?,?,?,?,?,?)''',(ts,sym,direction,p,tp,sl,val['strength'],val['burst'],delay,trend.get('5m'),trend.get('15m'),trend.get('1h')))
            id_=cur.lastrowid
            self.con.execute("UPDATE watches SET status='CONFIRMED',resolved=? WHERE symbol=? AND direction=? AND status='WATCH' AND created>=?",(ts,sym,direction,ts-600000))
            msg=(f'✅ ALPER V8.6 | ONAYLI SANAL {direction}\n{sym} | Giriş referansı: {p:.9g}\nTP (+1.5%): {tp:.9g}\nSTOP (-0.75%): {sl:.9g}\nAkış baskısı %{val["strength"]*100:.0f}, ivme {val["burst"]:.1f}x | hareket %{val["move"]:.2f}\n15m/1h trend onaylı; kalite filtresi {"aktif" if QUALITY_ENABLED else "kapalı"}\nKayıt #{id_} | Yatırım tavsiyesi değildir.')
            self._outbox(id_,'entry',msg,ts);self.con.commit()
            return id_,tp,sl,delay
    def expire_watches(self,ts):
        with self.lock:
            self.con.execute("UPDATE watches SET status='EXPIRED',resolved=? WHERE status='WATCH' AND created<?",(ts,ts-600000));self.con.commit()
    def update_price(self,sym,price,ts):
        if price<=0:return []
        with self.lock:
            rows=self.con.execute("SELECT id,direction,entry,tp,stop,created FROM signals WHERE status='OPEN' AND symbol=?",(sym,)).fetchall();finished=[]
            for id_,direction,entry,tp,sl,created in rows:
                if ts<created:continue
                result=('TP' if price>=tp else 'STOP' if price<=sl else None) if direction=='LONG' else ('TP' if price<=tp else 'STOP' if price>=sl else None)
                if not result:continue
                if not self.con.execute("UPDATE signals SET status=?,closed=?,exit_price=? WHERE id=? AND status='OPEN'",(result,ts,price,id_)).rowcount:continue
                gross=(price/entry-1)*100*(1 if direction=='LONG' else -1)
                net=gross-(FEE_BPS+SLIPPAGE_BPS)/100
                self.con.execute('UPDATE signals SET gross_pct=?,net_est_pct=? WHERE id=?',(gross,net,id_))
                msg=(f'🏁 ALPER V8.6 | SANAL SONUÇ #{id_}\n{sym} {direction} | {result}\nGiriş: {entry:.9g} | Gözlenen çıkış: {price:.9g}\nBrüt: {gross:+.2f}% | Tahmini net: {net:+.2f}% (varsayımsal masraflar)')
                self._outbox(id_,'result',msg,now_ms());finished.append((id_,result))
            if finished:self.con.commit()
            return finished
    def next_notification(self):
        with self.lock:return self.con.execute('SELECT id,message,kind,signal_id FROM notification_outbox WHERE delivered IS NULL ORDER BY id LIMIT 1').fetchone()
    def sent(self,id_,kind,signal_id):
        with self.lock:
            t=now_ms()
            self.con.execute('UPDATE notification_outbox SET delivered=? WHERE id=? AND delivered IS NULL',(t,id_))
            if kind=='entry':self.con.execute('UPDATE signals SET telegram_delay_ms=? WHERE id=?',(max(0,t-self.con.execute('SELECT created FROM signals WHERE id=?',(signal_id,)).fetchone()[0]),signal_id))
            self.con.commit()
    def failed(self,id_):
        with self.lock:self.con.execute('UPDATE notification_outbox SET attempts=attempts+1 WHERE id=?',(id_,));self.con.commit()
    def report(self):
        with self.lock:
            rows=self.con.execute('SELECT status,gross_pct,net_est_pct FROM signals').fetchall()
            pending=self.con.execute('SELECT COUNT(*) FROM notification_outbox WHERE delivered IS NULL').fetchone()[0]
        counts=collections.Counter(r[0] for r in rows);closed=[r for r in rows if r[0] in ('TP','STOP')]
        gross=sum(r[1] or 0 for r in closed);net=sum(r[2] if r[2] is not None else (r[1] or 0)-(FEE_BPS+SLIPPAGE_BPS)/100 for r in closed)
        return f'REPORT all-time TP={counts["TP"]} STOP={counts["STOP"]} OPEN={counts["OPEN"]} win={counts["TP"]}/{len(closed)} gross_observed_sum={gross:+.2f}% net_est_sum={net:+.2f}% (neither is portfolio return) telegram_pending={pending}'

class Radar:
    def __init__(self,db=None):
        self.db=db or Database();self.lock=threading.RLock();self.allowed=set();self.states={};self.trades={};self.followed=set();self.next_follow=set();self.rotation=0;self.candidate_updated=0;self.last_market=0;self.last_trade=0;self.last_status=0;self.trend={};self.last_report=now_ms();self.diagnostics=collections.Counter()
    def bootstrap(self):
        self.allowed=universe();ts=now_ms()
        for x in api('/fapi/v1/ticker/24hr'):
            sym=x.get('symbol','')
            if sym in self.allowed:self.on_price(sym,number(x.get('lastPrice')),number(x.get('quoteVolume')),ts,settle=False)
        LOG.info('Loaded %d Binance USDT perpetuals; existing OPEN=%d',len(self.allowed),len(self.db.open_symbols()))
    def on_price(self,sym,price,quote,ts,settle=True):
        if sym not in self.allowed or price<=0:return
        with self.lock:
            p=self.states.setdefault(sym,PriceState())
            if ts<p.updated:return
            p.price=price;p.quote=quote;p.updated=ts
            if not p.history or ts-p.history[-1][0]>=1500:p.history.append((ts,price))
            self.last_market=now_ms()
        if settle:self.close_from_price(sym,price,ts,'miniTicker')
    def close_from_price(self,sym,price,ts,source):
        for id_,result in self.db.update_price(sym,price,ts):LOG.info('RESULT %s #%d %s price=%.9g source=%s',result,id_,sym,price,source)
    def trend_snapshot(self,sym):
        with self.lock:
            row=self.trend.get(sym)
            return dict(row[0]) if row and now_ms()-row[1]<2*TREND_REFRESH*1000 else {}
    def trend_worker(self):
        while not STOP.is_set():
            with self.lock:symbols=sorted(self.next_follow)
            for sym in symbols:
                if STOP.is_set():break
                with self.lock:old=self.trend.get(sym)
                if old and now_ms()-old[1]<TREND_REFRESH*1000:continue
                readings={}
                for interval in ('5m','15m','1h'):
                    try:
                        path='/fapi/v1/klines?'+urllib.parse.urlencode({'symbol':sym,'interval':interval,'limit':22})
                        candles=api(path);closed=[c for c in candles if int(c[6])<now_ms()]
                        if len(closed)<20:break
                        closes=[number(c[4]) for c in closed]
                        fast=sum(closes[-5:])/5;slow=sum(closes[-20:])/20
                        readings[interval]='LONG' if fast>slow else 'SHORT' if fast<slow else 'FLAT'
                    except Exception as exc:
                        LOG.warning('TREND unavailable %s %s: %s',sym,interval,exc);break
                    STOP.wait(.5)
                if len(readings)==3:
                    with self.lock:self.trend[sym]=(readings,now_ms())
            STOP.wait(20)
    def on_trade(self,item):
        if item.get('e')!='aggTrade':return
        sym=item.get('s','');ts=int(item.get('T') or 0);px=number(item.get('p'));qty=number(item.get('q'))
        if sym not in self.allowed or px<=0 or qty<=0 or abs(now_ms()-ts)>5000:return
        side=-1 if item.get('m') is True else 1
        with self.lock:
            if sym not in self.followed:return
            state=self.trades.setdefault(sym,TradeState());state.trades.append((ts,px,px*qty,side))
            while state.trades and ts-state.trades[0][0]>95000:state.trades.popleft()
            self.last_trade=now_ms();p=self.states.get(sym)
            if p is None or now_ms()-p.updated>4500:return
            snapshot=list(state.trades);price=p.price;updated=p.updated
        current=now_ms()
        if self.db.open_exists(sym):return
        self.diagnostics['eligible_ticks']+=1
        watch=analyze(snapshot,price,updated,current,'watch')
        if watch and not self.db.recent('watches',sym,watch['direction'],current,WATCH_COOLDOWN):
            self.db.watch(current,sym,watch);self.diagnostics['watch_created']+=1
            LOG.info('WATCH %s %s recorded (Telegram muted)',sym,watch['direction'])
        confirmed=analyze(snapshot,price,updated,current,'confirm')
        if not confirmed:self.diagnostics['base_filter_no_match']+=1;return
        self.diagnostics['base_pass']+=1;trend=self.trend_snapshot(sym)
        if not trend:self.diagnostics['trend_missing_or_stale']+=1;return
        if not trend_allows(confirmed['direction'],trend):self.diagnostics['trend_mismatch']+=1;return
        self.diagnostics['trend_pass']+=1
        reason=quality_reason(confirmed,trend)
        if reason:self.diagnostics[reason]+=1;return
        self.diagnostics['quality_pass']+=1
        recorded=self.db.confirmed_if_eligible(current,sym,confirmed,trend)
        if not recorded:self.diagnostics['database_watch_cooldown_or_duplicate']+=1;return
        self.diagnostics['confirmed']+=1;id_,tp,sl,delay=recorded
        LOG.info('CONFIRMED %s %s #%d TP=%.9g STOP=%.9g delay_ms=%d',sym,confirmed['direction'],id_,tp,sl,delay)
    def choose_follow(self):
        t=now_ms()
        with self.lock:
            available=[]
            for sym,p in self.states.items():
                if p.quote<MIN_QUOTE or t-p.updated>20000:continue
                old=next((v for stamp,v in reversed(p.history) if t-stamp>=30000),None)
                if old is None or old<=0:continue
                move=abs(p.price/old-1)*100
                if move<=1.5:available.append((sym,p.quote,move))
            liquid=sorted(available,key=lambda x:x[1],reverse=True)
            moving=sorted(available,key=lambda x:x[2],reverse=True)
            eligible={x[0] for x in available};movers=[x[0] for x in moving[:FOLLOW_COUNT//2]]
            move_by_sym={s:m for s,_,m in available}
            retain=sorted((s for s in self.followed if s in eligible),key=lambda s:move_by_sym[s],reverse=True)
            stable=retain[:FOLLOW_COUNT//2]
            rest=[x[0] for x in liquid if x[0] not in set(stable+movers)]
            offset=self.rotation%len(rest) if rest else 0
            rotation=rest[offset:]+rest[:offset] if rest else []
            self.rotation+=max(1,FOLLOW_COUNT//2)
            chosen=list(dict.fromkeys(stable+movers+rotation))[:FOLLOW_COUNT]
            self.next_follow=set(chosen);self.candidate_updated=t
        LOG.info('Coverage %d eligible %d detailed %d',len(self.allowed),len(available),len(chosen))
    def notifier(self):
        while not STOP.is_set():
            row=self.db.next_notification()
            if not row:STOP.wait(1);continue
            id_,msg,kind,signal_id=row
            if not TOKEN or not CHAT:
                LOG.warning('Telegram credentials missing; notification #%d pending',id_);STOP.wait(30);continue
            body=urllib.parse.urlencode({'chat_id':CHAT,'text':msg}).encode()
            try:
                request=urllib.request.Request('https://api.telegram.org/bot'+TOKEN+'/sendMessage',data=body)
                with urllib.request.urlopen(request,timeout=10) as resp:out=json.load(resp)
                if not out.get('ok'):raise RuntimeError('Telegram ok=false')
                self.db.sent(id_,kind,signal_id);LOG.info('TELEGRAM delivered notification=%d kind=%s',id_,kind)
            except Exception as exc:
                self.db.failed(id_);LOG.warning('TELEGRAM retry pending notification=%d kind=%s error=%s',id_,kind,exc);STOP.wait(10)
    def market_worker(self):
        backoff=2
        while not STOP.is_set():
            sock=None
            try:
                sock=websocket.create_connection(WS+'/!miniTicker@arr',timeout=15);sock.settimeout(15);backoff=2
                LOG.info('MARKET websocket connected')
                while not STOP.is_set():
                    raw=json.loads(sock.recv());items=raw.get('data',[]) if isinstance(raw,dict) else raw
                    if not isinstance(items,list):continue
                    received=now_ms()
                    for item in items:
                        if not isinstance(item,dict):continue
                        event=int(item.get('E',received))
                        if abs(received-event)>10000:continue
                        self.on_price(item.get('s',''),number(item.get('c')),number(item.get('q')),event)
            except Exception as exc:LOG.warning('MARKET disconnected: %s',exc)
            finally:
                if sock:
                    try:sock.close()
                    except Exception:pass
            STOP.wait(backoff+random.random());backoff=min(60,backoff*2)
    def trade_worker(self):
        backoff=2
        while not STOP.is_set():
            sock=None
            with self.lock:target=set(self.next_follow)
            if not target:STOP.wait(2);continue
            try:
                sock=websocket.create_connection(WS,timeout=15);sock.settimeout(10)
                sock.send(json.dumps({'method':'SUBSCRIBE','params':[s.lower()+'@aggTrade' for s in sorted(target)],'id':84}))
                with self.lock:
                    self.followed=set(target)
                    # Reset rolling trade windows when subscription rotates; avoid stale 90s data.
                    self.trades={s:TradeState() for s in target}
                backoff=2;connected=time.monotonic();LOG.info('TRADES subscribed %d',len(target))
                while not STOP.is_set():
                    with self.lock:changed=target!=self.next_follow
                    if changed and time.monotonic()-connected>10:break
                    try:item=json.loads(sock.recv())
                    except websocket.WebSocketTimeoutException:continue
                    if isinstance(item,dict):self.on_trade(item.get('data',item))
            except Exception as exc:
                LOG.warning('TRADES disconnected: %s',exc);STOP.wait(backoff+random.random());backoff=min(60,backoff*2)
            finally:
                with self.lock:self.followed.clear()
                if sock:
                    try:sock.close()
                    except Exception:pass
    def result_worker(self):
        backoff=2
        while not STOP.is_set():
            target=self.db.open_symbols()
            if not target:STOP.wait(1);continue
            sock=None
            try:
                sock=websocket.create_connection(WS,timeout=15);sock.settimeout(2)
                sock.send(json.dumps({'method':'SUBSCRIBE','params':[s.lower()+'@aggTrade' for s in sorted(target)],'id':85}))
                LOG.info('RESULT_TRACKER subscribed %d open symbols',len(target));backoff=2
                while not STOP.is_set():
                    if self.db.open_symbols()!=target:break
                    try:raw=json.loads(sock.recv())
                    except websocket.WebSocketTimeoutException:continue
                    item=raw.get('data',raw) if isinstance(raw,dict) else {}
                    if not isinstance(item,dict) or item.get('e')!='aggTrade':continue
                    sym=item.get('s','');price=number(item.get('p'));ts=int(item.get('T') or 0)
                    if sym in target and price>0 and abs(now_ms()-ts)<=10000:self.close_from_price(sym,price,ts,'aggTrade')
            except Exception as exc:LOG.warning('RESULT_TRACKER disconnected: %s',exc)
            finally:
                if sock:
                    try:sock.close()
                    except Exception:pass
            STOP.wait(backoff+random.random());backoff=min(30,backoff*2)
    def run(self):
        self.bootstrap()
        for worker in (self.notifier,self.market_worker,self.trade_worker,self.trend_worker,self.result_worker):
            threading.Thread(target=worker,daemon=True,name=worker.__name__).start()
        while not STOP.wait(5):
            t=now_ms()
            if t-self.candidate_updated>REFRESH*1000 or (not self.next_follow and t-self.candidate_updated>15000):self.choose_follow()
            if t-self.last_status>60000:
                self.db.expire_watches(t)
                LOG.info('HEALTH market_age=%.1fs trade_age=%.1fs followed=%d open=%d',(t-self.last_market)/1000 if self.last_market else -1,(t-self.last_trade)/1000 if self.last_trade else -1,len(self.followed),len(self.db.open_symbols()))
                LOG.info('DIAG_60S %s',' '.join(f'{k}={v}' for k,v in sorted(self.diagnostics.items())) or 'no_eligible_ticks')
                self.diagnostics.clear();self.last_status=t
            if t-self.last_report>=REPORT_INTERVAL*1000:LOG.info(self.db.report());self.last_report=t

def self_test():
    import tempfile
    assert trend_allows('LONG',{'15m':'LONG','1h':'LONG'})
    assert trend_allows('SHORT',{'15m':'SHORT','1h':'SHORT'})
    assert not trend_allows('LONG',{'15m':'LONG','1h':'SHORT'})
    assert not trend_allows('LONG',{})
    good={'direction':'LONG','strength':.90,'burst':3.,'flow':30000.,'move':.1}
    assert quality_allows(good,{'5m':'LONG'})
    if QUALITY_ENABLED:
        assert quality_reason(dict(good,strength=.5),{'5m':'LONG'})=='quality_strength'
        if QUALITY_REQUIRE_5M:assert quality_reason(good,{'5m':'SHORT'})=='quality_5m_mismatch'
    t=now_ms();trades=[]
    for i in range(100):
        stamp=t-89000+i*890;px=100+.12*i/99
        trades.append((stamp,px,300 if i<50 else 900,1 if i%10 else -1))
    val=analyze(trades,100.12,t,t,'confirm')
    assert val and val['direction']=='LONG',val
    assert analyze(trades,100.12,t-8000,t,'confirm') is None
    short=[(stamp,200-2*(px-100),flow,-side) for stamp,px,flow,side in trades]
    sv=analyze(short,199.76,t,t,'confirm')
    assert sv and sv['direction']=='SHORT',sv
    val.update(strength=.90,burst=3.,flow=30000.,move=.1)
    sv.update(strength=.90,burst=3.,flow=30000.,move=.1)
    with tempfile.TemporaryDirectory() as tmp:
        path=str(Path(tmp)/'test.sqlite3');db=Database(path)
        lt={'5m':'LONG','15m':'LONG','1h':'LONG'};st={'5m':'SHORT','15m':'SHORT','1h':'SHORT'}
        assert db.confirmed_if_eligible(t,'TESTUSDT',val,lt) is None
        db.watch(t-1000,'TESTUSDT',val)
        assert db.confirmed_if_eligible(t,'TESTUSDT',val,{'15m':'LONG'}) is None
        assert db.confirmed_if_eligible(t,'TESTUSDT',val,{'15m':'LONG','1h':'SHORT'}) is None
        r=db.confirmed_if_eligible(t,'TESTUSDT',val,lt);assert r and len(r)==4
        id_,tp,sl,_=r
        assert db.confirmed_if_eligible(t+1000,'TESTUSDT',val,lt) is None
        assert db.update_price('TESTUSDT',tp+.01,t-100)==[]
        assert db.update_price('TESTUSDT',tp+.01,t+1000)==[(id_,'TP')]
        assert db.update_price('TESTUSDT',sl-.01,t+2000)==[]
        assert db.next_notification()[2]=='entry'
        first=db.next_notification();db.sent(first[0],first[2],first[3])
        assert db.next_notification()[2]=='result'
        assert db.con.execute('SELECT net_est_pct FROM signals WHERE id=?',(id_,)).fetchone()[0] is not None
        db.con.close();restored=Database(path)
        assert restored.next_notification()[2]=='result'
        assert 'TP=1' in restored.report()
        restored.watch(t-1000,'SHORTUSDT',sv)
        r=restored.confirmed_if_eligible(t,'SHORTUSDT',sv,st);assert r
        assert restored.update_price('SHORTUSDT',r[2]+.01,t+2000)==[(r[0],'STOP')]
        assert restored.update_price('SHORTUSDT',r[1]-.01,t+3000)==[]
        assert 'STOP=1' in restored.report()
    print('PASS V8.6: LONG/SHORT, trend, quality, watch, cooldown, TP/STOP, stale ticks, duplicate guard, persistent outbox, restart, estimated costs')
def main():
    logging.basicConfig(level=logging.INFO,format='%(asctime)s %(levelname)s %(message)s')
    if '--self-test' in sys.argv:return self_test()
    if websocket is None:sys.exit('Install dependency: pip install websocket-client==1.9.0')
    lock_path=Path(os.getenv('RADAR_LOCK','/tmp/alper_radar_v84.lock'))
    lock_path.parent.mkdir(parents=True,exist_ok=True)
    with lock_path.open('w') as lock:
        try:fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError:sys.exit('Another ALPER radar on this host')
        def close(signum,frame):STOP.set()
        signal.signal(signal.SIGTERM,close);signal.signal(signal.SIGINT,close)
        while not STOP.is_set():
            try:Radar().run()
            except Exception:LOG.exception('Radar failure; restarting in 30 seconds');STOP.wait(30)
if __name__=='__main__':main()