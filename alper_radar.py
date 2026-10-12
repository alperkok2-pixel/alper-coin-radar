"""ALPER Radar V8.3 MEASURE: PAPER-ONLY Binance USDT perpetual early watch + confirmation.
Python 3.11+, pip install websocket-client==1.9.0
Run: python -u alper_radar.py ; test: python alper_radar.py --self-test
Environment: TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, FOLLOW_COUNT=20,
FOLLOW_REFRESH_SECONDS=60, MIN_24H_QUOTE=15000000, RADAR_DB=alper_radar.sqlite3.
No exchange keys, no orders, no profitability guarantees.
"""
from __future__ import annotations
import collections
import datetime as dt
import statistics
import fcntl
import json
import logging
import math
import os
import queue
import random
import signal
import sqlite3
import sys
import threading
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
try:
    import websocket
except ImportError:
    websocket = None

LOG = logging.getLogger('ALPER')
REST = 'https://fapi.binance.com'
WS = os.getenv('BINANCE_WS_BASE', 'wss://fstream.binance.com/market/ws').rstrip('/')
DB = os.getenv('RADAR_DB', 'alper_radar.sqlite3')
TOKEN = os.getenv('TELEGRAM_TOKEN', '').strip()
CHAT = os.getenv('TELEGRAM_CHAT_ID', '').strip()
FOLLOW_COUNT = max(5, min(40, int(os.getenv('FOLLOW_COUNT', '20'))))
REFRESH = max(60, int(os.getenv('FOLLOW_REFRESH_SECONDS', '60')))
MIN_QUOTE = float(os.getenv('MIN_24H_QUOTE', '15000000'))
MIN_TRADES = max(8, int(os.getenv('MIN_AGG_TRADES', '20')))
MIN_FLOW_USDT = float(os.getenv('MIN_FLOW_USDT', '12000'))
FLOW_RATIO = max(.65, min(.95, float(os.getenv('FLOW_RATIO', '.72'))))
BURST_RATIO = max(1.3, float(os.getenv('BURST_RATIO', '1.8')))
COOLDOWN_SECONDS = 3 * 3600
WATCH_COOLDOWN = 20 * 60
# Measurements are advisory: trend never blocks a V8.2 confirmation.
REPORT_INTERVAL = max(1800, int(os.getenv("REPORT_INTERVAL_SECONDS", "3600")))
TREND_REFRESH = max(300, int(os.getenv("TREND_REFRESH_SECONDS", "900")))
STOP = threading.Event()

def now_ms(): return int(time.time() * 1000)
def number(value, default=0.):
    try:
        n = float(value)
        return n if math.isfinite(n) else default
    except (TypeError, ValueError, OverflowError): return default

def api(path):
    req = urllib.request.Request(REST+path, headers={'User-Agent':'ALPER-Paper-Radar/8.3'})
    with urllib.request.urlopen(req, timeout=20) as r: return json.load(r)

def universe():
    data = api('/fapi/v1/exchangeInfo')
    return {x['symbol'] for x in data.get('symbols', []) if x.get('status')=='TRADING'
            and x.get('contractType')=='PERPETUAL' and x.get('quoteAsset')=='USDT'}

@dataclass
class PriceState:
    price: float = 0
    quote: float = 0
    updated: int = 0
    history: collections.deque = field(default_factory=lambda: collections.deque(maxlen=180))

@dataclass
class TradeState:
    trades: collections.deque = field(default_factory=collections.deque)
    started: int = 0


def analyze(trades, price, updated, at=None, mode='confirm'):
    """Two stages, both require real trades and directional agreement.
    WATCH uses 20+20 sec, CONFIRM uses 45+45 sec. Not a prediction.
    """
    at = now_ms() if at is None else at
    if price <= 0 or not 0 <= at-updated <= 4500: return None
    window = 20_000 if mode=='watch' else 45_000
    minimum_trades = max(7, MIN_TRADES//2) if mode=='watch' else MIN_TRADES
    chosen = [(t,p,q,side) for t,p,q,side in trades if 0 <= at-t <= 2*window]
    first = [x for x in chosen if at-x[0] > window]
    last = [x for x in chosen if at-x[0] <= window]
    if len(first)<minimum_trades or len(last)<minimum_trades: return None
    before = sum(x[2] for x in first)
    current = sum(x[2] for x in last)
    min_flow = MIN_FLOW_USDT*.65 if mode=='watch' else MIN_FLOW_USDT
    min_burst = max(1.3, BURST_RATIO*.85) if mode=='watch' else BURST_RATIO
    min_ratio = max(.65, FLOW_RATIO-.04) if mode=='watch' else FLOW_RATIO
    if before < min_flow/4 or current < min_flow: return None
    burst = current/max(before,1)
    if burst < min_burst: return None
    buys = sum(x[2] for x in last if x[3]==1)
    frac = buys/current
    if frac>=min_ratio: direction='LONG'; strength=frac
    elif 1-frac>=min_ratio: direction='SHORT'; strength=1-frac
    else: return None
    drift = (price/first[-1][1]-1)*100
    move = drift if direction=='LONG' else -drift
    lower = .02 if mode=='watch' else .045
    upper = .18 if mode=='watch' else .24
    if not lower <= move <= upper: return None
    prices = [x[1] for x in last]+[price]
    if (max(prices)-min(prices))/price*100 > (.40 if mode=='watch' else .65): return None
    half_window = window//2
    halves = ([x for x in last if at-x[0]>half_window],
              [x for x in last if at-x[0]<=half_window])
    for half in halves:
        volume=sum(x[2] for x in half)
        if len(half)<4 or volume<=0: return None
        buy=sum(x[2] for x in half if x[3]==1)
        same=buy/volume if direction=='LONG' else 1-buy/volume
        if same<.60: return None
    recent = min(halves[1], key=lambda x:x[0], default=None)
    if recent is None:return None
    last_move = (price/recent[1]-1)*100*(1 if direction=='LONG' else -1)
    if last_move < (.008 if mode=='watch' else .015): return None
    return {'direction':direction,'price':price,'strength':strength,
            'burst':burst,'flow':current,'move':move,'age_ms':at-updated}

class Database:
    def __init__(self,path=DB):
        self.lock=threading.RLock()
        self.con=sqlite3.connect(path,check_same_thread=False,timeout=20)
        with self.lock:
            self.con.execute('PRAGMA journal_mode=WAL')
            self.con.execute('''CREATE TABLE IF NOT EXISTS signals(
                id INTEGER PRIMARY KEY, created INTEGER NOT NULL, symbol TEXT NOT NULL,
                direction TEXT NOT NULL, entry REAL NOT NULL, tp REAL NOT NULL,
                stop REAL NOT NULL, status TEXT NOT NULL DEFAULT 'OPEN',
                closed INTEGER, exit_price REAL, flow_ratio REAL, burst REAL)''')
            self.con.execute('CREATE INDEX IF NOT EXISTS open_status ON signals(status,symbol)')
            # Additive migration: compatible with existing V8.2 databases.
            existing={r[1] for r in self.con.execute('PRAGMA table_info(signals)')}
            for col,typ in [('watch_delay_ms','INTEGER'),('telegram_delay_ms','INTEGER'),
                            ('trend_5m','TEXT'),('trend_15m','TEXT'),('trend_1h','TEXT'),
                            ('gross_pct','REAL'),('net_est_pct','REAL')]:
                if col not in existing:self.con.execute(f'ALTER TABLE signals ADD COLUMN {col} {typ}')
            self.con.execute('''CREATE TABLE IF NOT EXISTS watches(
                id INTEGER PRIMARY KEY, created INTEGER NOT NULL, symbol TEXT NOT NULL,
                direction TEXT NOT NULL, price REAL NOT NULL, status TEXT NOT NULL DEFAULT 'WATCH',
                resolved INTEGER)''')
            self.con.commit()
    def recent(self,table,symbol,direction,ts,period):
        with self.lock:
            return self.con.execute(f'SELECT 1 FROM {table} WHERE symbol=? AND direction=? AND created>? LIMIT 1',
                                    (symbol,direction,ts-period*1000)).fetchone() is not None
    def open_exists(self,symbol):
        with self.lock:
            return self.con.execute("SELECT 1 FROM signals WHERE symbol=? AND status='OPEN' LIMIT 1",(symbol,)).fetchone() is not None
    def watch(self,ts,symbol,val):
        with self.lock:
            self.con.execute('INSERT INTO watches(created,symbol,direction,price) VALUES(?,?,?,?)',
                             (ts,symbol,val['direction'],val['price']))
            self.con.commit()
    def confirmed(self,ts,symbol,val,trend=None,watch_delay_ms=None):
        p=val['price']; long=val['direction']=='LONG'
        tp=p*(1.015 if long else .985)
        sl=p*(.9925 if long else 1.0075)
        with self.lock:
            cur=self.con.execute('''INSERT INTO signals(created,symbol,direction,entry,tp,stop,status,flow_ratio,burst)
                        VALUES(?,?,?,?,?,?,'OPEN',?,?)''',
                        (ts,symbol,val['direction'],p,tp,sl,val['strength'],val['burst']))
            self.con.execute('UPDATE signals SET watch_delay_ms=?,trend_5m=?,trend_15m=?,trend_1h=? WHERE id=?',
                             (watch_delay_ms,(trend or {}).get('5m'),(trend or {}).get('15m'),(trend or {}).get('1h'),cur.lastrowid))
            self.con.execute("UPDATE watches SET status='CONFIRMED',resolved=? WHERE symbol=? AND direction=? AND status='WATCH' AND created>=?",
                             (ts,symbol,val['direction'],ts-10*60*1000))
            self.con.commit()
            return cur.lastrowid,tp,sl
    def expire_watches(self,ts):
        with self.lock:
            self.con.execute("UPDATE watches SET status='EXPIRED',resolved=? WHERE status='WATCH' AND created<?",
                             (ts,ts-10*60*1000))
            self.con.commit()
    def set_telegram_delay(self,id_,delay_ms):
        with self.lock:
            self.con.execute('UPDATE signals SET telegram_delay_ms=? WHERE id=?',(delay_ms,id_))
            self.con.commit()
    def report(self):
        with self.lock:
            rows=self.con.execute("SELECT status,entry,exit_price,direction,trend_15m,watch_delay_ms,telegram_delay_ms FROM signals").fetchall()
        counts=collections.Counter(r[0] for r in rows)
        closed=[r for r in rows if r[0] in ('TP','STOP') and r[2] is not None and r[1]>0]
        pnl=[(r[2]/r[1]-1)*100*(1 if r[3]=='LONG' else -1) for r in closed]
        matching=[r for r in closed if r[4] in ('LONG','SHORT') and r[4]==r[3]]
        contrary=[r for r in closed if r[4] in ('LONG','SHORT') and r[4]!=r[3]]
        def wins(group):return f'{sum(r[0]=="TP" for r in group)}/{len(group)}' if group else '0/0'
        delays=[r[5]/1000 for r in rows if r[5] is not None]
        # Recorded movement is a proxy; no fee or slippage model is presumed.
        return (f'REPORT all-time recorded | TP={counts["TP"]} STOP={counts["STOP"]} OPEN={counts["OPEN"]} '
                f'win={wins(closed)} | gross_observed_sum={sum(pnl):+.2f}% '
                f'(not portfolio return) | trend15 agree={wins(matching)} disagree={wins(contrary)} '
                f'| watch-to-confirm median={statistics.median(delays):.1f}s' if delays else
                f'REPORT all-time recorded | TP={counts["TP"]} STOP={counts["STOP"]} OPEN={counts["OPEN"]} '
                f'win={wins(closed)} | gross_observed_sum={sum(pnl):+.2f}% (not portfolio return) '
                f'| trend15 agree={wins(matching)} disagree={wins(contrary)} | no delay samples')
    def update_price(self,symbol,price,ts):
        with self.lock:
            rows=self.con.execute("SELECT id,direction,tp,stop FROM signals WHERE status='OPEN' AND symbol=?",(symbol,)).fetchall()
            finished=[]
            for id_,direction,tp,sl in rows:
                status=('TP' if price>=tp else 'STOP' if price<=sl else None) if direction=='LONG' else ('TP' if price<=tp else 'STOP' if price>=sl else None)
                if status:
                    self.con.execute('UPDATE signals SET status=?,closed=?,exit_price=? WHERE id=?',(status,ts,price,id_))
                    finished.append((id_,status))
            if finished:self.con.commit()
            return finished

class Radar:
    def __init__(self,db=None):
        self.db=db or Database()
        self.lock=threading.RLock()
        self.allowed=set();self.states={};self.trades={};self.followed=set()
        self.next_follow=set();self.rotation=0;self.candidate_updated=0
        self.last_market=0;self.last_trade=0;self.last_status=0
        self.notify=queue.Queue(maxsize=500)
        self.trend={}  # Symbol -> ({5m,15m,1h}, fetched_at), advisory only
        self.last_report=now_ms()
    def bootstrap(self):
        self.allowed=universe()
        for item in api('/fapi/v1/ticker/24hr'):
            sym=item.get('symbol','')
            if sym in self.allowed:
                self.on_price(sym,number(item.get('lastPrice')),number(item.get('quoteVolume')),now_ms())
        LOG.info('Loaded %d active USDT perpetual contracts',len(self.allowed))
    def enqueue(self,message,ttl=25,kind='entry'):
        # Quiet Telegram mode: watch hypotheses and startup stay in the
        # internal database/logs; only confirmed paper entries and TP/STOP
        # outcomes reach Telegram. The watch/confirmation logic is unchanged.
        if kind not in ('entry', 'result'):
            return
        try:self.notify.put_nowait((message,now_ms(),ttl,kind))
        except queue.Full:LOG.warning('Telegram queue full; dropped %s',kind)
    def on_price(self,sym,price,quote,ts):
        if sym not in self.allowed or price<=0:return
        with self.lock:
            p=self.states.setdefault(sym,PriceState())
            if ts<p.updated:return
            p.price=price;p.quote=quote;p.updated=ts
            if not p.history or ts-p.history[-1][0]>=1500:p.history.append((ts,price))
            self.last_market=now_ms()
        for id_,status in self.db.update_price(sym,price,ts):
            self.enqueue(f'ALPER V8.2 | SANAL SONUÇ #{id_}: {sym} {status} | gözlenen {price:.9g}',180,'result')
    def trend_snapshot(self,sym):
        with self.lock:
            data=self.trend.get(sym)
            return dict(data[0]) if data and now_ms()-data[1]<2*TREND_REFRESH*1000 else {}
    def trend_worker(self):
        # Rate-limited REST lookups on shortlist only. Closed candles, no forming-candle lookahead.
        while not STOP.is_set():
            with self.lock: symbols=sorted(self.next_follow)
            for sym in symbols:
                if STOP.is_set():break
                with self.lock: old=self.trend.get(sym)
                if old and now_ms()-old[1]<TREND_REFRESH*1000:continue
                readings={}
                for interval in ('5m','15m','1h'):
                    try:
                        path='/fapi/v1/klines?'+urllib.parse.urlencode({'symbol':sym,'interval':interval,'limit':22})
                        candles=api(path)
                        closed=[c for c in candles if int(c[6])<now_ms()]
                        if len(closed)<20:continue
                        closes=[number(c[4]) for c in closed]
                        fast=sum(closes[-5:])/5; slow=sum(closes[-20:])/20
                        readings[interval]='LONG' if fast>slow else 'SHORT' if fast<slow else 'FLAT'
                    except Exception as exc:
                        LOG.warning('TREND unavailable %s %s: %s',sym,interval,exc)
                        break
                    STOP.wait(.5) # safety pacing, no effect on trade websocket
                if readings:
                    with self.lock:self.trend[sym]=(readings,now_ms())
            STOP.wait(20)
    def on_trade(self,item):
        if item.get('e')!='aggTrade':return
        sym=item.get('s','');ts=int(item.get('T') or 0)
        px=number(item.get('p'));qty=number(item.get('q'))
        if sym not in self.allowed or px<=0 or qty<=0 or abs(now_ms()-ts)>5000:return
        side=-1 if item.get('m') is True else 1
        with self.lock:
            if sym not in self.followed:return
            state=self.trades.setdefault(sym,TradeState(started=ts))
            state.trades.append((ts,px,px*qty,side))
            while state.trades and ts-state.trades[0][0]>95000:state.trades.popleft()
            self.last_trade=now_ms()
            p=self.states.get(sym)
            if p is None or now_ms()-p.updated>4500:return
            snapshot=list(state.trades);price=p.price;updated=p.updated
        current=now_ms()
        if self.db.open_exists(sym):return
        # A fresh WATCH is only a hypothesis: never open a paper position here.
        watch=analyze(snapshot,price,updated,current,'watch')
        if watch and not self.db.recent('watches',sym,watch['direction'],current,WATCH_COOLDOWN):
            self.db.watch(current,sym,watch)
            LOG.info('WATCH %s %s recorded (Telegram muted)',sym,watch['direction'])
            self.enqueue(f'👀 ALPER V8.2 | ÖN UYARI (İŞLEM DEĞİL)\n{sym} {watch["direction"]}\n'
                         f'Gözlenen: {price:.9g} | akış: %{watch["strength"]*100:.0f} | ivme: {watch["burst"]:.1f}x\n'
                         'Henüz doğrulanmadı. Pozisyon açma sinyali değildir.',25,'watch')
        confirmed=analyze(snapshot,price,updated,current,'confirm')
        if not confirmed or self.db.recent('signals',sym,confirmed['direction'],current,COOLDOWN_SECONDS):return
        # Conservative: require a matching watch (created before this observation) within last 10 minutes.
        with self.db.lock:
            eligible=self.db.con.execute("SELECT 1 FROM watches WHERE symbol=? AND direction=? AND status='WATCH' AND created<? AND created>=? LIMIT 1",
                                        (sym,confirmed['direction'],current,current-10*60*1000)).fetchone()
            if not eligible or self.db.open_exists(sym):return
            watch_row=self.db.con.execute("SELECT created FROM watches WHERE symbol=? AND direction=? AND status='WATCH' AND created<? AND created>=? ORDER BY created DESC LIMIT 1",
                                          (sym,confirmed['direction'],current,current-10*60*1000)).fetchone()
            delay=current-watch_row[0] if watch_row else None
            trend=self.trend_snapshot(sym)
            id_,tp,sl=self.db.confirmed(current,sym,confirmed,trend,delay)
        self.enqueue(f'✅ ALPER V8.3 | ONAYLI SANAL {confirmed["direction"]}\n'
                     f'{sym} | giriş referansı: {price:.9g}\nTP (+1.5%): {tp:.9g}\n'
                     f'STOP (-0.75%): {sl:.9g}\nAkış baskısı %{confirmed["strength"]*100:.0f}, '
                     f'ivme {confirmed["burst"]:.1f}x | fiyat hareketi %{confirmed["move"]:.2f}\n'
                     f'Kayıt #{id_} | işlem tavsiyesi değildir.',25,'entry')
        LOG.info('CONFIRMED %s %s #%d watch_delay=%s trend=%s',sym,confirmed['direction'],id_,delay,trend)
    def choose_follow(self):
        t=now_ms()
        with self.lock:
            available=[]
            for sym,p in self.states.items():
                if p.quote<MIN_QUOTE or t-p.updated>20000:continue
                old=next((v for stamp,v in reversed(p.history) if t-stamp>=30000),None)
                if old is None or old<=0:continue
                move=abs(p.price/old-1)*100
                if move>1.5:continue
                available.append((sym,p.quote,move))
            liquid=sorted(available,key=lambda x:x[1],reverse=True)
            moving=sorted(available,key=lambda x:x[2],reverse=True)
            eligible={x[0] for x in available}
            movers=[x[0] for x in moving[:FOLLOW_COUNT//2]]
            retain=sorted((s for s in self.followed if s in eligible),
                          key=lambda s: next((x[2] for x in available if x[0]==s),0),reverse=True)
            stable=retain[:FOLLOW_COUNT//2]
            rest=[x[0] for x in liquid if x[0] not in stable+movers]
            offset=self.rotation%len(rest) if rest else 0
            rotation=(rest[offset:]+rest[:offset]) if rest else []
            self.rotation+=max(1,FOLLOW_COUNT//2)
            chosen=list(dict.fromkeys(stable+movers+rotation))[:FOLLOW_COUNT]
            self.next_follow=set(chosen)
            self.candidate_updated=t
        LOG.info('Coverage %d eligible %d detailed %d',len(self.allowed),len(available),len(chosen))
    def notifier(self):
        while not STOP.is_set():
            try:msg,created,ttl,kind=self.notify.get(timeout=1)
            except queue.Empty:continue
            if now_ms()-created>ttl*1000:continue
            if not TOKEN or not CHAT:
                LOG.info('Telegram not configured: %s',msg.splitlines()[0]);continue
            body=urllib.parse.urlencode({'chat_id':CHAT,'text':msg}).encode()
            for attempt in range(3):
                if now_ms()-created>ttl*1000:break
                try:
                    request=urllib.request.Request('https://api.telegram.org/bot'+TOKEN+'/sendMessage',data=body)
                    with urllib.request.urlopen(request,timeout=10) as response:out=json.load(response)
                    if out.get('ok'):break
                    raise RuntimeError('Telegram ok=false')
                except Exception as exc:
                    LOG.warning('Telegram %s failed: %s',kind,exc)
                    STOP.wait(min(2**attempt,5))
    def market_worker(self):
        backoff=2
        while not STOP.is_set():
            sock=None
            try:
                sock=websocket.create_connection(WS+'/!miniTicker@arr',timeout=15)
                sock.settimeout(15);backoff=2
                LOG.info('MARKET websocket connected')
                while not STOP.is_set():
                    raw=json.loads(sock.recv())
                    items=raw.get('data',[]) if isinstance(raw,dict) else raw
                    if not isinstance(items,list):continue
                    received=now_ms()
                    for item in items:
                        if not isinstance(item,dict) or str(item.get('st','1'))!='1':continue
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
                sock=websocket.create_connection(WS,timeout=15)
                sock.settimeout(10)
                sock.send(json.dumps({'method':'SUBSCRIBE','params':[s.lower()+'@aggTrade' for s in sorted(target)],'id':'radar-v82'}))
                with self.lock:
                    self.followed=set(target)
                    self.trades={s:self.trades.get(s,TradeState()) for s in target}
                backoff=2;connected=time.monotonic()
                LOG.info('TRADES subscribed %d',len(target))
                while not STOP.is_set():
                    with self.lock:changed=target!=self.next_follow
                    if changed and time.monotonic()-connected>10:break
                    try:item=json.loads(sock.recv())
                    except websocket.WebSocketTimeoutException:continue
                    if isinstance(item,dict):self.on_trade(item.get('data',item))
            except Exception as exc:
                LOG.warning('TRADES disconnected: %s',exc)
                STOP.wait(backoff+random.random());backoff=min(60,backoff*2)
            finally:
                with self.lock:self.followed.clear()
                if sock:
                    try:sock.close()
                    except Exception:pass
    def run(self):
        self.bootstrap()
        self.enqueue(f'ALPER Radar V8.2 başladı | {len(self.allowed)} USDT perpetual | yalnızca sanal takip',120,'startup')
        for worker in (self.notifier,self.market_worker,self.trade_worker,self.trend_worker):
            threading.Thread(target=worker,daemon=True,name=worker.__name__).start()
        while not STOP.wait(5):
            t=now_ms()
            if t-self.candidate_updated>REFRESH*1000 or (not self.next_follow and t-self.candidate_updated>15000):
                self.choose_follow()
            if t-self.last_status>60000:
                self.db.expire_watches(t)
                LOG.info('HEALTH market_age=%.1fs trade_age=%.1fs followed=%d queue=%d',
                         (t-self.last_market)/1000 if self.last_market else -1,
                         (t-self.last_trade)/1000 if self.last_trade else -1,
                         len(self.followed),self.notify.qsize())
                self.last_status=t
            if t-self.last_report>=REPORT_INTERVAL*1000:
                LOG.info(self.db.report())
                self.last_report=t

def self_test():
    import tempfile
    original_flow = MIN_FLOW_USDT
    with tempfile.TemporaryDirectory() as temp:
        db=Database(str(Path(temp)/'test.sqlite3'))
        t=now_ms()
        # 90-second directional buying with volume acceleration and persistent participation.
        trades=[]
        for i in range(100):
            stamp=t-89_000+i*890
            px=100+.12*i/99
            trades.append((stamp,px,300 if i<50 else 900,1 if i%10 else -1))
        value=analyze(trades,100.12,t,t,'confirm')
        assert value and value['direction']=='LONG',value
        assert analyze(trades,100.12,t-8000,t,'confirm') is None
        assert analyze(trades,99.9,t,t,'confirm') is None
        short=[(stamp,200-2*(price-100),flow,-side) for stamp,price,flow,side in trades]
        val=analyze(short,199.76,t,t,'confirm')
        assert val and val['direction']=='SHORT',val
        db.watch(t-1000,'TESTUSDT',value)
        id_,tp,sl=db.confirmed(t,'TESTUSDT',value)
        assert db.recent('signals','TESTUSDT','LONG',t,COOLDOWN_SECONDS)
        assert db.update_price('TESTUSDT',tp+.01,t+1000)==[(id_,'TP')]
        assert db.update_price('TESTUSDT',sl-.01,t+2000)==[]
        assert db.con.execute("SELECT status FROM watches LIMIT 1").fetchone()[0]=='CONFIRMED'
        radar=Radar(db)
        radar.enqueue('PREVIEW',kind='watch')
        radar.enqueue('START',kind='startup')
        assert radar.notify.qsize()==0, 'watch and startup must not reach Telegram'
        radar.enqueue('CONFIRM',kind='entry')
        radar.enqueue('TP',kind='result')
        assert [radar.notify.get_nowait()[0] for _ in range(2)]==['CONFIRM','TP']
        assert 'TP=1' in db.report()
        assert 'trend_15m' in {r[1] for r in db.con.execute('PRAGMA table_info(signals)')}
    print('PASS V8.3: LONG, SHORT, stale rejection, countertrend rejection, watch confirmation, cooldown, TP closure, Telegram quiet filter')

def main():
    logging.basicConfig(level=logging.INFO,format='%(asctime)s %(levelname)s %(message)s')
    if '--self-test' in sys.argv:return self_test()
    if websocket is None:sys.exit('Install dependency: pip install websocket-client==1.9.0')
    lock_path=Path(os.getenv('RADAR_LOCK','/tmp/alper_radar_v82.lock'))
    lock_path.parent.mkdir(parents=True,exist_ok=True)
    with lock_path.open('w') as lock:
        try:fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError:sys.exit('Another V8.2 radar on this host')
        def close(signum,frame):STOP.set()
        signal.signal(signal.SIGTERM,close);signal.signal(signal.SIGINT,close)
        while not STOP.is_set():
            try:Radar().run()
            except Exception:
                LOG.exception('Radar failure, restarting in 30s')
                STOP.wait(30)

if __name__=='__main__':main()