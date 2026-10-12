"""ALPER Radar V8.4 | PAPER ONLY, Binance USD-M USDT perpetuals.
Python 3.11+; pip install websocket-client==1.9.0
Run: python -u alper_radar.py | Test: python alper_radar.py --self-test
Environment: TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, FOLLOW_COUNT=60,
FOLLOW_REFRESH_SECONDS=60, RADAR_DB=/var/data/alper_radar.sqlite3 (persistent Render disk),
MIN_24H_QUOTE=15000000. No orders, no exchange account keys.
This is an experimental paper signal system, NOT a validated trading strategy.
"""
from __future__ import annotations
import collections
import datetime as dt
import fcntl
import json
import logging
import math
import os
import queue
import random
import signal
import sqlite3
import statistics
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
FOLLOW_COUNT = max(5, min(100, int(os.getenv('FOLLOW_COUNT', '60'))))
REFRESH = max(30, int(os.getenv('FOLLOW_REFRESH_SECONDS', '60')))
MIN_QUOTE = float(os.getenv('MIN_24H_QUOTE', '15000000'))
MIN_TRADES = max(8, int(os.getenv('MIN_AGG_TRADES', '20')))
MIN_FLOW_USDT = float(os.getenv('MIN_FLOW_USDT', '12000'))
FLOW_RATIO = max(.65, min(.95, float(os.getenv('FLOW_RATIO', '.72'))))
BURST_RATIO = max(1.3, float(os.getenv('BURST_RATIO', '1.8')))
COOLDOWN_SECONDS = 3 * 3600
WATCH_COOLDOWN = 20 * 60
REPORT_INTERVAL = max(1800, int(os.getenv('REPORT_INTERVAL_SECONDS', '3600')))
TREND_REFRESH = max(300, int(os.getenv('TREND_REFRESH_SECONDS', '900')))
STOP = threading.Event()

def now_ms(): return int(time.time() * 1000)
def number(x, default=0.):
    try:
        v = float(x)
        return v if math.isfinite(v) else default
    except (TypeError, ValueError, OverflowError): return default

def api(path):
    req = urllib.request.Request(REST + path, headers={'User-Agent': 'ALPER-Paper-Radar/8.4'})
    with urllib.request.urlopen(req, timeout=20) as response: return json.load(response)

def universe():
    data = api('/fapi/v1/exchangeInfo')
    return {x['symbol'] for x in data.get('symbols', []) if x.get('status') == 'TRADING'
            and x.get('contractType') == 'PERPETUAL' and x.get('quoteAsset') == 'USDT'}

@dataclass
class PriceState:
    price: float = 0.
    quote: float = 0.
    updated: int = 0
    history: collections.deque = field(default_factory=lambda: collections.deque(maxlen=180))

@dataclass
class TradeState:
    trades: collections.deque = field(default_factory=collections.deque)
    started: int = 0

def analyze(trades, price, updated, at=None, mode='confirm'):
    """Unmodified V8.3 watch / confirmation filters."""
    at = now_ms() if at is None else at
    if price <= 0 or not 0 <= at - updated <= 4500: return None
    window = 20_000 if mode == 'watch' else 45_000
    minimum_trades = max(7, MIN_TRADES // 2) if mode == 'watch' else MIN_TRADES
    chosen = [(t,p,q,side) for t,p,q,side in trades if 0 <= at-t <= 2*window]
    first = [x for x in chosen if at-x[0] > window]
    last = [x for x in chosen if at-x[0] <= window]
    if len(first) < minimum_trades or len(last) < minimum_trades: return None
    before = sum(x[2] for x in first)
    current = sum(x[2] for x in last)
    min_flow = MIN_FLOW_USDT*.65 if mode == 'watch' else MIN_FLOW_USDT
    min_burst = max(1.3, BURST_RATIO*.85) if mode == 'watch' else BURST_RATIO
    min_ratio = max(.65, FLOW_RATIO-.04) if mode == 'watch' else FLOW_RATIO
    if before < min_flow/4 or current < min_flow: return None
    burst = current/max(before,1)
    if burst < min_burst: return None
    buys = sum(x[2] for x in last if x[3] == 1)
    frac = buys/current
    if frac >= min_ratio: direction, strength = 'LONG', frac
    elif 1-frac >= min_ratio: direction, strength = 'SHORT', 1-frac
    else: return None
    drift = (price/first[-1][1]-1)*100
    move = drift if direction == 'LONG' else -drift
    lower = .02 if mode == 'watch' else .045
    upper = .18 if mode == 'watch' else .24
    if not lower <= move <= upper: return None
    prices = [x[1] for x in last] + [price]
    if (max(prices)-min(prices))/price*100 > (.40 if mode == 'watch' else .65): return None
    half_window = window//2
    halves = ([x for x in last if at-x[0] > half_window],
              [x for x in last if at-x[0] <= half_window])
    for half in halves:
        volume = sum(x[2] for x in half)
        if len(half) < 4 or volume <= 0: return None
        buy = sum(x[2] for x in half if x[3] == 1)
        same = buy/volume if direction == 'LONG' else 1-buy/volume
        if same < .60: return None
    recent = min(halves[1], key=lambda x:x[0], default=None)
    if recent is None: return None
    last_move = (price/recent[1]-1)*100*(1 if direction == 'LONG' else -1)
    if last_move < (.008 if mode == 'watch' else .015): return None
    return {'direction': direction, 'price': price, 'strength': strength,
            'burst': burst, 'flow': current, 'move': move, 'age_ms': at-updated}

class Database:
    def __init__(self, path=DB):
        self.lock = threading.RLock()
        if path != ':memory:': Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.con = sqlite3.connect(path, check_same_thread=False, timeout=20)
        with self.lock:
            self.con.execute('PRAGMA journal_mode=WAL')
            self.con.execute('''CREATE TABLE IF NOT EXISTS signals(
                id INTEGER PRIMARY KEY, created INTEGER NOT NULL, symbol TEXT NOT NULL,
                direction TEXT NOT NULL, entry REAL NOT NULL, tp REAL NOT NULL,
                stop REAL NOT NULL, status TEXT NOT NULL DEFAULT 'OPEN',
                closed INTEGER, exit_price REAL, flow_ratio REAL, burst REAL)''')
            existing = {r[1] for r in self.con.execute('PRAGMA table_info(signals)')}
            for col,typ in [('watch_delay_ms','INTEGER'),('telegram_delay_ms','INTEGER'),
                            ('trend_5m','TEXT'),('trend_15m','TEXT'),('trend_1h','TEXT'),
                            ('gross_pct','REAL'),('net_est_pct','REAL')]:
                if col not in existing: self.con.execute(f'ALTER TABLE signals ADD COLUMN {col} {typ}')
            self.con.execute('CREATE INDEX IF NOT EXISTS open_status ON signals(status,symbol)')
            self.con.execute('''CREATE TABLE IF NOT EXISTS watches(
                id INTEGER PRIMARY KEY, created INTEGER NOT NULL, symbol TEXT NOT NULL,
                direction TEXT NOT NULL, price REAL NOT NULL,
                status TEXT NOT NULL DEFAULT 'WATCH', resolved INTEGER)''')
            self.con.execute('''CREATE TABLE IF NOT EXISTS notification_outbox(
                id INTEGER PRIMARY KEY, signal_id INTEGER NOT NULL,
                kind TEXT NOT NULL, message TEXT NOT NULL,
                created INTEGER NOT NULL, delivered INTEGER, attempts INTEGER NOT NULL DEFAULT 0,
                UNIQUE(signal_id,kind))''')
            self.con.commit()

    def recent(self, table, sym, direction, ts, period):
        if table not in ('signals', 'watches'): raise ValueError('Invalid table')
        with self.lock:
            return self.con.execute(f'SELECT 1 FROM {table} WHERE symbol=? AND direction=? AND created>? LIMIT 1',
                                    (sym,direction,ts-period*1000)).fetchone() is not None

    def open_exists(self, sym):
        with self.lock:
            return self.con.execute("SELECT 1 FROM signals WHERE symbol=? AND status='OPEN' LIMIT 1", (sym,)).fetchone() is not None

    def open_symbols(self):
        with self.lock:
            return {r[0] for r in self.con.execute("SELECT DISTINCT symbol FROM signals WHERE status='OPEN'")}

    def watch(self, ts, sym, val):
        with self.lock:
            self.con.execute('INSERT INTO watches(created,symbol,direction,price) VALUES(?,?,?,?)',
                             (ts,sym,val['direction'],val['price']))
            self.con.commit()

    def confirmed_if_eligible(self, ts, sym, val, trend=None):
        p=val['price']; direction=val['direction']; islong=direction=='LONG'
        tp=p*(1.015 if islong else .985)
        sl=p*(.9925 if islong else 1.0075)
        with self.lock:
            if self.open_exists(sym) or self.recent('signals',sym,direction,ts,COOLDOWN_SECONDS): return None
            row=self.con.execute("SELECT created FROM watches WHERE symbol=? AND direction=? AND status='WATCH' AND created<? AND created>=? ORDER BY created DESC LIMIT 1",
                                 (sym,direction,ts,ts-10*60*1000)).fetchone()
            if not row:return None
            delay=ts-row[0]
            cur=self.con.execute('''INSERT INTO signals(created,symbol,direction,entry,tp,stop,status,flow_ratio,burst,
                watch_delay_ms,trend_5m,trend_15m,trend_1h) VALUES(?,?,?,?,?,?,'OPEN',?,?,?,?,?,?)''',
                (ts,sym,direction,p,tp,sl,val['strength'],val['burst'],delay,
                 (trend or {}).get('5m'),(trend or {}).get('15m'),(trend or {}).get('1h')))
            id_=cur.lastrowid
            self.con.execute("UPDATE watches SET status='CONFIRMED',resolved=? WHERE symbol=? AND direction=? AND status='WATCH' AND created>=?",
                             (ts,sym,direction,ts-10*60*1000))
            msg=(f'✅ ALPER V8.4 | ONAYLI SANAL {direction}\n{sym} | giriş referansı: {p:.9g}\n'
                 f'TP (+1.5%): {tp:.9g}\nSTOP (-0.75%): {sl:.9g}\n'
                 f'Akış baskısı %{val["strength"]*100:.0f}, ivme {val["burst"]:.1f}x | '
                 f'fiyat hareketi %{val["move"]:.2f}\nKayıt #{id_} | işlem tavsiyesi değildir.')
            self._outbox(id_,'entry',msg,ts)
            self.con.commit()
            return id_,tp,sl,delay

    def _outbox(self, id_, kind, msg, ts):
        self.con.execute('INSERT OR IGNORE INTO notification_outbox(signal_id,kind,message,created) VALUES(?,?,?,?)',
                         (id_,kind,msg,ts))

    def expire_watches(self, ts):
        with self.lock:
            self.con.execute("UPDATE watches SET status='EXPIRED',resolved=? WHERE status='WATCH' AND created<?",
                             (ts,ts-10*60*1000))
            self.con.commit()

    def update_price(self, sym, price, ts):
        """Atomic first-touch settlement; ignore ticks before signal creation."""
        if price <= 0:return []
        with self.lock:
            rows=self.con.execute("SELECT id,direction,entry,tp,stop,created FROM signals WHERE status='OPEN' AND symbol=?",
                                  (sym,)).fetchall()
            finished=[]
            for id_,direction,entry,tp,sl,created in rows:
                if ts < created:continue
                result=(('TP' if price >= tp else 'STOP' if price <= sl else None)
                        if direction=='LONG' else
                        ('TP' if price <= tp else 'STOP' if price >= sl else None))
                if not result:continue
                changed=self.con.execute("UPDATE signals SET status=?,closed=?,exit_price=? WHERE id=? AND status='OPEN'",
                                         (result,ts,price,id_)).rowcount
                if not changed:continue
                gross=(price/entry-1)*100*(1 if direction=='LONG' else -1)
                self.con.execute('UPDATE signals SET gross_pct=? WHERE id=?',(gross,id_))
                msg=(f'🏁 ALPER V8.4 | SANAL SONUÇ #{id_}\n{sym} {direction} | {result}\n'
                     f'Giriş: {entry:.9g} | gözlenen çıkış: {price:.9g}\n'
                     f'Brüt fiyat hareketi: {gross:+.2f}% (komisyon/kayma hariç)')
                self._outbox(id_,'result',msg,now_ms())
                finished.append((id_,result))
            if finished:self.con.commit()
            return finished

    def next_notification(self):
        with self.lock:
            return self.con.execute('''SELECT id,message,kind FROM notification_outbox
                 WHERE delivered IS NULL ORDER BY id LIMIT 1''').fetchone()

    def sent(self, id_):
        with self.lock:
            self.con.execute('UPDATE notification_outbox SET delivered=? WHERE id=? AND delivered IS NULL',(now_ms(),id_))
            self.con.commit()

    def failed(self, id_):
        with self.lock:
            self.con.execute('UPDATE notification_outbox SET attempts=attempts+1 WHERE id=?',(id_,))
            self.con.commit()

    def report(self):
        with self.lock:
            rows=self.con.execute('SELECT status,gross_pct FROM signals').fetchall()
            pending=self.con.execute('SELECT COUNT(*) FROM notification_outbox WHERE delivered IS NULL').fetchone()[0]
        counts=collections.Counter(r[0] for r in rows)
        closed=[r for r in rows if r[0] in ('TP','STOP')]
        win=sum(r[0]=='TP' for r in closed)
        observed=sum(r[1] or 0 for r in closed)
        return (f'REPORT all-time TP={counts["TP"]} STOP={counts["STOP"]} OPEN={counts["OPEN"]} '
                f'win={win}/{len(closed)} gross_observed_sum={observed:+.2f}% '
                f'(not portfolio return) telegram_pending={pending}')

class Radar:
    def __init__(self, db=None):
        self.db=db or Database()
        self.lock=threading.RLock()
        self.allowed=set(); self.states={}; self.trades={}; self.followed=set()
        self.next_follow=set(); self.rotation=0; self.candidate_updated=0
        self.last_market=0; self.last_trade=0; self.last_status=0
        self.trend={}; self.last_report=now_ms()

    def bootstrap(self):
        self.allowed=universe()
        ts=now_ms()
        for x in api('/fapi/v1/ticker/24hr'):
            sym=x.get('symbol','')
            if sym in self.allowed:self.on_price(sym,number(x.get('lastPrice')),number(x.get('quoteVolume')),ts,settle=False)
        LOG.info('Loaded %d Binance USDT perpetuals',len(self.allowed))
        LOG.info('Existing OPEN paper positions: %d',len(self.db.open_symbols()))

    def on_price(self,sym,price,quote,ts,settle=True):
        if sym not in self.allowed or price<=0:return
        with self.lock:
            p=self.states.setdefault(sym,PriceState())
            if ts<p.updated:return
            p.price=price;p.quote=quote;p.updated=ts
            if not p.history or ts-p.history[-1][0]>=1500:p.history.append((ts,price))
            self.last_market=now_ms()
        # Existing miniTicker stream remains a secondary fallback for results.
        if settle:self.close_from_price(sym,price,ts,'miniTicker')

    def close_from_price(self,sym,price,ts,source):
        for id_,result in self.db.update_price(sym,price,ts):
            LOG.info('RESULT %s #%d %s price=%.9g source=%s',result,id_,sym,price,source)

    def trend_snapshot(self,sym):
        with self.lock:
            row=self.trend.get(sym)
            return dict(row[0]) if row and now_ms()-row[1]<2*TREND_REFRESH*1000 else {}

    def trend_worker(self):
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
                        fast=sum(closes[-5:])/5;slow=sum(closes[-20:])/20
                        readings[interval]='LONG' if fast>slow else 'SHORT' if fast<slow else 'FLAT'
                    except Exception as exc:
                        LOG.warning('TREND unavailable %s %s: %s',sym,interval,exc)
                        break
                    STOP.wait(.5)
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
        watch=analyze(snapshot,price,updated,current,'watch')
        if watch and not self.db.recent('watches',sym,watch['direction'],current,WATCH_COOLDOWN):
            self.db.watch(current,sym,watch)
            LOG.info('WATCH %s %s recorded (Telegram muted)',sym,watch['direction'])
        confirmed=analyze(snapshot,price,updated,current,'confirm')
        if not confirmed:return
        recorded=self.db.confirmed_if_eligible(current,sym,confirmed,self.trend_snapshot(sym))
        if recorded:
            id_,tp,sl,delay=recorded
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
                if move>1.5:continue
                available.append((sym,p.quote,move))
            liquid=sorted(available,key=lambda x:x[1],reverse=True)
            moving=sorted(available,key=lambda x:x[2],reverse=True)
            eligible={x[0] for x in available}
            movers=[x[0] for x in moving[:FOLLOW_COUNT//2]]
            retain=sorted((s for s in self.followed if s in eligible),
                          key=lambda s:next((x[2] for x in available if x[0]==s),0),reverse=True)
            stable=retain[:FOLLOW_COUNT//2]
            rest=[x[0] for x in liquid if x[0] not in stable+movers]
            offset=self.rotation%len(rest) if rest else 0
            rotation=(rest[offset:]+rest[:offset]) if rest else []
            self.rotation+=max(1,FOLLOW_COUNT//2)
            chosen=list(dict.fromkeys(stable+movers+rotation))[:FOLLOW_COUNT]
            self.next_follow=set(chosen);self.candidate_updated=t
        LOG.info('Coverage %d eligible %d detailed %d',len(self.allowed),len(available),len(chosen))

    def notifier(self):
        while not STOP.is_set():
            row=self.db.next_notification()
            if not row:STOP.wait(1);continue
            id_,msg,kind=row
            if not TOKEN or not CHAT:
                LOG.warning('Telegram credentials missing; notification #%d pending',id_)
                STOP.wait(30);continue
            body=urllib.parse.urlencode({'chat_id':CHAT,'text':msg}).encode()
            try:
                request=urllib.request.Request('https://api.telegram.org/bot'+TOKEN+'/sendMessage',data=body)
                with urllib.request.urlopen(request,timeout=10) as response:out=json.load(response)
                if not out.get('ok'):raise RuntimeError('Telegram ok=false')
                self.db.sent(id_)
                LOG.info('TELEGRAM delivered notification=%d kind=%s',id_,kind)
            except Exception as exc:
                self.db.failed(id_)
                LOG.warning('TELEGRAM retry pending notification=%d kind=%s error=%s',id_,kind,exc)
                STOP.wait(10)

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
                sock.send(json.dumps({'method':'SUBSCRIBE','params':[s.lower()+'@aggTrade' for s in sorted(target)],'id':84}))
                with self.lock:
                    self.followed=set(target)
                    self.trades={s:self.trades.get(s,TradeState()) for s in target}
                backoff=2; connected=time.monotonic()
                LOG.info('TRADES subscribed %d',len(target))
                while not STOP.is_set():
                    with self.lock: changed=target!=self.next_follow
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

    def result_worker(self):
        """Separate trade-price feed for ALL open signals, even when rotated out."""
        backoff=2
        while not STOP.is_set():
            target=self.db.open_symbols()
            if not target:STOP.wait(1);continue
            sock=None
            try:
                sock=websocket.create_connection(WS,timeout=15)
                sock.settimeout(2)
                sock.send(json.dumps({'method':'SUBSCRIBE','params':[s.lower()+'@aggTrade' for s in sorted(target)],'id':85}))
                LOG.info('RESULT_TRACKER subscribed %d open symbols',len(target))
                backoff=2
                while not STOP.is_set():
                    if self.db.open_symbols()!=target:break
                    try:raw=json.loads(sock.recv())
                    except websocket.WebSocketTimeoutException:continue
                    item=raw.get('data',raw) if isinstance(raw,dict) else {}
                    if not isinstance(item,dict) or item.get('e')!='aggTrade':continue
                    sym=item.get('s','');price=number(item.get('p'))
                    ts=int(item.get('T') or 0)
                    if sym not in target or price<=0 or abs(now_ms()-ts)>10000:continue
                    self.close_from_price(sym,price,ts,'aggTrade')
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
            if t-self.candidate_updated>REFRESH*1000 or (not self.next_follow and t-self.candidate_updated>15000):
                self.choose_follow()
            if t-self.last_status>60000:
                self.db.expire_watches(t)
                LOG.info('HEALTH market_age=%.1fs trade_age=%.1fs followed=%d open=%d',
                         (t-self.last_market)/1000 if self.last_market else -1,
                         (t-self.last_trade)/1000 if self.last_trade else -1,
                         len(self.followed),len(self.db.open_symbols()))
                self.last_status=t
            if t-self.last_report>=REPORT_INTERVAL*1000:
                LOG.info(self.db.report());self.last_report=t

def self_test():
    import tempfile
    with tempfile.TemporaryDirectory() as temp:
        path=str(Path(temp)/'test.sqlite3')
        db=Database(path);t=now_ms()
        trades=[]
        for i in range(100):
            stamp=t-89_000+i*890
            px=100+.12*i/99
            trades.append((stamp,px,300 if i<50 else 900,1 if i%10 else -1))
        val=analyze(trades,100.12,t,t,'confirm')
        assert val and val['direction']=='LONG',val
        assert analyze(trades,100.12,t-8000,t,'confirm') is None
        short=[(stamp,200-2*(price-100),flow,-side) for stamp,price,flow,side in trades]
        sv=analyze(short,199.76,t,t,'confirm')
        assert sv and sv['direction']=='SHORT',sv
        assert db.confirmed_if_eligible(t,'TESTUSDT',val) is None
        db.watch(t-1000,'TESTUSDT',val)
        recorded=db.confirmed_if_eligible(t,'TESTUSDT',val)
        assert recorded and len(recorded)==4
        id_,tp,sl,_=recorded
        assert db.confirmed_if_eligible(t+1000,'TESTUSDT',val) is None
        assert db.open_symbols()=={'TESTUSDT'}
        assert db.update_price('TESTUSDT',tp+.01,t-100)==[]  # earlier trade can't settle
        assert db.update_price('TESTUSDT',tp+.01,t+1000)==[(id_,'TP')]
        assert db.update_price('TESTUSDT',sl-.01,t+2000)==[]
        assert db.open_symbols()==set()
        assert db.next_notification()[2]=='entry'
        first=db.next_notification()[0];db.sent(first)
        assert db.next_notification()[2]=='result'
        db.con.close()
        restored=Database(path)
        assert restored.next_notification()[2]=='result'
        assert 'TP=1' in restored.report()
        restored.watch(t-1000,'SHORTUSDT',sv)
        result=restored.confirmed_if_eligible(t,'SHORTUSDT',sv)
        assert result
        assert restored.update_price('SHORTUSDT',result[2]+.01,t+2000)==[(result[0],'STOP')]
        assert restored.update_price('SHORTUSDT',result[1]-.01,t+3000)==[]
        assert 'STOP=1' in restored.report()
    print('PASS V8.4: LONG/SHORT engine, watches, cooldown, TP/STOP, stale-tick guard, duplicate guard, persistent outbox, restart')

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
            except Exception:
                LOG.exception('Radar failure; restarting in 30 seconds')
                STOP.wait(30)

if __name__=='__main__':main()