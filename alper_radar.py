"""ALPER Radar V8.1 — experimental Binance USD-M perpetual early-warning PAPER radar.

Python 3.11+. Install: pip install -r requirements.txt
Start: python -u alper_radar.py     Test: python alper_radar.py --self-test
Environment: TELEGRAM_TOKEN, TELEGRAM_CHAT_ID (optional for offline testing).
No account keys, no orders. Signals are hypotheses, not guaranteed predictions.

Design: all market mini-tickers via ONE stream; rotate a limited list of
aggTrade streams for *actual executed buy/sell flow*; reject stale, chased,
low-liquidity signals; SQLite outcomes; cooldown; retry stale-safe notifications.
Order-book/OI are deliberately NOT claimed in this version.
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
import sys
import threading
import time
import urllib.error
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
# Both legacy /ws/ and new /market/ws/ address formats exist; current docs show /market/ws/.
WS = os.getenv('BINANCE_WS_BASE', 'wss://fstream.binance.com/market/ws')
DB = os.getenv('RADAR_DB', 'alper_radar.sqlite3')
MIN_QUOTE = float(os.getenv('MIN_24H_QUOTE', '15000000'))
FOLLOW_COUNT = max(5, min(40, int(os.getenv('FOLLOW_COUNT', '20'))))
REFRESH = max(180, int(os.getenv('FOLLOW_REFRESH_SECONDS', '240')))
MIN_TRADES = max(8, int(os.getenv('MIN_AGG_TRADES', '20')))
MIN_FLOW_USDT = float(os.getenv('MIN_FLOW_USDT', '12000'))
FLOW_RATIO = max(0.65, float(os.getenv('FLOW_RATIO', '0.72')))
BURST_RATIO = max(1.3, float(os.getenv('BURST_RATIO', '1.8')))
MAX_CHASE_PCT = float(os.getenv('MAX_CHASE_PCT', '0.30'))
MAX_SQUEEZE_PCT = float(os.getenv('MAX_SQUEEZE_PCT', '0.65'))
ALERT_TTL = 25
COOLDOWN_SECONDS = 3 * 60 * 60
MIN_DIRECTION_MOVE_PCT = 0.045  # confirmed movement, not a predicted 3-5 minute lead
MAX_DIRECTION_MOVE_PCT = 0.24
MIN_RECENT_PRICE_MOVE_PCT = 0.015
TOKEN = os.getenv('TELEGRAM_TOKEN', '').strip()
CHAT = os.getenv('TELEGRAM_CHAT_ID', '').strip()
STOP = threading.Event()


def now_ms(): return int(time.time() * 1000)

def safe_num(x, default=0.0):
    try:
        val = float(x)
        return val if math.isfinite(val) else default
    except (TypeError, ValueError, OverflowError):
        return default


def api_json(path, params=None):
    url = REST + path
    if params: url += '?' + urllib.parse.urlencode(params)
    request = urllib.request.Request(url, headers={'User-Agent': 'ALPER-Paper-Radar/8.1'})
    with urllib.request.urlopen(request, timeout=20) as resp:
        return json.load(resp)


def universe():
    """Only trading USDT perpetual USD-M contracts; avoids COIN-M mixed streams."""
    d = api_json('/fapi/v1/exchangeInfo')
    return {x['symbol'] for x in d.get('symbols', []) if
            x.get('status') == 'TRADING' and x.get('contractType') == 'PERPETUAL'
            and x.get('quoteAsset') == 'USDT'}


@dataclass
class PriceState:
    price: float = 0.0
    quote: float = 0.0
    updated: int = 0
    history: collections.deque = field(default_factory=lambda: collections.deque(maxlen=360))


@dataclass
class TradeState:
    trades: collections.deque = field(default_factory=collections.deque)
    started: int = 0


def compute_signal(trades, price, fresh_ms, at_ms=None):
    """Pure scoring routine; return signal dict or None. Needs >90s market history.

    Two 45-second windows: current volume must accelerate vs prior window;
    current buyer/seller taker aggression strong; price should still be close
    to start-of-window (the move should NOT already have happened).
    """
    at_ms = at_ms or now_ms()
    if at_ms - fresh_ms > 4500 or price <= 0: return None
    recent = [(t,p,q,side) for t,p,q,side in trades if 0 <= at_ms-t <= 90000]
    first = [(t,p,q,side) for t,p,q,side in recent if at_ms-t > 45000]
    last = [(t,p,q,side) for t,p,q,side in recent if at_ms-t <= 45000]
    if len(first) < MIN_TRADES or len(last) < MIN_TRADES: return None
    before = sum(x[2] for x in first)
    current = sum(x[2] for x in last)
    if before < MIN_FLOW_USDT / 4 or current < MIN_FLOW_USDT: return None
    if current / max(before, 1) < BURST_RATIO: return None
    buying = sum(x[2] for x in last if x[3] == 1)
    frac = buying / current
    if frac >= FLOW_RATIO: direction = 'LONG'; strength = frac
    elif (1-frac) >= FLOW_RATIO: direction = 'SHORT'; strength = 1-frac
    else: return None
    prices = [x[1] for x in last] + [price]
    lo, hi = min(prices), max(prices)
    if (hi-lo) / price * 100 > MAX_SQUEEZE_PCT: return None
    first_price = first[-1][1]
    drift_pct = (price / first_price - 1) * 100
    # Avoid chasing a move already underway in intended direction.
    aligned_move = drift_pct if direction == 'LONG' else -drift_pct
    # Do not confuse contrarian absorption / reversal with continuation:
    # taker buy pressure plus a falling price is NOT an automatic long.
    if not MIN_DIRECTION_MOVE_PCT <= aligned_move <= min(MAX_DIRECTION_MOVE_PCT, MAX_CHASE_PCT): return None
    # The final 22s must also agree with the signal direction.
    recent_start = min((x for x in last if at_ms-x[0] <= 22000), key=lambda x:x[0], default=None)
    if recent_start is None: return None
    recent_move = (price / recent_start[1] - 1) * 100
    aligned_recent = recent_move if direction == 'LONG' else -recent_move
    if aligned_recent < MIN_RECENT_PRICE_MOVE_PCT: return None
    # Require order-flow persistence across subwindows; one large print isn't enough.
    halves = ([x for x in last if at_ms-x[0] > 22000],
              [x for x in last if at_ms-x[0] <= 22000])
    for part in halves:
        volume = sum(x[2] for x in part)
        if len(part) < 6 or volume <= 0: return None
        buy = sum(x[2] for x in part if x[3] == 1)
        same = buy / volume if direction == 'LONG' else 1-buy / volume
        if same < 0.62: return None
    return {'direction': direction, 'strength': strength,
            'burst': current/max(before,1), 'price': price,
            'flow': current, 'age_ms': at_ms-fresh_ms, 'confirmed_move_pct': aligned_move}


class Database:
    def __init__(self, path=DB):
        self.con = sqlite3.connect(path, check_same_thread=False, timeout=20)
        self.lock = threading.RLock()
        with self.lock:
            self.con.execute('PRAGMA journal_mode=WAL')
            self.con.execute('''CREATE TABLE IF NOT EXISTS signals(
              id INTEGER PRIMARY KEY, created INTEGER NOT NULL, symbol TEXT NOT NULL,
              direction TEXT NOT NULL, entry REAL NOT NULL, tp REAL NOT NULL,
              stop REAL NOT NULL, status TEXT NOT NULL DEFAULT 'OPEN',
              closed INTEGER, exit_price REAL, flow_ratio REAL, burst REAL)''')
            self.con.execute('CREATE INDEX IF NOT EXISTS open_status ON signals(status,symbol)')
            self.con.commit()
    def recent(self, symbol, direction, timestamp):
        with self.lock:
            result = self.con.execute('SELECT 1 FROM signals WHERE symbol=? AND direction=? AND created>? LIMIT 1',
                      (symbol,direction,timestamp-COOLDOWN_SECONDS*1000)).fetchone()
        return result is not None
    def insert(self, timestamp, symbol, value):
        p = value['price']; is_long = value['direction']=='LONG'
        tp = p * (1.015 if is_long else .985)
        sl = p * (.9925 if is_long else 1.0075)
        with self.lock:
            cur=self.con.execute('''INSERT INTO signals(created,symbol,direction,entry,tp,stop,status,flow_ratio,burst)
                                    VALUES(?,?,?,?,?,?,'OPEN',?,?)''',
                      (timestamp,symbol,value['direction'],p,tp,sl,value['strength'],value['burst']))
            self.con.commit()
            return cur.lastrowid, tp, sl
    def update_price(self, symbol, price, timestamp):
        with self.lock:
            rows=self.con.execute("SELECT id,direction,tp,stop FROM signals WHERE status='OPEN' AND symbol=?",(symbol,)).fetchall()
            completed=[]
            for id_,side,tp,sl in rows:
                tp_hit = price>=tp if side=='LONG' else price<=tp
                sl_hit = price<=sl if side=='LONG' else price>=sl
                status='TP' if tp_hit else 'STOP' if sl_hit else None
                if status:
                    self.con.execute('UPDATE signals SET status=?,closed=?,exit_price=? WHERE id=?',
                                     (status,timestamp,price,id_))
                    completed.append((id_,status))
            if completed: self.con.commit()
        return completed


class Radar:
    def __init__(self, database=None):
        self.db=database or Database()
        self.lock=threading.RLock()
        self.states={}
        self.trades={}
        self.allowed=set()
        self.followed=set()
        self.next_follow=set()
        self.last_market=0
        self.last_trade=0
        self.notify=queue.Queue(maxsize=500)
        self.last_status=0
        self.rotation=0
        self.candidate_updated=0
        self.follow_since={}

    def bootstrap(self):
        # Fail closed: no unverified symbols or stale ticker samples.
        self.allowed=universe()
        LOG.info('Loaded %d active USDT perpetual contracts',len(self.allowed))
        for item in api_json('/fapi/v1/ticker/24hr'):
            s=item.get('symbol','')
            if s in self.allowed:
                self.on_price(s, safe_num(item.get('lastPrice')), safe_num(item.get('quoteVolume')), now_ms())

    def on_price(self, symbol, price, quote, ts):
        if symbol not in self.allowed or price <= 0: return
        with self.lock:
            state=self.states.setdefault(symbol,PriceState())
            # Reject out-of-order stream updates.
            if ts < state.updated: return
            state.price=price; state.quote=quote; state.updated=ts
            if not state.history or ts-state.history[-1][0] >= 1500:
                state.history.append((ts,price))
            self.last_market=now_ms()
        outcomes=self.db.update_price(symbol,price,ts)
        for id_,status in outcomes:
            self.enqueue(f'ALPER Radar | Deneme sonucu #{id_}: {symbol} {status} (görülen fiyat {price:.9g})',
                         now_ms(), ttl=180, kind='result')

    def on_trade(self, item):
        if item.get('e') != 'aggTrade': return
        symbol=item.get('s','')
        ts=int(item.get('T',0)); price=safe_num(item.get('p')); qty=safe_num(item.get('q'))
        if symbol not in self.allowed or price<=0 or qty<=0 or abs(now_ms()-ts)>5000: return
        side = -1 if item.get('m') is True else 1 # m=true: buyer was maker => aggressive sell
        with self.lock:
            if symbol not in self.followed: return
            trade=self.trades.setdefault(symbol,TradeState(started=ts))
            trade.trades.append((ts,price,price*qty,side))
            while trade.trades and ts-trade.trades[0][0] > 95000: trade.trades.popleft()
            self.last_trade=now_ms()
            p=self.states.get(symbol)
            if not p or now_ms()-p.updated>4500: return
            val=compute_signal(trade.trades,p.price,p.updated,now_ms())
        if val is None or self.db.recent(symbol,val['direction'],ts): return
        # Enforce one paper position per symbol, preventing opposing repeated alerts.
        with self.db.lock:
            if self.db.con.execute('SELECT 1 FROM signals WHERE symbol=? AND status="OPEN" LIMIT 1',(symbol,)).fetchone():
                return
        # Never emit a late trade observation.
        if now_ms()-ts > ALERT_TTL*1000: return
        id_,tp,sl=self.db.insert(ts,symbol,val)
        price=val['price']
        text=(f'🔎 ALPER V8 | ERKEN FIRSAT (DENEME)\n{symbol} | {val["direction"]}\n'
              f'Gözlenen fiyat: {price:.9g}\nÖrnek TP (+1.5%): {tp:.9g}\n'
              f'Örnek stop (-0.75%): {sl:.9g}\n'
              f'İşlem akışı baskısı: %{100*val["strength"]:.0f} | hacim ivmesi: {val["burst"]:.1f}x\n'
              f'Fiyat-yön teyidi: %{val["confirmed_move_pct"]:.2f} (zaten oluşmuş hareket)\n'
              f'Veri yaşı: {val["age_ms"]/1000:.1f} sn\n'
              f'İşlem tavsiyesi değildir. Hareketin 3–5 dk önce öngörülmesi garanti değildir.')
        self.enqueue(text,now_ms(),ttl=ALERT_TTL,kind='entry')
        LOG.info('EARLY %s %s price=%s burst=%.2f flow=%.2f',symbol,val['direction'],price,val['burst'],val['strength'])

    def choose_follow(self):
        """Broad overview every second, detailed trades for changing liquid shortlist.
        Selection intentionally doesn't pretend to monitor every coin at tick level.
        """
        ts=now_ms()
        with self.lock:
            available=[]
            for s,p in self.states.items():
                if p.quote<MIN_QUOTE or ts-p.updated>20000 or len(p.history)<3: continue
                old=next((v for t,v in reversed(p.history) if ts-t>=30000),None)
                if old is None or old<=0: continue
                change=abs(p.price/old-1)*100
                # Prefer emerging movement but avoid huge late jumps.
                if change>1.5: continue
                available.append((s,p.quote,change))
            liquid=sorted(available,key=lambda x:x[1],reverse=True)
            moving=sorted(available,key=lambda x:x[2],reverse=True)
            n1=FOLLOW_COUNT//2
            chosen=[x[0] for x in moving[:n1]]
            # Rotate the liquidity half so different contracts get detailed attention.
            rest=[x[0] for x in liquid if x[0] not in chosen]
            if rest:
                offset=self.rotation%len(rest)
                chosen += (rest[offset:]+rest[:offset])[:FOLLOW_COUNT-len(chosen)]
                self.rotation += FOLLOW_COUNT-len(chosen)
            # Keep subscriptions stable long enough to build two full 45s windows.
            # Replacing all streams every refresh would repeatedly reset warm-up.
            retain=[s for s in self.followed if s in {x[0] for x in available}]
            stable=retain[:FOLLOW_COUNT//2]
            merged=(stable+[s for s in chosen if s not in stable])[:FOLLOW_COUNT]
            self.next_follow=set(merged)
            self.candidate_updated=ts
        LOG.info('Coverage %d market contracts, eligible %d, detailed shortlist %d',
                 len(self.allowed),len(available),len(self.next_follow))

    def enqueue(self,message, created, ttl=ALERT_TTL,kind='entry'):
        try: self.notify.put_nowait((message,created,ttl,kind))
        except queue.Full: LOG.error('Notification queue full; discarded %s',kind)

    def notifier(self):
        while not STOP.is_set():
            try: message,created,ttl,kind=self.notify.get(timeout=1)
            except queue.Empty: continue
            if now_ms()-created>ttl*1000:
                LOG.warning('Dropped expired %s notification',kind); continue
            if not TOKEN or not CHAT:
                LOG.info('Telegram not configured; signal logged only: %s',message.splitlines()[0]);continue
            payload=urllib.parse.urlencode({'chat_id':CHAT,'text':message}).encode()
            for attempt in range(3):
                if now_ms()-created>ttl*1000: break
                try:
                    req=urllib.request.Request('https://api.telegram.org/bot'+TOKEN+'/sendMessage',data=payload)
                    with urllib.request.urlopen(req,timeout=10) as response:
                        result=json.load(response)
                    if result.get('ok'): break
                    raise RuntimeError('Telegram replied ok=false')
                except Exception as exc:
                    LOG.warning('Telegram %s attempt %d failed: %s',kind,attempt+1,exc)
                    STOP.wait(min(2**attempt,5))

    def stream_market(self):
        backoff=2
        while not STOP.is_set():
            sock=None
            try:
                sock=websocket.create_connection(WS+'/!miniTicker@arr',timeout=15,enable_multithread=True)
                sock.settimeout(15);LOG.info('MARKET websocket connected')
                backoff=2
                while not STOP.is_set():
                    raw=sock.recv()
                    arr=json.loads(raw)
                    if isinstance(arr,dict): arr=arr.get('data',[])
                    if not isinstance(arr,list): continue
                    current=now_ms()
                    for i in arr:
                        if not isinstance(i,dict) or str(i.get('st',1))!='1': continue
                        s=i.get('s','')
                        # E is exchange event time; skip delayed/out-of-order events.
                        event=int(i.get('E',current))
                        if abs(current-event)>10000: continue
                        self.on_price(s,safe_num(i.get('c')),safe_num(i.get('q')),event)
            except Exception as exc:
                LOG.warning('MARKET disconnected: %s',exc)
            finally:
                if sock:
                    try:sock.close()
                    except Exception:pass
            STOP.wait(backoff+random.random());backoff=min(60,backoff*2)

    def stream_trades(self):
        backoff=2
        while not STOP.is_set():
            sock=None
            with self.lock: target=set(self.next_follow)
            if not target:
                STOP.wait(3);continue
            try:
                sock=websocket.create_connection(WS,timeout=15,enable_multithread=True)
                sock.settimeout(10)
                streams=[f'{s.lower()}@aggTrade' for s in sorted(target)]
                sock.send(json.dumps({'method':'SUBSCRIBE','params':streams,'id':'radar-trades'}))
                with self.lock:
                    self.followed=set(target)
                    self.trades={s:self.trades.get(s,TradeState()) for s in target}
                LOG.info('TRADES subscribed=%d',len(target));backoff=2
                connected=time.monotonic()
                while not STOP.is_set():
                    with self.lock: changed= self.next_follow != target
                    if changed and time.monotonic()-connected>10: break
                    try: item=json.loads(sock.recv())
                    except websocket.WebSocketTimeoutException: continue
                    if isinstance(item,dict):
                        self.on_trade(item.get('data',item))
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
        self.enqueue(f'ALPER Radar V8.1 başladı | {len(self.allowed)} USDT perpetual | DENEME, emir yok',now_ms(),120,'startup')
        for fn in (self.notifier,self.stream_market,self.stream_trades):
            threading.Thread(target=fn,daemon=True,name=fn.__name__).start()
        while not STOP.wait(5):
            t=now_ms()
            if t-self.candidate_updated>REFRESH*1000:self.choose_follow()
            if t-self.last_status>60000:
                LOG.info('HEALTH market_age=%.1fs trade_age=%.1fs followed=%d queued=%d',
                         (t-self.last_market)/1000 if self.last_market else -1,
                         (t-self.last_trade)/1000 if self.last_trade else -1,
                         len(self.followed),self.notify.qsize())
                self.last_status=t
            if self.last_market and t-self.last_market>90000:
                LOG.error('Market feed stale >90s; watchdog will restart process')
                # Stream worker reconnects itself. Do not spawn duplicate threads here.


def self_test():
    import tempfile
    # These tests exercise core logic, not live Binance uptime or predictive profitability.
    with tempfile.TemporaryDirectory() as d:
        db=Database(str(Path(d)/'test.db'))
        rd=Radar(db);rd.allowed={'TESTUSDT'}
        t=now_ms();p=100.0
        rd.on_price('TESTUSDT',p,20000000,t)
        trades=[]
        for i in range(100):
            ts=t-89000+i*890
            q=300 if i<50 else 900
            px = 100.0 + 0.12*i/99
            trades.append((ts,px,q,1 if i%10!=0 else -1))
        result=compute_signal(trades,100.12,t,t)
        assert result and result['direction']=='LONG',result
        assert compute_signal(trades,100.12,t-8000,t) is None
        assert compute_signal(trades,99.90,t,t) is None  # buying into down-move must not trigger LONG
        assert compute_signal(trades,102.0,t,t) is None
        id_,tp,sl=db.insert(t,'TESTUSDT',result)
        assert db.recent('TESTUSDT','LONG',t)
        assert db.update_price('TESTUSDT',tp+.001,t+1000)[0]==(id_,'TP')
        assert not db.update_price('TESTUSDT',sl-.001,t+2000)
        short_trades=[(ts,200-2*(px-100),q,-side) for ts,px,q,side in trades]
        short=compute_signal(short_trades,199.76,t,t)
        assert short and short['direction']=='SHORT',short
        assert compute_signal(short_trades,200.10,t,t) is None
        rd.on_price('TESTUSDT',101.0,20000000,t+3000)
        assert len(rd.states)==1
    print('PASS: bidirectional price confirmation, countertrend guard, freshness, chase filter, cooldown, SQLite TP, duplicate closure')


def main():
    logging.basicConfig(level=logging.INFO,format='%(asctime)s %(levelname)s %(message)s')
    if '--self-test' in sys.argv:return self_test()
    if websocket is None:sys.exit('Missing dependency: pip install websocket-client==1.9.0')
    # Linux Render single-process guard; Render services across separate hosts require a single deployment.
    lock_path=Path(os.getenv('RADAR_LOCK','/tmp/alper_radar_v8.lock'))
    lock_path.parent.mkdir(parents=True,exist_ok=True)
    lock=lock_path.open('w')
    try:fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    except BlockingIOError:sys.exit('Another radar process is already active on this host')
    def terminate(_signum,_frame):STOP.set()
    signal.signal(signal.SIGTERM,terminate)
    signal.signal(signal.SIGINT,terminate)
    while not STOP.is_set():
        try:Radar().run()
        except Exception:
            LOG.exception('Radar encountered failure; restart in 30 seconds')
            STOP.wait(30)

if __name__=='__main__':main()