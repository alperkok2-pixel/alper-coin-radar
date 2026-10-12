"""ALPER Radar V6.6 - Binance USD-M USDT perpetual PAPER-only radar.
Python 3.11+ standard library. No exchange keys, no order placement.
Run: python -u alper_radar_v66.py
Test: python alper_radar_v66.py --self-test
Env: TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, RADAR_DB, DETAIL_BATCH=12,
SCAN_SECONDS=60, REQUEST_GAP_SECONDS=2.5, MIN_24H_VOLUME=30000000,
MIN_SCORE=4, MIN_ADX=18, TP1_PCT=1.5, TP2_PCT=2.5, TP3_PCT=5,
MAX_STOP_PCT=1.5, SIGNAL_COOLDOWN_HOURS=12, PAPER_MODE=1.
Changes: confirmed pullback/retest, anti-exhaustion, closed-bar coherent trend
cache, rigorous candle-by-candle tracker; no win-rate claims.
"""
import datetime as dt
import json
import math
import os
import sqlite3
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

BASE = 'https://fapi.binance.com'
TOKEN = os.getenv('TELEGRAM_TOKEN', '').strip()
CHAT = os.getenv('TELEGRAM_CHAT_ID', '').strip()
DB = os.getenv('RADAR_DB', 'alper_radar_v66.sqlite3')
BATCH = max(2, min(20, int(os.getenv('DETAIL_BATCH', '12'))))
SCAN_SECONDS = max(60, int(os.getenv('SCAN_SECONDS', '60')))
GAP = max(2.2, float(os.getenv('REQUEST_GAP_SECONDS', '2.5')))
MIN_VOLUME = float(os.getenv('MIN_24H_VOLUME', '30000000'))
MIN_RATIO = float(os.getenv('MIN_VOLUME_RATIO', '1.3'))
MIN_SCORE = max(3, min(6, int(os.getenv('MIN_SCORE', '4'))))
MIN_ADX = float(os.getenv('MIN_ADX', '18'))
MAX_STOP = float(os.getenv('MAX_STOP_PCT', '1.5'))
COOLDOWN = float(os.getenv('SIGNAL_COOLDOWN_HOURS', '12')) * 3600
TPS = tuple(float(os.getenv(f'TP{i}_PCT', x)) for i, x in enumerate(('1.5','2.5','5'),1))
MONITOR_BATCH = max(1, min(12, int(os.getenv('MONITOR_BATCH', '3'))))
MAX_SIGNAL_AGE = max(60, int(os.getenv('MAX_SIGNAL_AGE_SECONDS','360')))
_last = 0.0
_pause = 0.0
_weight = 0
_weight_at = 0.0
cache = {}


def log(message):
    print(f"[{dt.datetime.now(dt.timezone.utc).isoformat(timespec='seconds')}] {message}", flush=True)


def fetch(path, params=None):
    global _last, _pause, _weight, _weight_at
    url = BASE + path + (('?' + urllib.parse.urlencode(params)) if params else '')
    for attempt in range(3):
        now = time.monotonic()
        wait = max(0, _pause-now, GAP-(now-_last))
        if _weight >= 450 and now-_weight_at < 65:
            wait = max(wait, 65-(now-_weight_at))
        if wait > 0:
            if wait > 8: log(f'API pacing {wait:.0f}s')
            time.sleep(wait)
        _last = time.monotonic()
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers={'User-Agent':'ALPER-Radar/6.6-paper'}), timeout=18) as response:
                w = response.headers.get('X-MBX-USED-WEIGHT-1M')
                if w is not None:
                    _weight = int(w); _weight_at = time.monotonic()
                return json.loads(response.read())
        except urllib.error.HTTPError as exc:
            if exc.code in (418,429):
                raw = exc.headers.get('Retry-After','180')
                try: retry = max(180, min(7200,int(raw)))
                except (TypeError, ValueError): retry = 180
                _pause = time.monotonic() + retry + 5
                raise RuntimeError(f'rate-limit {exc.code}: paused {retry+5}s') from exc
            if exc.code < 500 or attempt == 2: raise
        except (urllib.error.URLError,TimeoutError):
            if attempt == 2: raise
        time.sleep(2**attempt+1)
    raise RuntimeError('fetch failed')


def notify(s):
    if not TOKEN or not CHAT:
        log('Telegram not configured: '+s.replace('\n',' | ')); return False
    body = urllib.parse.urlencode({'chat_id':CHAT,'text':s}).encode()
    try:
        request = urllib.request.Request(f'https://api.telegram.org/bot{TOKEN}/sendMessage',data=body,method='POST')
        with urllib.request.urlopen(request,timeout=18) as r:
            return bool(json.loads(r.read()).get('ok'))
    except Exception as exc:
        log(f'Telegram failure: {exc}'); return False


def db_connect():
    path = Path(DB); path.parent.mkdir(parents=True, exist_ok=True)
    c = sqlite3.connect(path)
    c.execute('''CREATE TABLE IF NOT EXISTS trades (
        id INTEGER PRIMARY KEY, symbol TEXT, side TEXT, strategy TEXT,
        bar_close_ms INTEGER, entry REAL, stop REAL, tp1 REAL, tp2 REAL, tp3 REAL,
        opened INTEGER, status TEXT DEFAULT 'OPEN', closed INTEGER,
        last_checked_open_ms INTEGER, tp1_hit INTEGER DEFAULT 0,
        tp2_hit INTEGER DEFAULT 0, UNIQUE(symbol,side,bar_close_ms))''')
    c.execute('CREATE TABLE IF NOT EXISTS state (key TEXT PRIMARY KEY, val TEXT)')
    c.commit(); return c


def bars(symbol, interval, limit=120):
    # Cache by *next closed-bar boundary*, never time-since-request.
    seconds = {'5m':300,'15m':900,'1h':3600}[interval]
    now_ms = int(time.time()*1000)
    key = (symbol,interval,limit)
    entry = cache.get(key)
    if entry and entry[0] > now_ms: return entry[1]
    rows = fetch('/fapi/v1/klines',{'symbol':symbol,'interval':interval,'limit':limit})
    closed = [r for r in rows if int(r[6]) < now_ms-1800]
    if closed:
        # Inclusive close timestamp. Fetch again shortly after successor closes.
        expiry = int(closed[-1][6]) + seconds*1000 + 2200
        cache[key] = (max(now_ms+2000,expiry),closed)
    if len(cache)>1500:
        for k,v in list(cache.items()):
            if v[0] < now_ms: del cache[k]
    return closed


def ema(v,n):
    if len(v)<n:return None
    a=sum(v[:n])/n; q=2/(n+1)
    for x in v[n:]:a+=q*(x-a)
    return a


def rsi(v,n=14):
    if len(v)<=n:return None
    delta=[b-a for a,b in zip(v,v[1:])]
    g=sum(max(0,x) for x in delta[:n])/n
    l=sum(max(0,-x) for x in delta[:n])/n
    for d in delta[n:]:
        g=(g*(n-1)+max(0,d))/n; l=(l*(n-1)+max(0,-d))/n
    return 100 if l==0 else 100-100/(1+g/l)


def atr(rows,n=14):
    if len(rows)<n+1:return None
    vals=[]
    for prev,x in zip(rows,rows[1:]):
        h,l,pc=float(x[2]),float(x[3]),float(prev[4])
        vals.append(max(h-l,abs(h-pc),abs(l-pc)))
    a=sum(vals[:n])/n
    for x in vals[n:]:a=(a*(n-1)+x)/n
    return a


def adx(rows,n=14):
    if len(rows)<3*n+2:return None
    tr=[]; plus=[]; minus=[]
    for a,b in zip(rows,rows[1:]):
        ah,al,ac=float(a[2]),float(a[3]),float(a[4])
        bh,bl=float(b[2]),float(b[3]); up=bh-ah; down=al-bl
        tr.append(max(bh-bl,abs(bh-ac),abs(bl-ac)))
        plus.append(up if up>down and up>0 else 0)
        minus.append(down if down>up and down>0 else 0)
    def smoothed(v):
        x=sum(v[:n]); out=[x]
        for a in v[n:]:x=x-x/n+a;out.append(x)
        return out
    ts,ps,ms=smoothed(tr),smoothed(plus),smoothed(minus)
    dx=[]
    for t,p,m in zip(ts,ps,ms):
        p=100*p/t if t else 0; m=100*m/t if t else 0
        dx.append(100*abs(p-m)/(p+m) if p+m else 0)
    if len(dx)<n:return None
    x=sum(dx[:n])/n
    for d in dx[n:]:x=(x*(n-1)+d)/n
    return x


def macd_hist(v):
    if len(v)<40:return None
    def series(n):
        x=sum(v[:n])/n; result=[None]*(n-1)+[x]
        for a in v[n:]:x+=(a-x)*2/(n+1);result.append(x)
        return result
    f=series(12);s=series(26)
    line=[a-b for a,b in zip(f,s) if a is not None and b is not None]
    signal=sum(line[:9])/9
    for a in line[9:]:signal+=(a-signal)*0.2
    return line[-1]-signal


def evaluate(s5,s15,s1h):
    if min(map(len,(s5,s15,s1h)))<65:return None
    c5,c15,ch=([float(x[4]) for x in s] for s in (s5,s15,s1h))
    price=c5[-1]
    if price<=0:return None
    a=atr(s5);strength=adx(s15);hist=macd_hist(c15);r=rsi(c5)
    if None in (a,strength,hist,r) or a<=0 or strength<MIN_ADX:return None
    e5=ema(c5,21);e5_old=ema(c5[:-1],21)
    e15=ema(c15,21);e15slow=ema(c15,55)
    eh=ema(ch,21);ehslow=ema(ch,55)
    vol=float(s5[-1][7]);volbase=sum(float(x[7]) for x in s5[-21:-1])/20
    if volbase<=0:return None
    ratio=vol/volbase
    if ratio<MIN_RATIO or ratio>8: return None  # cap extreme-volume exhaustion
    prev=float(s5[-2][4]); op=float(s5[-1][1]); hi=float(s5[-1][2]);lo=float(s5[-1][3])
    width=max(hi-lo,price*1e-8); body=abs(price-op)/width
    if body>0.8 and abs(price-prev)>1.3*a:return None # chase-spike guard
    direction=0
    if eh>ehslow and e15>e15slow and c15[-1]>e15 and 48<=r<=67 and hist>0:direction=1
    if eh<ehslow and e15<e15slow and c15[-1]<e15 and 33<=r<=52 and hist<0:direction=-1
    if not direction:return None
    # Confirmed 5m EMA21 reclaim after a pullback; not immediate break-out chasing.
    near=abs(prev-e5_old)/e5_old<=max(.006,1.25*a/price)
    # Recent 5m close should have approached trend EMA in one of the last 4 candles.
    nearby=min(abs(float(x[4])-e5_old) for x in s5[-5:-1])/e5_old<max(.008,1.7*a/price)
    if not (near or nearby):return None
    if direction==1 and not (prev<=e5_old*1.004 and price>e5 and price>op and (hi-price)/width<.30):return None
    if direction==-1 and not (prev>=e5_old*.996 and price<e5 and price<op and (price-lo)/width<.30):return None
    # Do not chase a price already extended from 5m or 15m EMA.
    if abs(price-e5)>1.25*a or abs(price-e15)>3.0*a:return None
    score=2+int(strength>=25)+int(ratio>=1.7)+int(body>=.35)+int((r>=53 if direction==1 else r<=47))
    if score<MIN_SCORE:return None
    stop_pct=min(MAX_STOP,max(.6,2*a/price*100))
    if TPS[1]/stop_pct<1.5:return None
    side='LONG' if direction==1 else 'SHORT'
    return dict(side=side,strategy='PULLBACK_CONFIRM',score=score,adx=strength,rsi=r,
                volume_ratio=ratio,entry=price,stop_pct=stop_pct,
                stop=price*(1-direction*stop_pct/100),
                tp1=price*(1+direction*TPS[0]/100),
                tp2=price*(1+direction*TPS[1]/100),
                tp3=price*(1+direction*TPS[2]/100),close_ms=int(s5[-1][6]))


def stage(side,high,low,stop,tp1,tp2,tp3,hit1=False,hit2=False):
    reached=lambda p: high>=p if side=='LONG' else low<=p
    hitstop=low<=stop if side=='LONG' else high>=stop
    new1=reached(tp1);new2=reached(tp2);new3=reached(tp3)
    # Intrabar order indeterminate when STOP and an unhit TP are both touched.
    if hitstop and ((new1 and not hit1) or (new2 and not hit2) or new3):
        return hit1,hit2,'AMBIGUOUS',[]
    if hitstop:return hit1,hit2,'STOP',[]
    alerts=([1] if new1 and not hit1 else [])+([2] if new2 and not hit2 else [])
    if new3:alerts.append(3)
    return hit1 or new1,hit2 or new2,('TP3' if new3 else None),alerts


def monitor(con):
    active=con.execute("SELECT id,symbol,side,entry,stop,tp1,tp2,tp3,bar_close_ms,last_checked_open_ms,tp1_hit,tp2_hit,opened FROM trades WHERE status='OPEN' ORDER BY id").fetchall()
    if not active:return
    pos=int(state_get(con,'monitor_cursor','0'))%len(active)
    selected=(active[pos:]+active[:pos])[:MONITOR_BATCH]
    state_set(con,'monitor_cursor',str((pos+len(selected))%len(active)))
    for trade in selected:
        trade_id,symbol,side,entry,stop,tp1,tp2,tp3,close_ms,last_open,h1,h2,opened=trade
        try:
            rows=bars(symbol,'5m',150)
            # Start at first successor candle OPEN; compare consistently in open-time units.
            cursor=last_open if last_open is not None else (close_ms//300000)*300000
            newer=[x for x in rows if int(x[0])>cursor]
            if newer and int(newer[0][0])>cursor+300000:
                finish(con,trade_id,'UNKNOWN_GAP',symbol,side,entry,stop,h1,h2)
                continue
            resolved=False
            for row in newer:
                h1,h2,outcome,alerts=stage(side,float(row[2]),float(row[3]),stop,tp1,tp2,tp3,bool(h1),bool(h2))
                con.execute('UPDATE trades SET last_checked_open_ms=?,tp1_hit=?,tp2_hit=? WHERE id=?',(int(row[0]),int(h1),int(h2),trade_id));con.commit()
                for n in alerts:notify(f'🎯 PAPER {symbol} {side} TP{n} reached (+{TPS[n-1]:g}% price from reference)')
                if outcome:
                    finish(con,trade_id,outcome,symbol,side,entry,stop,h1,h2);resolved=True;break
            if not resolved and time.time()-opened>48*3600:
                finish(con,trade_id,'EXPIRED',symbol,side,entry,stop,h1,h2)
        except Exception as exc:
            if 'rate-limit' in str(exc):raise
            log(f'Monitor {symbol}: {type(exc).__name__}: {exc}')


def finish(con,tid,status,symbol,side,entry,stop,h1,h2):
    con.execute('UPDATE trades SET status=?,closed=? WHERE id=?',(status,int(time.time()),tid));con.commit()
    notify(f'📊 PAPER {symbol} {side}: {status}\nEntry {entry:.9g}; Stop {stop:.9g}\nTP1 {bool(h1)}; TP2 {bool(h2)}\nNot an executed trade')


def state_get(con,key,default=''):
    row=con.execute('SELECT val FROM state WHERE key=?',(key,)).fetchone()
    return row[0] if row else default


def state_set(con,key,value):
    con.execute('INSERT OR REPLACE INTO state(key,val) VALUES(?,?)',(key,value));con.commit()


def universe():
    exchange=fetch('/fapi/v1/exchangeInfo')
    return sorted(x['symbol'] for x in exchange['symbols'] if x.get('status')=='TRADING' and x.get('contractType')=='PERPETUAL' and x.get('quoteAsset')=='USDT')


def rank_market(tickers,valid):
    ranked=[]
    for t in tickers:
        try:
            sym=t.get('symbol');turn=float(t.get('quoteVolume',0));change=abs(float(t.get('priceChangePercent',0)))
            if sym in valid and math.isfinite(turn) and math.isfinite(change) and turn>=MIN_VOLUME:
                ranked.append((min(change,30)*2+math.log1p(turn/1e6),sym))
        except (TypeError,ValueError,OverflowError):continue
    return [s for _,s in sorted(ranked,key=lambda r:(-r[0],r[1]))]


def pick_batch(symbols,hot,cursor,cycle):
    if not symbols:return [],cursor
    chosen=[];hot_n=min(len(hot),BATCH//2)
    for j in range(hot_n):
        s=hot[(cycle*max(1,hot_n)+j)%len(hot)]
        if s not in chosen:chosen.append(s)
    visited=0
    while len(chosen)<min(BATCH,len(symbols)) and visited<len(symbols):
        s=symbols[cursor%len(symbols)];cursor=(cursor+1)%len(symbols);visited+=1
        if s not in chosen:chosen.append(s)
    return chosen,cursor


def scan(con,batch):
    found=0
    for symbol in batch:
        try:
            sig=evaluate(bars(symbol,'5m'),bars(symbol,'15m'),bars(symbol,'1h'))
            if not sig:continue
            if int(time.time()*1000)-sig['close_ms']>MAX_SIGNAL_AGE*1000:continue
            now=int(time.time())
            latest=con.execute('SELECT opened FROM trades WHERE symbol=? AND side=? ORDER BY opened DESC LIMIT 1',(symbol,sig['side'])).fetchone()
            if latest and now-latest[0]<COOLDOWN:continue
            try:
                con.execute('''INSERT INTO trades(symbol,side,strategy,bar_close_ms,entry,stop,tp1,tp2,tp3,opened,last_checked_open_ms)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?)''',(symbol,sig['side'],sig['strategy'],sig['close_ms'],sig['entry'],sig['stop'],sig['tp1'],sig['tp2'],sig['tp3'],now,(sig['close_ms']//300000)*300000))
                con.commit()
            except sqlite3.IntegrityError:continue
            notify(f"🚨 ALPER Radar V6.6 | PAPER {sig['side']}\n{symbol}\nReference entry {sig['entry']:.9g}\n"
                f"🎯 TP1 (+{TPS[0]:g}%) {sig['tp1']:.9g}\n🎯 TP2 (+{TPS[1]:g}%) {sig['tp2']:.9g}\n"
                f"🎯 TP3 (+{TPS[2]:g}%) {sig['tp3']:.9g}\n🛑 Stop ({sig['stop_pct']:.2f}%) {sig['stop']:.9g}\n"
                f"{sig['strategy']} | score {sig['score']}/6 | ADX {sig['adx']:.1f}\n"
                f"Volume {sig['volume_ratio']:.2f}x | RSI {sig['rsi']:.1f}\nClosed-candle paper signal; no trade executed")
            found+=1
        except Exception as exc:
            if 'rate-limit' in str(exc):raise
            log(f'Scan {symbol}: {type(exc).__name__}: {exc}')
    return found


def self_test():
    assert ema([1,2,3,4],3)==3
    assert round(rsi(list(range(1,40))))==100
    assert round(rsi(list(range(40,1,-1))))==0
    assert stage('LONG',101.6,100.3,99,101.5,102.5,105)==(True,False,None,[1])
    assert stage('LONG',102,98,99,101.5,102.5,105)[2]=='AMBIGUOUS'
    assert stage('SHORT',99.5,97.4,101,98.5,97.5,95)==(True,True,None,[1,2])
    assert evaluate([],[],[]) is None
    assert len(pick_batch([f'S{i}' for i in range(30)],['S15','S16'],0,0)[0])==BATCH
    print('SELF-TEST PASS (indicators, tracking, rotating universe)')


def main():
    if '--self-test' in sys.argv:self_test();return
    if os.getenv('PAPER_MODE','1')!='1':raise ValueError('V6.6 supports only PAPER_MODE=1')
    if not (.2<=TPS[0]<TPS[1]<TPS[2]<=30 and .3<=MAX_STOP<=5):raise ValueError('Invalid targets/stop')
    con=db_connect()
    log('ALPER V6.6 paper started; no orders')
    notify('✅ ALPER Radar V6.6 started — PAPER only; confirmed pullbacks, no breakout chasing')
    symbols=[];hot=[];cursor=int(state_get(con,'cursor','0'));cycle=0;u_at=0;t_at=0
    while True:
        start=time.monotonic()
        try:
            if not symbols or start-u_at>21600:
                symbols=universe();u_at=time.monotonic();cursor%=max(1,len(symbols))
                log(f'Listed: {len(symbols)} active USDT perpetuals')
            if not hot or start-t_at>300:
                hot=rank_market(fetch('/fapi/v1/ticker/24hr'),set(symbols))[:60]
                t_at=time.monotonic()
            if cycle%3==0:monitor(con)
            batch,cursor=pick_batch(symbols,hot,cursor,cycle)
            state_set(con,'cursor',str(cursor))
            signals=scan(con,batch)
            log(f'Detailed {len(batch)}/{len(symbols)}; new signals {signals}; elapsed {time.monotonic()-start:.1f}s')
            if cycle%30==0:
                count=con.execute("SELECT COUNT(*) FROM trades WHERE status='OPEN'").fetchone()[0]
                notify(f'💚 ALPER V6.6 health | {len(symbols)} listed | {len(batch)} detailed | {count} open paper tracks')
            cycle+=1
        except KeyboardInterrupt:raise
        except Exception as exc:
            log(f'Cycle warning: {type(exc).__name__}: {exc}')
            if 'rate-limit' in str(exc):time.sleep(max(65,_pause-time.monotonic()))
        time.sleep(max(5,SCAN_SECONDS-(time.monotonic()-start)))

if __name__=='__main__':main()