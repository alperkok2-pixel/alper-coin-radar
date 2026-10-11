"""ALPER Coin Radar — V2 experimental LONG/SHORT candidate scanner, Python 3.11+.

PHONE SETUP (no Binance account/API key required):
1. Create a private GitHub repository and upload this file as alper_radar.py.
2. Render: New > Background Worker > connect repository > Frankfurt.
   Runtime Python; Build command: python -m py_compile alper_radar.py
   Start command: python -u alper_radar.py
3. Set TELEGRAM_TOKEN as a secret environment variable. Never put it in GitHub.
4. Send /start to your bot. Set TELEGRAM_CHAT_ID to your own numeric chat ID.
   To obtain it safely, set PAIRING_CODE to a random private phrase, deploy,
   then send /pair YOUR_PHRASE to your bot. The worker prints ONLY the chat ID
   in Render logs. Add that ID as TELEGRAM_CHAT_ID and remove PAIRING_CODE.
5. PAPER_MODE defaults to 1. All messages are experimental, no orders placed.
6. Optional DATA_DIR: mount """ALPER Coin Radar — V2 experimental LONG/SHORT candidate scanner, Python 3.11+.

PHONE SETUP (no Binance account/API key required):
1. Create a private GitHub repository and upload this file as alper_radar.py.
2. Render: New > Background Worker > connect repository > Frankfurt.
   Runtime Python; Build command: python -m py_compile alper_radar.py
   Start command: python -u alper_radar.py
3. Set TELEGRAM_TOKEN as a secret environment variable. Never put it in GitHub.
4. Send /start to your bot. Set TELEGRAM_CHAT_ID to your own numeric chat ID.
   To obtain it safely, set PAIRING_CODE to a random private phrase, deploy,
   then send /pair YOUR_PHRASE to your bot. The worker prints ONLY the chat ID
   in Render logs. Add that ID as TELEGRAM_CHAT_ID and remove PAIRING_CODE.
5. PAPER_MODE defaults to 1. All messages are experimental, no orders placed.
6. Optional DATA_DIR: mount persistent disk there to retain results over deploys.
   Without a persistent disk Render can lose SQLite records on restart/deploy.

Scans active USDT perpetual contracts, filters illiquid markets, uses completed
5m/15m candles aggregated from 1m data. Polling, not tick-level prediction.
Scores are rule points, NOT probabilities. Targets use ATR, not predictions.
V2 includes optional BTC, 1h trend, retest, OI and funding filters, shorts,
and 1m candle target/stop tracking. Ambiguous bars are never counted as wins.
Evaluation is observed last-price return at/after +5/+15 minutes, with actual
observation time stored; it is not an executable backtest or a real order fill model.
Costs: COST_BPS is assumed round-trip commission + slippage (default 15 bps).
Local check: python alper_radar.py --self-test
"""
import concurrent.futures as cf
import datetime as dt
import json
import os
import sqlite3
import statistics as st
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

BASE = 'https://fapi.binance.com'
TOKEN = os.getenv('TELEGRAM_TOKEN', '')
CHAT = os.getenv('TELEGRAM_CHAT_ID', '')
PAPER = os.getenv('PAPER_MODE', '1') != '0'
DB = str(Path(os.getenv('DATA_DIR', '.')) / 'radar.sqlite3')
gate = threading.Lock()
next_call = 0.0
pause_until = 0.0

class HTTPFailure(RuntimeError):
    def __init__(self, source, status):
        self.source, self.status = source, status
        super().__init__(f'{source} HTTP {status}')

def error_detail(exc):
    # Only expose our own sanitized HTTP fields, never URLs or credentials.
    return str(exc) if isinstance(exc, HTTPFailure) else type(exc).__name__

def request(url, params=None, data=None, market=False):
    global next_call, pause_until
    source = 'Binance' if market else 'Telegram'
    if market:
        while True:
            with gate:
                delay = max(next_call, pause_until) - time.monotonic()
                if delay <= 0:
                    next_call = time.monotonic() + .4
                    break
            # Release the lock and recheck cooldown before sending.
            time.sleep(min(delay, 1.0))
    if params:
        url += '?' + urllib.parse.urlencode(params)
    payload = None if data is None else urllib.parse.urlencode(data).encode()
    try:
        with urllib.request.urlopen(url, payload, timeout=15) as response:
            return json.load(response)
    except urllib.error.HTTPError as exc:
        if market and exc.code in (418, 429):
            try:
                cooldown = max(120, float(exc.headers.get('Retry-After', '120')))
            except (ValueError, TypeError):
                cooldown = 120
            with gate:
                pause_until = max(pause_until, time.monotonic() + cooldown)
            print('Binance rate limit: HTTP', exc.code, 'cooldown seconds:', round(cooldown), flush=True)
        raise HTTPFailure(source, exc.code) from None

def market(path, **params):
    return request(BASE + path, params, market=True)

def send(message):
    if not TOKEN or not CHAT:
        return
    request('https://api.telegram.org/bot' + TOKEN + '/sendMessage',
            data={'chat_id': CHAT, 'text': message})

def connect():
    con = sqlite3.connect(DB, timeout=30)
    con.execute('PRAGMA journal_mode=WAL')
    return con

def init_db():
    Path(DB).parent.mkdir(parents=True, exist_ok=True)
    with connect() as con:
        con.execute('CREATE TABLE IF NOT EXISTS signals (id INTEGER PRIMARY KEY, symbol TEXT, ts REAL, entry REAL, stop REAL, target REAL, score INTEGER, candle INTEGER, UNIQUE(symbol,candle))')
        con.execute('CREATE TABLE IF NOT EXISTS results (signal_id INTEGER, minutes INTEGER, observed REAL, price REAL, gross_pct REAL, net_pct REAL, PRIMARY KEY(signal_id,minutes))')

def aggregate(raw, minutes, now_ms):
    width = minutes * 60000
    groups = {}
    for row in raw:
        stamp = int(row[0])
        if int(row[6]) >= now_ms:
            continue
        key = stamp // width * width
        groups.setdefault(key, []).append(row)
    out = []
    for key, rows in sorted(groups.items()):
        if len(rows) != minutes or key + width > now_ms:
            continue
        if [int(r[0]) for r in rows] != list(range(key, key + width, 60000)):
            continue
        out.append([key, float(rows[0][1]), max(float(r[2]) for r in rows),
                    min(float(r[3]) for r in rows), float(rows[-1][4]),
                    sum(float(r[7]) for r in rows)])
    return out

def ema(values, length):
    value = values[0]
    for item in values[1:]:
        value += 2 / (length + 1) * (item - value)
    return value

def monitor():
    while True:
        try:
            with connect() as con:
                pending = con.execute('SELECT id,symbol,ts,entry,side FROM signals WHERE ts > ? AND NOT EXISTS (SELECT 1 FROM results WHERE signal_id=signals.id AND minutes=15)', (time.time()-7200,)).fetchall()
                if pending:
                    prices = {r['symbol']: r for r in market('/fapi/v2/ticker/price')}
                    now = time.time()
                    for sid, symbol, stamp, entry, side in pending:
                        item = prices.get(symbol)
                        if not item:
                            continue
                        observed = float(item['time']) / 1000
                        if not 0 <= now - observed <= 30:
                            continue
                        price = float(item['price'])
                        for minutes in (5, 15):
                            if observed >= stamp + minutes*60:
                                gross = (price/entry-1)*100 * (1 if side == 'LONG' else -1)
                                con.execute('INSERT OR IGNORE INTO results VALUES (?,?,?,?,?,?)',
                                            (sid, minutes, observed, price, gross,
                                             gross-float(os.getenv('COST_BPS', '15'))/100))
        except Exception as exc:
            print('Evaluation error:', error_detail(exc), flush=True)
        if enabled('RESULT_TRACKING'):
            try:
                track_outcomes()
                track_diagnostics()
            except Exception as exc:
                print('Outcome error:', error_detail(exc), flush=True)
        time.sleep(15)

def pairing():
    code = os.getenv('PAIRING_CODE', '')
    if not TOKEN or not code:
        raise RuntimeError('Set TELEGRAM_TOKEN and PAIRING_CODE first; then TELEGRAM_CHAT_ID.')
    print('Pairing mode: send /pair <your private code> to your bot.', flush=True)
    offset = 0
    while True:
        response = request('https://api.telegram.org/bot' + TOKEN + '/getUpdates',
                           {'offset': offset, 'timeout': 0})
        if not response.get('ok'):
            raise RuntimeError('Telegram pairing failed')
        for update in response['result']:
            offset = update['update_id']+1
            message = update.get('message', {})
            if message.get('chat', {}).get('type') == 'private' and message.get('text') == '/pair ' + code:
                print('TELEGRAM_CHAT_ID=' + str(message['chat']['id']), flush=True)
                return
        time.sleep(3)

def scan_symbol(symbol, now_ms):
    raw = market('/fapi/v1/klines', symbol=symbol, interval='1m', limit=480)
    result = candidate(raw, now_ms)
    if result:
        print('Candidate:', symbol, result['side'], 'score:', result['score'], flush=True)
    if result and enabled('HTF_FILTER'):
        hourly = market('/fapi/v1/klines', symbol=symbol, interval='1h', limit=100)
        hourly_side = trend_direction(hourly, now_ms, 3600000)
        if hourly_side != result['side']:
            print('Rejected:', symbol, result['side'], 'filter: HTF_1H', 'trend:', hourly_side or 'NEUTRAL', flush=True)
            return symbol, None
    return symbol, result

def main():
    if not CHAT:
        pairing()
        return
    if not TOKEN:
        raise RuntimeError('Missing TELEGRAM_TOKEN')
    init_db()
    print('Diagnostics V2.2 | filters:', {name: enabled(name) for name in ('ENABLE_SHORT','BTC_FILTER','HTF_FILTER','RETEST_FILTER','FUNDING_FILTER','OI_FILTER')}, flush=True)
    send('ALPER Radar V2.3 started — ' + ('DENEME' if PAPER else 'ADAY BİLDİRİMLERİ') + '. Otomatik işlem yok; başarı oranı henüz ölçülmedi.')
    threading.Thread(target=monitor, daemon=True).start()
    while True:
        start = time.monotonic()
        try:
            now_ms = int(market('/fapi/v1/time')['serverTime'])
            btc_side = None
            if enabled('BTC_FILTER'):
                btc = market('/fapi/v1/klines', symbol='BTCUSDT', interval='15m', limit=100)
                btc_side = trend_direction(btc, now_ms, 900000)
            print('BTC 15m direction:', (btc_side or 'NEUTRAL') if enabled('BTC_FILTER') else 'FILTER_OFF', 'SHORT enabled:', enabled('ENABLE_SHORT'), flush=True)
            info = market('/fapi/v1/exchangeInfo')
            all_symbols = {s['symbol'] for s in info['symbols'] if s['status']=='TRADING' and s['contractType']=='PERPETUAL' and s['quoteAsset']=='USDT'}
            tickers = market('/fapi/v1/ticker/24hr')
            liquid = [t['symbol'] for t in tickers if t['symbol'] in all_symbols and float(t['quoteVolume']) >= float(os.getenv('MIN_VOLUME_USDT', '20000000'))]
            print('Active:', len(all_symbols), 'liquid:', len(liquid), flush=True)
            errors, found = 0, 0
            with cf.ThreadPoolExecutor(max_workers=4) as pool:
                futures = [pool.submit(scan_symbol, s, now_ms) for s in liquid]
                for future in cf.as_completed(futures):
                    try:
                        symbol, result = future.result()
                        if not result:
                            continue
                        if enabled('BTC_FILTER') and result['side'] != btc_side:
                            print('Rejected:', symbol, result['side'], 'filter: BTC', 'trend:', btc_side or 'NEUTRAL', flush=True)
                            continue
                        if not derivatives_filter(symbol, result):
                            print('Rejected:', symbol, result['side'], 'filter:', result.get('rejection', 'DERIVATIVES'), flush=True)
                            continue
                        with connect() as con:
                            exists = con.execute('SELECT 1 FROM signals WHERE symbol=? AND (candle=? OR ts>?)',
                                                 (symbol, result['candle'], time.time()-900)).fetchone()
                            if exists:
                                print('Rejected:', symbol, result['side'], 'filter: DUPLICATE_15M', flush=True)
                                continue
                            quote = market('/fapi/v2/ticker/price', symbol=symbol)
                            entry = float(quote['price'])
                            stamp = time.time()
                            if stamp - (result['candle']/1000 + 300) > 120:
                                print('Rejected:', symbol, result['side'], 'filter: STALE_CANDLE', flush=True)
                                continue
                            if not 0 <= stamp-float(quote['time'])/1000 <= 30:
                                print('Rejected:', symbol, result['side'], 'filter: STALE_QUOTE', flush=True)
                                continue
                            if abs(entry-result['close']) > .5*result['atr']:
                                print('Rejected:', symbol, result['side'], 'filter: PRICE_DRIFT', flush=True)
                                continue
                            risk = 1.2*result['atr']
                            direction = 1 if result['side'] == 'LONG' else -1
                            stop, target = entry-direction*risk, entry+direction*1.5*risk
                            if min(stop, target) <= 0:
                                continue
                            con.execute('INSERT INTO signals(symbol,ts,entry,stop,target,score,candle,side) VALUES (?,?,?,?,?,?,?,?)',
                                        (symbol, stamp, entry, stop, target, result['score'], result['candle'], result['side']))
                        clock = dt.datetime.fromtimestamp(stamp, dt.timezone(dt.timedelta(hours=3))).strftime('%H:%M:%S')
                        send(f"{'DENEME | ' if PAPER else ''}{result['side']} ADAYI: {symbol}\nSaat (TR): {clock}\nKural puanı: {result['score']}/100 (olasılık değil)\nReferans fiyat: {entry:.8g}\nStop referansı: {stop:.8g}\nHedef referansı: {target:.8g}\nHacim: {result['volume']:.2f}x\nOI değişimi (15dk): {result.get('oi_text', 'filtre kapalı')}\nFonlama: {result.get('funding_text', 'filtre kapalı')}\nİzleme: sonraki 5 ve 15 dk. Seviyeler otomatik emir değildir.")
                        print('Alert sent:', symbol, result['side'], flush=True)
                        found += 1
                    except Exception as exc:
                        errors += 1
                        print('Symbol error:', error_detail(exc), flush=True)
            print('Scan seconds:', round(time.monotonic()-start), 'alerts:', found, 'errors:', errors, flush=True)
        except Exception as exc:
            print('Scan unavailable:', error_detail(exc), flush=True)
        time.sleep(max(5, 90-(time.monotonic()-start)))

def self_test():
    raw = [[i*60000, '10', '12', '9', '11', '1', (i+1)*60000-1, '100'] for i in range(480)]
    assert len(aggregate(raw, 5, 480*60000)) == 96
    assert len(aggregate(raw, 15, 480*60000)) == 32
    assert len(aggregate(raw, 5, 479*60000)) == 95
    assert len(aggregate(raw[1:], 5, 480*60000)) == 95
    assert candidate(raw, 480*60000) is None
    assert ema([10]*30, 9) == 10
    assert len(aggregate(raw[:4], 5, 480*60000)) == 0
    import tempfile
    global DB
    with tempfile.TemporaryDirectory() as directory:
        DB = str(Path(directory)/'test.sqlite3')
        init_db()
        with connect() as con:
            con.execute('INSERT INTO signals(symbol,ts,entry,stop,target,score,candle) VALUES (?,?,?,?,?,?,?)', ('TEST', 1, 100, 99, 102, 90, 1))
            con.execute('INSERT INTO results VALUES (1,5,301,101,1,.85)')
            con.execute('INSERT OR IGNORE INTO results VALUES (1,5,302,102,2,1.85)')
            assert con.execute('SELECT count(*) FROM results').fetchone()[0] == 1
    print('PASS: candle alignment, partial/gapped bars, EMA, neutral-market rejection.')


# All default ON: BTC_FILTER, HTF_FILTER, RETEST_FILTER, FUNDING_FILTER,
# OI_FILTER, ENABLE_SHORT, RESULT_TRACKING. Set any to 0 to disable.
# Thresholds are unvalidated heuristics; more filters do not guarantee accuracy.
# V2 filters can be disabled individually using an environment value of 0.
def enabled(name):
    return os.getenv(name, '1') != '0'

def trend_direction(raw, now_ms, width):
    rows = [r for r in raw if int(r[6]) < now_ms]
    if len(rows) < 50 or now_ms - (int(rows[-1][0]) + width) > width:
        return None
    if any(int(b[0])-int(a[0]) != width for a,b in zip(rows,rows[1:])):
        return None
    closes = [float(r[4]) for r in rows]
    fast, slow = ema(closes, 9), ema(closes, 21)
    if closes[-1] > fast > slow:
        return 'LONG'
    if closes[-1] < fast < slow:
        return 'SHORT'
    return None

def retest_setup(a, atr, direction):
    # Breakout must precede the latest candle; never use future candles.
    last = a[-1]
    for index in range(len(a)-2, len(a)-5, -1):
        previous = a[index-20:index]
        br = a[index]
        level = max(r[2] for r in previous) if direction == 1 else min(r[3] for r in previous)
        ratio = br[5] / max(st.mean(r[5] for r in previous), 1e-12)
        broke = direction*(br[4]-level) > 0 and ratio >= 1.5
        held = all(direction*(r[4]-level) >= -.15*atr for r in a[index+1:])
        touched = (last[3] <= level+.3*atr and last[3] >= level-.5*atr) if direction == 1 else (last[2] >= level-.3*atr and last[2] <= level+.5*atr)
        if broke and held and touched and direction*(last[4]-level)>0 and direction*(last[4]-last[1])>0:
            return ratio
    return None

def candidate(raw, now_ms):
    a, b = aggregate(raw,5,now_ms), aggregate(raw,15,now_ms)
    if len(a)<30 or len(b)<25 or now_ms-(a[-1][0]+300000)>90000:
        return None
    if any(y[0]-x[0]!=300000 for x,y in zip(a,a[1:])):
        return None
    if any(y[0]-x[0]!=900000 for x,y in zip(b,b[1:])):
        return None
    closes,big=[r[4] for r in a],[r[4] for r in b]
    last,prev=a[-1],a[-21:-1]
    atr=st.mean(max(a[i][2]-a[i][3],abs(a[i][2]-a[i-1][4]),abs(a[i][3]-a[i-1][4])) for i in range(len(a)-14,len(a)))
    if atr<=0:
        return None
    for side,d in [('LONG',1),('SHORT',-1)]:
        if side=='SHORT' and not enabled('ENABLE_SHORT'):
            continue
        trend=d*(big[-1]-ema(big,9))>0 and d*(ema(big,9)-ema(big,20))>0
        momentum=d*(closes[-1]-ema(closes,9))>0 and d*(ema(closes,9)-ema(closes,21))>0
        volume=last[5]/max(st.mean(r[5] for r in prev),1e-12)
        breakout=last[4]>max(r[2] for r in prev) if d==1 else last[4]<min(r[3] for r in prev)
        setup=breakout and volume>=1.5
        if enabled('RETEST_FILTER'):
            retest_volume=retest_setup(a,atr,d)
            setup=retest_volume is not None
            if setup:
                volume=retest_volume
        location=(last[4]-last[3])/max(last[2]-last[3],1e-12)
        strong=location>=.7 if d==1 else location<=.3
        extension=d*(last[4]-ema(closes,21))/atr
        score=25*trend+25*setup+20*(volume>=1.5)+20*momentum+10*strong
        if trend and setup and momentum and score>=80 and 0<extension<3:
            return dict(side=side,score=score,atr=atr,close=last[4],candle=last[0],volume=volume)
    return None

def derivatives_filter(symbol, result):
    d=1 if result['side']=='LONG' else -1
    if enabled('FUNDING_FILTER'):
        premium=market('/fapi/v1/premiumIndex',symbol=symbol)
        if abs(time.time()-float(premium['time'])/1000)>120:
            result['rejection'] = 'FUNDING_STALE'
            return False
        rate=float(premium['lastFundingRate'])
        # Reject costly crowded direction; this is a configurable heuristic.
        if d*rate>float(os.getenv('MAX_FUNDING_RATE','0.001')):
            result['rejection'] = 'FUNDING_CROWDED'
            return False
        result['funding_text']=f'{rate*100:+.4f}%'
    if enabled('OI_FILTER'):
        rows=market('/futures/data/openInterestHist',symbol=symbol,period='5m',limit=4)
        if len(rows)!=4 or abs(time.time()-int(rows[-1]['timestamp'])/1000)>600:
            result['rejection'] = 'OI_STALE_OR_MISSING'
            return False
        if any(int(y['timestamp'])-int(x['timestamp'])!=300000 for x,y in zip(rows,rows[1:])):
            result['rejection'] = 'OI_DATA_GAP'
            return False
        first=float(rows[0]['sumOpenInterest'])
        if first<=0:
            result['rejection'] = 'OI_INVALID'
            return False
        change=(float(rows[-1]['sumOpenInterest'])/first-1)*100
        if change<float(os.getenv('MIN_OI_CHANGE_PCT','0')):
            result['rejection'] = 'OI_CHANGE'
            return False
        result['oi_text']=f'{change:+.2f}%'
    return True

original_init_db=init_db
def init_db():
    original_init_db()
    with connect() as con:
        columns={r[1] for r in con.execute('PRAGMA table_info(signals)')}
        if 'side' not in columns:
            con.execute("ALTER TABLE signals ADD COLUMN side TEXT NOT NULL DEFAULT 'LONG'")
        con.execute('CREATE TABLE IF NOT EXISTS diagnostics (signal_id INTEGER PRIMARY KEY, approach_pct REAL, adverse_pct REAL, coverage TEXT, notified INTEGER DEFAULT 0)')
        con.execute('CREATE TABLE IF NOT EXISTS outcomes (signal_id INTEGER PRIMARY KEY, status TEXT, observed REAL, reference_price REAL, notified INTEGER DEFAULT 0)')

def barrier_outcome(raw,stamp,stop,target,side,now_ms):
    # 1m OHLC cannot establish ordering inside a candle or the entry minute.
    start=int(stamp*1000)//60000*60000
    end=(int(stamp*1000)+900000)//60000*60000
    rows=sorted([r for r in raw if start<=int(r[0])<=end and int(r[6])<now_ms],key=lambda r:int(r[0]))
    expected=start
    for row in rows:
        if int(row[0])!=expected:
            return ('DATA_GAP',int(row[6])/1000,None)
        expected+=60000
        high,low=float(row[2]),float(row[3])
        hit_stop=low<=stop if side=='LONG' else high>=stop
        hit_target=high>=target if side=='LONG' else low<=target
        if hit_stop or hit_target:
            partial=int(row[0])<stamp*1000 or int(row[6])>stamp*1000+900000
            if partial or (hit_stop and hit_target):
                return ('AMBIGUOUS',int(row[6])/1000,None)
            return ('STOP' if hit_stop else 'TARGET',int(row[6])/1000,stop if hit_stop else target)
    if now_ms>end+60000:
        if expected!=end+60000:
            return ('DATA_GAP',now_ms/1000,None)
        return ('TIMEOUT',now_ms/1000,None)
    return None

def track_outcomes():
    with connect() as con:
        pending=con.execute('SELECT id,symbol,ts,stop,target,side FROM signals WHERE NOT EXISTS (SELECT 1 FROM outcomes WHERE signal_id=signals.id) ORDER BY ts LIMIT 30').fetchall()
    now_ms=int(market('/fapi/v1/time')['serverTime']) if pending else int(time.time()*1000)
    for sid,symbol,stamp,stop,target,side in pending:
        start=int(stamp*1000)//60000*60000
        raw=market('/fapi/v1/klines',symbol=symbol,interval='1m',startTime=start,endTime=min(now_ms,start+1020000),limit=20)
        outcome=barrier_outcome(raw,stamp,stop,target,side,now_ms)
        if outcome:
            with connect() as con:
                con.execute('INSERT OR IGNORE INTO outcomes(signal_id,status,observed,reference_price) VALUES (?,?,?,?)',(sid,*outcome))
    labels={'TARGET':'Hedef görüldü','STOP':'Stop görüldü','AMBIGUOUS':'Mum içi sıra/zaman belirsiz','TIMEOUT':'15 dk içinde hedef/stop görülmedi','DATA_GAP':'Veri eksik; değerlendirilemedi'}
    with connect() as con:
        notices=con.execute('SELECT o.signal_id,s.symbol,s.side,o.status FROM outcomes o JOIN signals s ON s.id=o.signal_id WHERE o.notified=0').fetchall()
        for sid,symbol,side,status in notices:
            send(f'DENEME SONUCU | {symbol} {side}: {labels[status]}. 1dk mum incelemesi; gerçek emir gerçekleşmesi değildir.')
            con.execute('UPDATE outcomes SET notified=1 WHERE signal_id=?',(sid,))


def excursion_metrics(raw, stamp, entry, target, side, now_ms):
    """Interior complete 1m candles only: excludes both boundary minutes.

    Returns a conservative observed maximum, not tick-exact 15m excursion.
    Measurements continue after target/stop; they describe the whole window.
    """
    deadline = int(stamp * 1000) + 900000
    first = ((int(stamp * 1000) + 59999) // 60000) * 60000
    rows = sorted([r for r in raw if int(r[0]) >= first
                   and int(r[0]) + 60000 <= deadline
                   and int(r[6]) < now_ms], key=lambda r: int(r[0]))
    expected = list(range(first, deadline - 59999, 60000))
    if not expected or [int(r[0]) for r in rows] != expected:
        return None, None, 'DATA_GAP'
    distance = abs(target - entry)
    if entry <= 0 or distance <= 0:
        return None, None, 'INVALID_LEVELS'
    if side == 'LONG':
        favorable = max(0.0, max(float(r[2]) for r in rows) - entry)
        adverse = max(0.0, entry - min(float(r[3]) for r in rows))
    else:
        favorable = max(0.0, entry - min(float(r[3]) for r in rows))
        adverse = max(0.0, max(float(r[2]) for r in rows) - entry)
    return favorable / distance * 100, adverse / entry * 100, 'INTERIOR_1M'


def diagnostic_message(symbol, side, approach, adverse, coverage, gross, net, delay):
    movement = (f'Hedef mesafesine yaklaşma: %{approach:.1f}\n'
                f'En büyük ters hareket: %{adverse:.2f}\n'
                'Yaklaşma ölçümü: sınır dakikaları hariç tam 1dk mumlar; '
                'hedef/stop sonrasında da 15dk pencere izlenir.') if coverage == 'INTERIOR_1M' else 'Yaklaşma ölçümü: veri eksik veya seviyeler geçersiz.'
    return (f'DENEME | 15 DK ANALİZİ | {symbol} {side}\n{movement}\n'
            f'15dk sonu brüt: %{gross:+.2f}\n'
            f'Varsayılan maliyet sonrası: %{net:+.2f}\n'
            f'Ölçüm gecikmesi: {delay:.0f} saniye\n'
            'Sonuç fiyat değişimidir; kaldıraç getirisi veya gerçekleşmiş işlem kârı değildir.')


def track_diagnostics():
    # Results retain the first fresh quote observed at/after +15m.
    # Restrict historical catch-up to 2h; persist notification state over restarts.
    with connect() as con:
        pending = con.execute("""SELECT s.id,s.symbol,s.ts,s.entry,s.target,s.side
            FROM signals s JOIN results r ON r.signal_id=s.id AND r.minutes=15
            WHERE s.ts>? AND NOT EXISTS
            (SELECT 1 FROM diagnostics d WHERE d.signal_id=s.id)
            ORDER BY s.ts LIMIT 30""", (time.time()-7200,)).fetchall()
    now_ms = int(market('/fapi/v1/time')['serverTime']) if pending else int(time.time()*1000)
    for sid,symbol,stamp,entry,target,side in pending:
        start = int(stamp*1000)//60000*60000
        if now_ms < int(stamp*1000)+960000:
            continue  # Allow the final boundary candle to finish.
        raw = market('/fapi/v1/klines', symbol=symbol, interval='1m',
                     startTime=start, endTime=start+1020000, limit=20)
        approach, adverse, coverage = excursion_metrics(raw,stamp,entry,target,side,now_ms)
        with connect() as con:
            con.execute('INSERT OR IGNORE INTO diagnostics VALUES (?,?,?,?,0)',
                        (sid,approach,adverse,coverage))
    with connect() as con:
        notices = con.execute("""SELECT d.signal_id,s.symbol,s.side,d.approach_pct,
            d.adverse_pct,d.coverage,r.gross_pct,r.net_pct,r.observed-s.ts-900
            FROM diagnostics d JOIN signals s ON s.id=d.signal_id
            JOIN results r ON r.signal_id=s.id AND r.minutes=15
            WHERE d.notified=0""").fetchall()
        for sid,*values in notices:
            send(diagnostic_message(*values))
            con.execute('UPDATE diagnostics SET notified=1 WHERE signal_id=?',(sid,))


if __name__ == '__main__':
    if '--self-test' in sys.argv:
        self_test()
    else:
        main()
 disk there to retain results over deploys.
   Without a persistent disk Render can lose SQLite records on restart/deploy.

Scans active USDT perpetual contracts, filters illiquid markets, uses completed
5m/15m candles aggregated from 1m data. Polling, not tick-level prediction.
Scores are rule points, NOT probabilities. Targets use ATR, not predictions.
V2 includes optional BTC, 1h trend, retest, OI and funding filters, shorts,
and 1m candle target/stop tracking. Ambiguous bars are never counted as wins.
Evaluation is observed last-price return at/after +5/+15 minutes, with actual
observation time stored; it is not an executable backtest or a real order fill model.
Costs: COST_BPS is assumed round-trip commission + slippage (default 15 bps).
Local check: python alper_radar.py --self-test
"""
import concurrent.futures as cf
import datetime as dt
import json
import os
import sqlite3
import statistics as st
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

BASE = 'https://fapi.binance.com'
TOKEN = os.getenv('TELEGRAM_TOKEN', '')
CHAT = os.getenv('TELEGRAM_CHAT_ID', '')
PAPER = os.getenv('PAPER_MODE', '1') != '0'
DB = str(Path(os.getenv('DATA_DIR', '.')) / 'radar.sqlite3')
gate = threading.Lock()
next_call = 0.0
pause_until = 0.0

class HTTPFailure(RuntimeError):
    def __init__(self, source, status):
        self.source, self.status = source, status
        super().__init__(f'{source} HTTP {status}')

def error_detail(exc):
    # Only expose our own sanitized HTTP fields, never URLs or credentials.
    return str(exc) if isinstance(exc, HTTPFailure) else type(exc).__name__

def request(url, params=None, data=None, market=False):
    global next_call, pause_until
    source = 'Binance' if market else 'Telegram'
    if market:
        while True:
            with gate:
                delay = max(next_call, pause_until) - time.monotonic()
                if delay <= 0:
                    next_call = time.monotonic() + .4
                    break
            # Release the lock and recheck cooldown before sending.
            time.sleep(min(delay, 1.0))
    if params:
        url += '?' + urllib.parse.urlencode(params)
    payload = None if data is None else urllib.parse.urlencode(data).encode()
    try:
        with urllib.request.urlopen(url, payload, timeout=15) as response:
            return json.load(response)
    except urllib.error.HTTPError as exc:
        if market and exc.code in (418, 429):
            try:
                cooldown = max(120, float(exc.headers.get('Retry-After', '120')))
            except (ValueError, TypeError):
                cooldown = 120
            with gate:
                pause_until = max(pause_until, time.monotonic() + cooldown)
            print('Binance rate limit: HTTP', exc.code, 'cooldown seconds:', round(cooldown), flush=True)
        raise HTTPFailure(source, exc.code) from None

def market(path, **params):
    return request(BASE + path, params, market=True)

def send(message):
    if not TOKEN or not CHAT:
        return
    request('https://api.telegram.org/bot' + TOKEN + '/sendMessage',
            data={'chat_id': CHAT, 'text': message})

def connect():
    con = sqlite3.connect(DB, timeout=30)
    con.execute('PRAGMA journal_mode=WAL')
    return con

def init_db():
    Path(DB).parent.mkdir(parents=True, exist_ok=True)
    with connect() as con:
        con.execute('CREATE TABLE IF NOT EXISTS signals (id INTEGER PRIMARY KEY, symbol TEXT, ts REAL, entry REAL, stop REAL, target REAL, score INTEGER, candle INTEGER, UNIQUE(symbol,candle))')
        con.execute('CREATE TABLE IF NOT EXISTS results (signal_id INTEGER, minutes INTEGER, observed REAL, price REAL, gross_pct REAL, net_pct REAL, PRIMARY KEY(signal_id,minutes))')

def aggregate(raw, minutes, now_ms):
    width = minutes * 60000
    groups = {}
    for row in raw:
        stamp = int(row[0])
        if int(row[6]) >= now_ms:
            continue
        key = stamp // width * width
        groups.setdefault(key, []).append(row)
    out = []
    for key, rows in sorted(groups.items()):
        if len(rows) != minutes or key + width > now_ms:
            continue
        if [int(r[0]) for r in rows] != list(range(key, key + width, 60000)):
            continue
        out.append([key, float(rows[0][1]), max(float(r[2]) for r in rows),
                    min(float(r[3]) for r in rows), float(rows[-1][4]),
                    sum(float(r[7]) for r in rows)])
    return out

def ema(values, length):
    value = values[0]
    for item in values[1:]:
        value += 2 / (length + 1) * (item - value)
    return value

def monitor():
    while True:
        try:
            with connect() as con:
                pending = con.execute('SELECT id,symbol,ts,entry,side FROM signals WHERE ts > ? AND NOT EXISTS (SELECT 1 FROM results WHERE signal_id=signals.id AND minutes=15)', (time.time()-7200,)).fetchall()
                if pending:
                    prices = {r['symbol']: r for r in market('/fapi/v2/ticker/price')}
                    now = time.time()
                    for sid, symbol, stamp, entry, side in pending:
                        item = prices.get(symbol)
                        if not item:
                            continue
                        observed = float(item['time']) / 1000
                        if not 0 <= now - observed <= 30:
                            continue
                        price = float(item['price'])
                        for minutes in (5, 15):
                            if observed >= stamp + minutes*60:
                                gross = (price/entry-1)*100 * (1 if side == 'LONG' else -1)
                                con.execute('INSERT OR IGNORE INTO results VALUES (?,?,?,?,?,?)',
                                            (sid, minutes, observed, price, gross,
                                             gross-float(os.getenv('COST_BPS', '15'))/100))
        except Exception as exc:
            print('Evaluation error:', error_detail(exc), flush=True)
        if enabled('RESULT_TRACKING'):
            try:
                track_outcomes()
            except Exception as exc:
                print('Outcome error:', error_detail(exc), flush=True)
        time.sleep(15)

def pairing():
    code = os.getenv('PAIRING_CODE', '')
    if not TOKEN or not code:
        raise RuntimeError('Set TELEGRAM_TOKEN and PAIRING_CODE first; then TELEGRAM_CHAT_ID.')
    print('Pairing mode: send /pair <your private code> to your bot.', flush=True)
    offset = 0
    while True:
        response = request('https://api.telegram.org/bot' + TOKEN + '/getUpdates',
                           {'offset': offset, 'timeout': 0})
        if not response.get('ok'):
            raise RuntimeError('Telegram pairing failed')
        for update in response['result']:
            offset = update['update_id']+1
            message = update.get('message', {})
            if message.get('chat', {}).get('type') == 'private' and message.get('text') == '/pair ' + code:
                print('TELEGRAM_CHAT_ID=' + str(message['chat']['id']), flush=True)
                return
        time.sleep(3)

def scan_symbol(symbol, now_ms):
    raw = market('/fapi/v1/klines', symbol=symbol, interval='1m', limit=480)
    result = candidate(raw, now_ms)
    if result:
        print('Candidate:', symbol, result['side'], 'score:', result['score'], flush=True)
    if result and enabled('HTF_FILTER'):
        hourly = market('/fapi/v1/klines', symbol=symbol, interval='1h', limit=100)
        hourly_side = trend_direction(hourly, now_ms, 3600000)
        if hourly_side != result['side']:
            print('Rejected:', symbol, result['side'], 'filter: HTF_1H', 'trend:', hourly_side or 'NEUTRAL', flush=True)
            return symbol, None
    return symbol, result

def main():
    if not CHAT:
        pairing()
        return
    if not TOKEN:
        raise RuntimeError('Missing TELEGRAM_TOKEN')
    init_db()
    print('Diagnostics V2.2 | filters:', {name: enabled(name) for name in ('ENABLE_SHORT','BTC_FILTER','HTF_FILTER','RETEST_FILTER','FUNDING_FILTER','OI_FILTER')}, flush=True)
    send('ALPER Radar V2 started — ' + ('DENEME' if PAPER else 'ADAY BİLDİRİMLERİ') + '. Otomatik işlem yok; başarı oranı henüz ölçülmedi.')
    threading.Thread(target=monitor, daemon=True).start()
    while True:
        start = time.monotonic()
        try:
            now_ms = int(market('/fapi/v1/time')['serverTime'])
            btc_side = None
            if enabled('BTC_FILTER'):
                btc = market('/fapi/v1/klines', symbol='BTCUSDT', interval='15m', limit=100)
                btc_side = trend_direction(btc, now_ms, 900000)
            print('BTC 15m direction:', (btc_side or 'NEUTRAL') if enabled('BTC_FILTER') else 'FILTER_OFF', 'SHORT enabled:', enabled('ENABLE_SHORT'), flush=True)
            info = market('/fapi/v1/exchangeInfo')
            all_symbols = {s['symbol'] for s in info['symbols'] if s['status']=='TRADING' and s['contractType']=='PERPETUAL' and s['quoteAsset']=='USDT'}
            tickers = market('/fapi/v1/ticker/24hr')
            liquid = [t['symbol'] for t in tickers if t['symbol'] in all_symbols and float(t['quoteVolume']) >= float(os.getenv('MIN_VOLUME_USDT', '20000000'))]
            print('Active:', len(all_symbols), 'liquid:', len(liquid), flush=True)
            errors, found = 0, 0
            with cf.ThreadPoolExecutor(max_workers=4) as pool:
                futures = [pool.submit(scan_symbol, s, now_ms) for s in liquid]
                for future in cf.as_completed(futures):
                    try:
                        symbol, result = future.result()
                        if not result:
                            continue
                        if enabled('BTC_FILTER') and result['side'] != btc_side:
                            print('Rejected:', symbol, result['side'], 'filter: BTC', 'trend:', btc_side or 'NEUTRAL', flush=True)
                            continue
                        if not derivatives_filter(symbol, result):
                            print('Rejected:', symbol, result['side'], 'filter:', result.get('rejection', 'DERIVATIVES'), flush=True)
                            continue
                        with connect() as con:
                            exists = con.execute('SELECT 1 FROM signals WHERE symbol=? AND (candle=? OR ts>?)',
                                                 (symbol, result['candle'], time.time()-900)).fetchone()
                            if exists:
                                print('Rejected:', symbol, result['side'], 'filter: DUPLICATE_15M', flush=True)
                                continue
                            quote = market('/fapi/v2/ticker/price', symbol=symbol)
                            entry = float(quote['price'])
                            stamp = time.time()
                            if stamp - (result['candle']/1000 + 300) > 120:
                                print('Rejected:', symbol, result['side'], 'filter: STALE_CANDLE', flush=True)
                                continue
                            if not 0 <= stamp-float(quote['time'])/1000 <= 30:
                                print('Rejected:', symbol, result['side'], 'filter: STALE_QUOTE', flush=True)
                                continue
                            if abs(entry-result['close']) > .5*result['atr']:
                                print('Rejected:', symbol, result['side'], 'filter: PRICE_DRIFT', flush=True)
                                continue
                            risk = 1.2*result['atr']
                            direction = 1 if result['side'] == 'LONG' else -1
                            stop, target = entry-direction*risk, entry+direction*1.5*risk
                            if min(stop, target) <= 0:
                                continue
                            con.execute('INSERT INTO signals(symbol,ts,entry,stop,target,score,candle,side) VALUES (?,?,?,?,?,?,?,?)',
                                        (symbol, stamp, entry, stop, target, result['score'], result['candle'], result['side']))
                        clock = dt.datetime.fromtimestamp(stamp, dt.timezone(dt.timedelta(hours=3))).strftime('%H:%M:%S')
                        send(f"{'DENEME | ' if PAPER else ''}{result['side']} ADAYI: {symbol}\nSaat (TR): {clock}\nKural puanı: {result['score']}/100 (olasılık değil)\nReferans fiyat: {entry:.8g}\nStop referansı: {stop:.8g}\nHedef referansı: {target:.8g}\nHacim: {result['volume']:.2f}x\nOI değişimi (15dk): {result.get('oi_text', 'filtre kapalı')}\nFonlama: {result.get('funding_text', 'filtre kapalı')}\nİzleme: sonraki 5 ve 15 dk. Seviyeler otomatik emir değildir.")
                        print('Alert sent:', symbol, result['side'], flush=True)
                        found += 1
                    except Exception as exc:
                        errors += 1
                        print('Symbol error:', error_detail(exc), flush=True)
            print('Scan seconds:', round(time.monotonic()-start), 'alerts:', found, 'errors:', errors, flush=True)
        except Exception as exc:
            print('Scan unavailable:', error_detail(exc), flush=True)
        time.sleep(max(5, 90-(time.monotonic()-start)))

def self_test():
    raw = [[i*60000, '10', '12', '9', '11', '1', (i+1)*60000-1, '100'] for i in range(480)]
    assert len(aggregate(raw, 5, 480*60000)) == 96
    assert len(aggregate(raw, 15, 480*60000)) == 32
    assert len(aggregate(raw, 5, 479*60000)) == 95
    assert len(aggregate(raw[1:], 5, 480*60000)) == 95
    assert candidate(raw, 480*60000) is None
    assert ema([10]*30, 9) == 10
    assert len(aggregate(raw[:4], 5, 480*60000)) == 0
    import tempfile
    global DB
    with tempfile.TemporaryDirectory() as directory:
        DB = str(Path(directory)/'test.sqlite3')
        init_db()
        with connect() as con:
            con.execute('INSERT INTO signals(symbol,ts,entry,stop,target,score,candle) VALUES (?,?,?,?,?,?,?)', ('TEST', 1, 100, 99, 102, 90, 1))
            con.execute('INSERT INTO results VALUES (1,5,301,101,1,.85)')
            con.execute('INSERT OR IGNORE INTO results VALUES (1,5,302,102,2,1.85)')
            assert con.execute('SELECT count(*) FROM results').fetchone()[0] == 1
    print('PASS: candle alignment, partial/gapped bars, EMA, neutral-market rejection.')


# All default ON: BTC_FILTER, HTF_FILTER, RETEST_FILTER, FUNDING_FILTER,
# OI_FILTER, ENABLE_SHORT, RESULT_TRACKING. Set any to 0 to disable.
# Thresholds are unvalidated heuristics; more filters do not guarantee accuracy.
# V2 filters can be disabled individually using an environment value of 0.
def enabled(name):
    return os.getenv(name, '1') != '0'

def trend_direction(raw, now_ms, width):
    rows = [r for r in raw if int(r[6]) < now_ms]
    if len(rows) < 50 or now_ms - (int(rows[-1][0]) + width) > width:
        return None
    if any(int(b[0])-int(a[0]) != width for a,b in zip(rows,rows[1:])):
        return None
    closes = [float(r[4]) for r in rows]
    fast, slow = ema(closes, 9), ema(closes, 21)
    if closes[-1] > fast > slow:
        return 'LONG'
    if closes[-1] < fast < slow:
        return 'SHORT'
    return None

def retest_setup(a, atr, direction):
    # Breakout must precede the latest candle; never use future candles.
    last = a[-1]
    for index in range(len(a)-2, len(a)-5, -1):
        previous = a[index-20:index]
        br = a[index]
        level = max(r[2] for r in previous) if direction == 1 else min(r[3] for r in previous)
        ratio = br[5] / max(st.mean(r[5] for r in previous), 1e-12)
        broke = direction*(br[4]-level) > 0 and ratio >= 1.5
        held = all(direction*(r[4]-level) >= -.15*atr for r in a[index+1:])
        touched = (last[3] <= level+.3*atr and last[3] >= level-.5*atr) if direction == 1 else (last[2] >= level-.3*atr and last[2] <= level+.5*atr)
        if broke and held and touched and direction*(last[4]-level)>0 and direction*(last[4]-last[1])>0:
            return ratio
    return None

def candidate(raw, now_ms):
    a, b = aggregate(raw,5,now_ms), aggregate(raw,15,now_ms)
    if len(a)<30 or len(b)<25 or now_ms-(a[-1][0]+300000)>90000:
        return None
    if any(y[0]-x[0]!=300000 for x,y in zip(a,a[1:])):
        return None
    if any(y[0]-x[0]!=900000 for x,y in zip(b,b[1:])):
        return None
    closes,big=[r[4] for r in a],[r[4] for r in b]
    last,prev=a[-1],a[-21:-1]
    atr=st.mean(max(a[i][2]-a[i][3],abs(a[i][2]-a[i-1][4]),abs(a[i][3]-a[i-1][4])) for i in range(len(a)-14,len(a)))
    if atr<=0:
        return None
    for side,d in [('LONG',1),('SHORT',-1)]:
        if side=='SHORT' and not enabled('ENABLE_SHORT'):
            continue
        trend=d*(big[-1]-ema(big,9))>0 and d*(ema(big,9)-ema(big,20))>0
        momentum=d*(closes[-1]-ema(closes,9))>0 and d*(ema(closes,9)-ema(closes,21))>0
        volume=last[5]/max(st.mean(r[5] for r in prev),1e-12)
        breakout=last[4]>max(r[2] for r in prev) if d==1 else last[4]<min(r[3] for r in prev)
        setup=breakout and volume>=1.5
        if enabled('RETEST_FILTER'):
            retest_volume=retest_setup(a,atr,d)
            setup=retest_volume is not None
            if setup:
                volume=retest_volume
        location=(last[4]-last[3])/max(last[2]-last[3],1e-12)
        strong=location>=.7 if d==1 else location<=.3
        extension=d*(last[4]-ema(closes,21))/atr
        score=25*trend+25*setup+20*(volume>=1.5)+20*momentum+10*strong
        if trend and setup and momentum and score>=80 and 0<extension<3:
            return dict(side=side,score=score,atr=atr,close=last[4],candle=last[0],volume=volume)
    return None

def derivatives_filter(symbol, result):
    d=1 if result['side']=='LONG' else -1
    if enabled('FUNDING_FILTER'):
        premium=market('/fapi/v1/premiumIndex',symbol=symbol)
        if abs(time.time()-float(premium['time'])/1000)>120:
            result['rejection'] = 'FUNDING_STALE'
            return False
        rate=float(premium['lastFundingRate'])
        # Reject costly crowded direction; this is a configurable heuristic.
        if d*rate>float(os.getenv('MAX_FUNDING_RATE','0.001')):
            result['rejection'] = 'FUNDING_CROWDED'
            return False
        result['funding_text']=f'{rate*100:+.4f}%'
    if enabled('OI_FILTER'):
        rows=market('/futures/data/openInterestHist',symbol=symbol,period='5m',limit=4)
        if len(rows)!=4 or abs(time.time()-int(rows[-1]['timestamp'])/1000)>600:
            result['rejection'] = 'OI_STALE_OR_MISSING'
            return False
        if any(int(y['timestamp'])-int(x['timestamp'])!=300000 for x,y in zip(rows,rows[1:])):
            result['rejection'] = 'OI_DATA_GAP'
            return False
        first=float(rows[0]['sumOpenInterest'])
        if first<=0:
            result['rejection'] = 'OI_INVALID'
            return False
        change=(float(rows[-1]['sumOpenInterest'])/first-1)*100
        if change<float(os.getenv('MIN_OI_CHANGE_PCT','0')):
            result['rejection'] = 'OI_CHANGE'
            return False
        result['oi_text']=f'{change:+.2f}%'
    return True

original_init_db=init_db
def init_db():
    original_init_db()
    with connect() as con:
        columns={r[1] for r in con.execute('PRAGMA table_info(signals)')}
        if 'side' not in columns:
            con.execute("ALTER TABLE signals ADD COLUMN side TEXT NOT NULL DEFAULT 'LONG'")
        con.execute('CREATE TABLE IF NOT EXISTS outcomes (signal_id INTEGER PRIMARY KEY, status TEXT, observed REAL, reference_price REAL, notified INTEGER DEFAULT 0)')

def barrier_outcome(raw,stamp,stop,target,side,now_ms):
    # 1m OHLC cannot establish ordering inside a candle or the entry minute.
    start=int(stamp*1000)//60000*60000
    end=(int(stamp*1000)+900000)//60000*60000
    rows=sorted([r for r in raw if start<=int(r[0])<=end and int(r[6])<now_ms],key=lambda r:int(r[0]))
    expected=start
    for row in rows:
        if int(row[0])!=expected:
            return ('DATA_GAP',int(row[6])/1000,None)
        expected+=60000
        high,low=float(row[2]),float(row[3])
        hit_stop=low<=stop if side=='LONG' else high>=stop
        hit_target=high>=target if side=='LONG' else low<=target
        if hit_stop or hit_target:
            partial=int(row[0])<stamp*1000 or int(row[6])>stamp*1000+900000
            if partial or (hit_stop and hit_target):
                return ('AMBIGUOUS',int(row[6])/1000,None)
            return ('STOP' if hit_stop else 'TARGET',int(row[6])/1000,stop if hit_stop else target)
    if now_ms>end+60000:
        if expected!=end+60000:
            return ('DATA_GAP',now_ms/1000,None)
        return ('TIMEOUT',now_ms/1000,None)
    return None

def track_outcomes():
    with connect() as con:
        pending=con.execute('SELECT id,symbol,ts,stop,target,side FROM signals WHERE NOT EXISTS (SELECT 1 FROM outcomes WHERE signal_id=signals.id) ORDER BY ts LIMIT 30').fetchall()
    now_ms=int(market('/fapi/v1/time')['serverTime']) if pending else int(time.time()*1000)
    for sid,symbol,stamp,stop,target,side in pending:
        start=int(stamp*1000)//60000*60000
        raw=market('/fapi/v1/klines',symbol=symbol,interval='1m',startTime=start,endTime=min(now_ms,start+1020000),limit=20)
        outcome=barrier_outcome(raw,stamp,stop,target,side,now_ms)
        if outcome:
            with connect() as con:
                con.execute('INSERT OR IGNORE INTO outcomes(signal_id,status,observed,reference_price) VALUES (?,?,?,?)',(sid,*outcome))
    labels={'TARGET':'Hedef görüldü','STOP':'Stop görüldü','AMBIGUOUS':'Mum içi sıra/zaman belirsiz','TIMEOUT':'15 dk içinde hedef/stop görülmedi','DATA_GAP':'Veri eksik; değerlendirilemedi'}
    with connect() as con:
        notices=con.execute('SELECT o.signal_id,s.symbol,s.side,o.status FROM outcomes o JOIN signals s ON s.id=o.signal_id WHERE o.notified=0').fetchall()
        for sid,symbol,side,status in notices:
            send(f'DENEME SONUCU | {symbol} {side}: {labels[status]}. 1dk mum incelemesi; gerçek emir gerçekleşmesi değildir.')
            con.execute('UPDATE outcomes SET notified=1 WHERE signal_id=?',(sid,))


if __name__ == '__main__':
    if '--self-test' in sys.argv:
        self_test()
    else:
        main()
