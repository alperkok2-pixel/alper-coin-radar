"""ALPER Coin Radar — experimental LONG candidate scanner, Python 3.11+.

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
Evaluation is observed last-price return at/after +5/+15 minutes, with actual
observation time stored; it is not an executable backtest or TP/SL fill model.
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

def request(url, params=None, data=None, market=False):
    global next_call, pause_until
    if market:
        with gate:
            delay = max(next_call, pause_until) - time.monotonic()
            if delay > 0:
                time.sleep(delay)
            next_call = time.monotonic() + .2  # at most 5 market requests/sec
    if params:
        url += '?' + urllib.parse.urlencode(params)
    payload = None if data is None else urllib.parse.urlencode(data).encode()
    try:
        with urllib.request.urlopen(url, payload, timeout=15) as response:
            return json.load(response)
    except urllib.error.HTTPError as exc:
        if market and exc.code in (418, 429):
            with gate:
                pause_until = time.monotonic() + max(120, int(exc.headers.get('Retry-After', '120')))
        # Avoid printing URLs containing Telegram credentials.
        raise RuntimeError('HTTP status ' + str(exc.code)) from None

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

def candidate(raw, now_ms):
    a, b = aggregate(raw, 5, now_ms), aggregate(raw, 15, now_ms)
    if len(a) < 30 or len(b) < 25 or now_ms - (a[-1][0] + 300000) > 90000:
        return None
    closes = [r[4] for r in a]
    big = [r[4] for r in b]
    last, prev = a[-1], a[-21:-1]
    atr = st.mean(max(a[i][2] - a[i][3], abs(a[i][2] - a[i-1][4]),
                      abs(a[i][3] - a[i-1][4])) for i in range(len(a)-14, len(a)))
    if atr <= 0:
        return None
    trend = big[-1] > ema(big, 9) > ema(big, 20)
    volume = last[5] / max(st.mean(r[5] for r in prev), 1e-12)
    breakout = last[4] > max(r[2] for r in prev)
    momentum = closes[-1] > ema(closes, 9) > ema(closes, 21)
    strong_close = (last[4]-last[3]) / max(last[2]-last[3], 1e-12) >= .7
    extension = (last[4]-ema(closes, 21)) / atr
    score = 25*trend + 25*breakout + 20*(volume >= 1.5) + 20*momentum + 10*strong_close
    if not (trend and breakout and volume >= 1.5 and score >= 80 and 0 < extension < 3):
        return None
    return {'score': score, 'atr': atr, 'close': last[4], 'candle': last[0], 'volume': volume}

def monitor():
    while True:
        try:
            with connect() as con:
                pending = con.execute('SELECT id,symbol,ts,entry FROM signals WHERE ts > ?', (time.time()-7200,)).fetchall()
                if pending:
                    prices = {r['symbol']: r for r in market('/fapi/v2/ticker/price')}
                    now = time.time()
                    for sid, symbol, stamp, entry in pending:
                        item = prices.get(symbol)
                        if not item:
                            continue
                        observed = float(item['time']) / 1000
                        if not 0 <= now - observed <= 30:
                            continue
                        price = float(item['price'])
                        for minutes in (5, 15):
                            if observed >= stamp + minutes*60:
                                gross = (price/entry-1)*100
                                con.execute('INSERT OR IGNORE INTO results VALUES (?,?,?,?,?,?)',
                                            (sid, minutes, observed, price, gross,
                                             gross-float(os.getenv('COST_BPS', '15'))/100))
        except Exception as exc:
            print('Evaluation error:', type(exc).__name__, flush=True)
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
    return symbol, candidate(raw, now_ms)

def main():
    if not CHAT:
        pairing()
        return
    if not TOKEN:
        raise RuntimeError('Missing TELEGRAM_TOKEN')
    init_db()
    send('ALPER Radar started — ' + ('DENEME' if PAPER else 'ADAY BİLDİRİMLERİ') + '. Otomatik işlem yok; başarı oranı henüz ölçülmedi.')
    threading.Thread(target=monitor, daemon=True).start()
    while True:
        start = time.monotonic()
        try:
            now_ms = int(market('/fapi/v1/time')['serverTime'])
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
                        with connect() as con:
                            exists = con.execute('SELECT 1 FROM signals WHERE symbol=? AND (candle=? OR ts>?)',
                                                 (symbol, result['candle'], time.time()-900)).fetchone()
                            if exists:
                                continue
                            quote = market('/fapi/v2/ticker/price', symbol=symbol)
                            entry = float(quote['price'])
                            stamp = time.time()
                            if stamp - (result['candle']/1000 + 300) > 120:
                                continue
                            if not 0 <= stamp-float(quote['time'])/1000 <= 30:
                                continue
                            if abs(entry-result['close']) > .5*result['atr']:
                                continue
                            risk = 1.2*result['atr']
                            stop, target = entry-risk, entry+1.5*risk
                            if stop <= 0:
                                continue
                            con.execute('INSERT INTO signals(symbol,ts,entry,stop,target,score,candle) VALUES (?,?,?,?,?,?,?)',
                                        (symbol, stamp, entry, stop, target, result['score'], result['candle']))
                        clock = dt.datetime.fromtimestamp(stamp, dt.timezone(dt.timedelta(hours=3))).strftime('%H:%M:%S')
                        send(f"{'DENEME | ' if PAPER else ''}LONG ADAYI: {symbol}\nSaat (TR): {clock}\nKural puanı: {result['score']}/100 (olasılık değil)\nReferans fiyat: {entry:.8g}\nStop referansı: {stop:.8g}\nHedef referansı: {target:.8g}\nHacim: {result['volume']:.2f}x\nİzleme: sonraki 5 ve 15 dk. Seviyeler otomatik emir değildir.")
                        found += 1
                    except Exception as exc:
                        errors += 1
                        print('Symbol error:', type(exc).__name__, flush=True)
            print('Scan seconds:', round(time.monotonic()-start), 'alerts:', found, 'errors:', errors, flush=True)
        except Exception as exc:
            print('Scan unavailable:', type(exc).__name__, flush=True)
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

if __name__ == '__main__':
    if '--self-test' in sys.argv:
        self_test()
    else:
        main()
