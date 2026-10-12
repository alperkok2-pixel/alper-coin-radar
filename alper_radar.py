"""ALPER Radar V7.1 | simple Binance USD-M perpetual PAPER alerts (never trades).
Python >=3.11, standard library only.
Run: python -u alper_radar.py    Test: python alper_radar.py --self-test
Env: TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, RADAR_DB (prefer persistent Render disk).
Optional: DETAIL_BATCH=8, LOOP_SECONDS=60, REQUEST_GAP_SECONDS=2.0,
MIN_24H_VOLUME=10000000, MAX_OPEN=12, COOLDOWN_HOURS=12,
TP1_PCT=1.5, TP2_PCT=2.5, TP3_PCT=5.0, STOP_PCT=1.5.
All eligible USD-M perpetual contracts rotate fairly: no high-volume-only hot pool.
Caveat: stop/target tracking is observational, not exchange fills; no profitability claim.
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
DB = os.getenv('RADAR_DB', 'alper_radar_v7.sqlite3')
MAX_CANDLE_AGE = max(15, int(os.getenv('MAX_CANDLE_AGE_SECONDS', '120')))
MAX_ENTRY_DRIFT = float(os.getenv('MAX_ENTRY_DRIFT_PCT', '0.35'))
ALERT_TTL = max(30, int(os.getenv('ENTRY_ALERT_TTL_SECONDS', '180')))
BATCH = max(1, min(20, int(os.getenv('DETAIL_BATCH', '8'))))
LOOP = max(60, int(os.getenv('LOOP_SECONDS', '60')))
GAP = max(1.0, float(os.getenv('REQUEST_GAP_SECONDS', '2.0')))
MIN_VOL = float(os.getenv('MIN_24H_VOLUME', '10000000'))
MAX_OPEN = max(1, int(os.getenv('MAX_OPEN', '12')))
COOLDOWN = max(1.0, float(os.getenv('COOLDOWN_HOURS', '12'))) * 3600
TPS = tuple(float(os.getenv(f'TP{i}_PCT', v)) for i, v in enumerate(('1.5', '2.5', '5.0'), 1))
STOP = float(os.getenv('STOP_PCT', '1.5'))
_last = 0.0
_block_until = 0.0


def log(msg):
    print(dt.datetime.now(dt.timezone.utc).isoformat(timespec='seconds'), msg, flush=True)


def http_json(url, data=None, timeout=15):
    request = urllib.request.Request(url, data=data, headers={'User-Agent': 'ALPER-Radar-V7-paper'}, method='POST' if data else 'GET')
    with urllib.request.urlopen(request, timeout=timeout) as r:
        return json.loads(r.read().decode('utf-8'))


def api(path, params=None):
    global _last, _block_until
    url = BASE + path
    if params:
        url += '?' + urllib.parse.urlencode(params)
    for attempt in range(3):
        pause = max(0.0, GAP - (time.monotonic() - _last), _block_until - time.monotonic())
        if pause:
            time.sleep(pause)
        _last = time.monotonic()
        try:
            return http_json(url)
        except urllib.error.HTTPError as e:
            if e.code in (418, 429):
                try:
                    wait = int(e.headers.get('Retry-After', '120'))
                except ValueError:
                    wait = 120
                _block_until = time.monotonic() + max(120, min(3600, wait))
                log(f'RATE LIMIT {e.code}; pause at least {int(_block_until-time.monotonic())} seconds')
                raise RuntimeError('rate limited') from e
            if e.code < 500 or attempt == 2:
                raise
        except (urllib.error.URLError, TimeoutError):
            if attempt == 2:
                raise
        time.sleep(2 * (attempt + 1))
    raise RuntimeError('API unsuccessful')


def connect():
    p = Path(DB)
    p.parent.mkdir(parents=True, exist_ok=True)
    c = sqlite3.connect(p)
    c.execute('PRAGMA journal_mode=WAL')
    c.execute('CREATE TABLE IF NOT EXISTS signals(id INTEGER PRIMARY KEY, symbol TEXT NOT NULL, side TEXT NOT NULL, entered REAL NOT NULL, stop REAL NOT NULL, tp1 REAL NOT NULL, tp2 REAL NOT NULL, tp3 REAL NOT NULL, opened INTEGER NOT NULL, progress INTEGER NOT NULL DEFAULT 0, closed INTEGER NOT NULL DEFAULT 0, closed_at INTEGER DEFAULT NULL)')
    c.execute('CREATE TABLE IF NOT EXISTS alerts(id INTEGER PRIMARY KEY, message TEXT NOT NULL, sent INTEGER NOT NULL DEFAULT 0, created INTEGER NOT NULL, kind TEXT NOT NULL DEFAULT \'info\')')
    cols = {r[1] for r in c.execute('PRAGMA table_info(alerts)')}
    if 'kind' not in cols:
        c.execute("ALTER TABLE alerts ADD COLUMN kind TEXT NOT NULL DEFAULT 'info'")
    c.commit()
    return c


def queue(c, message, kind='info'):
    c.execute('INSERT INTO alerts(message,created,kind) VALUES(?,?,?)', (message, int(time.time()), kind))
    c.commit()


def send_pending(c):
    for aid, message, created, kind in c.execute('SELECT id,message,created,kind FROM alerts WHERE sent=0 ORDER BY id LIMIT 20').fetchall():
        if kind == 'entry' and time.time() - created > ALERT_TTL:
            c.execute('UPDATE alerts SET sent=2 WHERE id=?', (aid,))
            c.commit()
            log(f'Expired late entry alert {aid}; not sent')
            continue
        if not TOKEN or not CHAT:
            log('Telegram keys missing; pending notification retained')
            return
        try:
            resp = http_json('https://api.telegram.org/bot' + TOKEN + '/sendMessage',
                             urllib.parse.urlencode({'chat_id': CHAT, 'text': message}).encode())
            if not resp.get('ok'):
                raise RuntimeError('Telegram ok=false')
            c.execute('UPDATE alerts SET sent=1 WHERE id=?', (aid,))
            c.commit()
        except Exception as e:
            log('Telegram notification retry later: ' + repr(e))
            return


def candles(symbol, interval, limit):
    raw = api('/fapi/v1/klines', {'symbol': symbol, 'interval': interval, 'limit': limit})
    now_ms = int(time.time() * 1000)
    # Never use the forming candle as a completed signal.
    return [{'t': int(x[0]), 'h': float(x[2]), 'l': float(x[3]), 'c': float(x[4]),
             'v': float(x[5]), 'close_ms': int(x[6])} for x in raw if int(x[6]) < now_ms]


def ema(values, n):
    if not values:
        return []
    a = 2 / (n + 1)
    out = [values[0]]
    for v in values[1:]:
        out.append(a*v + (1-a)*out[-1])
    return out


def rsi(values, n=14):
    if len(values) <= n:
        return 50.0
    changes = [values[i] - values[i-1] for i in range(1, len(values))]
    gain = sum(max(0, x) for x in changes[:n]) / n
    loss = sum(max(0, -x) for x in changes[:n]) / n
    for x in changes[n:]:
        gain = (gain*(n-1) + max(0, x))/n
        loss = (loss*(n-1) + max(0, -x))/n
    return 100.0 if loss == 0 else 100 - 100/(1 + gain/loss)


def evaluate(c5, c15, c1h):
    if min(len(c5), len(c15), len(c1h)) < 55:
        return None, 'history'
    p5, p15, p1h = ([x['c'] for x in c] for c in (c5, c15, c1h))
    e15 = ema(p15, 21)
    e1 = ema(p1h, 50)
    e5 = ema(p5, 9)
    direction = 'LONG' if p1h[-1] > e1[-1] and e1[-1] > e1[-5] and p15[-1] > e15[-1] else (
        'SHORT' if p1h[-1] < e1[-1] and e1[-1] < e1[-5] and p15[-1] < e15[-1] else None)
    if not direction:
        return None, 'trend'
    strength = rsi(p15)
    if direction == 'LONG' and not 52 <= strength <= 72:
        return None, 'rsi'
    if direction == 'SHORT' and not 28 <= strength <= 48:
        return None, 'rsi'
    # Confirm fresh 5-minute momentum, not a speculative intrabar crossover.
    good = (p5[-2] <= e5[-2] and p5[-1] > e5[-1] and p5[-1] > p5[-2]) if direction == 'LONG' else (
           p5[-2] >= e5[-2] and p5[-1] < e5[-1] and p5[-1] < p5[-2])
    if not good:
        return None, 'entry'
    avg_vol = sum(x['v'] for x in c5[-21:-1]) / 20
    if avg_vol <= 0 or c5[-1]['v'] < 1.2 * avg_vol:
        return None, 'volume'
    return direction, 'pass'


def symbols_list():
    info = api('/fapi/v1/exchangeInfo')
    names = {s['symbol'] for s in info['symbols'] if s.get('contractType') == 'PERPETUAL'
             and s.get('quoteAsset') == 'USDT' and s.get('status') == 'TRADING'}
    tick = api('/fapi/v1/ticker/24hr')
    return sorted(t['symbol'] for t in tick if t.get('symbol') in names and
                  float(t.get('quoteVolume', 0)) >= MIN_VOL)


def levels(side, price):
    sign = 1 if side == 'LONG' else -1
    targets = [price * (1 + sign * p/100) for p in TPS]
    stop = price * (1 - sign * STOP/100)
    return stop, targets


def entry_is_fresh(side, signal_bar, price, now_ms=None):
    if now_ms is None:
        now_ms = int(time.time() * 1000)
    age = (now_ms - signal_bar['close_ms']) / 1000
    if age < 0 or age > MAX_CANDLE_AGE:
        return False, 'stale_bar'
    close = signal_bar['c']
    if close <= 0 or price <= 0:
        return False, 'invalid_price'
    drift = 100 * (price / close - 1) * (1 if side == 'LONG' else -1)
    # A signal with a large favorable move has already departed; do not chase.
    if drift > MAX_ENTRY_DRIFT:
        return False, 'chased_price'
    # Reject a price that has reversed too far against the intended entry.
    if drift < -MAX_ENTRY_DRIFT:
        return False, 'reversal'
    return True, 'fresh'


def open_signal(c, symbol, side, price):
    now = int(time.time())
    last = c.execute('SELECT MAX(opened) FROM signals WHERE symbol=? AND side=?', (symbol,side)).fetchone()[0]
    if last is not None and now-last < COOLDOWN:
        return False
    if c.execute('SELECT COUNT(*) FROM signals WHERE closed=0').fetchone()[0] >= MAX_OPEN:
        return False
    stop, targets = levels(side, price)
    c.execute('INSERT INTO signals(symbol,side,entered,stop,tp1,tp2,tp3,opened) VALUES(?,?,?,?,?,?,?,?)',
              (symbol,side,price,stop,*targets,now))
    c.commit()
    queue(c, f'ALPER V7 PAPER {side} {symbol}\nEntry snapshot: {price:.10g}\n'
          f'TP1: {targets[0]:.10g} (+{TPS[0]}%)\nTP2: {targets[1]:.10g} (+{TPS[1]}%)\n'
          f'TP3: {targets[2]:.10g} (+{TPS[2]}%)\nSTOP: {stop:.10g} (-{STOP}%)\n'
          f'Signal sent: {dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")}\n'
          'PAPER only. Entry is observed price, not a fill.', kind='entry')
    return True


def monitor(c):
    active = c.execute('SELECT id,symbol,side,entered,stop,tp1,tp2,tp3,opened,progress FROM signals WHERE closed=0').fetchall()
    if not active:
        return
    for sid, symbol, side, entry, stop, t1, t2, t3, opened, progress in active:
        try:
            bars = candles(symbol, '5m', 4)
            # Bars close after signal was opened; ignore candle with pre-entry open.
            bars = [b for b in bars if b['t'] >= opened*1000]
            if not bars:
                continue
            for b in bars:
                highest, lowest = b['h'], b['l']
                hit_stop = lowest <= stop if side == 'LONG' else highest >= stop
                targets = (t1,t2,t3)
                achieved = [i+1 for i,t in enumerate(targets) if (highest >= t if side == 'LONG' else lowest <= t)]
                # Conservative outcome when target and stop occur within the same candle.
                if hit_stop:
                    c.execute('UPDATE signals SET closed=1,closed_at=? WHERE id=?', (int(time.time()),sid))
                    c.commit()
                    queue(c, f'ALPER V7 PAPER {side} {symbol}: STOP seen (same-candle ambiguity resolved against target)')
                    break
                new_level = max(achieved, default=0)
                if new_level > progress:
                    for level in range(progress+1,new_level+1):
                        queue(c, f'ALPER V7 PAPER {side} {symbol}: TP{level} observed on a closed 5m candle')
                    progress = new_level
                    if progress == 3:
                        c.execute('UPDATE signals SET progress=3,closed=1,closed_at=? WHERE id=?', (int(time.time()),sid))
                    else:
                        c.execute('UPDATE signals SET progress=? WHERE id=?', (progress,sid))
                    c.commit()
                    if progress == 3:
                        break
        except Exception as exc:
            log(f'Monitor {symbol} error: {type(exc).__name__}: {exc}')


def main():
    if not (0 < TPS[0] < TPS[1] < TPS[2] and 0 < STOP <= 5):
        raise ValueError('Target and stop configuration invalid')
    c = connect()
    log('ALPER V7 PAPER started; order execution DISABLED')
    queue(c, 'ALPER Radar V7 PAPER started; order execution DISABLED')
    universe, cursor, refreshed = [], 0, 0.0
    last_monitor_bucket = -1
    while True:
        begin = time.monotonic()
        try:
            send_pending(c)
            if not universe or time.monotonic()-refreshed > 3600:
                universe = symbols_list()
                refreshed = time.monotonic()
                cursor %= max(1,len(universe))
                log(f'Eligible symbols: {len(universe)} (24h volume >= {MIN_VOL:g} USDT)')
            bucket = int(time.time() // 300)
            if bucket != last_monitor_bucket:
                monitor(c)
                last_monitor_bucket = bucket
            if universe:
                picks = [universe[(cursor+i)%len(universe)] for i in range(min(BATCH,len(universe)))]
                cursor = (cursor+len(picks)) % len(universe)
                counts = {'pass':0,'history':0,'trend':0,'rsi':0,'entry':0,'volume':0,'api_error':0, 'stale_bar':0, 'chased_price':0, 'reversal':0, 'invalid_price':0}
                for symbol in picks:
                    try:
                        c1h = candles(symbol,'1h',65)
                        c15 = candles(symbol,'15m',65)
                        c5 = candles(symbol,'5m',65)
                        side, why = evaluate(c5,c15,c1h)
                        counts[why] += 1
                        if side:
                            # Snapshot price from public ticker (never use stale candle close as entry).
                            now_price = float(api('/fapi/v1/ticker/price',{'symbol':symbol})['price'])
                            fresh, cause = entry_is_fresh(side, c5[-1], now_price)
                            if not fresh:
                                counts[cause] += 1
                                log(f'Rejected {symbol} {side}: {cause}')
                            elif open_signal(c,symbol,side,now_price):
                                log(f'New PAPER {side} {symbol}')
                    except Exception as exc:
                        counts['api_error'] += 1
                        log(f'Scan {symbol} error {type(exc).__name__}: {exc}')
                        if 'rate limited' in str(exc):
                            break
                log(f'Scanned {len(picks)}/{len(universe)}; next index {cursor}; rejected: {counts}; open={c.execute("SELECT COUNT(*) FROM signals WHERE closed=0").fetchone()[0]}')
                send_pending(c)
        except Exception as e:
            log(f'Cycle error {type(e).__name__}: {e}')
        time.sleep(max(2, LOOP-(time.monotonic()-begin)))


def self_test():
    import tempfile
    global DB
    old = DB
    with tempfile.TemporaryDirectory() as folder:
        DB = str(Path(folder)/'test.db')
        c = connect()
        assert len(ema([1,2,3], 2)) == 3
        assert round(rsi(list(range(1,40)))) == 100
        s,t = levels('LONG',100)
        assert abs(s-98.5)<1e-8 and abs(t[0]-101.5)<1e-8
        s,t = levels('SHORT',100)
        assert abs(s-101.5)<1e-8 and abs(t[2]-95)<1e-8
        assert open_signal(c,'BTCUSDT','LONG',100)
        assert not open_signal(c,'BTCUSDT','LONG',100)
        assert c.execute('SELECT COUNT(*) FROM alerts').fetchone()[0] == 1
        now = int(time.time()*1000)
        bar = {'close_ms':now-30000, 'c':100}
        assert entry_is_fresh('LONG',bar,100.1,now)[0]
        assert entry_is_fresh('SHORT',bar,99.9,now)[0]
        assert entry_is_fresh('LONG',bar,101,now)[1] == 'chased_price'
        assert entry_is_fresh('LONG',bar,100,now+200000)[1] == 'stale_bar'
        assert entry_is_fresh('LONG',bar,99,now)[1] == 'reversal'
        queue(c,'old entry',kind='entry')
        c.execute('UPDATE alerts SET created=? WHERE message=?',(int(time.time())-ALERT_TTL-10,'old entry'))
        c.commit()
        c.close()
    DB = old
    print('SELF TEST PASS: indicators, levels, database, cooldown, freshness, late entry suppression')


if __name__ == '__main__':
    if '--self-test' in sys.argv:
        self_test()
    else:
        main()