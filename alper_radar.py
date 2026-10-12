"""ALPER Radar V4.3 - paper-only Binance USDT perpetual scanner and tracker.
Python 3.11+, standard library. No trading orders / API keys.
Render Start: python -u alper_radar.py
Required: TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, PAPER_MODE=1
Suggested: DATA_DIR=/var/data if a persistent Render disk is attached.
Test: python alper_radar.py --self-test
"""
import datetime as dt
import json
import os
import random
import sqlite3
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter
from pathlib import Path

BASE = 'https://fapi.binance.com'
TOKEN = os.getenv('TELEGRAM_TOKEN', '')
CHAT = os.getenv('TELEGRAM_CHAT_ID', '')
DB_PATH = Path(os.getenv('DATA_DIR', '.')) / 'alper_radar_results.sqlite3'
_last_request = 0.0
_pause_until = 0.0
_offset = 0
_last_health = 0.0
_last_warning = 0.0


def setting(name, default):
    return float(os.getenv(name, default))


def log(*args):
    print(dt.datetime.now(dt.timezone.utc).isoformat(timespec='seconds'), *args, flush=True)


def http(url, params=None, post=None, exchange=False):
    global _last_request, _pause_until
    if exchange:
        remain = _pause_until - time.monotonic()
        if remain > 0:
            raise RuntimeError('BINANCE_COOLDOWN')
        wait = setting('REQUEST_INTERVAL_SECONDS', 2.5) - (time.monotonic() - _last_request)
        if wait > 0:
            time.sleep(wait)
        _last_request = time.monotonic()
    if params:
        url += '?' + urllib.parse.urlencode(params)
    data = urllib.parse.urlencode(post).encode('utf-8') if post is not None else None
    try:
        with urllib.request.urlopen(url, data=data, timeout=18) as response:
            if exchange:
                used = int(response.headers.get('X-MBX-USED-WEIGHT-1M', '0'))
                if used >= setting('IP_WEIGHT_SOFT_LIMIT', 900):
                    _pause_until = max(_pause_until, time.monotonic() + 90)
                    log('Binance shared-IP weight high', used)
            return json.load(response)
    except urllib.error.HTTPError as exc:
        if exchange and exc.code in (418, 429):
            try:
                retry = float(exc.headers.get('Retry-After', '0'))
            except (TypeError, ValueError):
                retry = 0
            _pause_until = max(_pause_until, time.monotonic() + max(retry, 900 if exc.code == 418 else 300))
        raise RuntimeError(f'{"Binance" if exchange else "Telegram"} HTTP {exc.code}') from None
    except (TimeoutError, urllib.error.URLError) as exc:
        raise RuntimeError(f'Network: {type(exc).__name__}') from None


def market(path, **params):
    return http(BASE + path, params=params, exchange=True)


def send(message):
    if not TOKEN or not CHAT:
        log('Telegram credentials missing')
        return False
    response = http('https://api.telegram.org/bot' + TOKEN + '/sendMessage',
                    post={'chat_id': CHAT, 'text': message[:3900]})
    return bool(response.get('ok'))


def db_open():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(str(DB_PATH), timeout=20)
    con.execute('PRAGMA busy_timeout=20000')
    con.execute('''CREATE TABLE IF NOT EXISTS signals (
        id INTEGER PRIMARY KEY, symbol TEXT NOT NULL, side TEXT NOT NULL,
        method TEXT NOT NULL, candle INTEGER NOT NULL,
        start_ms INTEGER NOT NULL, expiry_ms INTEGER NOT NULL,
        entry REAL NOT NULL, stop REAL NOT NULL, target REAL NOT NULL,
        outcome TEXT NOT NULL DEFAULT 'OPEN', outcome_ms INTEGER,
        detail TEXT DEFAULT '', notified INTEGER NOT NULL DEFAULT 0,
        UNIQUE(symbol, side, candle))''')
    cols = {r[1] for r in con.execute('PRAGMA table_info(signals)')}
    if 'checked_ms' not in cols:
        con.execute('ALTER TABLE signals ADD COLUMN checked_ms INTEGER')
    if 'mid_notified' not in cols:
        con.execute('ALTER TABLE signals ADD COLUMN mid_notified INTEGER NOT NULL DEFAULT 0')
    con.commit()
    return con


def duration(method):
    return int(setting('BREAKOUT_TRACK_MINUTES', 30) if method == 'BREAKOUT'
               else setting('PULLBACK_TRACK_MINUTES', 60))


def add_signal(con, symbol, signal, timestamp_ms):
    # Observe only FULL 1m candles beginning after the notification.
    start_ms = ((timestamp_ms + 59999) // 60000) * 60000
    expiry_ms = start_ms + duration(signal['method']) * 60000
    con.execute('''INSERT OR IGNORE INTO signals
        (symbol,side,method,candle,start_ms,expiry_ms,entry,stop,target)
        VALUES (?,?,?,?,?,?,?,?,?)''',
        (symbol, signal['side'], signal['method'], signal['candle'], start_ms,
         expiry_ms, signal['close'], signal['stop'], signal['target']))
    con.commit()


def finish(con, sid, outcome, when_ms, detail=''):
    con.execute('''UPDATE signals SET outcome=?,outcome_ms=?,detail=?,notified=0
                   WHERE id=? AND outcome='OPEN' ''', (outcome, when_ms, detail, sid))
    con.commit()


def check_one(con, r, now_ms):
    sid, sym, side, method, start_ms, expiry_ms, entry, stop, target, checked_ms = r
    cursor = max(start_ms, checked_ms + 60000 if checked_ms is not None else start_ms)
    # The final candle must be fully closed; allow a small exchange close buffer.
    end_ms = min((now_ms - 2000) // 60000 * 60000 - 60000, expiry_ms - 60000)
    if cursor > end_ms:
        if now_ms >= expiry_ms + 2000 and (checked_ms is not None and checked_ms >= expiry_ms - 60000):
            finish(con, sid, 'SURE_DOLDU', expiry_ms, 'Hedef veya stop gorulmedi')
        return
    while cursor <= end_ms:
        candles = market('/fapi/v1/klines', symbol=sym, interval='1m',
                         startTime=cursor, endTime=min(end_ms, cursor + 499 * 60000) + 59999,
                         limit=500)
        if not candles:
            return  # Do not label API issues as a losing trade.
        previous_cursor = cursor
        for bar in candles:
            bar_ms = int(bar[0])
            if bar_ms < cursor:
                continue
            if bar_ms != cursor:
                finish(con, sid, 'VERI_EKSIK', now_ms, '1m mum sirasi eksik')
                return
            high, low = float(bar[2]), float(bar[3])
            stop_hit = low <= stop if side == 'LONG' else high >= stop
            target_hit = high >= target if side == 'LONG' else low <= target
            if stop_hit and target_hit:
                finish(con, sid, 'BELIRSIZ', bar_ms, 'Ayni 1m mumda hedef ve stop')
                return
            if stop_hit or target_hit:
                finish(con, sid, 'STOP' if stop_hit else 'HEDEF', bar_ms)
                return
            con.execute('UPDATE signals SET checked_ms=? WHERE id=?', (bar_ms, sid))
            cursor += 60000
        con.commit()
        if cursor == previous_cursor:
            return
    if cursor >= expiry_ms and now_ms >= expiry_ms + 2000:
        finish(con, sid, 'SURE_DOLDU', expiry_ms, 'Hedef veya stop gorulmedi')


def notify(con):
    rows = con.execute('''SELECT id,symbol,side,method,entry,stop,target,outcome,detail
                          FROM signals WHERE outcome!='OPEN' AND notified=0
                          ORDER BY id LIMIT 50''').fetchall()
    for sid, sym, side, method, entry, stop, target, outcome, detail in rows:
        message = (f'ALPER RADAR V4.3 | SONUC: {outcome}\n{sym} | {side} | {method}\n'
                   f'Giris referansi: {entry:.8g}\nStop: {stop:.8g}\n'
                   f'Hedef: {target:.8g}\n{detail}\n'
                   'Sanal izleme; emir acilmadi. Mum ici siralama bilinmeyebilir.')
        if send(message):
            con.execute('UPDATE signals SET notified=1 WHERE id=?', (sid,))
            con.commit()
        else:
            break


def mid_reports(con, now_ms):
    # Optional preliminary report at 30 min for 60-min PULLBACK positions.
    rows = con.execute('''SELECT id,symbol,side,start_ms,expiry_ms,checked_ms FROM signals
                          WHERE outcome='OPEN' AND method='PULLBACK' AND mid_notified=0''').fetchall()
    for sid, symbol, side, start, expiry, checked in rows:
        halfway = start + 30 * 60000
        if now_ms >= halfway + 2000 and checked is not None and checked >= halfway - 60000:
            if send(f'ALPER RADAR V4.3 | 30 DK ARA DURUM\n{symbol} | {side}\n'
                    'Ilk 30 dakikada hedef/stop tespit edilmedi. 60 dk takip suruyor.'):
                con.execute('UPDATE signals SET mid_notified=1 WHERE id=?', (sid,))
                con.commit()


def monitor(con):
    # Telegram pending notifications can be sent even during Binance cooldown.
    notify(con)
    now_ms = int(time.time() * 1000)
    if time.monotonic() < _pause_until:
        return
    rows = con.execute('''SELECT id,symbol,side,method,start_ms,expiry_ms,entry,stop,target,checked_ms
                          FROM signals WHERE outcome='OPEN' ORDER BY id''').fetchall()
    for r in rows:
        try:
            check_one(con, r, now_ms)
        except RuntimeError as exc:
            log('Tracker deferred', r[1], str(exc))
            if 'COOLDOWN' in str(exc) or '429' in str(exc) or '418' in str(exc):
                break
    notify(con)
    if time.monotonic() >= _pause_until:
        mid_reports(con, now_ms)


def ema(vals, n):
    if not vals:
        raise ValueError('Empty EMA input')
    result = vals[0]
    alpha = 2 / (n + 1)
    for value in vals[1:]:
        result += alpha * (value - result)
    return result


def closed(rows, now_ms):
    return [r for r in rows if int(r[6]) < now_ms]


def direction(rows, now_ms):
    bars = closed(rows, now_ms)
    if len(bars) < 35:
        return None
    c = [float(r[4]) for r in bars]
    e9, e21 = ema(c, 9), ema(c, 21)
    if c[-1] > e9 > e21:
        return 'LONG'
    if c[-1] < e9 < e21:
        return 'SHORT'
    return None


def target_filter(entry, stop, target, side):
    if min(entry, stop, target) <= 0:
        return False, 'INVALID_LEVELS', 0, 0
    sign = 1 if side == 'LONG' else -1
    gain = sign * (target - entry) / entry * 100
    loss = sign * (entry - stop) / entry * 100
    if gain <= 0 or loss <= 0:
        return False, 'INVALID_LEVELS', gain, 0
    rr = gain / loss
    if gain < setting('MIN_TARGET_PCT', 1.0):
        return False, 'LOW_TARGET_PCT', gain, rr
    if rr < setting('MIN_REWARD_RISK', 1.5) - 1e-8:
        return False, 'LOW_REWARD_RISK', gain, rr
    return True, 'OK', gain, rr


def evaluate(five, fifteen, hourly, now_ms):
    a, b, h = [closed(rows, now_ms) for rows in (five, fifteen, hourly)]
    if any(len(x) < 35 for x in (a, b, h)):
        return None, 'INSUFFICIENT_DATA'
    for bars, step in ((a, 300000), (b, 900000), (h, 3600000)):
        if any(int(y[0]) - int(x[0]) != step for x, y in zip(bars[-35:-1], bars[-34:])):
            return None, 'DATA_GAP'
    if now_ms - (int(a[-1][0]) + 300000) > 120000:
        return None, 'STALE_5M'
    t15, t1h = direction(b, now_ms), direction(h, now_ms)
    if not t15:
        return None, 'NO_15M_TREND'
    if os.getenv('HTF_FILTER', '1') != '0' and t1h != t15:
        return None, 'HTF_MISMATCH'
    side = t15
    sign = 1 if side == 'LONG' else -1
    closes = [float(r[4]) for r in a]
    volumes = [float(r[5]) for r in a]
    avg = sum(volumes[-21:-1]) / 20
    volume_ratio = volumes[-1] / avg if avg else 0
    if volume_ratio < setting('MIN_VOLUME_RATIO', 1.5):
        return None, 'LOW_VOLUME'
    e9, e21 = ema(closes, 9), ema(closes, 21)
    last = a[-1]
    price = closes[-1]
    trend5 = sign * (e9 - e21) > 0
    momentum = sign * (price - float(last[1])) > 0
    prev_high = max(float(r[2]) for r in a[-13:-1])
    prev_low = min(float(r[3]) for r in a[-13:-1])
    breakout = price > prev_high if sign == 1 else price < prev_low
    pullback = (min(float(r[3]) for r in a[-4:-1]) <= e21 * 1.002 if sign == 1
                else max(float(r[2]) for r in a[-4:-1]) >= e21 * .998)
    resumed = sign * (price - closes[-2]) > 0 and sign * (price - e9) > 0
    method = 'BREAKOUT' if breakout and momentum else ('PULLBACK' if pullback and resumed else None)
    if not method:
        return None, 'NO_SETUP'
    score = int(trend5) + int(momentum) + int(volume_ratio >= 1.3) + int(t1h == side)
    if score < int(setting('MIN_SCORE', 4)):
        return None, 'LOW_SCORE'
    ranges = []
    for i in range(len(a)-14, len(a)):
        bar = a[i]
        prev_close = float(a[i-1][4])
        ranges.append(max(float(bar[2]) - float(bar[3]),
                          abs(float(bar[2]) - prev_close), abs(float(bar[3]) - prev_close)))
    atr = sum(ranges) / len(ranges)
    if atr <= 0:
        return None, 'INVALID_ATR'
    stop = price - sign * 1.2 * atr
    target = price + sign * 1.8 * atr
    risk_pct = abs(price-stop) / price * 100
    if min(stop, target) <= 0 or not .15 <= risk_pct <= setting('MAX_RISK_PCT', 1.5):
        return None, 'RISK_DISTANCE'
    accept, reason, gain, rr = target_filter(price, stop, target, side)
    if not accept:
        return None, reason
    return {'target_pct': gain, 'reward_risk': rr, 'side': side, 'method': method,
            'close': price, 'stop': stop, 'target': target, 'risk_pct': risk_pct,
            'score': score, 'volume': volume_ratio, 'candle': int(last[0])}, 'OK'


def scan(con):
    global _offset
    now_ms = int(market('/fapi/v1/time')['serverTime'])
    exchange_info = market('/fapi/v1/exchangeInfo')
    symbols = {s['symbol'] for s in exchange_info['symbols'] if s['status'] == 'TRADING'
               and s['contractType'] == 'PERPETUAL' and s['quoteAsset'] == 'USDT'}
    tickers = market('/fapi/v1/ticker/24hr')
    eligible = sorted(t['symbol'] for t in tickers if t['symbol'] in symbols
                      and float(t['quoteVolume']) >= setting('MIN_VOLUME_USDT', 20000000)
                      and abs(float(t['priceChangePercent'])) <= setting('MAX_24H_CHANGE_PCT', 20))
    if not eligible:
        return 0, 0, Counter({'NO_ELIGIBLE_SYMBOLS': 1})
    count = min(len(eligible), max(1, int(setting('MAX_SYMBOLS_PER_SCAN', 12))))
    batch = (eligible[_offset:] + eligible[:_offset])[:count]
    _offset = (_offset + count) % len(eligible)
    btc = None
    if os.getenv('BTC_FILTER', '0') == '1':
        btc = direction(market('/fapi/v1/klines', symbol='BTCUSDT', interval='15m', limit=60), now_ms)
    reasons = Counter()
    alerts = 0
    for symbol in batch:
        if time.monotonic() < _pause_until:
            reasons['COOLDOWN'] += 1
            break
        try:
            f = market('/fapi/v1/klines', symbol=symbol, interval='5m', limit=80)
            q = market('/fapi/v1/klines', symbol=symbol, interval='15m', limit=60)
            h = market('/fapi/v1/klines', symbol=symbol, interval='1h', limit=60)
            result, reason = evaluate(f, q, h, now_ms)
            if result and btc and result['side'] != btc:
                result, reason = None, 'BTC_MISMATCH'
            if not result:
                reasons[reason] += 1
                continue
            exists = con.execute('SELECT 1 FROM signals WHERE symbol=? AND side=? AND candle=?',
                                 (symbol, result['side'], result['candle'])).fetchone()
            if exists:
                reasons['DUPLICATE'] += 1
                continue
            message = (f'ALPER RADAR V4.3 | DENEME | {result["side"]} ADAYI\n'
                       f'Parite: {symbol}\nYontem: {result["method"]}\n'
                       f'5m kapanis: {result["close"]:.8g}\nStop: {result["stop"]:.8g}\n'
                       f'Hedef: {result["target"]:.8g}\n'
                       f'Teorik hedef: %{result["target_pct"]:.2f}\n'
                       f'Stop mesafesi: %{result["risk_pct"]:.2f}\n'
                       f'Hacim: {result["volume"]:.2f}x\n'
                       f'Risk/getiri: 1:{result["reward_risk"]:.2f}\n'
                       f'Takip: {duration(result["method"])} dk\n'
                       'Deneysel sanal sinyal. Emir acilmaz; kar garantisi yok.')
            if send(message):
                add_signal(con, symbol, result, int(time.time() * 1000))
                alerts += 1
                log('ALERT', symbol, result['side'], result['method'])
        except RuntimeError as exc:
            log('Scan symbol error', symbol, str(exc))
            reasons[str(exc)] += 1
            if 'COOLDOWN' in str(exc) or '429' in str(exc) or '418' in str(exc):
                break
    return len(batch), alerts, reasons


def main():
    global _last_health, _last_warning
    if os.getenv('PAPER_MODE', '1') != '1':
        raise RuntimeError('PAPER_MODE must be 1')
    if not TOKEN or not CHAT:
        raise RuntimeError('Set TELEGRAM_TOKEN and TELEGRAM_CHAT_ID')
    con = db_open()
    send('ALPER Radar V4.3 baslatildi | SANAL takip | LONG + SHORT | 30/60 dk. Emir acilmaz.')
    next_scan = time.time()  # one initial scan, then five-minute boundaries
    while True:
        try:
            monitor(con)
            if time.time() >= next_scan:
                if time.monotonic() >= _pause_until:
                    count, alerts, reasons = scan(con)
                    log('Scan done', count, 'alerts', alerts, 'rejections', dict(reasons))
                    if time.monotonic() - _last_health >= setting('HEALTH_INTERVAL_SECONDS', 1800):
                        send(f'ALPER Radar V4.3 DURUM\nTarandi: {count}\nSinyal: {alerts}\n'
                             f'Eleme: {dict(reasons.most_common(6))}\n'
                             f'Acik takip: {con.execute("SELECT COUNT(*) FROM signals WHERE outcome=\'OPEN\'").fetchone()[0]}')
                        _last_health = time.monotonic()
                next_scan = (int(time.time() // 300) + 1) * 300 + 7
        except Exception as exc:
            log('Loop error', type(exc).__name__, str(exc))
            if time.monotonic() - _last_warning >= 600:
                try:
                    send(f'ALPER Radar V4.3 UYARI: {type(exc).__name__}: {str(exc)[:90]}')
                    _last_warning = time.monotonic()
                except Exception as e:
                    log('Notification failure', type(e).__name__)
            next_scan = max(next_scan, time.time() + 60)
        # Tracker is checked once per minute, even during Binance cooldown.
        time.sleep(60)


def self_test():
    assert abs(ema([10] * 40, 21) - 10) < 1e-10
    assert direction([], 1000000) is None
    assert evaluate([], [], [], 1000000)[1] == 'INSUFFICIENT_DATA'
    assert closed([[0, 1, 2, 0, 1, 1, 59999]], 60000)
    assert not closed([[0, 1, 2, 0, 1, 1, 59999]], 59999)
    assert duration('BREAKOUT') == 30 and duration('PULLBACK') == 60
    assert target_filter(100, 99, 101.5, 'LONG')[0]
    assert target_filter(100, 101, 98.5, 'SHORT')[0]
    assert target_filter(100, 99, 100.5, 'LONG')[1] == 'LOW_TARGET_PCT'
    assert target_filter(100, 101, 99.6, 'SHORT')[1] == 'LOW_TARGET_PCT'
    import tempfile
    with tempfile.TemporaryDirectory() as directory:
        global DB_PATH
        old_path = DB_PATH
        try:
            DB_PATH = Path(directory) / 'test.sqlite3'
            conn = db_open()
            sig = {'side':'LONG', 'method':'PULLBACK', 'candle':123,
                   'close':100, 'stop':99, 'target':101.5}
            add_signal(conn, 'TESTUSDT', sig, 100000)
            row = conn.execute('SELECT start_ms,expiry_ms FROM signals').fetchone()
            assert row == (120000, 3720000), row
            conn.close()
        finally:
            DB_PATH = old_path
    print('PASS: filters, candle rules, DB tracking windows')


if __name__ == '__main__':
    if '--self-test' in sys.argv:
        self_test()
    else:
        main()