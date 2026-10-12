"""ALPER Radar V4.2 + tracker: paper-only Binance USDT perpetual scanner.
Python 3.11+, standard library only. No order execution.
Render start: python -u alper_radar.py
Environment: TELEGRAM_TOKEN, TELEGRAM_CHAT_ID (secrets), PAPER_MODE=1.
Optional: MAX_SYMBOLS_PER_SCAN=20, MIN_VOLUME_USDT=20000000,
SCAN_INTERVAL_SECONDS=300, REQUEST_INTERVAL_SECONDS=2.2,
MIN_VOLUME_RATIO=1.0, MIN_SCORE=3, BTC_FILTER=0, HTF_FILTER=1,
MAX_24H_CHANGE_PCT=20, HEALTH_INTERVAL_SECONDS=1800.
Run: python alper_radar.py --self-test
"""
import datetime as dt
import json
import sqlite3
from pathlib import Path
import os
import random
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter

BASE = 'https://fapi.binance.com'
TOKEN = os.getenv('TELEGRAM_TOKEN', '')
CHAT = os.getenv('TELEGRAM_CHAT_ID', '')
PAPER = os.getenv('PAPER_MODE', '1') == '1'
_last_request = 0.0
_pause_until = 0.0
_offset = 0
_last_health = 0.0
_last_alert = {}
DB_PATH = Path(os.getenv("DATA_DIR", ".")) / "alper_radar_results.sqlite3"


def envfloat(name, default):
    return float(os.getenv(name, str(default)))


def log(*parts):
    print(dt.datetime.now(dt.timezone.utc).isoformat(timespec='seconds'), *parts, flush=True)


def http(url, params=None, post=None, exchange=False):
    global _last_request, _pause_until
    if exchange:
        if time.monotonic() < _pause_until:
            raise RuntimeError('Binance cooldown active')
        wait = envfloat('REQUEST_INTERVAL_SECONDS', 2.2) - (time.monotonic() - _last_request)
        if wait > 0:
            time.sleep(wait)
        _last_request = time.monotonic()
    if params:
        url += '?' + urllib.parse.urlencode(params)
    data = urllib.parse.urlencode(post).encode() if post else None
    try:
        with urllib.request.urlopen(url, data=data, timeout=18) as resp:
            if exchange:
                used = int(resp.headers.get('X-MBX-USED-WEIGHT-1M', '0'))
                if used >= int(envfloat('IP_WEIGHT_SOFT_LIMIT', 900)):
                    _pause_until = max(_pause_until, time.monotonic() + 90)
                    log('Shared IP weight high:', used)
            return json.load(resp)
    except urllib.error.HTTPError as exc:
        if exchange and exc.code in (418, 429):
            try:
                retry = float(exc.headers.get('Retry-After', '0'))
            except (ValueError, TypeError):
                retry = 0
            _pause_until = max(_pause_until, time.monotonic() + max(retry, 900 if exc.code == 418 else 300))
        raise RuntimeError(f'{"Binance" if exchange else "Telegram"} HTTP {exc.code}') from None
    except (TimeoutError, urllib.error.URLError) as exc:
        raise RuntimeError(f'Network error: {type(exc).__name__}') from None


def market(path, **params):
    return http(BASE + path, params, exchange=True)


def send(message):
    if not TOKEN or not CHAT:
        log('Telegram not configured; message not sent')
        return False
    response = http('https://api.telegram.org/bot' + TOKEN + '/sendMessage',
                    post={'chat_id': CHAT, 'text': message[:3900]})
    if not response.get('ok'):
        raise RuntimeError('Telegram rejected message')
    return True



def db_open():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(str(DB_PATH), timeout=20)
    con.execute("PRAGMA busy_timeout=20000")
    con.execute("""CREATE TABLE IF NOT EXISTS signals (
       id INTEGER PRIMARY KEY, symbol TEXT NOT NULL, side TEXT NOT NULL,
       method TEXT NOT NULL, candle INTEGER NOT NULL,
       start_ms INTEGER NOT NULL, expiry_ms INTEGER NOT NULL,
       entry REAL NOT NULL, stop REAL NOT NULL, target REAL NOT NULL,
       outcome TEXT NOT NULL DEFAULT 'OPEN', outcome_ms INTEGER,
       detail TEXT DEFAULT '', notified INTEGER NOT NULL DEFAULT 0,
       UNIQUE(symbol, side, candle))""")
    con.commit()
    return con


def track_minutes(method):
    return int(envfloat('BREAKOUT_TRACK_MINUTES', 30) if method == 'BREAKOUT'
               else envfloat('PULLBACK_TRACK_MINUTES', 60))


def add_signal(con, symbol, signal, sent_ms):
    # First completely observable minute starts AFTER message delivery.
    start_ms = ((sent_ms + 59999) // 60000) * 60000
    expiry_ms = sent_ms + track_minutes(signal['method']) * 60000
    row = con.execute("""INSERT OR IGNORE INTO signals
       (symbol,side,method,candle,start_ms,expiry_ms,entry,stop,target)
       VALUES (?,?,?,?,?,?,?,?,?)""",
       (symbol, signal['side'], signal['method'], signal['candle'], start_ms,
        expiry_ms, signal['close'], signal['stop'], signal['target']))
    con.commit()
    return row.rowcount > 0


def finish(con, signal_id, outcome, when_ms, detail=''):
    con.execute("""UPDATE signals SET outcome=?, outcome_ms=?, detail=?, notified=0
                   WHERE id=? AND outcome='OPEN' """,
                (outcome, when_ms, detail, signal_id))
    con.commit()


def check_one(con, r, now_ms):
    sid, symbol, side, method, start_ms, expiry_ms, entry, stop, target = r
    # Observe completed 1m candles only, and never after expiry.
    end_ms = min(now_ms - 60000, expiry_ms - 60000)
    if end_ms < start_ms:
        if now_ms >= expiry_ms:
            finish(con, sid, 'VERI_EKSIK', now_ms, 'Takip araliginda tam mum yok')
        return
    # Work through complete historic minutes from start, including after a restart.
    cursor = start_ms
    while cursor <= end_ms:
        limit_end = min(end_ms, cursor + 999 * 60000)
        candles = market('/fapi/v1/klines', symbol=symbol, interval='1m',
                         startTime=cursor, endTime=limit_end + 59999, limit=1000)
        if not candles:
            return  # API/data outage: do not invent a result.
        for bar in candles:
            bar_ms = int(bar[0])
            if bar_ms < cursor:
                continue
            if bar_ms != cursor:
                finish(con, sid, 'VERI_EKSIK', now_ms, 'Eksik 1m mum')
                return
            if int(bar[6]) >= now_ms:
                return
            high, low = float(bar[2]), float(bar[3])
            hit_stop = low <= stop if side == 'LONG' else high >= stop
            hit_target = high >= target if side == 'LONG' else low <= target
            if hit_stop and hit_target:
                finish(con, sid, 'BELIRSIZ', bar_ms, 'Ayni mumda hedef ve stop')
                return
            if hit_stop:
                finish(con, sid, 'STOP', bar_ms)
                return
            if hit_target:
                finish(con, sid, 'HEDEF', bar_ms)
                return
            cursor += 60000
        if cursor <= limit_end and len(candles) == 0:
            return
        if cursor <= limit_end and int(candles[-1][0]) < limit_end:
            finish(con, sid, 'VERI_EKSIK', now_ms, 'Eksik 1m mum')
            return
    if now_ms >= expiry_ms:
        finish(con, sid, 'SURE_DOLDU', expiry_ms, 'Hedef/stop gorulmedi')


def notify_results(con):
    rows = con.execute("""SELECT id,symbol,side,method,entry,stop,target,outcome,detail
                          FROM signals WHERE outcome != 'OPEN' AND notified=0
                          ORDER BY id LIMIT 25""").fetchall()
    for sid, symbol, side, method, entry, stop, target, outcome, detail in rows:
        msg = (f'ALPER RADAR | SONUC | {outcome}\n'
               f'{symbol} | {side} | {method}\n'
               f'Giris referansi: {entry:.8g}\nStop: {stop:.8g}\n'
               f'Hedef: {target:.8g}\n'
               f'{detail}\n'
               'Sanal takip: emir acilmadi. Sonuclar gecikmeli ve referans fiyat bazlidir.')
        if send(msg):
            con.execute('UPDATE signals SET notified=1 WHERE id=?', (sid,))
            con.commit()
        else:
            return


def monitor(con):
    now_ms = int(time.time() * 1000)
    rows = con.execute("""SELECT id,symbol,side,method,start_ms,expiry_ms,entry,stop,target
                          FROM signals WHERE outcome='OPEN' ORDER BY id""").fetchall()
    for r in rows:
        try:
            check_one(con, r, now_ms)
        except RuntimeError as exc:
            log('Monitor delayed', r[1], str(exc))
            if 'cooldown' in str(exc) or '429' in str(exc) or '418' in str(exc):
                break
    notify_results(con)


def stats(con):
    rows = con.execute("""SELECT method,outcome,COUNT(*) FROM signals
                          GROUP BY method,outcome""").fetchall()
    return '; '.join(f'{m} {o}: {n}' for m,o,n in rows) or 'Henuz kayit yok'


def ema(values, n):
    if not values:
        raise ValueError('EMA requires data')
    alpha = 2 / (n + 1)
    result = values[0]
    for v in values[1:]:
        result += alpha * (v - result)
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


def evaluate(five, fifteen, hourly, now_ms):
    """Returns (candidate, reason). Only completed, contiguous bars are used."""
    a, b, h = (closed(x, now_ms) for x in (five, fifteen, hourly))
    if len(a) < 35 or len(b) < 35 or len(h) < 35:
        return None, 'INSUFFICIENT_DATA'
    for bars, width in ((a, 300000), (b, 900000), (h, 3600000)):
        if any(int(y[0]) - int(x[0]) != width for x, y in zip(bars[-35:-1], bars[-34:])):
            return None, 'DATA_GAP'
    if now_ms - (int(a[-1][0]) + 300000) > 120000:
        return None, 'STALE_5M'
    trend15 = direction(b, now_ms)
    trend1h = direction(h, now_ms)
    if not trend15:
        return None, 'NO_15M_TREND'
    if os.getenv('HTF_FILTER', '1') != '0' and trend1h != trend15:
        return None, 'HTF_MISMATCH'
    side = trend15
    sign = 1 if side == 'LONG' else -1
    c = [float(r[4]) for r in a]
    v = [float(r[5]) for r in a]
    if min(c[-22:]) <= 0:
        return None, 'INVALID_PRICE'
    avgvol = sum(v[-21:-1]) / 20
    volume_ratio = v[-1] / avgvol if avgvol > 0 else 0
    if volume_ratio < envfloat('MIN_VOLUME_RATIO', 1.0):
        return None, 'LOW_VOLUME'
    e9, e21 = ema(c, 9), ema(c, 21)
    prev_high = max(float(r[2]) for r in a[-13:-1])
    prev_low = min(float(r[3]) for r in a[-13:-1])
    last = a[-1]
    close_price = c[-1]
    trend5 = sign * (e9 - e21) > 0
    momentum = sign * (close_price - float(last[1])) > 0
    breakout = close_price > prev_high if sign == 1 else close_price < prev_low
    pullback = (min(float(r[3]) for r in a[-4:-1]) <= e21 * 1.002 if sign == 1
                else max(float(r[2]) for r in a[-4:-1]) >= e21 * .998)
    resumed = sign * (close_price - c[-2]) > 0 and sign * (close_price - e9) > 0
    method = 'BREAKOUT' if breakout and momentum else ('PULLBACK' if pullback and resumed else None)
    if not method:
        return None, 'NO_SETUP'
    score = int(trend5) + int(momentum) + int(volume_ratio >= 1.3) + int(trend1h == side)
    if score < int(envfloat('MIN_SCORE', 3)):
        return None, 'LOW_SCORE'
    ranges = [max(float(r[2])-float(r[3]), abs(float(r[2])-float(a[i-1][4])),
                  abs(float(r[3])-float(a[i-1][4]))) for i, r in enumerate(a[-14:], len(a)-14)]
    atr = sum(ranges) / len(ranges)
    if atr <= 0:
        return None, 'INVALID_ATR'
    stop = close_price - sign * 1.2 * atr
    target = close_price + sign * 1.8 * atr
    risk_pct = abs(close_price - stop) / close_price * 100
    if stop <= 0 or target <= 0 or not .15 <= risk_pct <= envfloat('MAX_RISK_PCT', 2.0):
        return None, 'RISK_DISTANCE'
    return {'side': side, 'method': method, 'close': close_price, 'stop': stop,
            'target': target, 'risk_pct': risk_pct, 'score': score,
            'volume': volume_ratio, 'candle': int(last[0])}, 'OK'


def scan(con):
    global _offset
    now_ms = int(market('/fapi/v1/time')['serverTime'])
    info = market('/fapi/v1/exchangeInfo')
    symbols = {s['symbol'] for s in info['symbols'] if s['status'] == 'TRADING'
               and s['contractType'] == 'PERPETUAL' and s['quoteAsset'] == 'USDT'}
    tickers = market('/fapi/v1/ticker/24hr')
    eligible = sorted(t['symbol'] for t in tickers if t['symbol'] in symbols
                      and float(t['quoteVolume']) >= envfloat('MIN_VOLUME_USDT', 20000000)
                      and abs(float(t['priceChangePercent'])) <= envfloat('MAX_24H_CHANGE_PCT', 20))
    if not eligible:
        return 0, 0, Counter({'NO_ELIGIBLE_SYMBOLS': 1})
    count = min(len(eligible), max(1, int(envfloat('MAX_SYMBOLS_PER_SCAN', 20))))
    batch = (eligible[_offset:] + eligible[:_offset])[:count]
    _offset = (_offset + count) % len(eligible)
    btc = None
    if os.getenv('BTC_FILTER', '0') == '1':
        btc = direction(market('/fapi/v1/klines', symbol='BTCUSDT', interval='15m', limit=60), now_ms)
    reasons = Counter()
    alerts = 0
    for symbol in batch:
        try:
            five = market('/fapi/v1/klines', symbol=symbol, interval='5m', limit=80)
            fifteen = market('/fapi/v1/klines', symbol=symbol, interval='15m', limit=60)
            hourly = market('/fapi/v1/klines', symbol=symbol, interval='1h', limit=60)
            result, reason = evaluate(five, fifteen, hourly, now_ms)
            if result and btc and result['side'] != btc:
                result, reason = None, 'BTC_MISMATCH'
            if not result:
                reasons[reason] += 1
                continue
            key = (symbol, result['side'])
            exists = con.execute('SELECT 1 FROM signals WHERE symbol=? AND side=? AND candle=?',
                                 (symbol, result['side'], result['candle'])).fetchone()
            if exists or _last_alert.get(key) == result['candle']:
                reasons['DUPLICATE_CANDLE'] += 1
                continue
            message = (f'ALPER RADAR V4.2 | DENEME | {result["side"]} ADAYI\n'
                       f'Parite: {symbol}\nYöntem: {result["method"]}\n'
                       f'5m kapanış: {result["close"]:.8g}\n'
                       f'Stop referansı: {result["stop"]:.8g}\n'
                       f'Hedef referansı: {result["target"]:.8g}\n'
                       f'Stop fiyat mesafesi: %{result["risk_pct"]:.2f}\n'
                       f'Hacim oranı: {result["volume"]:.2f}x\n'
                       'Sinyal deneysel ve gecikmelidir. Referanslar emir değildir; başarı doğrulanmadı.')
            if send(message):
                add_signal(con, symbol, result, int(time.time() * 1000))
                _last_alert[key] = result['candle']
                alerts += 1
                log('ALERT', symbol, result['side'], result['method'])
            else:
                reasons['TELEGRAM_NOT_CONFIGURED'] += 1
        except RuntimeError as exc:
            reasons[str(exc)] += 1
            log('Symbol error', symbol, str(exc))
            if 'cooldown' in str(exc) or 'HTTP 429' in str(exc) or 'HTTP 418' in str(exc):
                break
    return len(batch), alerts, reasons


def main():
    global _last_health
    if not PAPER:
        raise RuntimeError('This version is PAPER ONLY. Set PAPER_MODE=1.')
    if not TOKEN or not CHAT:
        raise RuntimeError('Set TELEGRAM_TOKEN and TELEGRAM_CHAT_ID in Render environment secrets.')
    con = db_open()
    send('ALPER Radar V4.2 + TAKIP baslatildi | DENEME modu | 30/60 dk otomatik sonuc takibi. Emir acilmaz.')
    while True:
        try:
            if time.monotonic() < _pause_until:
                log('Cooldown active; remaining seconds', int(_pause_until-time.monotonic()))
                time.sleep(min(60, _pause_until-time.monotonic()))
                continue
            monitor(con)
            count, alerts, reasons = scan(con)
            log('Scan complete:', count, 'alerts:', alerts, 'rejections:', dict(reasons))
            if time.monotonic() - _last_health >= envfloat('HEALTH_INTERVAL_SECONDS', 1800):
                top = ', '.join(f'{k}: {v}' for k,v in reasons.most_common(7)) or 'yok'
                send(f'ALPER Radar V4.2 DURUM\nTaranan: {count}\nSinyal: {alerts}\nEleme nedenleri: {top}\nSinyal olmaması tek başına hata değildir.')
                _last_health = time.monotonic()
        except Exception as exc:
            log('Scan error:', type(exc).__name__, str(exc)[:120])
            try:
                if time.monotonic() - _last_health >= 300:
                    send(f'ALPER Radar V4.2 UYARI: {type(exc).__name__}: {str(exc)[:90]}')
                    _last_health = time.monotonic()
            except Exception as notification_exc:
                log('Notification error:', type(notification_exc).__name__)
        # Align to a fresh 5-minute close with a 7s buffer.
        now = time.time()
        delay = (300 - now % 300) + 7
        time.sleep(max(5, delay))


def self_test():
    assert abs(ema([10]*40, 21) - 10) < 1e-10
    assert direction([], 1000000) is None
    assert evaluate([], [], [], 1000000)[1] == 'INSUFFICIENT_DATA'
    assert closed([[0,1,2,0,1,1,59999]], 60000)
    assert not closed([[0,1,2,0,1,1,59999]], 59999)
    assert track_minutes('BREAKOUT') == 30 and track_minutes('PULLBACK') == 60
    print('PASS: EMA, empty data, closed candles, tracking windows')


if __name__ == '__main__':
    if '--self-test' in sys.argv:
        self_test()
    else:
        main()