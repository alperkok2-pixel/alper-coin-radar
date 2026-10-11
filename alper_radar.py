"""ALPER Radar V4.1 — experimental paper signal scanner, Python 3.11+.

Default SIGNAL_ENGINE=PULLBACK: established 15m/1h trend, a 5m pullback
near EMA21 and directional close confirmation with volume. Alternative
SIGNAL_ENGINE=V3 retains the retest/squeeze engine for comparisons.
No measured improvement is claimed. Thresholds are unvalidated heuristics.

Render: Build python -m py_compile alper_radar.py; Start python -u alper_radar.py.
Keep TELEGRAM_TOKEN and TELEGRAM_CHAT_ID in environment secrets, never in git.
PAPER_MODE=1; no exchange account keys or order placement.
V4.1: pinned closed-bar snapshots, rotating 24-symbol batches, weighted request
budget, hourly/metadata cache, shared-IP weight backoff and health messages.
The rotating universe means not every symbol is scanned every 5 minutes.
429 cannot be eliminated on shared hosting. No strategy improvement is claimed.
Default maximum stop-reference distance MAX_RISK_PCT=1.5 is PRICE distance,
not account risk or a guaranteed loss cap. MAX_24H_CHANGE_PCT=15 excludes
large absolute 24h changes. Existing BTC/HTF/OI/funding filters remain active.
REFERENCE stops/targets are not real orders. Gaps/slippage can exceed them.
Use DATA_DIR on a persistent disk to retain records over restarts/deploys.

Recommended Render environment (keep existing Telegram secrets):
REQUEST_INTERVAL_SECONDS=2
MAX_SYMBOLS_PER_SCAN=24
WEIGHT_BUDGET_PER_MINUTE=180
IP_WEIGHT_SOFT_LIMIT=1200
SIGNAL_ENGINE=PULLBACK
PAPER_MODE=1
MIN_TREND_EFFICIENCY=0.25
Keep exactly one Render instance/service for this scanner. These settings
reduce our traffic; they cannot control traffic from other users of an IP.
Health notification every 30m and immediately on rate-limit errors.
Live result summary: python alper_radar.py --stats
Tests: python alper_radar.py --self-test
Replay: python alper_radar.py --backtest --symbols BTCUSDT,ETHUSDT --days 7
Optional --data local.json uses the documented schema in --backtest --help.
Optional --output report.json. Fetch needs Binance access; do not use VPN
or bypass geographic restrictions. Replay never sends Telegram messages.
OI/funding/liquidity/24h cap are not replayed; see limitations in its report.
Scores stored for V3 compatibility are not success probabilities.
"""
import concurrent.futures as cf
from collections import deque
from collections import Counter
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
market_io = threading.Lock()
rate_failures = 0
last_rate_failure = 0.0
scan_reasons = Counter()
weight_events = deque()
market_cache = {}
cache_lock = threading.Lock()
last_health = 0.0
scan_offset = 0

def endpoint_weight(url, params):
    path = urllib.parse.urlsplit(url).path
    params = params or {}
    if path.endswith('/klines'):
        limit = int(params.get('limit', 500))
        return 1 if limit < 100 else 2 if limit < 500 else 5 if limit <= 1000 else 10
    if path.endswith('/ticker/24hr'):
        return 1 if params.get('symbol') else 40
    if path.endswith('/ticker/price'):
        return 2 if params.get('symbol') else 4
    if path.endswith('/premiumIndex'):
        return 1 if params.get('symbol') else 10
    return 1

def cached_market(path, ttl, **params):
    # Cache only immutable metadata or requests with explicit short validity.
    key = (path, tuple(sorted(params.items())))
    with cache_lock:
        hit = market_cache.get(key)
        if hit and time.monotonic() - hit[0] < ttl:
            return hit[1]
    value = market(path, **params)
    with cache_lock:
        if len(market_cache) > 2000:
            market_cache.clear()
        market_cache[key] = (time.monotonic(), value)
    return value

def select_scan_batch(eligible):
    global scan_offset
    cap = max(1, min(40, int(os.getenv('MAX_SYMBOLS_PER_SCAN', '24'))))
    if not eligible:
        return []
    offset = scan_offset % len(eligible)
    batch = (eligible[offset:] + eligible[:offset])[:cap]
    scan_offset = (offset + len(batch)) % len(eligible)
    return batch

def wait_for_close():
    # Initial deployment and recovery also wait for a fresh 5m close.
    while True:
        phase = time.time() % 300
        if 5 <= phase <= 15:
            return
        delay = 5 - phase if phase < 5 else 305 - phase
        time.sleep(min(30, delay))

def health(message, force=False):
    global last_health
    if force or time.monotonic() - last_health >= 1800:
        try:
            send('ALPER Radar DURUM | ' + message)
            last_health = time.monotonic()
        except Exception as exc:
            print('Health delivery error:', error_detail(exc), flush=True)

reasons_lock = threading.Lock()

def reject_candidate(reason):
    with reasons_lock:
        scan_reasons[reason] += 1
    return None

def btc_allows(side, btc_side):
    # Neutral BTC permits the candidate's own 5m/15m/1h trend checks.
    return not enabled("BTC_FILTER") or btc_side is None or side == btc_side

def next_scan_delay(server_ms, elapsed):
    # Run shortly after the next 5m close; never overlap completed scans.
    return max(5.0, 300 - (server_ms / 1000 % 300) + 5 - elapsed)

class HTTPFailure(RuntimeError):
    def __init__(self, source, status):
        self.source, self.status = source, status
        super().__init__(f'{source} HTTP {status}')

def error_detail(exc):
    # Only expose our own sanitized HTTP fields, never URLs or credentials.
    return str(exc) if isinstance(exc, HTTPFailure) else type(exc).__name__

class MarketPaused(RuntimeError):
    pass

def request(url, params=None, data=None, market=False):
    if market:
        # Serialize all scanner and monitor traffic, including rate-limit handling.
        with market_io:
            return _request(url, params, data, market=True)
    return _request(url, params, data, market=False)

def _request(url, params=None, data=None, market=False):
    global next_call, pause_until, rate_failures, last_rate_failure
    source = 'Binance' if market else 'Telegram'
    if market:
        while True:
            with gate:
                if time.monotonic() < pause_until:
                    raise MarketPaused("Binance cooldown active")
                current = time.monotonic()
                while weight_events and current - weight_events[0][0] >= 60:
                    weight_events.popleft()
                cost = endpoint_weight(url, params)
                budget = max(50, int(os.getenv('WEIGHT_BUDGET_PER_MINUTE', '180')))
                delay = next_call - current
                if sum(w for _, w in weight_events) + cost > budget:
                    delay = max(delay, weight_events[0][0] + 60 - current)
                if delay <= 0:
                    next_call = time.monotonic() + max(2.0, float(os.getenv("REQUEST_INTERVAL_SECONDS", "2.0")))
                    weight_events.append((time.monotonic(), cost))
                    break
            # Release the lock and recheck cooldown before sending.
            time.sleep(min(delay, 1.0))
    if params:
        url += '?' + urllib.parse.urlencode(params)
    payload = None if data is None else urllib.parse.urlencode(data).encode()
    try:
        with urllib.request.urlopen(url, payload, timeout=15) as response:
            if market:
                used = int(response.headers.get('X-MBX-USED-WEIGHT-1M', '0'))
                threshold = max(50, int(os.getenv('IP_WEIGHT_SOFT_LIMIT', '1200')))
                if used >= threshold:
                    with gate:
                        pause_until = max(pause_until, time.monotonic() + 65)
                    print('Shared IP weight high; pausing 65 seconds:', used, flush=True)
            return json.load(response)
    except urllib.error.HTTPError as exc:
        if market and exc.code in (418, 429):
            try:
                retry_after = max(0, float(exc.headers.get('Retry-After', '0')))
            except (ValueError, TypeError):
                retry_after = 0
            current = time.monotonic()
            rate_failures = 1 if current-last_rate_failure > 3600 else rate_failures+1
            last_rate_failure = current
            cooldown = max(retry_after, min(3600, 300 * 2**min(rate_failures-1, 4)),
                           900 if exc.code == 418 else 0)
            with gate:
                pause_until = max(pause_until, time.monotonic() + cooldown)
            print('Binance rate limit: HTTP', exc.code, 'cooldown seconds:', round(cooldown), flush=True)
            health(f'Binance HTTP {exc.code}; tarama {round(cooldown/60)} dk bekliyor. Sinyal ve sonuç verileri bu arada eksik olabilir.', force=True)
        raise HTTPFailure(source, exc.code) from None

def market(path, **params):
    return request(BASE + path, params, market=True)

def send(message):
    if not TOKEN or not CHAT:
        return
    response = request('https://api.telegram.org/bot' + TOKEN + '/sendMessage',
                       data={'chat_id': CHAT, 'text': message})
    if not response.get('ok'):
        raise RuntimeError('Telegram message was not accepted')

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

alert_delivery_lock = threading.Lock()

def flush_alerts():
    with alert_delivery_lock:
        with connect() as con:
            con.execute('UPDATE alert_outbox SET sent=2 WHERE sent=0 AND signal_id IN (SELECT id FROM signals WHERE ts<?)', (time.time()-120,))
            con.commit()
            pending=con.execute("SELECT signal_id,message FROM alert_outbox WHERE sent=0 ORDER BY signal_id LIMIT 20").fetchall()
            for sid,message in pending:
                send(message)
                con.execute("UPDATE alert_outbox SET sent=1 WHERE signal_id=?",(sid,))
                con.commit()

def monitor():
    while True:
        try:
            flush_alerts()
            with connect() as con:
                pending = con.execute('SELECT id,symbol,ts,entry,side FROM signals WHERE ts > ? AND NOT EXISTS (SELECT 1 FROM results WHERE signal_id=signals.id AND minutes=15)', (time.time()-7200,)).fetchall()
                if pending:
                    prices = {r['symbol']: r for r in cached_market('/fapi/v2/ticker/price', 15)}
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
        time.sleep(60)

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
    raw = market('/fapi/v1/klines', symbol=symbol, interval='1m', endTime=now_ms//300000*300000-1, limit=480)
    result = candidate(raw, now_ms)
    if result:
        print('Candidate:', symbol, result['side'], 'method:', result['method'], flush=True)
    if result and enabled('HTF_FILTER'):
        hourly = cached_market('/fapi/v1/klines', 240, symbol=symbol, interval='1h', limit=99, endTime=now_ms//3600000*3600000-1)
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
    print('Signal engine:', os.getenv('SIGNAL_ENGINE','PULLBACK'), 'max risk %:', os.getenv('MAX_RISK_PCT','1.5'), flush=True)
    print('Diagnostics V4.1 | filters:', {name: enabled(name) for name in ('ENABLE_SHORT','BTC_FILTER','HTF_FILTER','RETEST_FILTER','SQUEEZE_FILTER','FUNDING_FILTER','OI_FILTER')}, flush=True)
    send('ALPER Radar V4.1 started — ' + ('DENEME' if PAPER else 'ADAY BİLDİRİMLERİ') + '. Otomatik işlem yok; başarı oranı henüz ölçülmedi.')
    threading.Thread(target=monitor, daemon=True).start()
    while True:
        wait_for_close()
        start = time.monotonic()
        schedule_ms = int(time.time()*1000)
        with reasons_lock:
            scan_reasons.clear()
        if start < pause_until:
            time.sleep(min(30, pause_until-start))
            continue
        try:
            now_ms = int(market('/fapi/v1/time')['serverTime'])
            schedule_ms = now_ms
            btc_side = None
            if enabled('BTC_FILTER'):
                btc = market('/fapi/v1/klines', symbol='BTCUSDT', interval='15m', limit=100)
                btc_side = trend_direction(btc, now_ms, 900000)
            print('BTC 15m direction:', (btc_side or 'NEUTRAL') if enabled('BTC_FILTER') else 'FILTER_OFF', 'SHORT enabled:', enabled('ENABLE_SHORT'), flush=True)
            info = cached_market('/fapi/v1/exchangeInfo', 3600)
            all_symbols = {s['symbol'] for s in info['symbols'] if s['status']=='TRADING' and s['contractType']=='PERPETUAL' and s['quoteAsset']=='USDT'}
            tickers = market('/fapi/v1/ticker/24hr')
            liquid = [t['symbol'] for t in tickers if t['symbol'] in all_symbols and float(t['quoteVolume']) >= float(os.getenv('MIN_VOLUME_USDT', '20000000'))]
            changes = {t['symbol']: abs(float(t['priceChangePercent'])) for t in tickers}
            eligible = [symbol for symbol in liquid if changes.get(symbol,0) <= float(os.getenv('MAX_24H_CHANGE_PCT','15'))]
            print('Active:', len(all_symbols), 'liquid:', len(liquid), 'within 24h move cap:', len(eligible), flush=True)
            eligible.sort()
            batch = select_scan_batch(eligible)
            print('Rotating scan batch:', len(batch), 'of', len(eligible), flush=True)
            errors, found = 0, 0
            with cf.ThreadPoolExecutor(max_workers=1) as pool:
                futures = [pool.submit(scan_symbol, s, now_ms) for s in batch]
                for future in cf.as_completed(futures):
                    try:
                        symbol, result = future.result()
                        if not result:
                            continue
                        if not btc_allows(result['side'], btc_side):
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
                            levels = trade_levels(result, entry)
                            if levels is None:
                                print('Rejected:', symbol, result['side'], 'filter: RISK_OR_COST', flush=True)
                                continue
                            stop, target, risk_pct = levels
                            clock = dt.datetime.fromtimestamp(stamp, dt.timezone(dt.timedelta(hours=3))).strftime('%H:%M:%S')
                            alert_message = f"{'DENEME | ' if PAPER else ''}{result['side']} ADAYI: {symbol}\nYöntem: {METHOD_LABELS[result['method']]}\nSaat (TR): {clock}\nDurum: deneysel aday; başarı olasılığı hesaplanmadı\nReferans fiyat: {entry:.8g}\nStop mesafesi: %{risk_pct:.2f}\nStop referansı: {stop:.8g}\nHedef referansı: {target:.8g}\nHacim: {result['volume']:.2f}x\nOI değişimi (15dk): {result.get('oi_text', 'filtre kapalı')}\nFonlama: {result.get('funding_text', 'filtre kapalı')}\nİzleme: sonraki 5 ve 15 dk. Seviyeler otomatik emir değildir."
                            cursor = con.execute('INSERT INTO signals(symbol,ts,entry,stop,target,score,candle,side,method) VALUES (?,?,?,?,?,?,?,?,?)',
                                        (symbol, stamp, entry, stop, target, result['score'], result['candle'], result['side'], result['method']))
                            con.execute('INSERT INTO alert_outbox(signal_id,message) VALUES (?,?)',(cursor.lastrowid,alert_message))
                        flush_alerts()

                        print('Alert delivery processed:', symbol, result['side'], flush=True)
                        found += 1
                    except MarketPaused:
                        errors += 1
                        for queued in futures:
                            queued.cancel()
                        break
                    except Exception as exc:
                        errors += 1
                        print('Symbol error:', error_detail(exc), flush=True)
            print('Candidate rejections:', dict(scan_reasons), flush=True)
            health(f'Tarama tamamlandı: {len(batch)}/{len(eligible)} uygun parite dönüşümlü taranıyor; bu tur {found} aday, {errors} hata. Başarı henüz doğrulanmadı.')
            print('Scan seconds:', round(time.monotonic()-start), 'alerts:', found, 'errors:', errors, flush=True)
        except Exception as exc:
            print('Scan unavailable:', error_detail(exc), flush=True)
        time.sleep(next_scan_delay(schedule_ms, time.monotonic()-start))

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
    global DB, send
    saved_db, saved_send = DB, send
    with tempfile.TemporaryDirectory() as directory:
        DB = str(Path(directory)/'test.sqlite3')
        init_db()
        with connect() as con:
            con.execute('INSERT INTO signals(symbol,ts,entry,stop,target,score,candle) VALUES (?,?,?,?,?,?,?)', ('TEST', time.time(), 100, 99, 102, 90, 1))
            con.execute('INSERT INTO results VALUES (1,5,301,101,1,.85)')
            con.execute('INSERT OR IGNORE INTO results VALUES (1,5,302,102,2,1.85)')
            assert con.execute('SELECT count(*) FROM results').fetchone()[0] == 1
        with connect() as con:
            con.execute('INSERT INTO alert_outbox VALUES (1,?,0)', ('TEST ONLY',))
        def fail_send(message):
            raise RuntimeError('simulated delivery failure')
        send = fail_send
        try:
            flush_alerts()
        except RuntimeError:
            pass
        with connect() as con:
            assert con.execute('SELECT sent FROM alert_outbox').fetchone()[0] == 0
        deliveries = []
        send = deliveries.append
        flush_alerts()
        flush_alerts()
        assert deliveries == ['TEST ONLY']
    DB, send = saved_db, saved_send
    print('PASS: candle alignment, partial/gapped bars, EMA, neutral-market rejection, DB migration and delivery retry.')


# Independent signal paths: RETEST_FILTER and SQUEEZE_FILTER (default ON).
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

METHOD_LABELS = {'RETEST': 'Geri test', 'SQUEEZE_BREAKOUT': 'Sıkışma–kırılım', 'LEGACY': 'Önceki sürüm', 'PULLBACK': 'Trend içinde geri çekilme + kapanış teyidi'}

def squeeze_breakout(a, direction):
    # Compare only pre-breakout candles: 12 recent ranges vs 24 earlier ranges.
    if len(a) < 37:
        return None
    last, recent, baseline = a[-1], a[-13:-1], a[-37:-13]
    base_range = st.mean(r[2]-r[3] for r in baseline)
    recent_range = st.mean(r[2]-r[3] for r in recent)
    if base_range <= 0 or recent_range > .8*base_range:
        return None
    upper, lower = max(r[2] for r in recent), min(r[3] for r in recent)
    if upper-lower > 3*base_range:
        return None
    ratio = last[5]/max(st.mean(r[5] for r in a[-21:-1]),1e-12)
    level = upper if direction == 1 else lower
    location = (last[4]-last[3])/max(last[2]-last[3],1e-12)
    strong = location >= .7 if direction == 1 else location <= .3
    if direction*(last[4]-level)>0 and ratio>=1.5 and strong and direction*(last[4]-last[1])>0:
        return ratio
    return None

def legacy_candidate(raw, now_ms):
    a, b = aggregate(raw,5,now_ms), aggregate(raw,15,now_ms)
    if len(a)<30 or len(b)<25:
        return reject_candidate('INSUFFICIENT_BARS')
    if now_ms-(a[-1][0]+300000)>90000:
        return reject_candidate('CANDLE_WINDOW')
    if any(y[0]-x[0]!=300000 for x,y in zip(a,a[1:])):
        return reject_candidate('DATA_GAP_5M')
    if any(y[0]-x[0]!=900000 for x,y in zip(b,b[1:])):
        return reject_candidate('DATA_GAP_15M')
    closes,big=[r[4] for r in a],[r[4] for r in b]
    last,prev=a[-1],a[-21:-1]
    atr=st.mean(max(a[i][2]-a[i][3],abs(a[i][2]-a[i-1][4]),abs(a[i][3]-a[i-1][4])) for i in range(len(a)-14,len(a)))
    if atr<=0:
        return reject_candidate("INVALID_ATR")
    failures = []
    for side,d in [('LONG',1),('SHORT',-1)]:
        if side=='SHORT' and not enabled('ENABLE_SHORT'):
            continue
        trend=d*(big[-1]-ema(big,9))>0 and d*(ema(big,9)-ema(big,20))>0
        momentum=d*(closes[-1]-ema(closes,9))>0 and d*(ema(closes,9)-ema(closes,21))>0
        volume=last[5]/max(st.mean(r[5] for r in prev),1e-12)
        breakout=last[4]>max(r[2] for r in prev) if d==1 else last[4]<min(r[3] for r in prev)
        retest_volume = retest_setup(a,atr,d) if enabled('RETEST_FILTER') else None
        squeeze_volume = squeeze_breakout(a,d) if enabled('SQUEEZE_FILTER') else None
        method = 'RETEST' if retest_volume is not None else ('SQUEEZE_BREAKOUT' if squeeze_volume is not None else None)
        setup = method is not None
        if method == 'RETEST':
            volume = retest_volume
        elif method == 'SQUEEZE_BREAKOUT':
            volume = squeeze_volume
        location=(last[4]-last[3])/max(last[2]-last[3],1e-12)
        strong=location>=.7 if d==1 else location<=.3
        extension=d*(last[4]-ema(closes,21))/atr
        score=25*trend+25*setup+20*(volume>=1.5)+20*momentum+10*strong
        if not trend:
            failures.append(side+'_TREND_15M')
        elif not setup:
            failures.append(side+'_NO_RETEST_OR_SQUEEZE')
        elif not momentum:
            failures.append(side+'_MOMENTUM_5M')
        elif not 0<extension<3:
            failures.append(side+'_EXTENSION')
        elif score<80:
            failures.append(side+'_SCORE')
        if trend and setup and momentum and score>=80 and 0<extension<3:
            return dict(side=side,score=score,atr=atr,close=last[4],candle=last[0],volume=volume,method=method)
    return reject_candidate('|'.join(failures) or 'NO_SETUP')

def ema_series(values, length):
    out = [values[0]]
    for value in values[1:]:
        out.append(out[-1] + 2/(length+1)*(value-out[-1]))
    return out

def pullback_candidate(raw, now_ms):
    a, b = aggregate(raw,5,now_ms), aggregate(raw,15,now_ms)
    if len(a)<60 or len(b)<25:
        return reject_candidate('INSUFFICIENT_BARS')
    if now_ms-(a[-1][0]+300000)>90000:
        return reject_candidate('CANDLE_WINDOW')
    if any(y[0]-x[0]!=300000 for x,y in zip(a,a[1:])) or any(y[0]-x[0]!=900000 for x,y in zip(b,b[1:])):
        return reject_candidate('DATA_GAP')
    closes, big = [r[4] for r in a], [r[4] for r in b]
    fast, slow = ema_series(closes,9), ema_series(closes,21)
    atr = st.mean(max(a[i][2]-a[i][3],abs(a[i][2]-a[i-1][4]),abs(a[i][3]-a[i-1][4])) for i in range(len(a)-14,len(a)))
    if atr<=0 or closes[-1]<=0:
        return reject_candidate('INVALID_ATR')
    # Directional efficiency avoids some choppy paths; heuristic, not a win-rate claim.
    path = sum(abs(y-x) for x,y in zip(closes[-13:-1], closes[-12:]))
    efficiency = abs(closes[-1]-closes[-13])/max(path, 1e-12)
    if efficiency < float(os.getenv('MIN_TREND_EFFICIENCY', '0.25')):
        return reject_candidate('CHOPPY_PATH')
    last = a[-1]
    volume = last[5]/max(st.mean(r[5] for r in a[-21:-1]),1e-12)
    location = (last[4]-last[3])/max(last[2]-last[3],1e-12)
    reasons = []
    for side,d in [('LONG',1),('SHORT',-1)]:
        if side=='SHORT' and not enabled('ENABLE_SHORT'):
            continue
        # Established trend and slope must predate the signal candle.
        trend = (d*(big[-1]-ema(big,9))>0 and d*(ema(big,9)-ema(big,21))>0
                 and d*(slow[-2]-slow[-6])>.1*atr
                 and d*(fast[-2]-slow[-2])>0)
        if not trend:
            reasons.append(side+'_TREND'); continue
        touched = any(a[i][3] <= slow[i]+.25*atr and a[i][2] >= slow[i]-.25*atr for i in range(len(a)-4,len(a)-1))
        held = all(d*(a[i][4]-slow[i]) >= -.35*atr for i in range(len(a)-4,len(a)-1))
        reversed_close = d*(last[4]-(a[-2][2] if d==1 else a[-2][3]))>0
        strong = location>=.7 if d==1 else location<=.3
        extension = d*(last[4]-slow[-1])/atr
        if not (touched and held):
            reasons.append(side+'_PULLBACK'); continue
        if not (reversed_close and strong and d*(last[4]-last[1])>0 and d*(last[4]-fast[-1])>0):
            reasons.append(side+'_CLOSE_CONFIRMATION'); continue
        if volume<1.3:
            reasons.append(side+'_VOLUME'); continue
        if not 0<extension<1.5 or last[2]-last[3]>2*atr:
            reasons.append(side+'_OVEREXTENDED'); continue
        stop = min(r[3] for r in a[-4:])-.15*atr if d==1 else max(r[2] for r in a[-4:])+.15*atr
        result = dict(side=side,score=0,atr=atr,close=last[4],candle=last[0],volume=volume,method='PULLBACK',structure_stop=stop)
        if trade_levels(result,last[4]) is None:
            reasons.append(side+'_RISK_OR_COST'); continue
        return result
    return reject_candidate('|'.join(reasons) or 'NO_SETUP')

def candidate(raw, now_ms):
    engine = os.getenv('SIGNAL_ENGINE','PULLBACK').upper()
    if engine == 'V3':
        return legacy_candidate(raw,now_ms)
    if engine != 'PULLBACK':
        raise ValueError('SIGNAL_ENGINE must be PULLBACK or V3')
    return pullback_candidate(raw,now_ms)

def trade_levels(result,entry):
    if entry<=0 or result['atr']<=0:
        return None
    d = 1 if result['side']=='LONG' else -1
    stop = result.get('structure_stop',entry-d*1.2*result['atr'])
    risk = d*(entry-stop)
    risk_pct = risk/entry*100
    minimum = max(float(os.getenv('MIN_RISK_PCT','0.25')),2*float(os.getenv('COST_BPS','15'))/100)
    if not minimum<=risk_pct<=float(os.getenv('MAX_RISK_PCT','1.5')):
        return None
    target = entry+d*1.5*risk
    if min(stop,target)<=0:
        return None
    return stop,target,risk_pct

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
        print('OI check:', symbol, 'change %:', round(change,4), 'minimum %:', os.getenv('MIN_OI_CHANGE_PCT','0'), flush=True)
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
        if 'method' not in columns:
            con.execute("ALTER TABLE signals ADD COLUMN method TEXT NOT NULL DEFAULT 'LEGACY'")
        con.execute('CREATE TABLE IF NOT EXISTS alert_outbox (signal_id INTEGER PRIMARY KEY, message TEXT NOT NULL, sent INTEGER NOT NULL DEFAULT 0)')
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
    now_ms=int(cached_market('/fapi/v1/time', 5)['serverTime']) if pending else int(time.time()*1000)
    for sid,symbol,stamp,stop,target,side in pending:
        start=int(stamp*1000)//60000*60000
        raw=cached_market('/fapi/v1/klines', 55, symbol=symbol,interval='1m',startTime=start,endTime=min(now_ms//60000*60000-1,start+1020000),limit=20)
        outcome=barrier_outcome(raw,stamp,stop,target,side,now_ms)
        if outcome:
            with connect() as con:
                con.execute('INSERT OR IGNORE INTO outcomes(signal_id,status,observed,reference_price) VALUES (?,?,?,?)',(sid,*outcome))
    labels={'TARGET':'Hedef görüldü','STOP':'Stop görüldü','AMBIGUOUS':'Mum içi sıra/zaman belirsiz','TIMEOUT':'15 dk içinde hedef/stop görülmedi','DATA_GAP':'Veri eksik; değerlendirilemedi'}
    with connect() as con:
        notices=con.execute('SELECT o.signal_id,s.symbol,s.side,o.status,s.method FROM outcomes o JOIN signals s ON s.id=o.signal_id WHERE o.notified=0').fetchall()
        for sid,symbol,side,status,method in notices:
            send(f'DENEME SONUCU | {symbol} {side} | {METHOD_LABELS.get(method,method)}: {labels[status]}. 1dk mum incelemesi; gerçek emir gerçekleşmesi değildir.')
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


def diagnostic_message(symbol, side, approach, adverse, coverage, gross, net, delay, method):
    movement = (f'Hedef mesafesine yaklaşma: %{approach:.1f}\n'
                f'En büyük ters hareket: %{adverse:.2f}\n'
                'Yaklaşma ölçümü: sınır dakikaları hariç tam 1dk mumlar; '
                'hedef/stop sonrasında da 15dk pencere izlenir.') if coverage == 'INTERIOR_1M' else 'Yaklaşma ölçümü: veri eksik veya seviyeler geçersiz.'
    return (f'DENEME | 15 DK ANALİZİ | {symbol} {side}\nYöntem: {METHOD_LABELS.get(method,method)}\n{movement}\n'
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
    now_ms = int(cached_market('/fapi/v1/time', 5)['serverTime']) if pending else int(time.time()*1000)
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
            d.adverse_pct,d.coverage,r.gross_pct,r.net_pct,r.observed-s.ts-900,s.method
            FROM diagnostics d JOIN signals s ON s.id=d.signal_id
            JOIN results r ON r.signal_id=s.id AND r.minutes=15
            WHERE d.notified=0""").fetchall()
        for sid,*values in notices:
            send(diagnostic_message(*values))
            con.execute('UPDATE diagnostics SET notified=1 WHERE signal_id=?',(sid,))


def historical_rows(symbol, interval, start_ms, end_ms):
    widths = {'1m':60000,'15m':900000,'1h':3600000}
    out, cursor = [], start_ms
    while cursor<end_ms:
        rows = market('/fapi/v1/klines',symbol=symbol,interval=interval,startTime=cursor,endTime=end_ms-1,limit=1000)
        if not rows:
            break
        out.extend(rows)
        following = int(rows[-1][0])+widths[interval]
        if following<=cursor:
            raise RuntimeError('Historical pagination did not advance')
        cursor=following
    return out

def replay_trade(rows,entry_ms,result):
    # Entry at a subsequent 1m OPEN, 2m after signal close, to model scan delay.
    bars=[r for r in rows if entry_ms<=int(r[0])<entry_ms+900000]
    if len(bars)!=15 or [int(r[0]) for r in bars]!=list(range(entry_ms,entry_ms+900000,60000)):
        return None
    entry=float(bars[0][1])
    if abs(entry-result['close'])>.5*result['atr']:
        return None
    if result['method']=='PULLBACK':
        levels=trade_levels(result,entry)
        if levels is None: return None
        stop,target,_=levels
    else:
        d=1 if result['side']=='LONG' else -1
        stop,target=entry-d*1.2*result['atr'],entry+d*1.8*result['atr']
    status,price='TIMEOUT',float(bars[-1][4])
    d=1 if result['side']=='LONG' else -1
    for row in bars:
        high,low=float(row[2]),float(row[3])
        hit_stop=low<=stop if d==1 else high>=stop
        hit_target=high>=target if d==1 else low<=target
        if hit_stop or hit_target:
            status='AMBIGUOUS_STOP_ASSUMED' if hit_stop and hit_target else ('STOP' if hit_stop else 'TARGET')
            price=stop if hit_stop else target
            break
    net=d*(price/entry-1)*100-float(os.getenv('COST_BPS','15'))/100
    return dict(entry_ms=entry_ms,method=result['method'],side=result['side'],status=status,net_pct=net)

def run_backtest_cli():
    import argparse,bisect
    parser=argparse.ArgumentParser(description='V3 vs pullback replay; experimental, no Telegram or orders.')
    parser.add_argument('--backtest',action='store_true')
    parser.add_argument('--data',help='Local JSON with symbols:{SYMBOL:{1m:[klines],1h:[klines]}}, btc15m:[klines]')
    parser.add_argument('--symbols',default='BTCUSDT,ETHUSDT')
    parser.add_argument('--days',type=int,default=7)
    parser.add_argument('--output',help='Optional JSON report path')
    args=parser.parse_args()
    if args.data:
        data=json.loads(Path(args.data).read_text())
    else:
        if not 1<=args.days<=30: parser.error('--days must be 1..30')
        end=int(market('/fapi/v1/time')['serverTime'])//60000*60000
        start=end-args.days*86400000
        warm=start-60*3600000
        data={'symbols':{},'btc15m':historical_rows('BTCUSDT','15m',warm,end)}
        for symbol in args.symbols.split(','):
            symbol=symbol.strip().upper()
            if not symbol.isalnum(): parser.error('Invalid symbol')
            data['symbols'][symbol]={'1m':historical_rows(symbol,'1m',warm,end),'1h':historical_rows(symbol,'1h',warm,end)}
        data['evaluation_start_ms']=start
    btc=data['btc15m']
    btc_times=[int(r[0]) for r in btc]
    trades={'V3':[],'PULLBACK':[]}
    boundaries=[]
    # Freeze common filter settings. OI/funding cannot be reconstructed from these candles.
    original={key:os.environ.get(key) for key in ('ENABLE_SHORT','BTC_FILTER','HTF_FILTER','RETEST_FILTER','SQUEEZE_FILTER')}
    for key in original: os.environ[key]='1'
    try:
        for symbol,frames in data['symbols'].items():
            raw,hourly=frames['1m'],frames['1h']
            for rows,width in ((raw,60000),(hourly,3600000),(btc,900000)):
                if not rows or any(int(b[0])-int(a[0])!=width for a,b in zip(rows,rows[1:])):
                    raise ValueError('Missing, unordered or gapped historical candles')
            hourly_times=[int(r[0]) for r in hourly]
            evaluation_start=max(int(data.get('evaluation_start_ms',raw[0][0])),int(raw[0][0])+60*3600000)
            last_entry={'V3':-1e30,'PULLBACK':-1e30}
            end_ms=int(raw[-1][0])+60000
            boundary=evaluation_start+(end_ms-evaluation_start)*.7
            boundaries.append(boundary)
            for index in range(480,len(raw)-17):
                close_ms=int(raw[index][0])
                if close_ms<evaluation_start or close_ms%300000: continue
                now_ms=close_ms+5000
                window=raw[index-480:index]
                hpos=bisect.bisect_right(hourly_times,now_ms)
                hside=trend_direction(hourly[max(0,hpos-101):hpos],now_ms,3600000)
                bpos=bisect.bisect_right(btc_times,now_ms)
                bside=trend_direction(btc[max(0,bpos-101):bpos],now_ms,900000)
                for engine,function in (('V3',legacy_candidate),('PULLBACK',pullback_candidate)):
                    if close_ms-last_entry[engine]<900000: continue
                    result=function(window,now_ms)
                    if not result or hside!=result['side'] or not btc_allows(result['side'],bside): continue
                    item=replay_trade(raw[index:index+17],close_ms+120000,result)
                    if item:
                        item.update(symbol=symbol,segment='EARLIER_70_PERCENT' if close_ms<boundary else 'HOLDOUT_30_PERCENT')
                        trades[engine].append(item);last_entry[engine]=item['entry_ms']
    finally:
        for key,value in original.items():
            if value is None: os.environ.pop(key,None)
            else: os.environ[key]=value
    summaries={}
    for engine,items in trades.items():
        summaries[engine]={}
        for segment in ('EARLIER_70_PERCENT','HOLDOUT_30_PERCENT'):
            selected=sorted([t for t in items if t['segment']==segment],key=lambda t:t['entry_ms'])
            returns=[t['net_pct'] for t in selected]
            cumulative,peak,drawdown=0.,0.,0.
            for value in returns:
                cumulative+=value;peak=max(peak,cumulative);drawdown=max(drawdown,peak-cumulative)
            summaries[engine][segment]={'count':len(selected),'statuses':dict(Counter(t['status'] for t in selected)),
                'positive_net_fraction':sum(x>0 for x in returns)/len(returns) if returns else None,
                'mean_net_pct':st.mean(returns) if returns else None,
                'profit_factor':sum(x for x in returns if x>0)/abs(sum(x for x in returns if x<0)) if any(x<0 for x in returns) else None,
                'sum_net_pct_points':sum(returns),'drawdown_pct_points_equal_notional':drawdown}
    report={'limitations':['No measured performance until actual historical data is supplied.',
        'OI, funding, historical liquidity and 24h movement caps are NOT replayed; selected symbols may have selection bias.',
        'Entry assumes 2m scan delay then 1m open, 15m horizon, configured round-trip cost; stops assume reference fills, no gap fill model.',
        'Same-bar stop/target ambiguity assumes stop; reported separately. Not executable live PnL.',
        'Earlier/holdout split is chronological; manual tuning after viewing holdout invalidates it.',
        'Aggregated sums use equal notionals and can overlap across symbols; not a portfolio return.'],
        'summaries':summaries,'trades':trades}
    output=json.dumps(report,ensure_ascii=False,indent=2)
    if args.output: Path(args.output).write_text(output)
    print(output)


base_self_test = self_test

def self_test():
    base_self_test()
    from unittest.mock import patch
    from email.message import Message
    global scan_offset, pause_until, next_call, rate_failures, last_rate_failure
    saved = (scan_offset, pause_until, next_call, rate_failures, last_rate_failure, list(weight_events))
    try:
        assert endpoint_weight(BASE+'/fapi/v1/klines', {'limit':480}) == 2
        assert endpoint_weight(BASE+'/fapi/v1/ticker/24hr', {}) == 40
        scan_offset=0
        with patch.dict(os.environ, {'MAX_SYMBOLS_PER_SCAN':'24'}):
            seen=set()
            universe=[str(i) for i in range(73)]
            for _ in range(4):
                batch=select_scan_batch(universe)
                assert len(batch)==24
                seen.update(batch)
            assert seen==set(universe)
        pause_until=next_call=0
        weight_events.clear()
        headers=Message(); headers['Retry-After']='7200'
        failure=urllib.error.HTTPError(BASE,429,'rate limited',headers,None)
        with patch.object(urllib.request,'urlopen',side_effect=failure) as network, patch.dict(globals(),{'health':lambda *a,**k:None}):
            try:
                request(BASE+'/fapi/v1/time',market=True)
            except HTTPFailure as exc:
                assert exc.status==429
            else:
                raise AssertionError('Missing 429 handling')
            assert pause_until-time.monotonic()>7190
            try:
                request(BASE+'/fapi/v1/time',market=True)
            except MarketPaused:
                pass
            else:
                raise AssertionError('Missing cooldown')
            assert network.call_count==1
        raw=[[i*60000,100,101,99,100,1,(i+1)*60000-1,100] for i in range(480)]
        future=raw+[[480*60000,100,100000,0,99999,1,481*60000-1,100]]
        assert aggregate(raw,5,480*60000+5000)==aggregate(future,5,480*60000+5000)
        setup={'side':'LONG','atr':1,'close':100,'method':'PULLBACK','structure_stop':99}
        bars=[[i*60000,100,102,98,100,1,(i+1)*60000-1,100] for i in range(15)]
        trade=replay_trade(bars,0,setup)
        assert trade['status']=='AMBIGUOUS_STOP_ASSUMED' and trade['net_pct']<0
        assert replay_trade(bars[:-1],0,setup) is None
        print('PASS: weighted traffic, rotation, Retry-After, shared cooldown, future-bar exclusion, conservative replay and missing-data rejection.')
    finally:
        scan_offset,pause_until,next_call,rate_failures,last_rate_failure,events=saved
        weight_events.clear(); weight_events.extend(events)

def print_stats():
    init_db()
    with connect() as con:
        counts = dict(con.execute('SELECT status,count(*) FROM outcomes GROUP BY status'))
        closed = counts.get('TARGET',0) + counts.get('STOP',0)
        returns = [r[0] for r in con.execute('SELECT net_pct FROM results WHERE minutes=15')]
        report = {'barrier_outcomes':counts, 'target_fraction_among_unambiguous_target_stop':counts.get('TARGET',0)/closed if closed else None,
                  'unambiguous_target_stop_count':closed, 'fifteen_minute_observations':len(returns),
                  'positive_net_15m_fraction':sum(x>0 for x in returns)/len(returns) if returns else None,
                  'mean_net_15m_pct':st.mean(returns) if returns else None,
                  'limitations':'Target/stop fraction excludes timeout, ambiguity and gaps. 15m returns are price observations with assumed costs, not strategy fills. Legacy signals included; use separate DATA_DIR for version comparisons.'}
    print(json.dumps(report, ensure_ascii=False, indent=2))

if __name__ == '__main__':
    if '--stats' in sys.argv:
        print_stats()
    elif '--backtest' in sys.argv:
        run_backtest_cli()
    elif '--self-test' in sys.argv:
        self_test()
    else:
        main()
