"""ALPER Radar V6.1 — Binance USD-M futures PAPER signal radar.
Python 3.11+; standard library only. NEVER submits orders.
Start: python -u alper_radar.py   Test: python alper_radar.py --self-test
Telegram secrets: TELEGRAM_TOKEN, TELEGRAM_CHAT_ID (Render environment).
Optional: MIN_TARGET_PCT=5, MIN_VOLUME_RATIO=1.3, BATCH_SYMBOLS=16,
SCAN_SECONDS=300, REQUEST_GAP_SECONDS=1.5, MAX_STOP_PCT=1.5,
MIN_24H_VOLUME=30000000, SIGNAL_COOLDOWN_HOURS=12, MIN_ADX=18, MIN_SCORE=4.
Backtest on recent closed data: python alper_radar.py --backtest
"""
import datetime as dt
import json
import math
import statistics
import os
import sqlite3
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

BASE = "https://fapi.binance.com"
TOKEN = os.getenv("TELEGRAM_TOKEN", "").strip()
CHAT = os.getenv("TELEGRAM_CHAT_ID", "").strip()
TARGET_PCT = float(os.getenv("MIN_TARGET_PCT", "5"))
VOLUME_RATIO = float(os.getenv("MIN_VOLUME_RATIO", "1.3"))
BATCH_SYMBOLS = max(1, min(40, int(os.getenv("BATCH_SYMBOLS", "16"))))
WEIGHT_SOFT_LIMIT = max(100, int(os.getenv("WEIGHT_SOFT_LIMIT", "800")))
UNIVERSE_REFRESH_SECONDS = max(3600, int(os.getenv("UNIVERSE_REFRESH_SECONDS", "21600")))
SCAN_SECONDS = max(300, int(os.getenv("SCAN_SECONDS", "300")))
REQUEST_GAP = max(2.0, float(os.getenv("REQUEST_GAP_SECONDS", "2.5")))
STOP_LIMIT = float(os.getenv("MAX_STOP_PCT", "1.5"))
MIN_TURNOVER = float(os.getenv("MIN_24H_VOLUME", "30000000"))
COOLDOWN = float(os.getenv("SIGNAL_COOLDOWN_HOURS", "12")) * 3600
DB_PATH = os.getenv("RADAR_DB", "alper_radar.sqlite3")
MIN_ADX = float(os.getenv("MIN_ADX", "18"))
MIN_SCORE = max(3, min(6, int(os.getenv("MIN_SCORE", "4"))))
BACKTEST_SYMBOLS = [s.strip().upper() for s in os.getenv("BACKTEST_SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT").split(",") if s.strip()][:5]
# Respect candle timestamps and never assume the remote server clock equals ours.

_last_api_call = 0.0
_pause_until = 0.0
_used_weight = 0
_weight_seen_at = 0.0


def log(s):
    print(f"[{dt.datetime.now(dt.timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}] {s}", flush=True)


def fetch(base, path, params=None, attempts=3):
    global _last_api_call, _pause_until, _used_weight, _weight_seen_at
    url = base + path
    if params:
        url += "?" + urllib.parse.urlencode(params)
    for attempt in range(attempts):
        delay = max(0, _pause_until - time.monotonic())
        if base == BASE:
            delay = max(delay, REQUEST_GAP - (time.monotonic() - _last_api_call))
            if _used_weight >= WEIGHT_SOFT_LIMIT and time.monotonic() - _weight_seen_at < 62:
                delay = max(delay, 65 - (time.monotonic() - _weight_seen_at))
        if delay:
            time.sleep(delay)
        if base == BASE:
            _last_api_call = time.monotonic()
        try:
            request = urllib.request.Request(url, headers={"User-Agent": "ALPER-Radar-Paper/6.0"})
            with urllib.request.urlopen(request, timeout=18) as response:
                if base == BASE:
                    raw_weight = response.headers.get("X-MBX-USED-WEIGHT-1M")
                    if raw_weight is not None:
                        _used_weight = int(raw_weight)
                        _weight_seen_at = time.monotonic()
                return json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            if exc.code in (418, 429):
                raw = exc.headers.get("Retry-After", "180")
                try:
                    retry = max(180, min(7200, int(raw)))
                except (TypeError, ValueError):
                    retry = 180
                _pause_until = time.monotonic() + retry + 5
                log(f"Binance HTTP {exc.code}: {retry+5}s pause")
                # Avoid repeatedly hitting a blocked shared IP.
                raise RuntimeError(f"Binance rate-limit ({exc.code}), retry later") from exc
            if exc.code < 500 or attempt == attempts - 1:
                raise
        except (urllib.error.URLError, TimeoutError):
            if attempt == attempts - 1:
                raise
        time.sleep(min(12, 2 ** attempt + 1))
    raise RuntimeError("Request unsuccessful")


def notify(message):
    if not TOKEN or not CHAT:
        log("Telegram credentials missing; printing notification only: " + message.replace("\n", " | "))
        return False
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    body = urllib.parse.urlencode({"chat_id": CHAT, "text": message}).encode("utf-8")
    try:
        req = urllib.request.Request(url, data=body, method="POST")
        with urllib.request.urlopen(req, timeout=18) as result:
            reply = json.loads(result.read().decode("utf-8"))
            if not reply.get("ok"):
                raise RuntimeError("Telegram returned ok=false")
        return True
    except Exception as exc:
        log(f"Telegram send failed: {type(exc).__name__}: {exc}")
        return False


def db_connect():
    path = Path(DB_PATH)
    path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(path)
    con.execute("""CREATE TABLE IF NOT EXISTS trades (
        id INTEGER PRIMARY KEY, symbol TEXT NOT NULL, side TEXT NOT NULL,
        bar_close_ms INTEGER NOT NULL, entry REAL NOT NULL, target REAL NOT NULL,
        stop REAL NOT NULL, opened INTEGER NOT NULL, status TEXT NOT NULL DEFAULT 'OPEN',
        closed INTEGER, strategy TEXT NOT NULL DEFAULT 'BREAKOUT',
        UNIQUE(symbol, side, bar_close_ms))""")
    columns = {r[1] for r in con.execute("PRAGMA table_info(trades)")}
    if "strategy" not in columns:
        con.execute("ALTER TABLE trades ADD COLUMN strategy TEXT NOT NULL DEFAULT 'BREAKOUT'")
    con.execute("CREATE TABLE IF NOT EXISTS radar_state (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
    con.commit()
    return con


def closes_only(klines):
    """Exclude currently forming candles according to Binance close times."""
    now_ms = int(time.time() * 1000)
    return [k for k in klines if int(k[6]) < now_ms - 1500]


def candles(symbol, interval, limit=120):
    rows = fetch(BASE, "/fapi/v1/klines", {"symbol": symbol, "interval": interval, "limit": limit})
    return closes_only(rows)


def ema(values, length):
    if len(values) < length:
        return None
    result = sum(values[:length]) / length
    k = 2 / (length + 1)
    for v in values[length:]:
        result += k * (v - result)
    return result


def rsi(values, n=14):
    if len(values) <= n:
        return None
    changes = [b - a for a, b in zip(values[:-1], values[1:])]
    gains = sum(max(x, 0) for x in changes[:n]) / n
    losses = sum(max(-x, 0) for x in changes[:n]) / n
    for x in changes[n:]:
        gains = (gains * (n - 1) + max(x, 0)) / n
        losses = (losses * (n - 1) + max(-x, 0)) / n
    return 100 if losses == 0 else 100 - 100 / (1 + gains / losses)


def atr(rows, n=14):
    if len(rows) < n + 1:
        return None
    tr = []
    for a, b in zip(rows[:-1], rows[1:]):
        high, low, prev_close = float(b[2]), float(b[3]), float(a[4])
        tr.append(max(high - low, abs(high - prev_close), abs(low - prev_close)))
    value = sum(tr[:n]) / n
    for x in tr[n:]:
        value = (value * (n - 1) + x) / n
    return value


def adx(rows, n=14):
    """Wilder ADX from completed candles; returns trend strength, not direction."""
    if len(rows) < n * 3 + 2:
        return None
    trs, pos, neg = [], [], []
    for prev, row in zip(rows[:-1], rows[1:]):
        ph, pl, pc = float(prev[2]), float(prev[3]), float(prev[4])
        h, l = float(row[2]), float(row[3])
        up, down = h - ph, pl - l
        pos.append(up if up > down and up > 0 else 0.0)
        neg.append(down if down > up and down > 0 else 0.0)
        trs.append(max(h - l, abs(h-pc), abs(l-pc)))
    def smoothed(v):
        value = sum(v[:n])
        results = []
        for i in range(n, len(v)):
            value = value - value / n + v[i]
            results.append(value)
        return results
    t, p, m = smoothed(trs), smoothed(pos), smoothed(neg)
    dx = []
    for a,b,c in zip(t,p,m):
        plus, minus = (100*b/a, 100*c/a) if a > 0 else (0,0)
        dx.append(100*abs(plus-minus)/(plus+minus) if plus+minus else 0)
    if len(dx) < n:
        return None
    v = sum(dx[:n])/n
    for d in dx[n:]:
        v = (v*(n-1)+d)/n
    return v


def macd_hist(values):
    if len(values) < 40:
        return None
    def series(n):
        result = sum(values[:n])/n
        out = [None]*(n-1) + [result]
        a = 2/(n+1)
        for price in values[n:]:
            result += a*(price-result)
            out.append(result)
        return out
    fast, slow = series(12), series(26)
    line = [a-b for a,b in zip(fast,slow) if a is not None and b is not None]
    signal = series9 = sum(line[:9])/9
    for val in line[9:]:
        signal += (val - signal)*0.2
    return line[-1]-signal


def evaluate(s5, s15, s1h):
    """Use only already-closed data; independent confirmations, no claimed win rate."""
    if min(map(len, (s5, s15, s1h))) < 65:
        return None
    c5,c15,ch = [[float(k[4]) for k in s] for s in (s5,s15,s1h)]
    current = c5[-1]
    if current <= 0:
        return None
    e15fast,e15slow = ema(c15,21),ema(c15,55)
    e1fast,e1slow = ema(ch,21),ema(ch,55)
    r = rsi(c5)
    vol_now = float(s5[-1][7])
    vol_avg = sum(float(x[7]) for x in s5[-21:-1])/20
    if vol_avg <= 0 or vol_now / vol_avg < VOLUME_RATIO:
        return None
    a5,a1h = atr(s5),atr(s1h)
    strength = adx(s15)
    hist = macd_hist(c15)
    if not a5 or not a1h or strength is None or hist is None:
        return None
    # Heuristic volatility feasibility filter; NOT an implied price forecast.
    if a1h/ch[-1]*100*8 < TARGET_PCT:
        return None
    prior_high = max(float(x[2]) for x in s5[-21:-1])
    prior_low = min(float(x[3]) for x in s5[-21:-1])
    bull = e1fast > e1slow and e15fast > e15slow and c15[-1] > e15fast
    bear = e1fast < e1slow and e15fast < e15slow and c15[-1] < e15fast
    candidates = []
    if bull and 50 <= r <= 78:
        if current > prior_high:
            candidates.append(("LONG", "BREAKOUT"))
        if c5[-2] <= ema(c5[:-1],21)*1.003 and current > ema(c5,21) and current > float(s5[-1][1]):
            candidates.append(("LONG", "PULLBACK"))
    if bear and 22 <= r <= 50:
        if current < prior_low:
            candidates.append(("SHORT", "BREAKOUT"))
        if c5[-2] >= ema(c5[:-1],21)*0.997 and current < ema(c5,21) and current < float(s5[-1][1]):
            candidates.append(("SHORT", "PULLBACK"))
    if not candidates:
        return None
    side, strategy = candidates[0]
    sign = 1 if side == "LONG" else -1
    score = 2  # aligned 15m and 1h trend + pattern
    score += int(strength >= MIN_ADX)
    score += int(hist*sign > 0)
    score += int(vol_now/vol_avg >= max(VOLUME_RATIO,1.7))
    score += int((r >= 55 if sign == 1 else r <= 45))
    if strength < MIN_ADX or score < MIN_SCORE:
        return None
    stop_pct = min(STOP_LIMIT, max(0.5,(a5/current)*100*2))
    if stop_pct <= 0 or TARGET_PCT / stop_pct < 2:
        return None
    return {"side":side,"strategy":strategy,"score":score,"adx":strength,
        "entry":current,"target":current*(1+sign*TARGET_PCT/100),
        "stop":current*(1-sign*stop_pct/100), "close_ms":int(s5[-1][6]),
        "volume_ratio":vol_now/vol_avg,"rsi":r,"stop_pct":stop_pct}


def backtest():
    """Short, transparent historical sanity check; no optimization or proof of performance."""
    print("ALPER V6 recent-history backtest: completed candles only; approximate candle-close entry")
    print("Not out-of-sample. Fees/slippage/funding not included. Same-candle TP+SL = AMBIGUOUS.")
    total = {"TARGET":0,"STOP":0,"AMBIGUOUS":0,"OPEN":0}
    for symbol in BACKTEST_SYMBOLS:
        try:
            s5 = candles(symbol,"5m",1000)
            s15 = candles(symbol,"15m",500)
            s1h = candles(symbol,"1h",250)
            result = {x:0 for x in total}
            raw_signals = 0
            last_trade_ms = -10**18
            # Historical windows ending at each i; next candle begins after signal time.
            for i in range(120,len(s5)-1,3):
                ts = int(s5[i][6])
                if ts-last_trade_ms < COOLDOWN*1000:
                    continue
                h15 = [x for x in s15 if int(x[6]) <= ts]
                h1h = [x for x in s1h if int(x[6]) <= ts]
                candidate = evaluate(s5[:i+1],h15,h1h)
                if not candidate:
                    continue
                raw_signals += 1
                last_trade_ms = ts
                state = "OPEN"
                for k in s5[i+1:min(len(s5),i+577)]:
                    hi,lo = float(k[2]),float(k[3])
                    tp = hi >= candidate["target"] if candidate["side"] == "LONG" else lo <= candidate["target"]
                    sl = lo <= candidate["stop"] if candidate["side"] == "LONG" else hi >= candidate["stop"]
                    if tp or sl:
                        state = "AMBIGUOUS" if tp and sl else ("TARGET" if tp else "STOP")
                        break
                result[state] += 1
            for k,v in result.items():
                total[k]+=v
            print(f"{symbol}: signals={raw_signals} " + ", ".join(f"{k}={v}" for k,v in result.items()))
        except Exception as exc:
            print(f"{symbol}: unavailable ({type(exc).__name__}: {exc})")
    decided = total["TARGET"]+total["STOP"]
    print(f"TOTAL {total}; decided target hit rate={total['TARGET']/decided:.1%}" if decided else f"TOTAL {total}; no resolved signals")
    print("CAUTION: Small samples and selection effects; use walk-forward tests before live use.")


def universe():
    exchange = fetch(BASE, "/fapi/v1/exchangeInfo")
    valid = {x["symbol"] for x in exchange["symbols"]
             if x.get("status") == "TRADING" and x.get("contractType") == "PERPETUAL"
             and x.get("quoteAsset") == "USDT"}
    # Include EVERY currently trading USDT-margined perpetual, not just top-volume contracts.
    # Deterministic order keeps rotation predictable after a restart.
    return sorted(valid)


def monitor(con):
    # Track by candle highs/lows after signal bar, never unfinished candles.
    active = con.execute("SELECT id,symbol,side,entry,target,stop,bar_close_ms,opened FROM trades WHERE status='OPEN'").fetchall()
    # Monitoring remains independent of universe batching.
    for trade_id, symbol, side, entry, target, stop, bar_ms, opened in active:
        try:
            now = int(time.time())
            if now - opened > 48 * 3600:
                con.execute("UPDATE trades SET status='EXPIRED',closed=? WHERE id=?", (now, trade_id))
                con.commit()
                notify(f"⏳ {symbol} {side} | Paper signal expired after 48h, neither target nor stop confirmed")
                continue
            # Limit 576 5m candles = 48h. Higher weight; tracking is bounded to last 100 candles,
            # so older candle gaps will be explicitly reported rather than fabricated.
            rows = candles(symbol, "5m", 100)
            newer = [x for x in rows if int(x[0]) > bar_ms]
            if not newer:
                continue
            # If the first returned candle skips part of the tracking period, outcome is unknown.
            if int(newer[0][0]) > bar_ms + 300001:
                con.execute("UPDATE trades SET status='UNKNOWN_GAP',closed=? WHERE id=?", (now, trade_id))
                con.commit()
                notify(f"⚠️ {symbol} {side} | Insufficient candles for continuous tracking; result unknown")
                continue
            status = None
            for x in newer:
                hi, lo = float(x[2]), float(x[3])
                tp = hi >= target if side == "LONG" else lo <= target
                sl = lo <= stop if side == "LONG" else hi >= stop
                if tp and sl:
                    status = "AMBIGUOUS"  # sequence within a candle unknown
                elif tp:
                    status = "TARGET"
                elif sl:
                    status = "STOP"
                if status:
                    break
            if status:
                con.execute("UPDATE trades SET status=?,closed=? WHERE id=?", (status, now, trade_id))
                con.commit()
                notify(f"📊 ALPER Paper Tracker | {symbol} {side}\nOutcome: {status}\nEntry(reference): {entry:.9g}\nTarget: {target:.9g} | Stop: {stop:.9g}\nNot an executed trade")
        except Exception as exc:
            log(f"Monitor error {symbol}: {exc}")


def scan(con, symbols):
    sent = 0
    for symbol in symbols:
        try:
            s5 = candles(symbol, "5m")
            s15 = candles(symbol, "15m")
            s1h = candles(symbol, "1h")
            found = evaluate(s5, s15, s1h)
            if not found:
                continue
            side = found["side"]
            now = int(time.time())
            old = con.execute("SELECT opened FROM trades WHERE symbol=? AND side=? ORDER BY opened DESC LIMIT 1",
                              (symbol, side)).fetchone()
            if old and now - old[0] < COOLDOWN:
                continue
            duplicate = con.execute("SELECT 1 FROM trades WHERE symbol=? AND side=? AND bar_close_ms=?",
                                    (symbol, side, found["close_ms"])).fetchone()
            if duplicate:
                continue
            msg = (f"🚨 ALPER Radar V6 | PAPER {side}\n{symbol}\n"
                   f"Reference entry: {found['entry']:.9g}\n"
                   f"🎯 Target (+{TARGET_PCT:g}% price move): {found['target']:.9g}\n"
                   f"🛑 Stop ({found['stop_pct']:.2f}% price move): {found['stop']:.9g}\n"
                   f"{found['strategy']} | score {found['score']}/6 | ADX {found['adx']:.1f}\n"
                   f"15m + 1h trend | Volume {found['volume_ratio']:.2f}x | RSI {found['rsi']:.1f}\n"
                   "Closed candles; not a live fill or profit guarantee")
            # Store before sending so transient Telegram errors never spam repeated signals.
            con.execute("INSERT INTO trades(symbol,side,bar_close_ms,entry,target,stop,opened,status,strategy) "
                        "VALUES(?,?,?,?,?,?,?,'OPEN',?)",
                        (symbol, side, found["close_ms"], found["entry"], found["target"], found["stop"], now, found["strategy"]))
            con.commit()
            notify(msg)
            sent += 1
        except RuntimeError as exc:
            if "rate-limit" in str(exc):
                raise
            log(f"Scan skipped {symbol}: {exc}")
        except Exception as exc:
            log(f"Scan skipped {symbol}: {type(exc).__name__} {exc}")
    return sent


def self_test():
    assert round(ema([1, 2, 3, 4], 3), 6) == 3
    assert round(rsi(list(range(1, 40)))) == 100
    assert round(rsi(list(range(40, 1, -1)))) == 0
    assert abs(atr([[0, "1", "2", "1", "1.5", "0", 0, "1"]] * 20) - 1) < 1e-9
    assert evaluate([], [], []) is None
    assert adx([[0,"1","2","1","1.5","0",0,"1"]]*70) == 0
    assert abs(macd_hist([float(x) for x in range(1,80)])) < 1e-4
    con = sqlite3.connect(":memory:")
    assert con.execute("SELECT 1").fetchone()[0] == 1
    print("SELF-TEST PASS: EMA / RSI / ATR / ADX / MACD / empty data")


def main():
    if "--self-test" in sys.argv:
        self_test()
        return
    if "--backtest" in sys.argv:
        backtest()
        return
    if not (0.2 <= TARGET_PCT <= 30 and 0.3 <= STOP_LIMIT <= 5):
        raise ValueError("Check MIN_TARGET_PCT and MAX_STOP_PCT")
    con = db_connect()
    log("ALPER Radar V6.1 PAPER started; order execution DISABLED")
    notify(f"✅ ALPER Radar V6.1 started (PAPER only)\nMinimum target {TARGET_PCT:g}% (coin price, not leverage)\nLONG + SHORT | 5m/15m/1h | ALL active USDT perpetual symbols (rotating)")
    cycle = 0
    symbols = []
    saved_cursor = con.execute("SELECT value FROM radar_state WHERE key='cursor'").fetchone()
    cursor = int(saved_cursor[0]) if saved_cursor else 0
    last_universe_refresh = 0.0
    while True:
        begun = time.monotonic()
        try:
            if not symbols or time.monotonic() - last_universe_refresh >= UNIVERSE_REFRESH_SECONDS:
                symbols = universe()
                last_universe_refresh = time.monotonic()
                cursor %= max(1, len(symbols))
                log(f"Universe: ALL {len(symbols)} active USDT perpetual contracts; rotating in batches of {BATCH_SYMBOLS}")
            if not symbols:
                log("No active USDT perpetual contracts returned; waiting")
                time.sleep(SCAN_SECONDS)
                continue
            # Round-robin: cover entire exchange without firing hundreds of requests at once.
            batch = [symbols[(cursor + i) % len(symbols)] for i in range(min(BATCH_SYMBOLS, len(symbols)))]
            cursor = (cursor + len(batch)) % len(symbols)
            con.execute("INSERT OR REPLACE INTO radar_state(key,value) VALUES('cursor',?)", (str(cursor),))
            con.commit()
            monitor(con)
            found = scan(con, batch)
            log(f"Scan completed: {len(batch)} / {len(symbols)} symbols; {found} new signal(s); next index={cursor}")
            if cycle % 6 == 0:
                open_count = con.execute("SELECT COUNT(*) FROM trades WHERE status='OPEN'").fetchone()[0]
                stats = con.execute("SELECT strategy,status,COUNT(*) FROM trades GROUP BY strategy,status").fetchall()
                overview = "; ".join(f"{strategy}:{status}={count}" for strategy,status,count in stats)[:700] or "no trades yet"
                notify(f"💚 ALPER Radar V6.1 health | {len(batch)} scanned in batch / {len(symbols)} total | {found} new signals | {open_count} open paper signals\n{overview}")
            cycle += 1
        except KeyboardInterrupt:
            raise
        except Exception as exc:
            log(f"Cycle warning: {type(exc).__name__} {exc}")
            if "rate-limit" in str(exc):
                time.sleep(max(65, _pause_until - time.monotonic()))
        time.sleep(max(5, SCAN_SECONDS - (time.monotonic() - begun)))


if __name__ == "__main__":
    main()