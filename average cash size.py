#!/usr/bin/env python3
"""
Monitor the average cash size (USD per trade) of BTC-USD matches on Coinbase.

- Connects to Coinbase's public WebSocket feed, subscribes to `matches` for BTC-USD.
- Windows of TIME_WINDOW seconds start from the moment the connection is established.
- Each window's average = sum(price * size) / trade_count.
- Keeps the last SERIES_N averages; when a new value is a strict new max / min
  of the SERIES_N values, a notification is sent to ntfy.

Install:  pip install websockets aiohttp
Run:      python btc_cash_size_monitor.py
"""

import asyncio
import json
import logging
import math
import time
from collections import deque
from datetime import datetime, timedelta, timezone

import aiohttp
import websockets

# ----------------------------- parameters -----------------------------
NOTIFICATION_URL = "https://ntfy.sh/average_cash_size"
TIME_WINDOW = 60      # seconds (can be any value, e.g. 25 or 76)
SERIES_N = 30         # number of window averages kept

# ------------------------------ settings ------------------------------
WS_URL = "wss://ws-feed.exchange.coinbase.com"
PRODUCT = "BTC-USD"
GRACE = 1.0           # seconds to wait after a window ends for late-arriving trades
RECONNECT_DELAY = 5   # seconds before reconnecting after a disconnect / error
UTC7 = timezone(timedelta(hours=7))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("cash_size")


def parse_exchange_time(s: str) -> float:
    """Coinbase ISO timestamp (e.g. 2026-11-18T06:11:26.123456Z) -> epoch seconds."""
    return datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()


def format_duration(total_seconds: float) -> str:
    """e.g. '01 hr 12 min 52 sec', '08 min 42 sec', '08 min', '01 hr', '25 sec'.
    Any part that is zero is omitted."""
    total = int(round(total_seconds))
    h, rem = divmod(total, 3600)
    m, s = divmod(rem, 60)
    parts = []
    if h:
        parts.append(f"{h:02d} hr")
    if m:
        parts.append(f"{m:02d} min")
    if s:
        parts.append(f"{s:02d} sec")
    return " ".join(parts) or "00 sec"


class Monitor:
    def __init__(self, session: aiohttp.ClientSession):
        self.session = session
        self._tasks = set()
        self.reset()

    # ------------------------------ state ------------------------------
    def reset(self):
        """Reset everything: series, buckets, window clock."""
        self.series = deque(maxlen=SERIES_N)   # oldest item drops automatically
        self.buckets = {}                      # window index -> [sum_usd, count, last_trade_ts]
        self.next_idx = 0                      # next window index to be closed
        self.start = time.time()               # wall-clock start of window 0

    # --------------------------- notifications --------------------------
    def notify(self, kind: str, value: float, last_trade_ts: float):
        title = f"New {kind} found in {format_duration(SERIES_N * TIME_WINDOW)}"
        last_dt = datetime.fromtimestamp(last_trade_ts, tz=UTC7)
        body = (
            f"Last trade at {last_dt:%Y-%m-%d} (UTC+7) {last_dt:%H:%M:%S}\n"
            f"Average trade size in the last {TIME_WINDOW} seconds is ${value:,.0f}"
        )
        task = asyncio.create_task(self._send(title, body))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def _send(self, title: str, body: str):
        try:
            async with self.session.post(
                NOTIFICATION_URL,
                data=body.encode("utf-8"),
                headers={"Title": title},
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status >= 300:
                    log.error("ntfy returned HTTP %s", resp.status)
                else:
                    log.info("Notification sent: %s", title)
        except Exception as e:  # ntfy failure must not reset the monitor
            log.error("Failed to send notification: %s", e)

    # ----------------------------- windows ------------------------------
    def finalize_window(self, idx: int):
        bucket = self.buckets.pop(idx, None)
        if not bucket:
            log.info("Window %d: no trades -> deque reset", idx)
            self.series.clear()
            return

        total_usd, count, last_ts = bucket
        avg = total_usd / count
        self.series.append(avg)  # drops the oldest automatically when full
        log.info(
            "Window %d: trades=%d avg=$%s series_len=%d",
            idx, count, f"{avg:,.0f}", len(self.series),
        )

        if len(self.series) < SERIES_N:
            return

        others = list(self.series)[:-1]  # the other SERIES_N-1 values
        if avg > max(others):
            self.notify("max", avg, last_ts)
        if avg < min(others):
            self.notify("min", avg, last_ts)

    async def window_closer(self):
        """Wall clock: closes each window TIME_WINDOW (+ grace) after the previous one."""
        while True:
            close_at = self.start + (self.next_idx + 1) * TIME_WINDOW + GRACE
            delay = close_at - time.time()
            if delay > 0:
                await asyncio.sleep(delay)
            idx = self.next_idx
            self.next_idx += 1  # increment first so late trades for idx are rejected
            self.finalize_window(idx)

    # ------------------------------ trades ------------------------------
    def on_match(self, msg: dict):
        t = parse_exchange_time(msg["time"])
        usd = float(msg["price"]) * float(msg["size"])
        idx = math.floor((t - self.start) / TIME_WINDOW)
        if idx < self.next_idx:
            return  # before our start (history) or arrived after its window closed
        b = self.buckets.setdefault(idx, [0.0, 0, 0.0])
        b[0] += usd
        b[1] += 1
        b[2] = max(b[2], t)

    # ----------------------------- connection ----------------------------
    async def run_once(self):
        self.reset()
        async with websockets.connect(WS_URL) as ws:
            await ws.send(json.dumps({
                "type": "subscribe",
                "product_ids": [PRODUCT],
                "channels": ["matches"],
            }))
            self.start = time.time()  # window clock starts once subscribed
            log.info("Connected, subscribed to %s matches. Window=%ss, N=%d",
                     PRODUCT, TIME_WINDOW, SERIES_N)

            closer = asyncio.create_task(self.window_closer())
            try:
                async for raw in ws:
                    msg = json.loads(raw)
                    mtype = msg.get("type")
                    if mtype == "match":
                        self.on_match(msg)
                    elif mtype == "error":
                        raise RuntimeError(f"Feed error: {msg}")
                    # "last_match" (snapshot of an old trade), "subscriptions",
                    # heartbeats, etc. are ignored.
                raise ConnectionError("WebSocket closed")
            finally:
                closer.cancel()


async def main():
    async with aiohttp.ClientSession() as session:
        monitor = Monitor(session)
        while True:
            try:
                await monitor.run_once()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.error("Disconnected / error: %s -> resetting everything", e)
            monitor.reset()
            await asyncio.sleep(RECONNECT_DELAY)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Stopped.")
