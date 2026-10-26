#!/usr/bin/env python3
"""
BTC-USD support/resistance break monitor.

Connects to Coinbase Exchange public WebSocket feed (matches channel),
tracks rolling min/max over a deque of recent trades, and sends an ntfy
notification when a new low/high breaks a sufficiently old extreme.

Requires: Python 3.9+, `pip install websockets aiohttp`
"""

import asyncio
import json
import re
import logging
from collections import deque
from datetime import datetime, timedelta, timezone

import aiohttp
import websockets

# ----------------------------- Parameters ---------------------------------
NOTIFICATION_URL = "https://ntfy.sh/new_min_max"
TIME_WINDOW = 1            # minutes (fixed time unit)
WINDOW_SERIES = 10         # deque holds ~WINDOW_SERIES * TIME_WINDOW minutes
INITIAL_SERIES = 2         # old extreme must be >= this many windows after deque start
CONSOLIDATION_SERIES = 5   # old extreme must be >= this many windows old when broken

WS_URL = "wss://ws-feed.exchange.coinbase.com"
PRODUCT_ID = "BTC-USD"
RECONNECT_DELAY_START = 5  # seconds
RECONNECT_DELAY_MAX = 60   # seconds
DISPLAY_TZ = timezone(timedelta(hours=7))  # UTC+7
# ---------------------------------------------------------------------------

WINDOW = timedelta(minutes=TIME_WINDOW)
TRIM_SPAN = timedelta(minutes=TIME_WINDOW * (WINDOW_SERIES + 1))
INITIAL_MIN_AGE = timedelta(minutes=TIME_WINDOW * INITIAL_SERIES)
CONSOLIDATION_MIN_AGE = timedelta(minutes=TIME_WINDOW * CONSOLIDATION_SERIES)

log = logging.getLogger("btc_monitor")


def parse_time(s: str) -> datetime:
    """Parse Coinbase ISO 8601 time (e.g. 2026-10-26T07:23:28.123456Z)."""
    s = s.replace("Z", "+00:00")
    # fromisoformat (3.9/3.10) needs exactly 3 or 6 fractional digits
    s = re.sub(r"\.(\d+)", lambda m: "." + (m.group(1) + "000000")[:6], s)
    return datetime.fromisoformat(s)


def format_duration(td: timedelta) -> str:
    """e.g. '10 minutes 23 seconds', '1 hour 8 minutes 1 second'."""
    total = int(td.total_seconds())
    h, rem = divmod(total, 3600)
    m, s = divmod(rem, 60)
    parts = []
    for value, unit in ((h, "hour"), (m, "minute"), (s, "second")):
        if value:
            parts.append(f"{value} {unit}{'' if value == 1 else 's'}")
    return " ".join(parts) if parts else "0 seconds"


class Monitor:
    """Holds all state. Create a new instance to 'reset everything'."""

    def __init__(self):
        self.ticks = deque()  # (timestamp, price)
        self.min_price = None
        self.min_ts = None
        self.max_price = None
        self.max_ts = None

    # -- helpers ----------------------------------------------------------
    def _recompute_extremes(self):
        """Recompute min/max over the entire deque (earliest tick wins ties)."""
        min_p = max_p = None
        min_t = max_t = None
        for ts, p in self.ticks:
            if min_p is None or p < min_p:
                min_p, min_t = p, ts
            if max_p is None or p > max_p:
                max_p, max_t = p, ts
        self.min_price, self.min_ts = min_p, min_t
        self.max_price, self.max_ts = max_p, max_t

    def _trim(self):
        """When span >= (WINDOW_SERIES+1) windows, drop the oldest window."""
        trimmed = False
        while self.ticks and (self.ticks[-1][0] - self.ticks[0][0]) >= TRIM_SPAN:
            cutoff = self.ticks[0][0] + WINDOW
            while self.ticks and self.ticks[0][0] < cutoff:
                self.ticks.popleft()
            trimmed = True
        if trimmed:
            self._recompute_extremes()

    @staticmethod
    def _make_alert(direction, old_ts, new_ts, price):
        title = f"New {direction} after {format_duration(new_ts - old_ts)}"
        local = new_ts.astimezone(DISPLAY_TZ).strftime("%Y-%m-%d (UTC+7) %H:%M:%S")
        body = f"Break out at $ {price:,.2f}\n{local}"
        return title, body

    # -- main entry -------------------------------------------------------
    def on_tick(self, ts: datetime, price: float):
        """Process one match. Returns a list of (title, body) alerts."""
        alerts = []

        if self.ticks and ts < self.ticks[-1][0]:
            return alerts  # ignore out-of-order tick

        if not self.ticks:
            self.min_price = self.max_price = price
            self.min_ts = self.max_ts = ts
        else:
            first_ts = self.ticks[0][0]

            if price < self.min_price:
                if (self.min_ts - first_ts >= INITIAL_MIN_AGE
                        and ts - self.min_ts >= CONSOLIDATION_MIN_AGE):
                    alerts.append(self._make_alert("low", self.min_ts, ts, price))
                self.min_price, self.min_ts = price, ts

            if price > self.max_price:
                if (self.max_ts - first_ts >= INITIAL_MIN_AGE
                        and ts - self.max_ts >= CONSOLIDATION_MIN_AGE):
                    alerts.append(self._make_alert("high", self.max_ts, ts, price))
                self.max_price, self.max_ts = price, ts

        self.ticks.append((ts, price))
        self._trim()
        return alerts


async def send_notification(session: aiohttp.ClientSession, title: str, body: str):
    try:
        async with session.post(
            NOTIFICATION_URL,
            data=body.encode("utf-8"),
            headers={"Title": title},
            timeout=aiohttp.ClientTimeout(total=10),
        ) as resp:
            log.info("Notification sent (%s): %s", resp.status, title)
    except Exception as e:  # never let a notify failure kill the monitor
        log.warning("Notification failed: %s", e)


async def run_once(session: aiohttp.ClientSession) -> None:
    """One connection lifetime. Raises on any failure/disconnect."""
    monitor = Monitor()  # fresh state every (re)connect
    pending = set()
    subscribe = {
        "type": "subscribe",
        "product_ids": [PRODUCT_ID],
        "channels": ["matches"],
    }
    async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=20) as ws:
        await ws.send(json.dumps(subscribe))
        log.info("Connected and subscribed to %s matches", PRODUCT_ID)
        async for raw in ws:
            msg = json.loads(raw)
            mtype = msg.get("type")
            if mtype == "error":
                raise RuntimeError(f"Feed error: {msg}")
            if mtype != "match":  # ignores subscriptions, last_match, etc.
                continue
            run_once.got_data = True
            ts = parse_time(msg["time"])
            price = float(msg["price"])
            for title, body in monitor.on_tick(ts, price):
                task = asyncio.create_task(send_notification(session, title, body))
                pending.add(task)
                task.add_done_callback(pending.discard)
    raise ConnectionError("WebSocket closed")


async def main():
    delay = RECONNECT_DELAY_START
    async with aiohttp.ClientSession() as session:
        while True:
            run_once.got_data = False
            try:
                await run_once(session)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.warning("Disconnected/failed: %r", e)
            if run_once.got_data:
                delay = RECONNECT_DELAY_START  # healthy session -> reset backoff
            log.info("Resetting state; reconnecting in %ss", delay)
            await asyncio.sleep(delay)
            delay = min(delay * 2, RECONNECT_DELAY_MAX)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
