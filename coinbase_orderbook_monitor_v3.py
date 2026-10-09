	#!/usr/bin/env python3
"""
Coinbase level2_batch monitor: detects up-down-up-down fluctuation patterns in
8 per-batch metrics and sends notifications to ntfy.

Install:  pip install websockets aiohttp
Run:      python coinbase_orderbook_monitor.py
"""

import asyncio
import json
import logging
import time
from datetime import datetime, timedelta, timezone

import aiohttp
import websockets

# ----------------------------------------------------------------------------
# 1) Parameters
# ----------------------------------------------------------------------------
PAIR = "BTC-USD"
NTFY_URL = "https://ntfy.sh/orderbook_mobile"
CASH_N = 48
ACTIVE_LEVEL_N = 52
EXECUTED_LEVEL_N = 40
TOUCH_N = 26

WS_URL = "wss://ws-feed.exchange.coinbase.com"
PRIMARY_CHANNEL = "level2_batch"
FALLBACK_CHANNEL = "level2"      # used only if the primary subscription is rejected
THROTTLE_SECONDS = 5
LOCAL_TZ = timezone(timedelta(hours=7))  # UTC+7

# (title, side, value key, price key shown in the notification, minimum list length)
SPECS = [
    ("Bid amount Flatten",      "buy",  "bid_amount",         "highest_bid",          CASH_N),
    ("Ask amount Flatten",      "sell", "ask_amount",         "lowest_ask",           CASH_N),
    ("Bidding Level off",       "buy",  "bid_active_level",   "highest_bid",          ACTIVE_LEVEL_N),
    ("Asking Level off",        "sell", "ask_active_level",   "lowest_ask",           ACTIVE_LEVEL_N),
    ("Bid execution level off", "buy",  "bid_executed_level", "highest_executed_bid", EXECUTED_LEVEL_N),
    ("Ask execution level off", "sell", "ask_executed_level", "lowest_executed_ask",  EXECUTED_LEVEL_N),
    ("Bid price fluctuation",   "buy",  "highest_bid",        "highest_bid",          TOUCH_N),
    ("Ask price fluctuation",   "sell", "lowest_ask",         "lowest_ask",           TOUCH_N),
]

log = logging.getLogger("monitor")


# ----------------------------------------------------------------------------
# 2) Per-batch calculations
# ----------------------------------------------------------------------------
def compute_metrics(changes):
    """changes: [[side, price, size], ...] from one l2update message."""
    m = {
        "bid_amount": 0.0, "ask_amount": 0.0,
        "bid_active_level": 0, "ask_active_level": 0,
        "bid_executed_level": 0, "ask_executed_level": 0,
        "highest_bid": None, "lowest_ask": None,
        "highest_executed_bid": None, "lowest_executed_ask": None,
    }
    for side, price_s, size_s in changes:
        price, size = float(price_s), float(size_s)
        if side == "buy":
            m["bid_amount"] += price * size
            if size != 0:
                m["bid_active_level"] += 1
                if m["highest_bid"] is None or price > m["highest_bid"]:
                    m["highest_bid"] = price
            else:
                m["bid_executed_level"] += 1
                if m["highest_executed_bid"] is None or price > m["highest_executed_bid"]:
                    m["highest_executed_bid"] = price
        elif side == "sell":
            m["ask_amount"] += price * size
            if size != 0:
                m["ask_active_level"] += 1
                if m["lowest_ask"] is None or price < m["lowest_ask"]:
                    m["lowest_ask"] = price
            else:
                m["ask_executed_level"] += 1
                if m["lowest_executed_ask"] is None or price < m["lowest_executed_ask"]:
                    m["lowest_executed_ask"] = price
    return m


def parse_time(s):
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        dt = datetime.now(timezone.utc)
    return dt.astimezone(LOCAL_TZ)


# ----------------------------------------------------------------------------
# 4) Notification with throttling
# ----------------------------------------------------------------------------
class Notifier:
    def __init__(self, session):
        self.session = session
        self.last_sent = {}   # title -> (monotonic time, count)
        self._tasks = set()

    def notify(self, title, side, count, price, ts):
        now = time.monotonic()
        prev = self.last_sent.get(title)
        if prev is not None:
            prev_time, prev_count = prev
            # Within the throttle window only a larger count is allowed through
            if now - prev_time < THROTTLE_SECONDS and count <= prev_count:
                log.debug("Dropped %s (count %d)", title, count)
                return
        self.last_sent[title] = (now, count)

        label = "Last buy at" if side == "buy" else "Last sell at"
        price_txt = f"${price:,.2f}" if price is not None else "N/A"
        body = f"{label} {price_txt}\n{ts:%Y-%m-%d} (UTC+7) {ts:%H:%M:%S}"
        msg_title = f"{title} - {count} batches"

        task = asyncio.create_task(self._send(msg_title, body))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def _send(self, title, body):
        try:
            async with self.session.post(
                NTFY_URL,
                data=body.encode("utf-8"),
                headers={"Title": title},
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status >= 300:
                    log.warning("ntfy returned %s for '%s'", resp.status, title)
                else:
                    log.info("Sent: %s | %s", title, body.replace("\n", " | "))
        except Exception as exc:
            log.warning("ntfy send failed: %s", exc)


# ----------------------------------------------------------------------------
# 3) Zigzag analysis
# ----------------------------------------------------------------------------
def sign(x):
    return (x > 0) - (x < 0)


class ZigzagList:
    def __init__(self, title, side, value_key, price_key, min_len, notifier):
        self.title, self.side = title, side
        self.value_key, self.price_key = value_key, price_key
        self.min_len = min_len
        self.notifier = notifier
        self.items = []  # (value, price, timestamp)

    def reset(self):
        self.items = []

    def update(self, metrics, ts):
        value = metrics[self.value_key]
        price = metrics[self.price_key]
        new_item = (value, price, ts)

        # Not found, 0, or identical to the latest value -> ignore
        if not value or (self.items and value == self.items[-1][0]):
            return

        # 0 or 1 item: just insert
        if len(self.items) < 2:
            self.items.append(new_item)
            return

        x, y = self.items[-2][0], self.items[-1][0]
        if sign(value - y) != sign(y - x):
            self.items.append(new_item)          # direction flipped: pattern continues
            return

        # Same direction: fluctuation ended
        if len(self.items) >= self.min_len:
            _, last_price, last_ts = self.items[-1]
            self.notifier.notify(self.title, self.side, len(self.items), last_price, last_ts)
        self.items = [new_item]                  # reset whenever the pattern is broken


# ----------------------------------------------------------------------------
# Streaming
# ----------------------------------------------------------------------------
async def stream(session):
    notifier = Notifier(session)
    lists = [ZigzagList(t, s, v, p, n, notifier) for t, s, v, p, n in SPECS]
    channel = PRIMARY_CHANNEL
    backoff = 1

    while True:
        for lst in lists:            # reset lists on every (re)connect
            lst.reset()
        try:
            async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=20,
                                          max_size=None) as ws:
                await ws.send(json.dumps({
                    "type": "subscribe",
                    "product_ids": [PAIR],
                    "channels": [channel],
                }))
                log.info("Subscribed to %s on %s", channel, PAIR)
                backoff = 1

                async for raw in ws:
                    msg = json.loads(raw)
                    mtype = msg.get("type")

                    if mtype == "l2update":
                        ts = parse_time(msg.get("time", ""))
                        metrics = compute_metrics(msg.get("changes", []))
                        for lst in lists:
                            lst.update(metrics, ts)
                    elif mtype == "error":
                        log.error("Server error: %s", msg)
                        if channel == PRIMARY_CHANNEL:
                            log.warning("Falling back to channel '%s'", FALLBACK_CHANNEL)
                            channel = FALLBACK_CHANNEL
                        break  # reconnect
                    # "snapshot", "subscriptions", etc. are ignored
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.warning("Connection lost: %s", exc)

        log.info("Reconnecting in %ds (lists reset)", backoff)
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, 30)


async def main():
    async with aiohttp.ClientSession() as session:
        await stream(session)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
