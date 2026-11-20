#!/usr/bin/env python3
"""
Monitor the number of distinct BTC-USD maker (resting) orders executed per
time window from Coinbase's public WebSocket feed (matches channel) and send
ntfy notifications on sustained rises/falls.

Requirements: Python 3.9+, `pip install websockets requests`
"""

import asyncio
import json
import re
from datetime import datetime, timedelta, timezone

import requests
import websockets

# ----------------------------- Parameters -----------------------------
NOTIFY_URL = "https://ntfy.sh/order_count"
TIME_WINDOW = 60      # seconds (can be any positive number, e.g. 47)
RISE_N = 5            # min rise_list size to notify
FALL_N = 5            # min fall_list size to notify

WS_URL = "wss://ws-feed.exchange.coinbase.com"
PRODUCT_ID = "BTC-USD"
RECONNECT_DELAY = 5   # seconds
# ----------------------------------------------------------------------

UTC7 = timezone(timedelta(hours=7))


def parse_exchange_time(s: str) -> datetime:
    """Parse Coinbase ISO timestamp, e.g. 2026-11-18T09:17:21.123456Z"""
    s = s.replace("Z", "+00:00")
    s = re.sub(r"(\.\d{6})\d+", r"\1", s)
    return datetime.fromisoformat(s)


def fmt_duration(seconds: float) -> str:
    """Short form like '1 hr 3 min 20 sec', skipping zero parts."""
    total = int(round(seconds))
    h, rem = divmod(total, 3600)
    m, s = divmod(rem, 60)
    parts = []
    if h:
        parts.append(f"{h} hr")
    if m:
        parts.append(f"{m} min")
    if s:
        parts.append(f"{s} sec")
    return " ".join(parts) if parts else "0 sec"


def fmt_ts(dt: datetime) -> str:
    """e.g. '2026-11-18 (UTC+7) 16:17:21'"""
    local = dt.astimezone(UTC7)
    return f"{local:%Y-%m-%d} (UTC+7) {local:%H:%M:%S}"


def send_ntfy(title: str, body: str) -> None:
    try:
        r = requests.post(
            NOTIFY_URL,
            data=body.encode("utf-8"),
            headers={"Title": title},
            timeout=10,
        )
        r.raise_for_status()
    except Exception as e:  # a failed notification must not break monitoring
        print(f"   !! Notification failed: {e}")


class Monitor:
    def __init__(self):
        self.reset()

    def reset(self):
        self.window_end = None      # exchange-time end of current window
        self.orders = set()         # distinct maker order IDs in current window
        self.first_window = True    # first (partial) window is discarded
        self.prev_count = None
        self.rise_list = []
        self.fall_list = []
        self.last_trade = None      # latest trade time seen (exchange time)

    async def on_trade(self, t: datetime, maker_order_id: str):
        # Anchor windows at the first trade after connecting (exchange time)
        if self.window_end is None:
            self.window_end = t + timedelta(seconds=TIME_WINDOW)
            self.orders = set()

        # Close every window that ended before this trade
        while t >= self.window_end:
            await self.close_window()

        self.orders.add(maker_order_id)
        if self.last_trade is None or t > self.last_trade:
            self.last_trade = t

    async def close_window(self):
        end = self.window_end
        count = len(self.orders)

        if self.first_window:
            print(f"{fmt_ts(end)} | orders = {count}  (first partial window, discarded)")
            self.first_window = False
        else:
            print(f"{fmt_ts(end)} | orders = {count}")
            await self.update_lists(count)

        self.window_end = end + timedelta(seconds=TIME_WINDOW)
        self.orders = set()

    async def update_lists(self, count: int):
        if self.prev_count is None:
            self.rise_list = [count]
            self.fall_list = [count]
        elif count > self.prev_count:
            self.rise_list.append(count)
            self.fall_list = [count]
        elif count < self.prev_count:
            self.fall_list.append(count)
            self.rise_list = [count]
        else:  # tie: reset both
            self.rise_list = [count]
            self.fall_list = [count]
        self.prev_count = count

        # Each list is checked and notified independently
        if len(self.rise_list) >= RISE_N:
            await self.notify("rise", self.rise_list)
        if len(self.fall_list) >= FALL_N:
            await self.notify("fall", self.fall_list)

    async def notify(self, kind: str, lst: list):
        size = len(lst)
        verb = "rises" if kind == "rise" else "falls"
        title = f"Order count {verb} for {fmt_duration(TIME_WINDOW * size)}"
        last = fmt_ts(self.last_trade) if self.last_trade else "n/a"
        body = "\n".join([
            f"Last trade at {last}",
            f"{kind.capitalize()} across {size} windows of {fmt_duration(TIME_WINDOW)}",
            " -> ".join(str(c) for c in lst),
        ])
        print(f"   >> Notification sent: {title}")
        await asyncio.to_thread(send_ntfy, title, body)


async def run():
    monitor = Monitor()
    subscribe_msg = json.dumps({
        "type": "subscribe",
        "product_ids": [PRODUCT_ID],
        "channels": ["matches"],
    })

    while True:
        monitor.reset()  # fresh start on every (re)connect
        try:
            async with websockets.connect(
                WS_URL, ping_interval=20, ping_timeout=20
            ) as ws:
                await ws.send(subscribe_msg)
                print(f"Connected to {WS_URL}, subscribed to {PRODUCT_ID} matches "
                      f"(window = {fmt_duration(TIME_WINDOW)})")

                async for raw in ws:
                    msg = json.loads(raw)
                    mtype = msg.get("type")
                    if mtype == "match":          # ignore 'last_match' snapshot
                        await monitor.on_trade(
                            parse_exchange_time(msg["time"]), msg["maker_order_id"]
                        )
                    elif mtype == "error":
                        raise RuntimeError(f"Feed error: {msg}")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print(f"Disconnected/error: {e!r}")

        print(f"State reset. Reconnecting in {RECONNECT_DELAY}s...")
        await asyncio.sleep(RECONNECT_DELAY)


if __name__ == "__main__":
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        print("\nStopped.")
