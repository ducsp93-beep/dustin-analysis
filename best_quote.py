#!/usr/bin/env python3
"""
Monitor how often the best bid / best ask changes for BTC-USD on Coinbase
(Exchange public feed, level2_batch channel) and send ntfy notifications
when the per-window change count keeps growing or declining.

Just double-click this file (or run_monitor.bat). Missing packages
(websockets, requests, sortedcontainers) are installed automatically on first run.
"""

import asyncio
import importlib
import json
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone


def _ensure_packages():
    """Install any missing third-party packages so a double-click just works."""
    needed = {"websockets": "websockets", "requests": "requests",
              "sortedcontainers": "sortedcontainers"}
    for module, pip_name in needed.items():
        try:
            importlib.import_module(module)
        except ImportError:
            print(f"Installing missing package: {pip_name} ...")
            subprocess.check_call([sys.executable, "-m", "pip", "install", pip_name])


_ensure_packages()

import requests  # noqa: E402
import websockets  # noqa: E402
from sortedcontainers import SortedDict  # noqa: E402

# ----------------------------------------------------------------------
# Parameters
# ----------------------------------------------------------------------
WS_URL = "wss://ws-feed.exchange.coinbase.com"
PRODUCT_ID = "BTC-USD"
CHANNEL = "level2_batch"

NOTIFY_URL = "https://ntfy.sh/best_quote"
TIME_WINDOW = 80      # seconds
GROW_RATE = 0.05       # 0..1
DECLINE_RATE = 0.05    # 0..1
GROW_N = 5
DECLINE_N = 5

RECONNECT_DELAY = 5   # seconds
TZ = timezone(timedelta(hours=7))


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------
def now_str() -> str:
    return datetime.now(TZ).strftime("%Y-%m-%d (UTC+7) %H:%M:%S")


def short_duration(seconds: int) -> str:
    """e.g. 400 -> '6 min 40 sec', 4800 -> '1 hr 20 min' (zero parts skipped)."""
    h, rem = divmod(int(seconds), 3600)
    m, s = divmod(rem, 60)
    parts = []
    if h:
        parts.append(f"{h} hr")
    if m:
        parts.append(f"{m} min")
    if s:
        parts.append(f"{s} sec")
    return " ".join(parts) if parts else "0 sec"


def count_changes(values) -> int:
    """Number of runs of identical consecutive values (matches the spec example:
    5 x 84132.16, 84133.57, 2 x 84133.85, ... -> 5).  Empty window -> 0.
    To count only the transitions instead (4 in that example), drop the '1 +'."""
    if not values:
        return 0
    return 1 + sum(1 for i in range(1, len(values)) if values[i] != values[i - 1])


def fmt_values(values) -> str:
    return " -> ".join(str(v) for v in values)


# ----------------------------------------------------------------------
# Per-connection state (everything is rebuilt after a disconnect)
# ----------------------------------------------------------------------
class SideTracker:
    """Tracks the 2 trend lists (grow / decline) for one side (bid or ask)."""

    def __init__(self, name: str):
        self.name = name              # "bid" or "ask"
        self.prev_count = None
        self.grow = []
        self.decline = []
        self.notified_grow = 0        # list length at last notification
        self.notified_decline = 0

    def add_count(self, c: int):
        p = self.prev_count
        if p is None or p == 0:
            self._reset_grow(c)
            self._reset_decline(c)
        else:
            if c > p * (1 + GROW_RATE):
                self.grow.append(c)
            else:
                self._reset_grow(c)
            if c < p * (1 - DECLINE_RATE):
                self.decline.append(c)
            else:
                self._reset_decline(c)
        self.prev_count = c

    def _reset_grow(self, c):
        self.grow = [c]
        self.notified_grow = 0

    def _reset_decline(self, c):
        self.decline = [c]
        self.notified_decline = 0

    def pending_event(self):
        """Return (kind, list) if a notification is due for this side, else None."""
        cands = []
        if len(self.grow) >= GROW_N and len(self.grow) > self.notified_grow:
            cands.append(("Growing", self.grow))
        if len(self.decline) >= DECLINE_N and len(self.decline) > self.notified_decline:
            cands.append(("Declining", self.decline))
        if not cands:
            return None
        return max(cands, key=lambda x: len(x[1]))

    def mark_notified(self, kind: str):
        if kind == "Growing":
            self.notified_grow = len(self.grow)
        else:
            self.notified_decline = len(self.decline)


class Session:
    def __init__(self):
        self.bids = SortedDict()   # price -> size
        self.asks = SortedDict()
        self.best_bid_list = []
        self.best_ask_list = []
        self.bid = SideTracker("bid")
        self.ask = SideTracker("ask")
        self.snapshot_event = asyncio.Event()

    # --- order book -----------------------------------------------------
    def load_snapshot(self, msg):
        self.bids.clear()
        self.asks.clear()
        for price, size in msg.get("bids", []):
            if float(size) > 0:
                self.bids[float(price)] = float(size)
        for price, size in msg.get("asks", []):
            if float(size) > 0:
                self.asks[float(price)] = float(size)

    def apply_update(self, msg):
        for side, price, size in msg.get("changes", []):
            book = self.bids if side == "buy" else self.asks
            price, size = float(price), float(size)
            if size == 0:
                book.pop(price, None)
            else:
                book[price] = size
        if self.bids and self.asks:
            self.best_bid_list.append(self.bids.peekitem(-1)[0])  # highest bid
            self.best_ask_list.append(self.asks.peekitem(0)[0])   # lowest ask


# ----------------------------------------------------------------------
# Notification
# ----------------------------------------------------------------------
def build_notification(events, ts: str):
    """events: list of (side_name, kind, values). Returns (title, body)."""
    by_side = {e[0]: e for e in events}

    # Title (bid first)
    if len(events) == 1:
        side, kind, _ = events[0]
        title = f"{kind} {side}"
    else:
        kb, ka = by_side["bid"][1], by_side["ask"][1]
        if kb == ka:
            title = f"{kb} bid and ask"
        else:
            title = f"{kb} bid but {ka.lower()} ask"

    # Body (longest list first)
    ordered = sorted(events, key=lambda e: len(e[2]), reverse=True)
    lines = [ts]
    for side, kind, values in ordered:
        dur = short_duration(len(values) * TIME_WINDOW)
        lines.append(f"{kind} {side} in {dur}")
        lines.append(fmt_values(values))
    return title, "\n".join(lines)


def send_ntfy(title: str, body: str):
    try:
        r = requests.post(
            NOTIFY_URL,
            data=body.encode("utf-8"),
            headers={"Title": title},
            timeout=10,
        )
        r.raise_for_status()
        print(f"  -> notification sent: {title}")
    except Exception as e:
        print(f"  !! notification failed: {e}")


# ----------------------------------------------------------------------
# Window timer
# ----------------------------------------------------------------------
async def window_timer(sess: Session):
    await sess.snapshot_event.wait()
    next_tick = time.monotonic() + TIME_WINDOW
    while True:
        await asyncio.sleep(max(0, next_tick - time.monotonic()))
        next_tick += TIME_WINDOW

        # Stop growing the lists: take them and start fresh ones (no await here).
        bid_list, ask_list = sess.best_bid_list, sess.best_ask_list
        sess.best_bid_list, sess.best_ask_list = [], []

        bid_count = count_changes(bid_list)
        ask_count = count_changes(ask_list)
        ts = now_str()
        print(f"{ts} | best_bid_count = {bid_count} , best_ask_count = {ask_count}")

        sess.bid.add_count(bid_count)
        sess.ask.add_count(ask_count)

        events = []
        for tracker in (sess.bid, sess.ask):
            ev = tracker.pending_event()
            if ev:
                kind, values = ev
                events.append((tracker.name, kind, list(values)))
                tracker.mark_notified(kind)

        if events:
            title, body = build_notification(events, ts)
            await asyncio.to_thread(send_ntfy, title, body)


# ----------------------------------------------------------------------
# WebSocket receiver
# ----------------------------------------------------------------------
async def receiver(ws, sess: Session):
    async for raw in ws:
        msg = json.loads(raw)
        t = msg.get("type")
        if t == "snapshot":
            sess.load_snapshot(msg)
            sess.snapshot_event.set()
        elif t == "l2update":
            if sess.snapshot_event.is_set():
                sess.apply_update(msg)
        elif t == "error":
            raise RuntimeError(f"server error: {msg.get('message')} {msg.get('reason', '')}")
        # "subscriptions" and other types are ignored


async def run_once():
    sess = Session()
    async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=20,
                                  max_size=None) as ws:
        await ws.send(json.dumps({
            "type": "subscribe",
            "product_ids": [PRODUCT_ID],
            "channels": [CHANNEL],
        }))
        print(f"{now_str()} | connected, subscribed to {PRODUCT_ID} {CHANNEL}")

        tasks = [asyncio.create_task(receiver(ws, sess)),
                 asyncio.create_task(window_timer(sess))]
        try:
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)
            for d in done:
                d.result()  # re-raise
        finally:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)


async def main():
    while True:
        try:
            await run_once()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print(f"{now_str()} | disconnected / error: {e!r}")
        # Session state is discarded; a fresh one is built on reconnect.
        print(f"{now_str()} | reconnecting in {RECONNECT_DELAY}s ...")
        await asyncio.sleep(RECONNECT_DELAY)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nStopped.")
    except Exception as e:
        print(f"\nFatal error: {e!r}")
    # Keep the console window open so any message can be read after a double-click.
    try:
        input("\nPress Enter to close this window...")
    except Exception:
        pass
