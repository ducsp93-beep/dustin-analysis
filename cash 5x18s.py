#!/usr/bin/env python3
"""
BTC-USD traded cash monitor.

Connects to Coinbase's public WebSocket feed (matches channel), sums
price * size per time window, tracks rising / falling streaks and sends
notifications to ntfy.sh.

Install:  pip install websockets requests
Run:      python btc_cash_monitor.py
"""

import asyncio
import json
import re
import time
from datetime import datetime, timedelta, timezone

import requests
import websockets

# ----------------------------------------------------------------------
# Parameters
# ----------------------------------------------------------------------
NOTIFICATION_URL = "https://ntfy.sh/traded_cash"
TIME_WINDOW = 18      # seconds (any positive number, e.g. 18, 36)
RISE_N = 6            # min queue length (windows) to notify rising
FALL_N = 5            # min queue length (windows) to notify falling

WS_URL = "wss://ws-feed.exchange.coinbase.com"
PRODUCT_ID = "BTC-USD"
RECONNECT_DELAY = 5   # seconds
TZ_LOCAL = timezone(timedelta(hours=7))  # UTC+7


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------
def plural(value: int, unit: str) -> str:
    return f"{value} {unit}" + ("" if value == 1 else "s")


def format_duration(total_seconds: float) -> str:
    """90 -> '1 minute 30 seconds', 45 -> '45 seconds', 3725 -> '1 hour 2 minutes 5 seconds'."""
    total = int(round(total_seconds))
    h, rem = divmod(total, 3600)
    m, s = divmod(rem, 60)
    parts = []
    if h:
        parts.append(plural(h, "hour"))
    if m:
        parts.append(plural(m, "minute"))
    if s or not parts:
        parts.append(plural(s, "second"))
    return " ".join(parts)


def parse_coinbase_time(ts: str) -> datetime:
    """Parse e.g. '2026-11-13T03:29:59.123456Z' into an aware UTC datetime."""
    ts = ts.replace("Z", "+00:00")
    # Trim fractional seconds to 6 digits for fromisoformat compatibility
    ts = re.sub(r"(\.\d{6})\d+", r"\1", ts)
    ts = re.sub(r"\.(\d{1,5})(?=[+-])", lambda m: "." + m.group(1).ljust(6, "0"), ts)
    return datetime.fromisoformat(ts)


def format_local(dt: datetime) -> str:
    return dt.astimezone(TZ_LOCAL).strftime("%Y-%m-%d (UTC+7) %H:%M:%S")


def log(msg: str) -> None:
    now = datetime.now(TZ_LOCAL).strftime("%H:%M:%S")
    print(f"[{now}] {msg}", flush=True)


def _post_notification(title: str, body: str) -> None:
    try:
        r = requests.post(
            NOTIFICATION_URL,
            data=body.encode("utf-8"),
            headers={"Title": title},
            timeout=10,
        )
        r.raise_for_status()
    except Exception as exc:  # never let a notification failure kill the monitor
        log(f"Notification failed: {exc}")


async def send_notification(title: str, body: str) -> None:
    log(f"NOTIFY | {title} | {body.replace(chr(10), ' | ')}")
    await asyncio.to_thread(_post_notification, title, body)


# ----------------------------------------------------------------------
# Monitor state
# ----------------------------------------------------------------------
class Monitor:
    def __init__(self) -> None:
        self.reset()

    def reset(self) -> None:
        self.window_total = 0.0
        self.window_last_trade_time = None   # datetime (UTC) of last trade in window
        self.prev_amount = None
        self.rise_queue = []
        self.fall_queue = []

    def add_trade(self, price: float, size: float, trade_time: datetime) -> None:
        self.window_total += price * size
        self.window_last_trade_time = trade_time

    async def close_window(self) -> None:
        """Called at the end of every time window."""
        amount = self.window_total
        last_time = self.window_last_trade_time or datetime.now(timezone.utc)

        # Start a fresh accumulator immediately
        self.window_total = 0.0
        self.window_last_trade_time = None

        if self.prev_amount is None:
            # First window: baseline, seeds both queues
            self.rise_queue = [amount]
            self.fall_queue = [amount]
        else:
            if amount >= self.prev_amount:
                self.rise_queue.append(amount)
            else:
                self.rise_queue = [amount]

            if amount <= self.prev_amount:
                self.fall_queue.append(amount)
            else:
                self.fall_queue = [amount]
        self.prev_amount = amount

        log(
            f"window total ${amount:,.0f} | "
            f"rise_queue={len(self.rise_queue)} fall_queue={len(self.fall_queue)}"
        )

        body = (
            f"${amount:,.0f} in last {format_duration(TIME_WINDOW)}\n"
            f"{format_local(last_time)}"
        )

        if len(self.rise_queue) >= RISE_N:
            duration = format_duration(len(self.rise_queue) * TIME_WINDOW)
            await send_notification(f"Rising cashflow - in {duration}", body)

        if len(self.fall_queue) >= FALL_N:
            duration = format_duration(len(self.fall_queue) * TIME_WINDOW)
            await send_notification(f"Falling cashflow - in {duration}", body)


# ----------------------------------------------------------------------
# Window clock
# ----------------------------------------------------------------------
async def window_clock(monitor: Monitor) -> None:
    """Fires every TIME_WINDOW seconds, counted from when it was started (no drift)."""
    next_deadline = time.monotonic() + TIME_WINDOW
    while True:
        await asyncio.sleep(max(0.0, next_deadline - time.monotonic()))
        await monitor.close_window()
        next_deadline += TIME_WINDOW


# ----------------------------------------------------------------------
# WebSocket session
# ----------------------------------------------------------------------
async def run_session(monitor: Monitor) -> None:
    clock_task = None
    try:
        async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=20) as ws:
            log(f"Connected to {WS_URL}")
            await ws.send(json.dumps({
                "type": "subscribe",
                "product_ids": [PRODUCT_ID],
                "channels": ["matches"],
            }))

            async for raw in ws:
                msg = json.loads(raw)
                mtype = msg.get("type")

                if mtype == "subscriptions":
                    # Subscription confirmed: the wall clock starts now
                    log(f"Subscribed to matches/{PRODUCT_ID}. "
                        f"Window clock started ({TIME_WINDOW}s windows).")
                    clock_task = asyncio.create_task(window_clock(monitor))

                elif mtype == "match":
                    # Live trades only ('last_match' snapshot is ignored)
                    monitor.add_trade(
                        float(msg["price"]),
                        float(msg["size"]),
                        parse_coinbase_time(msg["time"]),
                    )

                elif mtype == "error":
                    raise RuntimeError(f"Feed error: {msg.get('message')} {msg.get('reason', '')}")

            raise ConnectionError("WebSocket closed")
    finally:
        if clock_task:
            clock_task.cancel()
            try:
                await clock_task
            except asyncio.CancelledError:
                pass


async def main() -> None:
    monitor = Monitor()
    while True:
        try:
            await run_session(monitor)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log(f"Disconnected / error: {exc!r}")
        monitor.reset()
        log(f"State reset. Reconnecting in {RECONNECT_DELAY}s...")
        await asyncio.sleep(RECONNECT_DELAY)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log("Stopped by user.")
