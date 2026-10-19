#!/usr/bin/env python3
"""
BTC-USD price continuity monitor.

Connects to Coinbase's public WebSocket feed, subscribes to the `matches`
channel and tracks consecutive higher highs (price_rising) and consecutive
lower lows (price_falling) over fixed wall-clock candles. When a run of at
least `CANDLE_COUNT` entries ends, a notification is sent to ntfy.

Requires: Python 3.9+, `pip install websockets requests`
"""

import asyncio
import json
import re
from datetime import datetime, timedelta, timezone

import requests
import websockets

# ----------------------------------------------------------------------------
# Parameters
# ----------------------------------------------------------------------------
WS_URL = "wss://ws-feed.exchange.coinbase.com"
PRODUCT_ID = "BTC-USD"
NOTIFICATION_URL = "https://ntfy.sh/price_continuity"
CANDLE_DURATION = 6      # seconds
CANDLE_COUNT = 8        # minimum list length for a notification
RECONNECT_DELAY = 5      # seconds
UTC7 = timezone(timedelta(hours=7))


# ----------------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------------
def parse_time(ts: str) -> datetime:
    """Parse Coinbase ISO timestamps like 2026-10-19T02:11:22.123456Z."""
    ts = ts.replace("Z", "+00:00")
    # Normalise fractional seconds to exactly 6 digits for fromisoformat.
    ts = re.sub(
        r"\.(\d+)",
        lambda m: "." + (m.group(1) + "000000")[:6],
        ts,
    )
    return datetime.fromisoformat(ts)


def send_ntfy(title: str, body: str) -> None:
    """Blocking POST to ntfy (called from a worker thread)."""
    try:
        r = requests.post(
            NOTIFICATION_URL,
            data=body.encode("utf-8"),
            headers={"Title": title},
            timeout=10,
        )
        r.raise_for_status()
        print(f"[notify] sent: {title}")
    except Exception as exc:  # noqa: BLE001
        print(f"[notify] FAILED: {exc}")


async def notify(name: str, run_length: int, price: float, ts: datetime) -> None:
    total_seconds = CANDLE_DURATION * run_length
    title = f"{name} - in {total_seconds} seconds"
    local = ts.astimezone(UTC7).strftime("%Y-%m-%d (UTC+7) %H:%M:%S")
    body = f"Last matched price at ${price:,.2f}\n{local}"
    print(f"[notify] {title} | {body.replace(chr(10), ' | ')}")
    await asyncio.to_thread(send_ntfy, title, body)


# ----------------------------------------------------------------------------
# State / candle logic
# ----------------------------------------------------------------------------
class Monitor:
    """All state lives here; a fresh instance == a fresh run."""

    def __init__(self) -> None:
        self.price_rising: list = []
        self.price_falling: list = []
        self.previous_max = None
        self.previous_min = None
        # current (in-progress) candle
        self.candle_id = None
        self.cur_max = None
        self.cur_min = None
        self.last_price = None
        self.last_time = None

    async def on_trade(self, price: float, ts: datetime) -> None:
        cid = int(ts.timestamp() // CANDLE_DURATION)

        # A trade from a later window closes the current candle.
        # (Late/out-of-order trades are folded into the current candle.)
        if self.candle_id is not None and cid > self.candle_id:
            await self.close_candle()
            self.candle_id = None

        if self.candle_id is None:
            self.candle_id = cid
            self.cur_max = price
            self.cur_min = price
        else:
            self.cur_max = max(self.cur_max, price)
            self.cur_min = min(self.cur_min, price)

        self.last_price = price
        self.last_time = ts

    async def close_candle(self) -> None:
        c_max, c_min = self.cur_max, self.cur_min
        price, ts = self.last_price, self.last_time

        if self.previous_max is None:
            # First candle of the run: seed both lists.
            self.price_rising = [c_max]
            self.price_falling = [c_min]
        else:
            # ---- rising side ----
            if c_max > self.previous_max:
                self.price_rising.append(c_max)
            else:
                if len(self.price_rising) >= CANDLE_COUNT:
                    await notify("Price rising", len(self.price_rising), price, ts)
                self.price_rising = [c_max]

            # ---- falling side ----
            if c_min < self.previous_min:
                self.price_falling.append(c_min)
            else:
                if len(self.price_falling) >= CANDLE_COUNT:
                    await notify("Price falling", len(self.price_falling), price, ts)
                self.price_falling = [c_min]

        # Update for the next candle comparison.
        self.previous_max = c_max
        self.previous_min = c_min


# ----------------------------------------------------------------------------
# WebSocket loop
# ----------------------------------------------------------------------------
async def run_once() -> None:
    monitor = Monitor()  # fresh state on every (re)connect
    async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=20) as ws:
        await ws.send(
            json.dumps(
                {
                    "type": "subscribe",
                    "product_ids": [PRODUCT_ID],
                    "channels": ["matches"],
                }
            )
        )
        print(f"[ws] connected, subscribed to {PRODUCT_ID} matches")

        async for raw in ws:
            msg = json.loads(raw)
            mtype = msg.get("type")
            if mtype == "match":
                await monitor.on_trade(float(msg["price"]), parse_time(msg["time"]))
            elif mtype == "error":
                raise RuntimeError(f"feed error: {msg}")
            # "subscriptions" / "last_match" / others are ignored


async def main() -> None:
    while True:
        try:
            await run_once()
            print("[ws] connection closed")
        except Exception as exc:  # noqa: BLE001
            print(f"[ws] error: {exc!r}")
        print(f"[ws] reconnecting in {RECONNECT_DELAY}s (state reset)")
        await asyncio.sleep(RECONNECT_DELAY)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nStopped.")
