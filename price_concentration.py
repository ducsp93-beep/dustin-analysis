#!/usr/bin/env python3
"""
BTC-USD price concentration monitor (Coinbase public WebSocket, `matches` channel).

Install:  pip install websockets requests
Run:      python btc_concentration_monitor.py
"""

import asyncio
import json
import statistics
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import requests
import websockets

# ----------------------------- Parameters ----------------------------------
NOTIFICATION_URL = "https://ntfy.sh/price_concentration"
CANDLE_DURATION = 180        # seconds per window (clock-aligned)
CONCENTRATION_N = 5         # min number of windows to count as a concentration
DEBUG = True                # print a line for each completed window

WS_URL = "wss://ws-feed.exchange.coinbase.com"
PRODUCT_ID = "BTC-USD"
RECONNECT_DELAY = 5         # seconds
UTC7 = timezone(timedelta(hours=7))


# ----------------------------- Helpers -------------------------------------
def log(msg: str) -> None:
    print(f"[{datetime.now(UTC7):%Y-%m-%d %H:%M:%S}] {msg}", flush=True)


def parse_time(s: str) -> datetime:
    """Parse Coinbase ISO timestamp like 2026-10-15T06:09:46.123456Z (UTC)."""
    s = s.rstrip("Z")
    if "." in s:
        base, frac = s.split(".", 1)
        s = f"{base}.{frac[:6].ljust(6, '0')}"
    return datetime.fromisoformat(s).replace(tzinfo=timezone.utc)


def whole_price(price_str: str) -> int:
    """'82732.98' -> 82732 (round down, 0 decimals, no float error)."""
    return int(price_str.split(".")[0])


@dataclass
class Record:
    low: int
    high: int
    median: float           # raw median (may be x.5); floored only for display
    last_trade_time: datetime


def send_notification(title: str, body: str) -> None:
    try:
        r = requests.post(
            NOTIFICATION_URL,
            data=body.encode("utf-8"),
            headers={"Title": title},
            timeout=10,
        )
        r.raise_for_status()
        log(f"Notification sent: {title}")
    except Exception as e:  # never let a notification failure kill the monitor
        log(f"Notification FAILED: {e}")


# ----------------------------- Core state ----------------------------------
class ConcentrationMonitor:
    def __init__(self) -> None:
        self.reset()

    def reset(self) -> None:
        self.window_id = None            # current clock-aligned window index
        self.prices: list[int] = []      # whole-number prices in current window
        self.last_trade_time = None      # time of latest trade in current window
        self.records: list[Record] = [] # current concentration list

    # --- called for every `match` message ---
    def on_trade(self, price: int, trade_time: datetime, notify) -> None:
        wid = int(trade_time.timestamp() // CANDLE_DURATION)

        if self.window_id is None:
            self.window_id = wid

        if wid > self.window_id:
            # The previous window is complete -> finalize it.
            # Windows with no trades are simply skipped (concentration continues).
            self._close_window(notify)
            self.window_id = wid
            self.prices = []
            self.last_trade_time = None

        # (Late/out-of-order trade from an older window is folded into the current one.)
        self.prices.append(price)
        if self.last_trade_time is None or trade_time > self.last_trade_time:
            self.last_trade_time = trade_time

    # --- finalize a window into a record and update the concentration list ---
    def _close_window(self, notify) -> None:
        if not self.prices:
            return
        new = Record(
            low=min(self.prices),
            high=max(self.prices),
            median=statistics.median(self.prices),
            last_trade_time=self.last_trade_time,
        )

        if DEBUG:
            start = datetime.fromtimestamp(self.window_id * CANDLE_DURATION, UTC7)
            log(
                f"Window {start:%H:%M:%S} | trades={len(self.prices)} "
                f"min={new.low} max={new.high} median={int(new.median)}"
            )

        if not self.records:
            self.records.append(new)
        else:
            prev = self.records[-1]
            joins = (prev.low <= new.median <= prev.high) and (
                new.low <= prev.median <= new.high
            )
            if joins:
                self.records.append(new)
            else:
                # Concentration ended.
                if len(self.records) >= CONCENTRATION_N:
                    self._notify(notify)
                self.records = [new]  # restart with the latest record

        if DEBUG:
            log(f"  concentration count = {len(self.records)}")

    def _notify(self, notify) -> None:
        count = len(self.records)
        last = self.records[-1]
        duration = CANDLE_DURATION * count
        ts = last.last_trade_time.astimezone(UTC7)

        title = f"Concentration lasted {duration} seconds"
        body = (
            f"Median price is ${int(last.median):,}\n"
            f"{ts:%Y-%m-%d} (UTC+7) {ts:%H:%M:%S}"
        )
        notify(title, body)


# ----------------------------- WebSocket loop ------------------------------
async def run_once(monitor: ConcentrationMonitor) -> None:
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
        log(f"Connected, subscribed to matches for {PRODUCT_ID}")

        def notify(title: str, body: str) -> None:
            # Run the blocking HTTP call in a thread so the feed isn't stalled.
            asyncio.get_running_loop().run_in_executor(
                None, send_notification, title, body
            )

        async for raw in ws:
            msg = json.loads(raw)
            mtype = msg.get("type")

            if mtype == "match":
                try:
                    monitor.on_trade(
                        whole_price(msg["price"]), parse_time(msg["time"]), notify
                    )
                except (KeyError, ValueError) as e:
                    log(f"Skipping malformed match message: {e}")
            elif mtype == "error":
                raise RuntimeError(f"Feed error: {msg}")
            # 'last_match' (snapshot) and 'subscriptions' are intentionally ignored


async def main() -> None:
    monitor = ConcentrationMonitor()
    while True:
        try:
            await run_once(monitor)
            log("Connection closed.")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log(f"Disconnected / error: {e!r}")
        # Reset everything like a new run, then reconnect.
        monitor.reset()
        log(f"Reconnecting in {RECONNECT_DELAY}s ...")
        await asyncio.sleep(RECONNECT_DELAY)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log("Stopped.")
