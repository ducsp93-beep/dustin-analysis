#!/usr/bin/env python3
"""
Monitor BTC-USD price delta per trade on Coinbase's public WebSocket feed.

- Subscribes to the `matches` channel for BTC-USD (Coinbase Exchange feed).
- Groups trades into clock-aligned time windows (by the trade's own timestamp).
- The first partial window after (re)connecting is discarded.
- At the end of each window: delta_per_trade = (last_price - first_price) / trade_count
  If abs(delta_per_trade) >= SIGNIFICANT_DELTA, a notification is sent to ntfy.sh.

Requirements:  pip install websockets requests    (Python 3.9+)
Run:           python price_delta_monitor.py
"""

import asyncio
import json
import sys
from datetime import datetime, timedelta, timezone

import requests
import websockets

# ----------------------------------------------------------------------------
# Parameters
# ----------------------------------------------------------------------------
NOTIFICATION_URL = "https://ntfy.sh/price_delta_per_trade"
TIME_WINDOW = 1            # always in minutes
SIGNIFICANT_DELTA = 0.15   # threshold on abs(delta_per_trade)

WS_URL = "wss://ws-feed.exchange.coinbase.com"
PRODUCT_ID = "BTC-USD"

LOCAL_TZ = timezone(timedelta(hours=7))   # UTC+7
LOCAL_TZ_LABEL = "UTC+7"

BACKOFF_START = 1.0
BACKOFF_MAX = 60.0


# ----------------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------------
def parse_trade_time(value: str) -> datetime:
    """Parse Coinbase ISO timestamp like 2026-11-26T08:30:00.123456Z (UTC)."""
    value = value.replace("Z", "+00:00")
    # Trim fractional seconds to 6 digits (fromisoformat on older Pythons).
    if "." in value:
        head, rest = value.split(".", 1)
        frac, tz = rest[:-6], rest[-6:]          # tz part is e.g. "+00:00"
        value = f"{head}.{frac[:6]}{tz}"
    return datetime.fromisoformat(value).astimezone(timezone.utc)


def window_start_of(ts: datetime) -> datetime:
    """Clock-aligned start of the time window containing ts (UTC)."""
    ts = ts.replace(second=0, microsecond=0)
    return ts - timedelta(minutes=ts.minute % TIME_WINDOW)


def log(msg: str) -> None:
    now = datetime.now(LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{now}] {msg}", flush=True)


def send_notification(title: str, body: str) -> None:
    """Blocking POST to ntfy.sh. Errors are logged, never raised."""
    try:
        resp = requests.post(
            NOTIFICATION_URL,
            data=body.encode("utf-8"),
            headers={"Title": title},
            timeout=10,
        )
        resp.raise_for_status()
    except Exception as exc:  # noqa: BLE001
        log(f"Notification failed: {exc}")


# ----------------------------------------------------------------------------
# Window aggregation
# ----------------------------------------------------------------------------
class WindowAggregator:
    """Aggregates trades into windows and emits a summary when a window closes."""

    def __init__(self) -> None:
        self.last_trade_id = None   # kept across reconnects to skip duplicates
        self.reset()

    def reset(self) -> None:
        """Forget the in-progress window (called on every (re)connect)."""
        self.window_start = None
        self.first_price = None
        self.last_price = None
        self.count = 0
        self.partial = True   # first window after a connect is partial

    def add_trade(self, trade_id, ts: datetime, price: float):
        """
        Add a trade. Returns a summary dict if this trade closed a complete
        window, otherwise None.
        """
        if self.last_trade_id is not None and trade_id <= self.last_trade_id:
            return None   # duplicate / replayed trade
        self.last_trade_id = trade_id

        ws = window_start_of(ts)
        summary = None

        if self.window_start is None:
            self._start(ws, price)
            return None

        if ws < self.window_start:
            return None   # out-of-order trade from an already-closed window

        if ws > self.window_start:
            if not self.partial and self.count > 0:
                summary = self._summarize()
            self._start(ws, price)
            self.partial = False   # every window after the first is complete
            return summary

        # same window
        self.last_price = price
        self.count += 1
        return None

    def _start(self, ws: datetime, price: float) -> None:
        self.window_start = ws
        self.first_price = price
        self.last_price = price
        self.count = 1

    def _summarize(self) -> dict:
        delta = self.last_price - self.first_price
        return {
            "start": self.window_start,
            "count": self.count,
            "delta": delta,
            "delta_per_trade": delta / self.count,
        }


# ----------------------------------------------------------------------------
# Reporting
# ----------------------------------------------------------------------------
def format_local(ts: datetime, with_seconds: bool) -> str:
    local = ts.astimezone(LOCAL_TZ)
    fmt = "%H:%M:%S" if with_seconds else "%H:%M"
    return f"{local:%Y-%m-%d} ({LOCAL_TZ_LABEL}) {local.strftime(fmt)}"


async def handle_summary(s: dict) -> None:
    dpt = s["delta_per_trade"]
    notify = abs(dpt) >= SIGNIFICANT_DELTA

    line = (
        f"{format_local(s['start'], False)} "
        f"Trade = {s['count']}, Delta = {s['delta']:+.2f}, "
        f"Delta per trade = {dpt:+.4f}"
    )
    if notify:
        line += "  [NOTIFIED]"
    print(line, flush=True)

    if notify:
        title = f"Price delta per trade = {dpt:+.4f}"
        body = (
            f"Candle started at {format_local(s['start'], True)}\n"
            f"Trade count = {s['count']}\n"
            f"Delta = {s['delta']:+.2f}"
        )
        await asyncio.to_thread(send_notification, title, body)


# ----------------------------------------------------------------------------
# WebSocket loop
# ----------------------------------------------------------------------------
async def run_session(agg: WindowAggregator) -> None:
    subscribe = {
        "type": "subscribe",
        "product_ids": [PRODUCT_ID],
        "channels": ["matches"],
    }
    async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=20) as ws:
        await ws.send(json.dumps(subscribe))
        agg.reset()   # first window of this session is partial -> discarded
        log(f"Connected, subscribed to {PRODUCT_ID} matches")

        async for raw in ws:
            msg = json.loads(raw)
            mtype = msg.get("type")

            if mtype == "error":
                raise RuntimeError(f"Feed error: {msg.get('message')} {msg.get('reason', '')}")
            if mtype != "match":   # ignore subscriptions, last_match snapshot, heartbeats
                continue

            summary = agg.add_trade(
                trade_id=int(msg["trade_id"]),
                ts=parse_trade_time(msg["time"]),
                price=float(msg["price"]),
            )
            if summary:
                await handle_summary(summary)


async def main() -> None:
    agg = WindowAggregator()
    backoff = BACKOFF_START
    log(
        f"Monitoring {PRODUCT_ID}: window={TIME_WINDOW} min, "
        f"threshold={SIGNIFICANT_DELTA}, notify -> {NOTIFICATION_URL}"
    )
    while True:
        try:
            await run_session(agg)
            backoff = BACKOFF_START   # clean close; reconnect promptly
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            log(f"Connection problem: {exc!r}. Reconnecting in {backoff:.0f}s")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, BACKOFF_MAX)
            continue
        # Server closed the connection normally; the window in progress is
        # discarded by agg.reset() on the next connect.
        log("Connection closed. Reconnecting...")
        await asyncio.sleep(backoff)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nStopped.", file=sys.stderr)
