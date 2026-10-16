#!/usr/bin/env python3
"""
Monitor the BTC-USD match rate on Coinbase's public WebSocket feed and send
ntfy notifications when a 10-second window is unusually quiet or busy.

Install:  pip install websockets aiohttp
Run:      python coinbase_btc_monitor.py
"""

import asyncio
import json
import math
import time
from datetime import datetime, timedelta, timezone

import aiohttp
import websockets

# ----------------------------- Parameters ----------------------------------
MIN_RATE = 2          # count <= MIN_RATE  -> "Lowest activities" alert
MAX_RATE = 150        # count >= MAX_RATE  -> "Highest activities" alert
NOTIFICATION_URL = "https://ntfy.sh/trade_count_mb"

WS_URL = "wss://ws-feed.exchange.coinbase.com"
PRODUCT_ID = "BTC-USD"
WINDOW_SECONDS = 12
BACKOFF_START = 1     # seconds
BACKOFF_MAX = 30      # seconds
# ---------------------------------------------------------------------------

TZ_UTC7 = timezone(timedelta(hours=7))


def log(msg: str) -> None:
    print(f"{datetime.now(TZ_UTC7):%Y-%m-%d %H:%M:%S} (UTC+7) | {msg}", flush=True)


def fmt_trade_time(iso_str: str) -> str:
    """Coinbase ISO timestamp (UTC) -> '2026-10-14 (UTC+7) 11:04:37'."""
    dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
    return dt.astimezone(TZ_UTC7).strftime("%Y-%m-%d (UTC+7) %H:%M:%S")


def hms(epoch: float) -> str:
    return datetime.fromtimestamp(epoch, TZ_UTC7).strftime("%H:%M:%S")


class Monitor:
    def __init__(self, http: aiohttp.ClientSession):
        self.http = http
        self.backoff = BACKOFF_START
        # Most recent trade seen since startup: (trade_id, side, price, time)
        self.last_trade = None
        self._tasks = set()

    # ------------------------------ ntfy ------------------------------------
    async def _post_ntfy(self, title: str, body: str) -> None:
        try:
            async with self.http.post(
                NOTIFICATION_URL,
                data=body.encode("utf-8"),
                headers={"Title": title},
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status >= 300:
                    log(f"ntfy POST failed: HTTP {resp.status}")
                else:
                    log(f"Notification sent: {title}")
        except Exception as e:  # log and continue, no retry
            log(f"ntfy POST failed: {e!r}")

    def send_ntfy(self, title: str, body: str) -> None:
        """Fire-and-forget so a slow POST never blocks the feed loop."""
        task = asyncio.create_task(self._post_ntfy(title, body))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    # --------------------------- window handling ----------------------------
    def evaluate_window(self, window_start: float, trades: dict) -> None:
        count = len(trades)
        label = f"{hms(window_start)}-{hms(window_start + WINDOW_SECONDS)}"

        if count <= MIN_RATE:
            kind = "Lowest"
        elif count >= MAX_RATE:
            kind = "Highest"
        else:
            log(f"Window {label} | {count} trades | no alert")
            return

        # Last trade = highest trade_id in the window, else last known trade.
        if trades:
            last = trades[max(trades)]
        else:
            last = self.last_trade

        if last is None:
            log(f"Window {label} | {count} trades | {kind.lower()} but no trade "
                f"seen since startup -> nothing sent")
            return

        _, side, price, ts = last
        title = f"{kind} activities - {count} {'trade' if count == 1 else 'trades'}"
        body = f"Resting {side} at ${price:,.2f}\n{fmt_trade_time(ts)}"
        log(f"Window {label} | {count} trades | ALERT: {title}")
        self.send_ntfy(title, body)

    # ------------------------------ session ---------------------------------
    async def run_session(self, ws) -> None:
        await ws.send(json.dumps({
            "type": "subscribe",
            "product_ids": [PRODUCT_ID],
            "channels": ["matches"],
        }))
        log(f"Connected, subscribed to matches for {PRODUCT_ID}")

        # Fresh state on every (re)connect: counts reset, and the partial
        # window we joined in the middle of is discarded.
        trades = {}
        now = time.time()
        window_end = (math.floor(now / WINDOW_SECONDS) + 1) * WINDOW_SECONDS
        window_is_full = False
        got_first_message = False

        while True:
            timeout = window_end - time.time()

            if timeout <= 0:
                now = time.time()
                if window_is_full:
                    if now - window_end > 2:
                        log("Event loop was delayed; discarding window")
                    else:
                        self.evaluate_window(window_end - WINDOW_SECONDS, trades)
                trades = {}
                if now - window_end > 2:
                    # Realign to the clock and skip the partial window
                    window_end = (math.floor(now / WINDOW_SECONDS) + 1) * WINDOW_SECONDS
                    window_is_full = False
                else:
                    window_end += WINDOW_SECONDS
                    window_is_full = True
                continue

            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=timeout)
            except asyncio.TimeoutError:
                continue  # window boundary reached

            if not got_first_message:
                got_first_message = True
                self.backoff = BACKOFF_START  # connection is healthy

            msg = json.loads(raw)
            mtype = msg.get("type")

            if mtype == "error":
                raise RuntimeError(f"Feed error: {msg.get('message')} {msg.get('reason', '')}")
            if mtype != "match":
                continue  # ignores 'subscriptions' and the 'last_match' snapshot
            if msg.get("product_id") != PRODUCT_ID:
                continue

            trade_id = int(msg["trade_id"])
            trade = (trade_id, msg["side"], float(msg["price"]), msg["time"])

            if self.last_trade is None or trade_id > self.last_trade[0]:
                self.last_trade = trade

            if window_is_full:
                trades[trade_id] = trade  # dict keys -> unique IDs only

    # ------------------------------ main loop -------------------------------
    async def run(self) -> None:
        while True:
            try:
                async with websockets.connect(WS_URL) as ws:
                    await self.run_session(ws)
            except Exception as e:
                log(f"Connection problem: {e!r}")
            log(f"Reconnecting in {self.backoff}s ...")
            await asyncio.sleep(self.backoff)
            self.backoff = min(self.backoff * 2, BACKOFF_MAX)


async def main() -> None:
    async with aiohttp.ClientSession() as http:
        await Monitor(http).run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nStopped.")
