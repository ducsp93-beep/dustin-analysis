#!/usr/bin/env python3
"""
BTC-USD match summary monitor (Coinbase public WebSocket, 'matches' channel).

Install:  pip install websockets requests
Run:      python match_summary_monitor.py
"""

import asyncio
import json
import shutil
import threading
import time
from datetime import datetime, timezone, timedelta

import requests
import websockets

# ----------------------------------------------------------------------
# Parameters
# ----------------------------------------------------------------------
NOTIFICATION_URL = "https://ntfy.sh/Match_summary"
TIME_WINDOW = 60          # seconds
RISE_N = 5
RISE_RATE = 0.10          # between 0 and 1
FALL_N = 5
FALL_RATE = 0.12          # between 0 and 1

WS_URL = "wss://ws-feed.exchange.coinbase.com"
PRODUCT_ID = "BTC-USD"
RECONNECT_DELAY = 5       # seconds
TIMER_TICK = 0.5          # seconds, local clock check / live console refresh
CLOSE_GRACE = 2.0         # seconds the local timer waits past a window end
                          # before closing it, so slightly delayed trades
                          # still land in the right window

UTC7 = timezone(timedelta(hours=7))

# Priority order (also used for tie-breaking)
COUNT_TYPES = ["trade_count", "order_count", "price_level_count", "size_count"]
COUNT_LABELS = {
    "trade_count": "trade count",
    "order_count": "order count",
    "price_level_count": "price level count",
    "size_count": "size count",
}


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------
def parse_time(s: str) -> float:
    """Parse Coinbase ISO time (e.g. 2026-11-18T09:17:21.123456Z) to epoch seconds."""
    s = s.rstrip("Z")
    if "." in s:
        main, frac = s.split(".")
        s = f"{main}.{frac[:6].ljust(6, '0')}"
    else:
        s += ".000000"
    dt = datetime.strptime(s, "%Y-%m-%dT%H:%M:%S.%f").replace(tzinfo=timezone.utc)
    return dt.timestamp()


def fmt_utc7(epoch: float) -> str:
    return datetime.fromtimestamp(epoch, UTC7).strftime("%Y-%m-%d %H:%M:%S")


def fmt_label(epoch: float) -> str:
    """'2026-11-21 (UTC+7) 21:28:05'"""
    return fmt_utc7(epoch).replace(" ", " (UTC+7) ", 1)


def fmt_counts(c: dict) -> str:
    return (
        f"trade {c['trade_count']}, order {c['order_count']}, "
        f"price_level {c['price_level_count']}, size {c['size_count']}"
    )


def fmt_duration(total_seconds: int) -> str:
    """Short form, skipping zero parts: '1 hr 6 min', '2 min 20 sec', '6 min 40 sec'."""
    total_seconds = int(round(total_seconds))
    h, rem = divmod(total_seconds, 3600)
    m, s = divmod(rem, 60)
    parts = []
    if h:
        parts.append(f"{h} hr")
    if m:
        parts.append(f"{m} min")
    if s or not parts:
        parts.append(f"{s} sec")
    return " ".join(parts)


def update_list(lst: list, new: int, side: str) -> list:
    """Apply the rise/fall list rule. Returns the updated list."""
    if not lst:
        return [new]
    prev = lst[-1]
    eps = 1e-9
    if side == "rise":
        # strictly greater and rate >= RISE_RATE; prev == 0 -> rate undefined -> reset
        if prev > 0 and new > prev and (new - prev) >= RISE_RATE * prev - eps:
            lst.append(new)
            return lst
    else:
        # strictly smaller and rate >= FALL_RATE; prev == 0 -> reset
        if prev > 0 and new < prev and (prev - new) >= FALL_RATE * prev - eps:
            lst.append(new)
            return lst
    return [new]


def send_ntfy(title: str, body: str) -> None:
    try:
        r = requests.post(
            NOTIFICATION_URL,
            data=body.encode("utf-8"),
            headers={"Title": title},
            timeout=10,
        )
        if r.status_code >= 300:
            print(f"\n[ntfy] HTTP {r.status_code}: {r.text[:100]}")
    except Exception as e:  # never let a notification failure kill the monitor
        print(f"\n[ntfy] send failed: {e}")


def notify(title: str, body: str) -> None:
    threading.Thread(target=send_ntfy, args=(title, body), daemon=True).start()


# ----------------------------------------------------------------------
# State
# ----------------------------------------------------------------------
class Monitor:
    def __init__(self):
        self.t0 = None             # epoch of first trade = start of window 0
        self.k = 0                 # number of windows already closed
        self.offset = None         # local_time - trade_time at first trade
        self.last_trade_ts = None  # epoch of last trade received
        self._live_len = 0         # width of the live line currently on screen
        self.lists = {
            (side, c): [] for side in ("rise", "fall") for c in COUNT_TYPES
        }
        self._reset_window_data()

    def _reset_window_data(self):
        self.trade_ids = set()
        self.order_ids = set()
        self.price_levels = set()
        self.sizes = set()

    # -- window geometry ------------------------------------------------
    @property
    def window_start(self):
        return self.t0 + self.k * TIME_WINDOW

    @property
    def window_end(self):
        return self.t0 + (self.k + 1) * TIME_WINDOW

    def current_counts(self) -> dict:
        return {
            "trade_count": len(self.trade_ids),
            "order_count": len(self.order_ids),
            "price_level_count": len(self.price_levels),
            "size_count": len(self.sizes),
        }

    # -- incoming trade -------------------------------------------------
    def on_trade(self, msg: dict):
        ts = parse_time(msg["time"])
        local_now = time.time()

        if self.t0 is None:
            self.t0 = ts
            self.offset = local_now - ts

        # Close any finished windows (including empty ones)
        while ts >= self.window_end:
            self.close_window()

        if ts < self.window_start:
            return  # late trade for a window already closed by the local timer

        trade_id = msg.get("trade_id")
        if trade_id in self.trade_ids:
            return  # duplicate

        self.trade_ids.add(trade_id)
        maker = msg.get("maker_order_id")
        if maker:
            self.order_ids.add(maker)
        self.price_levels.add(str(msg["price"]).split(".")[0])  # truncate to whole dollar
        self.sizes.add(str(msg["size"]))

        if self.last_trade_ts is None or ts > self.last_trade_ts:
            self.last_trade_ts = ts

    # -- local clock timer ---------------------------------------------
    def on_timer(self):
        if self.t0 is None:
            return
        feed_now = time.time() - self.offset
        while feed_now >= self.window_end + CLOSE_GRACE:
            self.close_window()

    # -- close window ---------------------------------------------------
    def close_window(self):
        counts = self.current_counts()
        w_start = self.window_start

        for side in ("rise", "fall"):
            for c in COUNT_TYPES:
                self.lists[(side, c)] = update_list(self.lists[(side, c)], counts[c], side)

        self._print_closed(w_start, counts)
        self._check_notifications()

        self.k += 1
        self._reset_window_data()

    def _check_notifications(self):
        for side, n in (("rise", RISE_N), ("fall", FALL_N)):
            best_type, best_len = None, 0
            for c in COUNT_TYPES:  # priority order -> first wins ties
                length = len(self.lists[(side, c)])
                if length >= n and length > best_len:
                    best_type, best_len = c, length
            if best_type is None:
                continue

            lst = self.lists[(side, best_type)]
            duration = fmt_duration(TIME_WINDOW * best_len)
            title = f"{side.capitalize()} in {COUNT_LABELS[best_type]} - for {duration}"
            last_ts = self.last_trade_ts if self.last_trade_ts is not None else self.window_end
            body = (
                f"Last trade at {fmt_label(last_ts)}\n"
                f"{side.capitalize()} across {best_len} windows of {fmt_duration(TIME_WINDOW)}\n"
                + " -> ".join(str(x) for x in lst)
            )
            self._clear_live()
            print(f"[NOTIFY] {title} | {' -> '.join(str(x) for x in lst)}")
            notify(title, body)

    # -- console --------------------------------------------------------
    def _clear_live(self):
        """Erase the live line (only as many characters as it actually used)."""
        if self._live_len:
            print("\r" + " " * self._live_len + "\r", end="")
            self._live_len = 0

    def _print_closed(self, w_start: float, counts: dict):
        # Replace the live line with the final line, then move to a new line
        self._clear_live()
        print(f"{fmt_label(w_start + TIME_WINDOW)} | counts = {fmt_counts(counts)}", flush=True)

    def print_live(self):
        if self.t0 is None:
            line = "Waiting for first trade..."
        else:
            line = f"{fmt_label(self.window_end)} | counts = {fmt_counts(self.current_counts())}"
        # Never write the last column of the console, otherwise it wraps and
        # '\r' can no longer return to the start of the line.
        cols = shutil.get_terminal_size((120, 30)).columns
        line = line[: max(cols - 1, 10)]
        out = line.ljust(self._live_len)  # blank out leftovers from a longer previous line
        print("\r" + out, end="", flush=True)
        self._live_len = len(out)


# ----------------------------------------------------------------------
# Connection loop
# ----------------------------------------------------------------------
async def timer_task(mon: Monitor):
    while True:
        await asyncio.sleep(TIMER_TICK)
        mon.on_timer()
        mon.print_live()


async def run_once():
    mon = Monitor()  # fresh state on every (re)connection
    timer = None
    try:
        async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=20) as ws:
            await ws.send(json.dumps({
                "type": "subscribe",
                "product_ids": [PRODUCT_ID],
                "channels": ["matches"],
            }))
            print(f"Connected to {WS_URL}, subscribed to matches for {PRODUCT_ID}")
            timer = asyncio.create_task(timer_task(mon))

            async for raw in ws:
                msg = json.loads(raw)
                mtype = msg.get("type")
                if mtype == "match":          # 'last_match' (snapshot) is ignored
                    mon.on_trade(msg)
                elif mtype == "error":
                    raise RuntimeError(f"Feed error: {msg}")
    finally:
        if timer:
            timer.cancel()


async def main():
    while True:
        try:
            await run_once()
            print("\nConnection closed.")
        except Exception as e:
            print(f"\nDisconnected / error: {e!r}")
        print(f"Resetting state, reconnecting in {RECONNECT_DELAY} seconds...")
        await asyncio.sleep(RECONNECT_DELAY)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nStopped.")
