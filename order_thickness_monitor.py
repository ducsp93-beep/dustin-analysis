#!/usr/bin/env python3
"""
BTC-USD order thickness monitor (Coinbase Exchange public WebSocket, level2_batch).

- Ignores the initial snapshot, only uses l2update messages.
- For each update change [side, price, size]: amount = price * size
  (a removed level, size 0, contributes 0). Bid = "buy", Ask = "sell".
- Amounts are summed per time window, separately for bid and ask.
- Four independent run-lists (rise_bid, rise_ask, fall_bid, fall_ask) are tracked
  from window to window; a notification is sent at the end of every window in which
  a list has reached its N.

Install:  pip install websockets requests
Run:      python order_thickness_monitor.py
"""

import asyncio
import json
import time
from datetime import datetime, timedelta, timezone

import requests
import websockets

# ----------------------------- Parameters ------------------------------------
PRODUCT_ID = "BTC-USD"
WS_URL = "wss://ws-feed.exchange.coinbase.com"
NOTIFICATION_URL = "https://ntfy.sh/order_thickness"

TIME_WINDOW = 60   # seconds
RISE_N = 5
FALL_N = 5

RECONNECT_DELAY = 5  # seconds
# -----------------------------------------------------------------------------

UTC7 = timezone(timedelta(hours=7))


# ------------------------------ Formatting -----------------------------------
def fmt_duration(total_seconds: int) -> str:
    """e.g. 3750 -> '1 hr 2 min 30 sec'; zero parts are trimmed."""
    total_seconds = int(total_seconds)
    hours, rem = divmod(total_seconds, 3600)
    minutes, seconds = divmod(rem, 60)
    parts = []
    if hours:
        parts.append(f"{hours} hr")
    if minutes:
        parts.append(f"{minutes} min")
    if seconds:
        parts.append(f"{seconds} sec")
    return " ".join(parts) if parts else "0 sec"


def fmt_time(dt: datetime) -> str:
    return dt.astimezone(UTC7).strftime("%Y-%m-%d (UTC+7) %H:%M:%S")


# ----------------------------- Notification ----------------------------------
def _post_notification(title: str, body: str) -> None:
    try:
        r = requests.post(
            NOTIFICATION_URL,
            data=body.encode("utf-8"),
            headers={"Title": title},
            timeout=10,
        )
        if r.status_code >= 300:
            print(f"  ! ntfy returned {r.status_code}: {r.text[:200]}")
    except Exception as exc:  # noqa: BLE001
        print(f"  ! ntfy error: {exc}")


async def notify(title: str, body: str) -> None:
    await asyncio.to_thread(_post_notification, title, body)


# -------------------------------- State --------------------------------------
class State:
    def __init__(self) -> None:
        self.reset()

    def reset(self) -> None:
        self.window_start_set = False
        self.deadline = 0.0           # monotonic time when current window ends
        self.bid_sum = 0.0
        self.ask_sum = 0.0
        self.last_msg_time = None     # last message time within the current window
        self.last_msg_time_overall = None
        self.window_count = 0
        self.rise_bid: list[float] = []
        self.rise_ask: list[float] = []
        self.fall_bid: list[float] = []
        self.fall_ask: list[float] = []


def update_rise(lst: list, value: float) -> None:
    if lst and value < lst[-1]:
        lst.clear()
    lst.append(value)


def update_fall(lst: list, value: float) -> None:
    if lst and value > lst[-1]:
        lst.clear()
    lst.append(value)


async def close_window(st: State) -> None:
    """Finish the current window: print, update lists, send notifications."""
    bid, ask = st.bid_sum, st.ask_sum
    last_time = st.last_msg_time or st.last_msg_time_overall or datetime.now(UTC7)
    st.window_count += 1

    update_rise(st.rise_bid, bid)
    update_rise(st.rise_ask, ask)
    update_fall(st.fall_bid, bid)
    update_fall(st.fall_ask, ask)

    print(
        f"[{last_time.astimezone(UTC7).strftime('%Y-%m-%d %H:%M:%S')}] "
        f"window #{st.window_count}  bid={bid:,.2f}  ask={ask:,.2f} | "
        f"rise_bid={len(st.rise_bid)} rise_ask={len(st.rise_ask)} "
        f"fall_bid={len(st.fall_bid)} fall_ask={len(st.fall_ask)}",
        flush=True,
    )

    checks = [
        ("Thicker bid", st.rise_bid, RISE_N),
        ("Thicker ask", st.rise_ask, RISE_N),
        ("Thinner bid", st.fall_bid, FALL_N),
        ("Thinner ask", st.fall_ask, FALL_N),
    ]
    tasks = []
    for name, lst, n in checks:
        if len(lst) >= n:
            title = f"{name} - after {fmt_duration(len(lst) * TIME_WINDOW)}"
            body = (
                f"Last received at {fmt_time(last_time)}\n"
                f"Run for {len(lst)} windows of {fmt_duration(TIME_WINDOW)}"
            )
            print(f"  -> notify: {title}", flush=True)
            tasks.append(notify(title, body))
    if tasks:
        await asyncio.gather(*tasks)

    # Start the next window
    st.bid_sum = 0.0
    st.ask_sum = 0.0
    st.last_msg_time = None
    st.deadline += TIME_WINDOW


async def roll_windows(st: State) -> None:
    """Close every window whose deadline has passed (empty windows count as 0)."""
    if not st.window_start_set:
        return
    while time.monotonic() >= st.deadline:
        await close_window(st)


# ------------------------------- Main loop -----------------------------------
async def run_once(st: State) -> None:
    async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=20) as ws:
        await ws.send(
            json.dumps(
                {
                    "type": "subscribe",
                    "product_ids": [PRODUCT_ID],
                    "channels": ["level2_batch"],
                }
            )
        )
        print(f"Connected. Subscribed to level2_batch for {PRODUCT_ID}.", flush=True)

        while True:
            if st.window_start_set:
                timeout = max(0.0, st.deadline - time.monotonic())
            else:
                timeout = None

            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=timeout)
            except asyncio.TimeoutError:
                await roll_windows(st)
                continue

            # A message arriving after the deadline belongs to the next window
            await roll_windows(st)

            msg = json.loads(raw)
            mtype = msg.get("type")

            if mtype == "error":
                print(f"Server error: {msg}", flush=True)
                continue
            if mtype != "l2update":
                continue  # ignore snapshot, subscriptions, etc.

            now_local = datetime.now(UTC7)
            if not st.window_start_set:
                # Window starts at the first l2update after connecting
                st.window_start_set = True
                st.deadline = time.monotonic() + TIME_WINDOW
                print(
                    f"First update received, window starts at {fmt_time(now_local)}",
                    flush=True,
                )

            for side, price, size in msg.get("changes", []):
                amount = float(price) * float(size)
                if side == "buy":
                    st.bid_sum += amount
                elif side == "sell":
                    st.ask_sum += amount

            st.last_msg_time = now_local
            st.last_msg_time_overall = now_local


async def main() -> None:
    st = State()
    while True:
        st.reset()  # any (re)connect starts from scratch
        try:
            await run_once(st)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            print(f"Disconnected ({type(exc).__name__}: {exc}). Resetting state.", flush=True)
        st.reset()
        print(f"Reconnecting in {RECONNECT_DELAY}s...", flush=True)
        await asyncio.sleep(RECONNECT_DELAY)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nStopped.")
