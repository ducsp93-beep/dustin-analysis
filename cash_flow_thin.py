#!/usr/bin/env python3
"""
BTC-USD cash flow monitor (Coinbase public WebSocket, 'matches' channel).

- Aggregates cash amount (price * size) into fixed, clock-aligned time windows.
- Tracks a decrease streak and an increase streak across closed windows.
- Sends a ntfy notification on every closed window once a streak reaches its threshold.
- On any failure / disconnect, resets all state and reconnects.

Install:  pip install websockets requests
Run:      python cash_flow_monitor.py
"""

import asyncio
import json
from datetime import datetime, timedelta, timezone

import requests
import websockets

# ----------------------------- Parameters ----------------------------------
WS_URL = "wss://ws-feed.exchange.coinbase.com"
PRODUCT_ID = "BTC-USD"
NTFY_URL = "https://ntfy.sh/mobile_cash_thin"

TIME_WINDOW = 4   # seconds
DECREASE_N = 6     # min list length to notify on decreasing flow
INCREASE_N = 6     # min list length to notify on increasing flow

RECONNECT_DELAY = 5  # seconds
UTC7 = timezone(timedelta(hours=7))


# ------------------------------- State -------------------------------------
class State:
    """All mutable run state. A fresh instance == a brand new run."""

    def __init__(self):
        self.decrease_list: list[float] = []
        self.increase_list: list[float] = []
        self.current_window: int | None = None  # window index (epoch // TIME_WINDOW)
        self.current_sum: float = 0.0
        self.current_partial: bool = True       # first window after connect is partial
        self.last_trade_ts: datetime | None = None  # timestamp of latest trade seen


# ----------------------------- Notification --------------------------------
def _post_ntfy(title: str, body: str) -> None:
    try:
        r = requests.post(
            NTFY_URL,
            data=body.encode("utf-8"),
            headers={"Title": title},
            timeout=10,
        )
        r.raise_for_status()
    except Exception as e:
        print(f"[ntfy] failed to send '{title}': {e}")


async def notify(title: str, amount: float, ts: datetime) -> None:
    line1 = f"Last {TIME_WINDOW} seconds - ${amount:,.2f}"
    line2 = ts.astimezone(UTC7).strftime("%Y-%m-%d (UTC+7) %H:%M:%S")
    body = f"{line1}\n{line2}"
    print(f"[notify] {title} | {line1} | {line2}")
    # run blocking HTTP call off the event loop so the feed isn't stalled
    await asyncio.to_thread(_post_ntfy, title, body)


# ------------------------------- Analysis ----------------------------------
async def on_window_closed(state: State, amount: float, ts: datetime) -> None:
    """Append the closed window's amount to both lists and analyse each."""
    # ---- decrease list ----
    dl = state.decrease_list
    dl.append(amount)
    if len(dl) > 1:
        if dl[-1] < dl[-2]:
            if len(dl) >= DECREASE_N:
                duration = TIME_WINDOW * len(dl)
                await notify(f"Cash flow decreasing - in {duration} seconds", amount, ts)
        else:
            state.decrease_list = [amount]

    # ---- increase list ----
    il = state.increase_list
    il.append(amount)
    if len(il) > 1:
        if il[-1] > il[-2]:
            if len(il) >= INCREASE_N:
                duration = TIME_WINDOW * len(il)
                await notify(f"Cash flow increasing - in {duration} seconds", amount, ts)
        else:
            state.increase_list = [amount]

    print(
        f"[window] ${amount:,.2f} | decrease_list={len(state.decrease_list)} "
        f"increase_list={len(state.increase_list)}"
    )


# ---------------------------- Trade handling -------------------------------
def parse_time(s: str) -> datetime:
    # Coinbase format: 2026-10-15T06:09:46.123456Z
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


async def handle_match(state: State, msg: dict) -> None:
    ts = parse_time(msg["time"])
    amount = float(msg["price"]) * float(msg["size"])
    wid = int(ts.timestamp() // TIME_WINDOW)

    # First trade of this run: open a (partial) window, to be discarded on close.
    if state.current_window is None:
        state.current_window = wid
        state.current_sum = 0.0
        state.current_partial = True

    # Late / out-of-order trade belonging to an already-closed window: ignore.
    if wid < state.current_window:
        return

    # Trade belongs to a later window -> close current (and any empty ones in between).
    if wid > state.current_window:
        if not state.current_partial and state.last_trade_ts is not None:
            await on_window_closed(state, state.current_sum, state.last_trade_ts)

        # Empty windows between the closed one and this trade's window -> $0
        for _ in range(state.current_window + 1, wid):
            if state.last_trade_ts is not None:
                await on_window_closed(state, 0.0, state.last_trade_ts)

        state.current_window = wid
        state.current_sum = 0.0
        state.current_partial = False

    state.current_sum += amount
    state.last_trade_ts = ts


# ------------------------------ Main loop ----------------------------------
async def run_once() -> None:
    state = State()  # fresh state on every (re)connect
    subscribe = {
        "type": "subscribe",
        "product_ids": [PRODUCT_ID],
        "channels": ["matches"],
    }
    async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=20) as ws:
        await ws.send(json.dumps(subscribe))
        print(f"[ws] connected, subscribed to {PRODUCT_ID} matches")
        async for raw in ws:
            msg = json.loads(raw)
            mtype = msg.get("type")
            if mtype == "match":  # ignore 'last_match' snapshot, 'subscriptions', etc.
                await handle_match(state, msg)
            elif mtype == "error":
                raise RuntimeError(f"feed error: {msg}")


async def main() -> None:
    while True:
        try:
            await run_once()
            print("[ws] connection closed")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print(f"[ws] error: {e!r}")
        print(f"[ws] state reset, reconnecting in {RECONNECT_DELAY}s...")
        await asyncio.sleep(RECONNECT_DELAY)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nStopped.")
