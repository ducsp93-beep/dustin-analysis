"""
BTC-USD Coinbase "matches" stream analyzer.

Pipeline
--------
1. Connects to Coinbase Exchange public WebSocket feed, subscribes to the
   "matches" channel for BTC-USD.
2. Each incoming match is filed into 2 of 4 in-memory tables:
       taker_buy, taker_sell, maker_buy, maker_sell
3. Calculation 1 - "Aggressive order taker": when a taker_order_id streak
   ends (>= AGGRESSIVENESS_N consecutive trades), notify.
4. Calculation 2 - "Large resting order hit": when a maker_order_id streak
   ends (>= RESTING_N consecutive trades), notify.
5. Loop back to receiving data. Tables are naturally trimmed to only the
   current (latest) order id's rows -- see StreakTracker.
6. Notifications go to ntfy.sh, throttled to 1 per type per 5s unless the
   count is escalating.

Requirements:
    pip install websocket-client requests
"""

import json
import time
import threading
import logging
from datetime import datetime, timedelta, timezone

import requests
import websocket  # pip install websocket-client

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
COINBASE_WS_URL = "wss://ws-feed.exchange.coinbase.com"
PRODUCT_ID = "BTC-USD"
NTFY_URL = "https://ntfy.sh/btc-tick-stream-notif"

AGGRESSIVENESS_N = 48   # min consecutive trades on one taker order -> alert
RESTING_N = 68          # min trades matched on one maker (resting) order -> alert
NOTIFY_COOLDOWN_SEC = 5 # per message-type throttle window

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("btc_tick_stream")


# ---------------------------------------------------------------------------
# Notification throttling
# ---------------------------------------------------------------------------
class NotificationThrottle:
    """
    Only one notification per "type" (header) every NOTIFY_COOLDOWN_SEC seconds.
    Within that window, a new trigger for the same type is still sent if its
    count is higher than the last sent count (an "escalation" update).
    """

    def __init__(self, cooldown_sec=NOTIFY_COOLDOWN_SEC):
        self.cooldown_sec = cooldown_sec
        self._last_sent_time = {}
        self._last_sent_count = {}
        self._lock = threading.Lock()

    def should_send(self, msg_type: str, count: int) -> bool:
        now = time.time()
        with self._lock:
            last_time = self._last_sent_time.get(msg_type)
            last_count = self._last_sent_count.get(msg_type, -1)

            if last_time is None or (now - last_time) >= self.cooldown_sec:
                self._last_sent_time[msg_type] = now
                self._last_sent_count[msg_type] = count
                return True

            if count > last_count:
                self._last_sent_time[msg_type] = now
                self._last_sent_count[msg_type] = count
                return True

            return False


throttle = NotificationThrottle()

UTC7 = timezone(timedelta(hours=7))


def format_datetime_utc7(iso_ts: str):
    """
    Convert a Coinbase UTC timestamp (e.g. '2026-09-25T06:13:47.342815Z')
    into ('2026-09-25', '13:13:47') in UTC+7.
    """
    try:
        dt = datetime.fromisoformat(iso_ts.replace("Z", "+00:00"))
        dt_utc7 = dt.astimezone(UTC7)
        return dt_utc7.strftime("%Y-%m-%d"), dt_utc7.strftime("%H:%M:%S")
    except (ValueError, AttributeError, TypeError):
        return str(iso_ts), ""


def format_price(price: float) -> str:
    return f"${price:,.2f}"


def send_ntfy(title: str, message: str):
    headers = {"Title": title}
    try:
        requests.post(NTFY_URL, data=message.encode("utf-8"), headers=headers, timeout=5)
        log.info(f"Notification sent: [{title}] {message.splitlines()[0]}")
    except requests.RequestException as e:
        log.error(f"Failed to send ntfy notification: {e}")


# ---------------------------------------------------------------------------
# Streak tracker (one per table)
# ---------------------------------------------------------------------------
class StreakTracker:
    """
    Tracks the current run of consecutive trades sharing the same order_id
    for one of the 4 tables (taker_buy, taker_sell, maker_buy, maker_sell).

    Keeps only the rows belonging to the still-active order_id -- this is
    equivalent to "trim the table to the latest order id" but avoids
    unbounded growth / an explicit separate trim step.
    """

    def __init__(self, name):
        self.name = name
        self.order_id = None
        self.rows = []

    def add(self, row, order_id_field):
        """
        Add a new row keyed on order_id_field ('taker_order_id' or
        'maker_order_id'). Returns (completed_order_id, completed_rows) if
        this row ended a previous streak, else None.
        """
        oid = row[order_id_field]

        if self.order_id is None:
            self.order_id = oid
            self.rows = [row]
            return None

        if oid == self.order_id:
            self.rows.append(row)
            return None

        # order id changed -> previous streak just ended
        completed_id = self.order_id
        completed_rows = self.rows

        # start new streak (== trimming the table to the latest order id)
        self.order_id = oid
        self.rows = [row]

        return completed_id, completed_rows


taker_buy = StreakTracker("taker_buy")
taker_sell = StreakTracker("taker_sell")
maker_buy = StreakTracker("maker_buy")
maker_sell = StreakTracker("maker_sell")


# ---------------------------------------------------------------------------
# Calculations
# ---------------------------------------------------------------------------
def calc1_aggressive_taker(side_label, completed_id, completed_rows):
    """side_label: 'BUY' or 'SELL'"""
    count = len(completed_rows)
    if count < AGGRESSIVENESS_N:
        return

    prices = [r["price"] for r in completed_rows]
    price_level_count = len(set(prices))
    extreme_price = min(prices) if side_label == "BUY" else max(prices)
    date_str, time_str = format_datetime_utc7(completed_rows[-1]["time"])

    msg_type = f"aggressive_taker_{side_label.lower()}"
    if not throttle.should_send(msg_type, count):
        return

    title = f"Aggressive order taker: {side_label}"
    price_desc = "Lowest match" if side_label == "BUY" else "Highest match"
    body = (
        f"Trades in streak: {count}\n"
        f"Price level count: {price_level_count}\n"
        f"{price_desc} at {format_price(extreme_price)}\n"
        f"{date_str} (UTC+7) {time_str}"
    )
    send_ntfy(title, body)


def calc2_large_resting_order(side_label, completed_id, completed_rows):
    """side_label: 'Bid' (maker_buy) or 'Ask' (maker_sell)"""
    count = len(completed_rows)
    if count < RESTING_N:
        return

    order_price = completed_rows[0]["price"]
    date_str, time_str = format_datetime_utc7(completed_rows[-1]["time"])

    msg_type = f"resting_hit_{side_label.lower()}"
    if not throttle.should_send(msg_type, count):
        return

    title = f"Large resting order hit: {side_label}"
    body = (
        f"Trades matched: {count}\n"
        f"Hit at $ {order_price:,.2f}\n"
        f"{date_str} (UTC+7) {time_str}"
    )
    send_ntfy(title, body)


# ---------------------------------------------------------------------------
# Match message handling
# ---------------------------------------------------------------------------
def handle_match(msg: dict):
    """
    Coinbase 'match' fields used: price, size, time, trade_id,
    maker_order_id, taker_order_id, side.

    side == "sell" -> maker was a SELL (resting ask) -> taker BOUGHT
    side == "buy"  -> maker was a BUY  (resting bid) -> taker SOLD
    """
    try:
        row = {
            "trade_id": msg.get("trade_id"),
            "taker_order_id": msg.get("taker_order_id"),
            "maker_order_id": msg.get("maker_order_id"),
            "price": float(msg["price"]),
            "size": float(msg["size"]),
            "time": msg.get("time"),
        }
    except (KeyError, TypeError, ValueError) as e:
        log.warning(f"Skipping malformed match message: {e} | {msg}")
        return

    side = msg.get("side")  # the MAKER's side

    if side == "sell":
        completed = taker_buy.add(row, "taker_order_id")
        if completed:
            calc1_aggressive_taker("BUY", *completed)

        completed = maker_sell.add(row, "maker_order_id")
        if completed:
            calc2_large_resting_order("Ask", *completed)

    elif side == "buy":
        completed = taker_sell.add(row, "taker_order_id")
        if completed:
            calc1_aggressive_taker("SELL", *completed)

        completed = maker_buy.add(row, "maker_order_id")
        if completed:
            calc2_large_resting_order("Bid", *completed)

    else:
        log.warning(f"Unknown side in match message: {side}")


# ---------------------------------------------------------------------------
# WebSocket plumbing
# ---------------------------------------------------------------------------
def on_open(ws):
    log.info("WebSocket opened, subscribing to matches channel...")
    subscribe_msg = {
        "type": "subscribe",
        "product_ids": [PRODUCT_ID],
        "channels": ["matches"],
    }
    ws.send(json.dumps(subscribe_msg))


def on_message(ws, message):
    try:
        msg = json.loads(message)
    except json.JSONDecodeError:
        log.warning(f"Non-JSON message received: {message[:200]}")
        return

    msg_type = msg.get("type")

    if msg_type in ("match", "last_match"):
        handle_match(msg)
    elif msg_type == "subscriptions":
        log.info(f"Subscribed: {msg}")
    elif msg_type == "error":
        log.error(f"Coinbase WS error message: {msg}")
    # ignore other message types (heartbeats etc.)


def on_error(ws, error):
    log.error(f"WebSocket error: {error}")


def on_close(ws, close_status_code, close_msg):
    log.warning(f"WebSocket closed: {close_status_code} {close_msg}")


def run_forever_with_reconnect():
    backoff = 1
    while True:
        try:
            ws = websocket.WebSocketApp(
                COINBASE_WS_URL,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
            )
            ws.run_forever(ping_interval=20, ping_timeout=10)
        except Exception as e:
            log.error(f"Unhandled exception in WS loop: {e}")

        log.warning(f"Reconnecting in {backoff}s...")
        time.sleep(backoff)
        backoff = min(backoff * 2, 30)


if __name__ == "__main__":
    log.info(f"Starting BTC-USD match stream. Aggressiveness_N={AGGRESSIVENESS_N}, Resting_N={RESTING_N}")
    log.info(f"Notifications -> {NTFY_URL}")
    run_forever_with_reconnect()
