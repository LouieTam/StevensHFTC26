import shift
import time
import csv
import os
import threading
from datetime import timedelta

POLL_INTERVAL        = 1.0    # seconds between data collection ticks
LEVELS               = 10     # order book depth
ORDER_LOOP_SYMBOL    = "NVDA" # symbol for the alternating market order loop
ORDER_LOOP_INTERVAL  = 1.0    # seconds between each market order submission
DUMP_BEFORE_END_SECS = 300    # dump submitted orders 5 min before end_time
DATA_DIR             = "market_data"   # folder for per-ticker CSVs

# ---------------------------------------------------------------------------
# CSV helpers
# ---------------------------------------------------------------------------

def get_csv_path(symbol):
    return os.path.join(DATA_DIR, f"{symbol}.csv")

def ensure_dir():
    os.makedirs(DATA_DIR, exist_ok=True)

def write_header(symbol):
    path = get_csv_path(symbol)
    if os.path.exists(path):
        return  # don't overwrite existing data
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        # Build header: metadata + best price + close price + 10 bid levels + 10 ask levels
        cols = [
            "sim_time",
            "mid_price",
            "last_price",
            "last_size",
            "best_bid_price", "best_bid_size",
            "best_ask_price", "best_ask_size",
            "close_price_buy",   # get_close_price(symbol, True,  100)
            "close_price_sell",  # get_close_price(symbol, False, 100)
        ]
        for i in range(1, LEVELS + 1):
            cols += [f"bid{i}_price", f"bid{i}_size"]
        for i in range(1, LEVELS + 1):
            cols += [f"ask{i}_price", f"ask{i}_size"]
        w.writerow(cols)

def append_row(symbol, row):
    with open(get_csv_path(symbol), "a", newline="") as f:
        csv.writer(f).writerow(row)

# ---------------------------------------------------------------------------
# Data collection helpers
# ---------------------------------------------------------------------------

def collect_ticker(trader, symbol):
    """Collect one row of data for a symbol. Returns a list or None on failure."""
    try:
        sim_time = str(trader.get_last_trade_time())

        # ── Mid price ─────────────────────────────────────────────────────────
        bo = trader.get_order_book(symbol, shift.OrderBookType.GLOBAL_BID)
        ao = trader.get_order_book(symbol, shift.OrderBookType.GLOBAL_ASK)

        bids = [(float(b.price), int(b.size)) for b in bo[:LEVELS]]
        asks = [(float(a.price), int(a.size)) for a in ao[:LEVELS]]

        if bids and asks and bids[0][0] > 0 and asks[0][0] > 0:
            mid = round(0.5 * (bids[0][0] + asks[0][0]), 4)
        else:
            mid = ""

        # ── Last trade ────────────────────────────────────────────────────────
        try:
            last_price = float(trader.get_last_price(symbol))
        except Exception:
            last_price = ""

        try:
            last_size = int(trader.get_last_size(symbol))
        except Exception:
            last_size = ""

        # ── Best price ────────────────────────────────────────────────────────
        try:
            bp = trader.get_best_price(symbol)
            best_bid_price = float(bp.get_bid_price())
            best_bid_size  = int(bp.get_bid_size())
            best_ask_price = float(bp.get_ask_price())
            best_ask_size  = int(bp.get_ask_size())
        except Exception:
            best_bid_price = best_bid_size = best_ask_price = best_ask_size = ""

        # ── Close price (buy and sell, size=1 lot per API docs) ─────────────────
        try:
            close_buy  = float(trader.get_close_price(symbol, True,  1))
        except Exception:
            close_buy  = ""
        try:
            close_sell = float(trader.get_close_price(symbol, False, 1))
        except Exception:
            close_sell = ""

        # ── Order book levels ─────────────────────────────────────────────────
        row = [
            sim_time, mid, last_price, last_size,
            best_bid_price, best_bid_size,
            best_ask_price, best_ask_size,
            close_buy, close_sell,
        ]

        # Pad bids and asks to exactly LEVELS entries
        for i in range(LEVELS):
            if i < len(bids):
                row += [bids[i][0], bids[i][1]]
            else:
                row += ["", ""]

        for i in range(LEVELS):
            if i < len(asks):
                row += [asks[i][0], asks[i][1]]
            else:
                row += ["", ""]

        return row

    except Exception as e:
        print(f"[COLLECT ERROR] {symbol}: {e}", flush=True)
        return None

# ---------------------------------------------------------------------------
# NVDA order loop thread
# ---------------------------------------------------------------------------

def nvda_order_loop(trader, end_time, order_records):
    """
    Submits alternating BUY/SELL 1-lot market orders for NVDA every second.
    Stores (order_id, side, sim_time) in order_records list.
    Runs until end_time.
    """
    side_cycle = ["BUY", "SELL"]
    idx        = 0
    dump_done  = False

    print(f"[ORDER LOOP] Starting NVDA alternating order loop", flush=True)

    while trader.get_last_trade_time() < end_time:
        sim_time = trader.get_last_trade_time()

        # ── Dump submitted orders 5 min before end ────────────────────────────
        remaining = (end_time - sim_time).total_seconds()
        if remaining <= DUMP_BEFORE_END_SECS and not dump_done:
            dump_done = True
            print("\n" + "=" * 120, flush=True)
            print(f"[ORDER DUMP] {len(order_records)} orders submitted. "
                  f"Pulling full submitted orders list from SHIFT...", flush=True)
            print(
                f"{'Symbol':>8}\t{'Type':>20}\t{'Price':>8}\t{'Size':>6}\t"
                f"{'Exec':>6}\t{'ID':>36}\t{'Status':>26}\t{'Timestamp':>26}",
                flush=True
            )
            for order in trader.get_submitted_orders():
                if order.status == shift.Order.Status.FILLED:
                    price = order.executed_price
                else:
                    price = order.price
                print(
                    "%6s\t%16s\t%7.2f\t\t%4d\t\t%4d\t%36s\t%23s\t\t%26s" % (
                        order.symbol,
                        order.type,
                        price,
                        order.size,
                        order.executed_size,
                        order.id,
                        order.status,
                        order.timestamp,
                    ),
                    flush=True
                )
            print("=" * 120 + "\n", flush=True)

            # Also write to CSV
            dump_path = os.path.join(DATA_DIR, "nvda_submitted_orders.csv")
            with open(dump_path, "w", newline="") as f:
                w = csv.writer(f)
                w.writerow(["symbol", "type", "price", "size",
                            "executed_size", "order_id", "status", "timestamp"])
                for order in trader.get_submitted_orders():
                    price = order.executed_price if order.status == shift.Order.Status.FILLED else order.price
                    w.writerow([
                        order.symbol, str(order.type), price,
                        order.size, order.executed_size,
                        order.id, str(order.status), order.timestamp
                    ])
            print(f"[ORDER DUMP] Written to {dump_path}", flush=True)

        # ── Submit alternating order ──────────────────────────────────────────
        side  = side_cycle[idx % 2]
        idx  += 1

        if side == "BUY":
            order = shift.Order(shift.Order.Type.MARKET_BUY,  ORDER_LOOP_SYMBOL, 1)
        else:
            order = shift.Order(shift.Order.Type.MARKET_SELL, ORDER_LOOP_SYMBOL, 1)

        trader.submit_order(order)
        order_records.append({
            "id":       order.id,
            "side":     side,
            "sim_time": str(sim_time),
        })
        print(f"[ORDER LOOP] {side} 1L MARKET {ORDER_LOOP_SYMBOL} "
              f"| id={order.id} | sim={sim_time}", flush=True)

        time.sleep(ORDER_LOOP_INTERVAL)

    print(f"[ORDER LOOP] Finished. Total orders submitted: {len(order_records)}",
          flush=True)

# ---------------------------------------------------------------------------
# Main data collection loop
# ---------------------------------------------------------------------------

def run_collector(trader, end_time=None):
    ensure_dir()

    # Get full ticker list
    symbols = trader.get_stock_list()
    print(f"[COLLECTOR] Tickers: {symbols}", flush=True)

    # Write CSV headers
    for sym in symbols:
        write_header(sym)

    # Start NVDA order loop in a background thread
    order_records = []
    order_thread  = threading.Thread(
        target=nvda_order_loop,
        args=(trader, end_time, order_records),
        daemon=True
    )
    order_thread.start()
    print(f"[COLLECTOR] Order loop thread started", flush=True)

    tick = 0
    while trader.get_last_trade_time() < end_time:
        t0 = time.time()

        sim_time = trader.get_last_trade_time()

        # Collect data for every ticker
        for sym in symbols:
            row = collect_ticker(trader, sym)
            if row is not None:
                append_row(sym, row)

        tick += 1
        if tick % 60 == 0:
            print(f"[COLLECTOR] Tick {tick} | sim={sim_time} | "
                  f"orders_submitted={len(order_records)}", flush=True)

        # Sleep for remainder of POLL_INTERVAL
        elapsed = time.time() - t0
        sleep_time = max(0.0, POLL_INTERVAL - elapsed)
        time.sleep(sleep_time)

    # Wait for order thread to finish
    order_thread.join(timeout=10)
    print(f"[COLLECTOR] Done. Data written to ./{DATA_DIR}/", flush=True)
    print(f"[COLLECTOR] Total order records: {len(order_records)}", flush=True)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    with shift.Trader("columbia-traders") as trader:
        trader.connect("initiator.cfg", "aRkkZSrj")
        time.sleep(1.0)
        trader.sub_all_order_book()
        time.sleep(1.0)

        # Full trading day: 09:30 → 16:00
        start = trader.get_last_trade_time()
        end_time = start.replace(hour=16, minute=0, second=0, microsecond=0)
        if end_time <= start:
            # Already past 16:00 — run for 500 min as fallback
            end_time = start + timedelta(minutes=380)

        print(f"[COLLECTOR] Session: {start} → {end_time}", flush=True)

        try:
            run_collector(trader, end_time=end_time)
        except KeyboardInterrupt:
            print("[COLLECTOR] Interrupted.", flush=True)
        finally:
            trader.cancel_all_pending_orders()
            trader.disconnect()