import shift
import time
import csv
import os
import gc
from datetime import timedelta

POLL_INTERVAL = 1.0
LEVELS = 10
DATA_DIR = "market_data_20241015"

file_handles = {}
csv_writers = {}


def get_csv_path(symbol):
    return os.path.join(DATA_DIR, f"{symbol}.csv")


def ensure_dir():
    os.makedirs(DATA_DIR, exist_ok=True)


def setup_files(symbols):
    ensure_dir()

    for sym in symbols:
        path = get_csv_path(sym)
        file_exists = os.path.exists(path)

        f = open(path, "a", newline="")
        file_handles[sym] = f
        writer = csv.writer(f)
        csv_writers[sym] = writer

        if not file_exists:
            cols = [
                "sim_time", "mid_price", "last_price", "last_size",
                "best_bid_price", "best_bid_size", "best_ask_price", "best_ask_size",
                "close_price_buy", "close_price_sell"
            ]
            for i in range(1, LEVELS + 1):
                cols += [f"bid{i}_price", f"bid{i}_size"]
            for i in range(1, LEVELS + 1):
                cols += [f"ask{i}_price", f"ask{i}_size"]
            writer.writerow(cols)
            f.flush()


def close_files():
    for f in file_handles.values():
        try:
            f.close()
        except Exception:
            pass


def collect_ticker(trader, symbol):
    try:
        sim_time = str(trader.get_last_trade_time())

        bo = trader.get_order_book(symbol, shift.OrderBookType.GLOBAL_BID)
        ao = trader.get_order_book(symbol, shift.OrderBookType.GLOBAL_ASK)

        bids = [(float(b.price), int(b.size)) for b in bo[:LEVELS]]
        asks = [(float(a.price), int(a.size)) for a in ao[:LEVELS]]

        if bids:
            best_bid_price, best_bid_size = bids[0]
        else:
            best_bid_price, best_bid_size = "", ""

        if asks:
            best_ask_price, best_ask_size = asks[0]
        else:
            best_ask_price, best_ask_size = "", ""

        if bids and asks and best_bid_price > 0 and best_ask_price > 0:
            mid = round(0.5 * (best_bid_price + best_ask_price), 4)
        else:
            mid = ""

        try:
            last_price = float(trader.get_last_price(symbol))
        except Exception:
            last_price = ""

        try:
            last_size = int(trader.get_last_size(symbol))
        except Exception:
            last_size = ""

        close_buy = best_ask_price if asks else ""
        close_sell = best_bid_price if bids else ""

        row = [
            sim_time, mid, last_price, last_size,
            best_bid_price, best_bid_size,
            best_ask_price, best_ask_size,
            close_buy, close_sell,
        ]

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

        del bo
        del ao
        return row

    except Exception as e:
        print(f"[COLLECT ERROR] {symbol}: {e}", flush=True)
        return None


def run_collector(trader, end_time=None):
    symbols = trader.get_stock_list()
    print(f"[COLLECTOR] Tickers: {symbols}", flush=True)

    setup_files(symbols)

    tick = 0
    while trader.get_last_trade_time() < end_time:
        t0 = time.time()
        sim_time = trader.get_last_trade_time()

        for sym in symbols:
            row = collect_ticker(trader, sym)
            if row is not None:
                csv_writers[sym].writerow(row)

        tick += 1
        if tick % 10 == 0:
            print(f"[COLLECTOR] Tick {tick} | sim={sim_time}", flush=True)
            for f in file_handles.values():
                f.flush()
            gc.collect()

        elapsed = time.time() - t0
        sleep_time = max(0.0, POLL_INTERVAL - elapsed)
        time.sleep(sleep_time)

    close_files()
    print(f"[COLLECTOR] Done. Data written to ./{DATA_DIR}/", flush=True)


if __name__ == "__main__":
    with shift.Trader("columbia-traders") as trader:
        trader.connect("initiator.cfg", "aRkkZSrj")
        time.sleep(1.0)
        trader.sub_all_order_book()
        time.sleep(1.0)

        start = trader.get_last_trade_time()
        end_time = start.replace(hour=16, minute=0, second=0, microsecond=0)
        if end_time <= start:
            end_time = start + timedelta(minutes=380)

        print(f"[COLLECTOR] Session: {start} → {end_time}", flush=True)

        try:
            run_collector(trader, end_time=end_time)
        except KeyboardInterrupt:
            print("[COLLECTOR] Interrupted.", flush=True)
        finally:
            trader.disconnect()
            close_files()