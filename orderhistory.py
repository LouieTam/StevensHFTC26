import shift
import time
import csv
import os

TICKERS     = ["CS1", "CS2", "CS3"]
OB_LEVELS   = 10
SLEEP       = 0.01    # 10ms inner loop
OB_INTERVAL = 1.0     # record order book every N seconds

def open_writers(ticker):
    """Open trade and order book CSV files for a ticker."""
    trades_f = open(f"trades1_{ticker}.csv",     mode="w", newline="")
    ob_f     = open(f"order_book1_{ticker}.csv", mode="w", newline="")

    trades_w = csv.writer(trades_f)
    ob_w     = csv.writer(ob_f)

    trades_w.writerow(["timestamp", "price", "size"])

    ob_header = ["timestamp"]
    for i in range(OB_LEVELS):
        ob_header.extend([f"bid_price_{i+1}", f"bid_size_{i+1}"])
    for i in range(OB_LEVELS):
        ob_header.extend([f"ask_price_{i+1}", f"ask_size_{i+1}"])
    ob_w.writerow(ob_header)

    return (trades_f, ob_f), (trades_w, ob_w)


def record_market_data(trader):
    files   = {}
    writers = {}
    for ticker in TICKERS:
        f, w = open_writers(ticker)
        files[ticker]   = f    # (trades_f, ob_f)
        writers[ticker] = w    # (trades_w, ob_w)

    last_trade_time = {t: trader.get_last_trade_time() for t in TICKERS}
    last_ob_time    = {t: 0.0 for t in TICKERS}

    print(f"[RECORDER] Started | tickers={TICKERS} | "
          f"OB every {OB_INTERVAL}s", flush=True)

    try:
        while True:
            now = time.time()

            for ticker in TICKERS:
                trades_w, ob_w = writers[ticker]
                current_trade_time = trader.get_last_trade_time()

                # ── Trade event: record every new trade ───────────────────
                if current_trade_time > last_trade_time[ticker]:
                    price = float(trader.get_last_price(ticker))
                    size  = trader.get_last_size(ticker)
                    if size > 0:
                        trades_w.writerow([
                            current_trade_time,
                            f"{price:.4f}",
                            size,
                        ])
                    last_trade_time[ticker] = current_trade_time

                # ── Order book snapshot: once per second ──────────────────
                if now - last_ob_time[ticker] < OB_INTERVAL:
                    continue

                last_ob_time[ticker] = now

                bids = trader.get_order_book(
                    ticker, shift.OrderBookType.LOCAL_BID)
                asks = trader.get_order_book(
                    ticker, shift.OrderBookType.LOCAL_ASK)

                row = [current_trade_time]
                for i in range(OB_LEVELS):
                    if i < len(bids):
                        row.extend([f"{float(bids[i].price):.4f}",
                                    bids[i].size])
                    else:
                        row.extend(["0.0000", 0])
                for i in range(OB_LEVELS):
                    if i < len(asks):
                        row.extend([f"{float(asks[i].price):.4f}",
                                    asks[i].size])
                    else:
                        row.extend(["0.0000", 0])

                ob_w.writerow(row)

            time.sleep(SLEEP)

    except KeyboardInterrupt:
        print("\n[RECORDER] Interrupted — closing files", flush=True)

    finally:
        for ticker in TICKERS:
            trades_f, ob_f = files[ticker]
            trades_f.flush(); trades_f.close()
            ob_f.flush();     ob_f.close()
            print(f"[RECORDER] {ticker} files closed", flush=True)


if __name__ == "__main__":
    with shift.Trader("columbia-traders") as trader:
        trader.connect("initiator.cfg", "aRkkZSrj")
        time.sleep(3.0)
        trader.sub_all_order_book()
        time.sleep(3.0)
        record_market_data(trader)