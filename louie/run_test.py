import shift
from time import sleep
from datetime import datetime, timedelta
import datetime as dt
from threading import Thread, Lock
import csv
import os

SUBMISSION_LOG_PATH = "template_market_submissions.csv"
EXECUTION_LOG_PATH = "template_market_executions.csv"
LOT_SIZE = 100

log_lock = Lock()

def ensure_csv_headers():
    if not os.path.exists(SUBMISSION_LOG_PATH):
        with open(SUBMISSION_LOG_PATH, "w", newline="") as f:
            csv.writer(f).writerow([
                "logged_at", "order_id", "symbol", "side", "limit_price",
                "shares", "lots", "reason", "step", "pos_before",
            ])
    if not os.path.exists(EXECUTION_LOG_PATH):
        with open(EXECUTION_LOG_PATH, "w", newline="") as f:
            csv.writer(f).writerow([
                "logged_at", "order_id", "symbol", "side", "executed_price",
                "executed_size", "order_size", "status", "exec_timestamp",
            ])

def log_submission(sim_time, order_id, symbol, side, price, shares, reason, step, pos):
    with log_lock:
        with open(SUBMISSION_LOG_PATH, "a", newline="") as f:
            csv.writer(f).writerow([
                sim_time, order_id, symbol, side, f"{float(price):.4f}",
                int(shares), int(shares // LOT_SIZE), reason, step, int(pos),
            ])

def log_execution(sim_time, order_id, symbol, side, exec_price, exec_size, order_size, status, exec_ts):
    with log_lock:
        with open(EXECUTION_LOG_PATH, "a", newline="") as f:
            csv.writer(f).writerow([
                sim_time, order_id, symbol, side, f"{float(exec_price):.4f}",
                int(exec_size), int(order_size), str(status), exec_ts,
            ])

def poll_executions(trader, tracked_orders, seen_keys):
    sim_time = trader.get_last_trade_time()
    for oid, meta in list(tracked_orders.items()):
        try:
            for ex in trader.get_executed_orders(oid):
                sz = int(getattr(ex, "executed_size", 0))
                px = float(getattr(ex, "executed_price", 0.0000))
                if sz > 0:
                    key = (oid, str(getattr(ex, "timestamp", "")), sz, px, str(getattr(ex, "status", "")))
                    if key not in seen_keys:
                        seen_keys.add(key)
                        log_execution(sim_time, oid, getattr(ex, "symbol", meta["symbol"]), meta["side"], px, sz, meta["lots"] * LOT_SIZE, str(getattr(ex, "status", "")), getattr(ex, "timestamp", ""))
            cur = trader.get_order(oid)
            if cur is not None:
                s = str(getattr(cur, "status", ""))
                if "FILLED" in s or "CANCELED" in s or "REJECTED" in s or int(getattr(cur, "executed_size", 0)) >= meta["lots"] * LOT_SIZE:
                    tracked_orders[oid]["done"] = True
        except Exception:
            pass
    for oid in [k for k, v in tracked_orders.items() if v.get("done")]:
        del tracked_orders[oid]

def cancel_orders(trader, ticker):
    for order in trader.get_waiting_list():
        if order.symbol == ticker:
            trader.submit_cancellation(order)
            sleep(1.0000)

def close_positions(trader, ticker, tracked_orders, step):
    print(f"running close positions function for {ticker}", flush=True)
    item = trader.get_portfolio_item(ticker)
    sim_time = trader.get_last_trade_time()
    
    long_shares = item.get_long_shares()
    if long_shares > 0:
        print(f"market selling because {ticker} long shares = {long_shares}", flush=True)
        lots = int(long_shares / 100)
        order = shift.Order(shift.Order.Type.MARKET_SELL, ticker, lots)
        trader.submit_order(order)
        tracked_orders[order.id] = {"symbol": ticker, "side": "SELL", "lots": lots, "done": False}
        log_submission(sim_time, order.id, ticker, "SELL", 0.0000, long_shares, "close_long", step, long_shares)
        sleep(1.0000)

    short_shares = item.get_short_shares()
    if short_shares > 0:
        print(f"market buying because {ticker} short shares = {short_shares}", flush=True)
        lots = int(short_shares / 100)
        order = shift.Order(shift.Order.Type.MARKET_BUY, ticker, lots)
        trader.submit_order(order)
        tracked_orders[order.id] = {"symbol": ticker, "side": "BUY", "lots": lots, "done": False}
        log_submission(sim_time, order.id, ticker, "BUY", 0.0000, short_shares, "close_short", step, -short_shares)
        sleep(1.0000)

def strategy(trader: shift.Trader, ticker: str, endtime):
    initial_pl = trader.get_portfolio_item(ticker).get_realized_pl()
    check_freq = 1.0000
    order_size = 5

    best_price = trader.get_best_price(ticker)
    best_bid = best_price.get_bid_price()
    best_ask = best_price.get_ask_price()
    previous_price = (best_bid + best_ask) / 2.0000

    tracked_orders = {}
    seen_keys = set()
    step = 0

    while trader.get_last_trade_time() < endtime:
        cancel_orders(trader, ticker)

        best_price = trader.get_best_price(ticker)
        best_bid = best_price.get_bid_price()
        best_ask = best_price.get_ask_price()
        midprice = (best_bid + best_ask) / 2.0000
        
        sim_time = trader.get_last_trade_time()
        pos_shares = trader.get_portfolio_item(ticker).get_long_shares() - trader.get_portfolio_item(ticker).get_short_shares()

        if midprice > previous_price:
            order = shift.Order(shift.Order.Type.MARKET_BUY, ticker, order_size)
            trader.submit_order(order)
            tracked_orders[order.id] = {"symbol": ticker, "side": "BUY", "lots": order_size, "done": False}
            log_submission(sim_time, order.id, ticker, "BUY", best_ask, order_size * LOT_SIZE, "midprice_up", step, pos_shares)
            print(f"[ORDER_SUBMITTED] | TYPE: MARKET_BUY | TKR: {ticker} | SIZE: {order_size}", flush=True)
            
        elif midprice < previous_price:
            order = shift.Order(shift.Order.Type.MARKET_SELL, ticker, order_size)
            trader.submit_order(order)
            tracked_orders[order.id] = {"symbol": ticker, "side": "SELL", "lots": order_size, "done": False}
            log_submission(sim_time, order.id, ticker, "SELL", best_bid, order_size * LOT_SIZE, "midprice_down", step, pos_shares)
            print(f"[ORDER_SUBMITTED] | TYPE: MARKET_SELL | TKR: {ticker} | SIZE: {order_size}", flush=True)

        poll_executions(trader, tracked_orders, seen_keys)
        previous_price = midprice
        step += 1
        sleep(check_freq)

    cancel_orders(trader, ticker)
    close_positions(trader, ticker, tracked_orders, step)
    poll_executions(trader, tracked_orders, seen_keys)

    print(f"total profits/losses for {ticker}: {trader.get_portfolio_item(ticker).get_realized_pl() - initial_pl:.4f}", flush=True)


def main(trader):
    ensure_csv_headers()
    check_frequency = 1.0000
    current = trader.get_last_trade_time()
    start_time = current
    end_time = start_time + timedelta(seconds=15)

    while trader.get_last_trade_time() < start_time:
        print("still waiting for market open", flush=True)
        sleep(check_frequency)

    initial_pl = trader.get_portfolio_summary().get_total_realized_pl()
    threads = []
    tickers = ["AAPL", "MSFT"]

    print("START", flush=True)

    for ticker in tickers:
        threads.append(Thread(target=strategy, args=(trader, ticker, end_time)))

    for thread in threads:
        thread.start()
        sleep(1.0000)

    while trader.get_last_trade_time() < end_time:
        sleep(check_frequency)

    for thread in threads:
        thread.join()

    for ticker in tickers:
        cancel_orders(trader, ticker)

    print("END", flush=True)
    print(f"final bp: {trader.get_portfolio_summary().get_total_bp():.4f}", flush=True)
    print(f"final profits/losses: {trader.get_portfolio_summary().get_total_realized_pl() - initial_pl:.4f}", flush=True)

if __name__ == "__main__":
    with shift.Trader("columbia-traders") as trader:
        trader.connect("initiator.cfg", "aRkkZSrj")
        sleep(1.0000)
        trader.sub_all_order_book()
        sleep(1.0000)
        main(trader)