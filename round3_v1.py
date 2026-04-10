import shift
import pandas as pd
import time
import sys
import numpy as np
import helper
import datetime as dt
import math
import bisect
from collections import defaultdict
import os

# Constants
TRADER_ID = "columbia-traders"
CFG_FILE = "initiator.cfg"
PASSWORD = "aRkkZSrj"
max_balance = 1000000.0
perStock_Limit = max_balance / 5
buffer = 80000

TICK_INTERVAL = 1.0
midNotLast = True


# ─────────────────────────────────────────────────────────────
# PRINTING HELPERS
# ─────────────────────────────────────────────────────────────

def print_mode_header(mode, current_time):
    print(f"\n{'=' * 60}")
    print(f"  {mode}  |  {current_time}")
    print(f"{'=' * 60}")


def print_ticker_action(ticker, action, state, bid_quote=None, ask_quote=None, volume=None, signal=None):
    print(f"\n  [{action}] {ticker}")
    print(f"    Market:  bid={state['best_bid']:.2f}  ask={state['best_ask']:.2f}  "
          f"quote={state['quote_price']:.2f}  skew={state['order_book_skew']:.4f}")
    if signal is not None:
        print(f"    Signal:  {signal:.4f}")
    if bid_quote is not None or ask_quote is not None:
        bid_str = f"{bid_quote:.2f}" if bid_quote and bid_quote != 0 else "OFF"
        ask_str = f"{ask_quote:.2f}" if ask_quote and not math.isinf(ask_quote) else "OFF"
        print(f"    Quoting: bid={bid_str}  ask={ask_str}  vol={volume}")


def print_portfolio(trader):
    portfolio = list(trader.get_portfolio_items().values())
    if len(portfolio) == 0:
        print("  Portfolio is empty")
        return

    print("  PORTFOLIO SUMMARY")
    print("  " + "-" * 100)
    print(f"  {'Ticker':<10}{'Shares':>10}{'Price':>12}{'Exposure':>14}{'Realized P/L':>16}{'Timestamp':>32}")
    print("  " + "-" * 100)

    total_exposure = 0
    for item in portfolio:
        shares = item.get_shares()
        price = item.get_price()
        realized_pl = item.get_realized_pl()
        timestamp = item.get_timestamp()
        exposure = shares * price
        total_exposure += exposure
        print(
            f"  {item.get_symbol():<10}"
            f"{shares:>10}"
            f"{price:>12.2f}"
            f"{exposure:>14.2f}"
            f"{realized_pl:>16.2f}"
            f"{str(timestamp):>32}"
        )
    print("  " + "-" * 100)
    print(f"  {'TOTAL':<10}{'':>10}{'':>12}{total_exposure:>14.2f}")
    print()


def print_orders(trader):
    orders_list = trader.get_submitted_orders()[-10:]
    if len(orders_list) == 0:
        print("  No Orders")
        return

    print("  RECENT ORDERS")
    print("  " + "-" * 130)
    print(f"  {'Symbol':<8}{'Type':<22}{'Price':>8}{'Size':>6}{'Exec':>6}  {'Status':<22}{'Timestamp'}")
    print("  " + "-" * 130)
    for order in orders_list:
        price = order.executed_price if order.status == shift.Order.Status.FILLED else order.price
        print(
            f"  {order.symbol:<8}"
            f"{str(order.type):<22}"
            f"{price:>8.2f}"
            f"{order.size:>6}"
            f"{order.executed_size:>6}  "
            f"{str(order.status):<22}"
            f"{order.timestamp}"
        )
    print("  " + "-" * 130)
    print()


# ─────────────────────────────────────────────────────────────
# QUOTING & SIGNAL
# ─────────────────────────────────────────────────────────────

def quote(best_prices, last_trade, mid_price):
    last_price, last_size = last_trade

    bid = best_prices[0]
    ask = best_prices[1]
    bid_price, bid_size = bid
    ask_price, ask_size = ask

    if last_price * mid_price == 0:
        return 0

    numerator = last_size * last_price + bid_price * bid_size + ask_price * ask_size
    denominator = last_size + bid_size + ask_size
    result = math.floor((numerator / denominator) * 100 + 0.5) / 100
    return result


def marketMakingAggression(current_time, end_time, start_time, skew, inventory):
    old = False
    if old:
        difference = (end_time - start_time).total_seconds() / 60
        time_left = end_time - current_time
        minutes_left = max(0, time_left.total_seconds() / 60)
        signal = minutes_left / difference
        sigmoidFactor = helper.sigmoid(signal, 3, 1, 2)
        return sigmoidFactor
    else:
        skew = abs(skew)
        sigmoidFactor = helper.sigmoidNew(skew, 0.75, 1, 2, 2)
        return sigmoidFactor


# ─────────────────────────────────────────────────────────────
# MARKET MAKING DECISION
# ─────────────────────────────────────────────────────────────

def marketMakingDecision(trader, current_time, ticker, state, order_log, end_time, start_time):
    TICK = 0.01
    quote_price = state['quote_price']
    best_bid = state["best_bid"]
    best_ask = state["best_ask"]
    skew = state["order_book_skew"]
    volume = state["quantity"]

    if quote_price == 0 or best_bid == 0 or best_ask == 0:
        return None

    portfolio_inventory = state['inventory'] or 0
    signal = marketMakingAggression(current_time, end_time, start_time, skew, portfolio_inventory)

    maxPos = 3
    inventory_ratio = portfolio_inventory / maxPos

    if inventory_ratio > 0:
        adjusted_signal = signal * 0.8
        ask_quote = round(math.floor((adjusted_signal * best_ask + (1 - adjusted_signal) * quote_price) / TICK) * TICK, 2)
        ask_quote = min(ask_quote, best_ask - TICK)
        bid_quote = 0
    elif inventory_ratio < 0:
        adjusted_signal = signal * 0.8
        bid_quote = round(math.ceil((adjusted_signal * best_bid + (1 - adjusted_signal) * quote_price) / TICK) * TICK, 2)
        bid_quote = max(bid_quote, best_bid + TICK)
        ask_quote = float("inf")
    else:
        bid_quote = round(math.ceil((signal * best_bid + (1 - signal) * quote_price) / TICK) * TICK, 2)
        ask_quote = round(math.floor((signal * best_ask + (1 - signal) * quote_price) / TICK) * TICK, 2)
        bid_quote = max(bid_quote, best_bid + TICK)
        ask_quote = min(ask_quote, best_ask - TICK)

    bid_quote = round(bid_quote, 2)
    ask_quote = round(ask_quote, 2) if not math.isinf(ask_quote) else ask_quote

    if bid_quote >= ask_quote:
        return None

    bid = (bid_quote, volume)
    ask = (ask_quote, volume)
    return bid, ask, signal


# ─────────────────────────────────────────────────────────────
# MARKET MAKING EXECUTION
# ─────────────────────────────────────────────────────────────

def marketMakingExecution(trader, ticker, bid_side, ask_side, order_log, current_time, inventory=0, offload=False, emergent=False):
    order_list = trader.get_waiting_list()

    if emergent:
        staleThreshold = 10
    elif offload:
        staleThreshold = 15
    else:
        staleThreshold = 30

    for order in order_list:
        if order.symbol == ticker:
            age = (dt.datetime.now() - order.timestamp).total_seconds()
            if age < staleThreshold:
                print(f"    >> WAITING (order age {age:.0f}s < {staleThreshold}s threshold)")
                return

    # Cancel existing orders
    pending = [o for o in trader.get_waiting_list() if o.symbol == ticker]
    if pending:
        print(f"    >> CANCELLING {len(pending)} stale order(s)...")
        max_retries = 10
        for attempt in range(max_retries):
            pending = [o for o in trader.get_waiting_list() if o.symbol == ticker]
            if not pending:
                print(f"    >> All orders cancelled (attempt {attempt + 1})")
                break
            for order in pending:
                trader.submit_cancellation(order)
            time.sleep(0.5)
        else:
            print(f"    >> WARNING — could not cancel after {max_retries} attempts, skipping")
            return

    bid_price, bid_size = bid_side
    ask_price, ask_size = ask_side

    if offload:
        if inventory > 0:
            if not math.isinf(ask_price):
                helper.submit_limit_order(trader, ticker, "sell", ask_size, ask_price)
                order_log.append({
                    "submit_time": current_time, "ticker": ticker,
                    "side": "sell", "price": ask_price, "size": ask_size, "status": "submitted"
                })
                print(f"    >> SUBMITTED SELL {ask_size} @ {ask_price:.2f} (offload long, inv={inventory})")
            else:
                print(f"    >> SELL side OFF (offload long)")
        elif inventory < 0:
            if bid_price != 0:
                helper.submit_limit_order(trader, ticker, "buy", bid_size, bid_price)
                order_log.append({
                    "submit_time": current_time, "ticker": ticker,
                    "side": "buy", "price": bid_price, "size": bid_size, "status": "submitted"
                })
                print(f"    >> SUBMITTED BUY {bid_size} @ {bid_price:.2f} (offload short, inv={inventory})")
            else:
                print(f"    >> BUY side OFF (offload short)")
    else:
        if bid_price != 0:
            helper.submit_limit_order(trader, ticker, "buy", bid_size, bid_price)
            order_log.append({
                "submit_time": current_time, "ticker": ticker,
                "side": "buy", "price": bid_price, "size": bid_size, "status": "submitted"
            })
            print(f"    >> SUBMITTED BUY  {bid_size} @ {bid_price:.2f}")
        else:
            print(f"    >> BUY side OFF (long inventory)")

        if not math.isinf(ask_price):
            helper.submit_limit_order(trader, ticker, "sell", ask_size, ask_price)
            order_log.append({
                "submit_time": current_time, "ticker": ticker,
                "side": "sell", "price": ask_price, "size": ask_size, "status": "submitted"
            })
            print(f"    >> SUBMITTED SELL {ask_size} @ {ask_price:.2f}")
        else:
            print(f"    >> SELL side OFF (short inventory)")


# ─────────────────────────────────────────────────────────────
# EMERGENT MARKET MAKING
# ─────────────────────────────────────────────────────────────

def emergentMarketMaking(trader, current_time, end_time, number):
    threshold = 10
    if end_time - current_time <= dt.timedelta(minutes=threshold):
        tradeCount = helper.tradeCount(trader)
        if tradeCount < number:
            print(f"  Trades: {tradeCount}/{number} — need {number - tradeCount} more!")
            return True
        else:
            print(f"  Trades: {tradeCount}/{number} — target reached!")
            return False
    else:
        return False


def emergentMarketMakingDecision(trader, number, ticker, state):
    TICK = 0.01
    quote_price = state['quote_price']
    best_bid = state["best_bid"]
    best_ask = state["best_ask"]
    volume = state["quantity"]

    if quote_price == 0 or best_bid == 0 or best_ask == 0:
        return None

    bid_quote = round(quote_price - 2 * TICK, 2)
    ask_quote = round(quote_price + 2 * TICK, 2)

    bid = (bid_quote, volume)
    ask = (ask_quote, volume)
    return bid, ask, 0.0


# ─────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────

def merge_orders(orders1, orders2, descending=True):
    combined = defaultdict(int)
    for o in orders1 + orders2:
        combined[o.price] += o.size
    return sorted(combined.items(), key=lambda x: x[0], reverse=descending)


def precise_sleep(interval):
    if interval > 0:
        time.sleep(interval)


# ─────────────────────────────────────────────────────────────
# PROCESS TICKER
# ─────────────────────────────────────────────────────────────

def process_ticker(trader, ticker, state, order_log, current_time, fixed_end, fixed_start,
                   action, inventory=0, emergent=False):
    if action == "EMERGENT MM":
        result = emergentMarketMakingDecision(trader, 200, ticker, state)
    else:
        result = marketMakingDecision(trader, current_time, ticker, state, order_log, fixed_end, fixed_start)

    if result is None:
        print(f"\n  [{action} — SKIPPED] {ticker}  (no data or crossed quotes)")
        state['submitted_ask_price'].append(0)
        state['submitted_bid_price'].append(0)
        return

    bid_side, ask_side, signal = result
    print_ticker_action(ticker, action, state, bid_side[0], ask_side[0], bid_side[1], signal)

    offload = (action == "LIQUIDATING")
    marketMakingExecution(trader, ticker, bid_side, ask_side, order_log, current_time,
                          inventory=inventory, offload=offload, emergent=emergent)

    state['submitted_ask_price'].append(ask_side[0])
    state['submitted_bid_price'].append(bid_side[0])


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────

def main():
    trader = shift.Trader(TRADER_ID)
    connected = False
    try:
        trader.connect(CFG_FILE, PASSWORD)
        connected = True
    except Exception as e:
        print(f"Connection failed: {e}")
        return
    
    print("Sleep 1 minute to let the time be updated")
    time.sleep(60) 
   

    ticker_list = trader.get_stock_list()
    numberOfTrade = len(ticker_list)
    trader.sub_all_order_book()
    stock_list = {}
    order_log = []
    counter = 0

    while counter < 5:
        print("Initialization Protocol")
        current_time = trader.get_last_trade_time()
        current_time = current_time.replace(microsecond=0)
        print(f"Current time is {current_time}")
        counter += 1
        time.sleep(1)

    print("Initialization Complete")
    last_trade_time = trader.get_last_trade_time()
    sim_date = last_trade_time.date()
    start_time = last_trade_time.time()
    real_start = time.perf_counter()

    testTime = dt.time(hour=16, minute=00)
    end_time = dt.datetime.combine(sim_date, testTime)

    current_time = dt.datetime.combine(sim_date, start_time)
    last_time = current_time

    fixed_start = current_time
    fixed_end = end_time

    print(f"Simulation starts at: {fixed_start}")
    print(f"Simulation ends at:   {fixed_end}")
    print()

    try:
        while True:
            tick = time.perf_counter()
            elapsed = tick - real_start
            current_time = dt.datetime.combine(sim_date, start_time) + dt.timedelta(seconds=elapsed)
            current_time = current_time.replace(microsecond=0)

            if current_time != last_time:
                last_time = current_time
                print("\n" + "*" * 60)
                print(f"  {current_time}")
                print("*" * 60)

                print_orders(trader)

                # ── Update state for all tickers ──
                for ticker in ticker_list:
                    if ticker not in stock_list:
                        stock_list[ticker] = {
                            "time": current_time,
                            "times": [],
                            "last_prices": [],
                            "last_price": 0,
                            "mid_prices": [],
                            "mid_price": 0,
                            "quote_prices": [],
                            "quote_price": 0,
                            "order_book_skews": [],
                            "order_book_skew": 0,
                            "best_bid": 0,
                            "best_bids": [],
                            "best_ask": 0,
                            "best_asks": [],
                            "quantity": 1,
                            "submitted_ask_price": [],
                            "submitted_bid_price": [],
                            "inventories": [],
                            "inventory": 0,
                            "actions": [],
                        }

                    state = stock_list[ticker]

                    try:
                        bid_orders = trader.get_order_book(ticker, shift.OrderBookType.GLOBAL_BID) or []
                        ask_orders = trader.get_order_book(ticker, shift.OrderBookType.GLOBAL_ASK) or []
                    except Exception as e:
                        print(f"  Order book unavailable for {ticker}: {e}")
                        break

                    l_bid_orders = []
                    l_ask_orders = []

                    bid_levels = merge_orders(bid_orders, l_bid_orders, descending=True)
                    ask_levels = merge_orders(ask_orders, l_ask_orders, descending=False)

                    bid_prices, bid_volumes = map(list, zip(*bid_levels)) if bid_levels else ([], [])
                    ask_prices, ask_volumes = map(list, zip(*ask_levels)) if ask_levels else ([], [])

                    last_price = trader.get_last_price(ticker) or 0
                    last_size = trader.get_last_size(ticker) or 0
                    last_trade = (last_price, last_size)

                    best_bid = max(bid_orders + l_bid_orders, key=lambda o: o.price, default=None)
                    best_ask = min(ask_orders + l_ask_orders, key=lambda o: o.price, default=None)

                    if best_bid and best_ask:
                        best_bid_price = best_bid.price
                        best_bid_size = best_bid.size
                        best_ask_price = best_ask.price
                        best_ask_size = best_ask.size
                    else:
                        best_bid_price = 0
                        best_bid_size = 0
                        best_ask_price = 0
                        best_ask_size = 0

                    best_prices = [(best_bid_price, best_bid_size), (best_ask_price, best_ask_size)]

                    if best_bid_price == 0 or best_ask_price == 0:
                        mid_price = 0
                    else:
                        mid_price = (best_bid_price + best_ask_price) / 2

                    if midNotLast:
                        ask_score = helper.bookAnalysisScore(np.array(ask_prices), np.array(ask_volumes), mid_price) or 0
                        bid_score = helper.bookAnalysisScore(np.array(bid_prices), np.array(bid_volumes), mid_price) or 0
                    else:
                        ask_score = helper.bookAnalysisScore(np.array(ask_prices), np.array(ask_volumes), last_price) or 0
                        bid_score = helper.bookAnalysisScore(np.array(bid_prices), np.array(bid_volumes), last_price) or 0

                    if bid_score == 0:
                        skew = -float('inf') if ask_score != 0 else 0
                    elif ask_score == 0:
                        skew = float("inf")
                    elif bid_score > ask_score:
                        skew = bid_score / ask_score
                    else:
                        skew = -ask_score / bid_score

                    if not math.isinf(skew):
                        quote_price = quote(best_prices, last_trade, mid_price)
                    else:
                        quote_price = 0

                    state['time'] = current_time
                    state["times"].append(current_time)
                    state['last_prices'].append(last_price)
                    state['last_price'] = last_price
                    state['mid_prices'].append(mid_price)
                    state['mid_price'] = mid_price
                    state['quote_prices'].append(quote_price)
                    state['quote_price'] = quote_price

                    skew_to_save = 0 if math.isinf(skew) else skew
                    state['order_book_skews'].append(skew_to_save)
                    state['order_book_skew'] = skew_to_save

                    state["best_bid"] = best_bid_price
                    state["best_ask"] = best_ask_price
                    state['best_bids'].append(best_bid_price)
                    state['best_asks'].append(best_ask_price)

                    item = trader.get_portfolio_item(ticker)
                    position = item.get_shares()
                    state["inventories"].append(position)
                    state["inventory"] = position

                # ── Trading Logic ──
                checker = emergentMarketMaking(trader, current_time, fixed_end, 200)

                if checker:
                    print_mode_header("EMERGENT MODE", current_time)
                    candidates = sorted(stock_list.keys(),
                                        key=lambda x: abs(stock_list[x]['order_book_skew']))[:numberOfTrade]
                else:
                    print_mode_header("NORMAL MODE", current_time)
                    candidates = sorted(stock_list.keys(),
                                        key=lambda x: abs(stock_list[x]['order_book_skew']))[:numberOfTrade]

                for ticker in stock_list:
                    state = stock_list[ticker]

                    if ticker in candidates:
                        if checker:
                            state['actions'].append("EMERGENT MM")
                            process_ticker(trader, ticker, state, order_log, current_time,
                                           fixed_end, fixed_start, action="EMERGENT MM", emergent=True)
                        else:
                            state['actions'].append("MARKET MAKING")
                            process_ticker(trader, ticker, state, order_log, current_time,
                                           fixed_end, fixed_start, action="MARKET MAKING")
                    else:
                        inventory = state['inventory']
                        if inventory == 0:
                            state['actions'].append("IDLE")
                            state['submitted_ask_price'].append(0)
                            state['submitted_bid_price'].append(0)
                        else:
                            state['actions'].append("LIQUIDATING")
                            process_ticker(trader, ticker, state, order_log, current_time,
                                           fixed_end, fixed_start, action="LIQUIDATING",
                                           inventory=inventory)

            tick_duration = time.perf_counter() - tick
            precise_sleep(max(0.0, TICK_INTERVAL - tick_duration))

    except KeyboardInterrupt:
        print("\n\nStopping....")
    finally:
        os.makedirs("tickers_data", exist_ok=True)

        if order_log:
            order_df = pd.DataFrame(order_log)
            order_df.to_excel("tickers_data/order_log.xlsx", index=False)
            print("Exported order log")

        all_frames = []
        for ticker, state in stock_list.items():
            n = len(state['times'])
            df = pd.DataFrame({
                "ticker": ticker,
                "time": state['times'][:n],
                "action": state['actions'][:n],
                "last_price": state['last_prices'][:n],
                "mid_price": state['mid_prices'][:n],
                "quote_price": state['quote_prices'][:n],
                "order_book_skew": state['order_book_skews'][:n],
                "best_ask": state["best_asks"][:n],
                "best_bid": state["best_bids"][:n],
                "submitted_ask": state["submitted_ask_price"][:n],
                "submitted_bid": state["submitted_bid_price"][:n],
                "inventories": state["inventories"][:n],
            })
            all_frames.append(df)

        if all_frames:
            combined = pd.concat(all_frames, ignore_index=True)
            combined = combined.sort_values(["time", "ticker"]).set_index("time")
            combined.to_excel("tickers_data/all_tickers.xlsx")
            print("Exported all_tickers.xlsx")

        if connected:
            try:
                trader.disconnect()
                print("Disconnected.")
            except Exception as e:
                print(f"Disconnect failed: {e}")


if __name__ == "__main__":
    main()