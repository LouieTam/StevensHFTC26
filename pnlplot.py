import shift
from time import sleep
from datetime import timedelta

def cancel_orders(trader, ticker):
    for order in trader.get_waiting_list():
        if order.symbol == ticker:
            trader.submit_cancellation(order)
    sleep(1)

def close_positions(trader, ticker):
    item = trader.get_portfolio_item(ticker)
    
    long_shares = item.get_long_shares()
    if long_shares > 0:
        order = shift.Order(shift.Order.Type.MARKET_SELL, ticker, int(long_shares / 100))
        trader.submit_order(order)
        sleep(1)

    short_shares = item.get_short_shares()
    if short_shares > 0:
        order = shift.Order(shift.Order.Type.MARKET_BUY, ticker, int(short_shares / 100))
        trader.submit_order(order)
        sleep(1)

def market_maker_strategy(trader: shift.Trader, ticker: str, end_time):
    initial_pl = trader.get_portfolio_item(ticker).get_realized_pl()
    
    order_size = 1
    wait_time = 3
    price_improvement = 0.01

    while trader.get_last_trade_time() < end_time:
        cancel_orders(trader, ticker)

        item = trader.get_portfolio_item(ticker)
        long_lots = int(item.get_long_shares() / 100)
        short_lots = int(item.get_short_shares() / 100)

        best_price = trader.get_best_price(ticker)
        best_bid = best_price.get_bid_price()
        best_ask = best_price.get_ask_price()

        if best_bid > 0 and best_ask > 0 and best_ask > best_bid:
            my_bid_price = best_bid + price_improvement
            my_ask_price = best_ask - price_improvement

            if my_bid_price < my_ask_price:
                if long_lots > 0:
                    sell_order = shift.Order(shift.Order.Type.LIMIT_SELL, ticker, long_lots, my_ask_price)
                    trader.submit_order(sell_order)
                    print(f"[{trader.get_last_trade_time()}] Inventory LONG. Quoting ASK {long_lots} @ {my_ask_price:.2f}", flush=True)
                
                elif short_lots > 0:
                    buy_order = shift.Order(shift.Order.Type.LIMIT_BUY, ticker, short_lots, my_bid_price)
                    trader.submit_order(buy_order)
                    print(f"[{trader.get_last_trade_time()}] Inventory SHORT. Quoting BID {short_lots} @ {my_bid_price:.2f}", flush=True)
                
                else:
                    buy_order = shift.Order(shift.Order.Type.LIMIT_BUY, ticker, order_size, my_bid_price)
                    trader.submit_order(buy_order)
                    
                    sell_order = shift.Order(shift.Order.Type.LIMIT_SELL, ticker, order_size, my_ask_price)
                    trader.submit_order(sell_order)
                    
                    print(f"[{trader.get_last_trade_time()}] Inventory FLAT. Quoting BID {order_size} @ {my_bid_price:.2f} | ASK {order_size} @ {my_ask_price:.2f}", flush=True)

        sleep(wait_time)

    cancel_orders(trader, ticker)
    close_positions(trader, ticker)

    final_pl = trader.get_portfolio_item(ticker).get_realized_pl() - initial_pl
    print(f"Total realized profits/losses for {ticker}: {final_pl:.2f}", flush=True)

def main(trader):
    ticker = "GS"
    current = trader.get_last_trade_time()
    start_time = current
    
    end_time = start_time + timedelta(seconds=300)

    while trader.get_last_trade_time() < start_time:
        sleep(1)

    market_maker_strategy(trader, ticker, end_time)

if __name__ == "__main__":
    with shift.Trader("columbia-traders") as trader:
        trader.connect("initiator.cfg", "aRkkZSrj")
        sleep(1)
        trader.sub_all_order_book()
        sleep(1)

        main(trader)