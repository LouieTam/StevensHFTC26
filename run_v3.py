import shift
from time import sleep
from datetime import timedelta, datetime

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
    price_improvement = 0.01
    liquidation_attempts = 0

    while datetime.now() < end_time:
        item = trader.get_portfolio_item(ticker)
        long_lots = int(item.get_long_shares() / 100)
        short_lots = int(item.get_short_shares() / 100)

        best_price = trader.get_best_price(ticker)
        best_bid = best_price.get_bid_price()
        best_ask = best_price.get_ask_price()

        my_open_buy = None
        my_open_sell = None
        for order in trader.get_waiting_list():
            if order.symbol == ticker:
                if order.type == shift.Order.Type.LIMIT_BUY:
                    my_open_buy = order
                elif order.type == shift.Order.Type.LIMIT_SELL:
                    my_open_sell = order

        current_wait = 3

        if best_bid > 0 and best_ask > 0:
            my_bid_price = best_bid + price_improvement
            my_ask_price = best_ask - price_improvement

            if my_bid_price < my_ask_price:
                if long_lots > 0:
                    if my_open_sell and abs(my_open_sell.price - best_ask) < 0.001:
                        current_wait = 2
                        print(f"[{datetime.now()}] Inventory LONG. Order already at top of book (ASK @ {my_open_sell.price:.2f}). Waiting...", flush=True)
                    else:
                        cancel_orders(trader, ticker)
                        liquidation_attempts += 1
                        current_wait = 2
                        
                        if liquidation_attempts > 10:
                            order = shift.Order(shift.Order.Type.MARKET_SELL, ticker, long_lots)
                            trader.submit_order(order)
                            print(f"[{datetime.now()}] Liquidation timeout. MARKET SELL {long_lots}", flush=True)
                            liquidation_attempts = 0
                        else:
                            sell_order = shift.Order(shift.Order.Type.LIMIT_SELL, ticker, long_lots, my_ask_price)
                            trader.submit_order(sell_order)
                            print(f"[{datetime.now()}] Inventory LONG. Quoting ASK {long_lots} @ {my_ask_price:.2f}", flush=True)

                elif short_lots > 0:
                    if my_open_buy and abs(my_open_buy.price - best_bid) < 0.001:
                        current_wait = 1
                        print(f"[{datetime.now()}] Inventory SHORT. Order already at top of book (BID @ {my_open_buy.price:.2f}). Waiting...", flush=True)
                    else:
                        cancel_orders(trader, ticker)
                        liquidation_attempts += 1
                        current_wait = 1
                        
                        if liquidation_attempts > 10:
                            order = shift.Order(shift.Order.Type.MARKET_BUY, ticker, short_lots)
                            trader.submit_order(order)
                            print(f"[{datetime.now()}] Liquidation timeout. MARKET BUY {short_lots}", flush=True)
                            liquidation_attempts = 0
                        else:
                            buy_order = shift.Order(shift.Order.Type.LIMIT_BUY, ticker, short_lots, my_bid_price)
                            trader.submit_order(buy_order)
                            print(f"[{datetime.now()}] Inventory SHORT. Quoting BID {short_lots} @ {my_bid_price:.2f}", flush=True)

                elif (best_ask - best_bid) > 0.05:
                    cancel_orders(trader, ticker)
                    liquidation_attempts = 0
                    
                    buy_order = shift.Order(shift.Order.Type.LIMIT_BUY, ticker, order_size, my_bid_price)
                    trader.submit_order(buy_order)
                    
                    sell_order = shift.Order(shift.Order.Type.LIMIT_SELL, ticker, order_size, my_ask_price)
                    trader.submit_order(sell_order)
                    
                    print(f"[{datetime.now()}] Inventory FLAT. Quoting BID {order_size} @ {my_bid_price:.2f} | ASK {order_size} @ {my_ask_price:.2f}", flush=True)
                else:
                    cancel_orders(trader, ticker)
        else:
            cancel_orders(trader, ticker)

        sleep(current_wait)

    cancel_orders(trader, ticker)
    close_positions(trader, ticker)

    final_pl = trader.get_portfolio_item(ticker).get_realized_pl() - initial_pl
    print(f"Total realized profits/losses for {ticker}: {final_pl:.2f}", flush=True)

def main(trader):
    ticker = "SHW"
    start_time = datetime.now()
    end_time = start_time + timedelta(seconds=22000)

    market_maker_strategy(trader, ticker, end_time)

if __name__ == "__main__":
    with shift.Trader("columbia-traders") as trader:
        trader.connect("initiator.cfg", "aRkkZSrj")
        sleep(1)
        trader.sub_all_order_book()
        sleep(1)

        main(trader)