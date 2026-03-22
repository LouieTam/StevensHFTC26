import shift
import time
from datetime import datetime

def print_top_order_book(trader, ticker, levels=10):
    bids = trader.get_order_book(ticker, shift.OrderBookType.GLOBAL_BID)
    asks = trader.get_order_book(ticker, shift.OrderBookType.GLOBAL_ASK)

    top_bids = bids[:levels]
    top_asks = asks[:levels]

    print(f"\n--- Order Book for {ticker} at {datetime.now().strftime('%H:%M:%S')} ---")
    
    print(f"{'Ask Size':>10} | {'Ask Price':>10}")
    print("-" * 25)
    for ask in reversed(top_asks):
        print(f"{ask.size:>10} | {ask.price:>10.4f}")
        
    print("-" * 25)
    
    print(f"{'Bid Price':>10} | {'Bid Size':>10}")
    print("-" * 25)
    for bid in top_bids:
        print(f"{bid.price:>10.4f} | {bid.size:>10}")
        
    print("-" * 25)

def main():
    ticker = "GS" 
    
    with shift.Trader("columbia-traders") as trader:
        trader.connect("initiator.cfg", "aRkkZSrj")
        time.sleep(1)
        
        trader.sub_all_order_book()
        time.sleep(1)
        
        try:
            while True:
                print_top_order_book(trader, ticker, levels=10)
                time.sleep(1)
        except KeyboardInterrupt:
            trader.disconnect()

if __name__ == "__main__":
    main()