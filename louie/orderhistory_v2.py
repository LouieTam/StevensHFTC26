import shift
import time

def print_order_book(trader, ticker):
    last_ob_time = time.time()
    
    while True:
        current_time = time.time()
        
        if current_time - last_ob_time >= 0.2000:
            bids = trader.get_order_book(ticker, shift.OrderBookType.GLOBAL_BID)
            asks = trader.get_order_book(ticker, shift.OrderBookType.GLOBAL_ASK)
            
            row = [f"{current_time:.4f}"]
            
            for i in range(10):
                if i < len(bids):
                    row.extend([f"{float(bids[i].price):.4f}", bids[i].size])
                else:
                    row.extend(["0.0000", 0])
                    
            for i in range(10):
                if i < len(asks):
                    row.extend([f"{float(asks[i].price):.4f}", asks[i].size])
                else:
                    row.extend(["0.0000", 0])
                    
            print(row)
            last_ob_time = current_time
            
        time.sleep(0.0100)

if __name__ == "__main__":
    with shift.Trader("columbia-traders") as trader:
        trader.connect("initiator.cfg", "aRkkZSrj")
        
        time.sleep(3.0000)
        trader.sub_all_order_book()
        time.sleep(3.0000)
        
        try:
            print_order_book(trader, "GS")
        except KeyboardInterrupt:
            pass