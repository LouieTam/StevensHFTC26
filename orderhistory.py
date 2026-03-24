import shift
import time
import csv

def record_market_data(trader, ticker):
    with open('trades_nov02.csv', mode='w', newline='') as trades_file, \
         open('order_book_nov02.csv', mode='w', newline='') as ob_file:
        
        trades_writer = csv.writer(trades_file)
        ob_writer = csv.writer(ob_file)
        
        trades_writer.writerow(['timestamp', 'price', 'size'])
        
        ob_header = ['timestamp']
        for i in range(10):
            ob_header.extend([f'bid_price_{i+1}', f'bid_size_{i+1}'])
        for i in range(10):
            ob_header.extend([f'ask_price_{i+1}', f'ask_size_{i+1}'])
        ob_writer.writerow(ob_header)
        
        last_trade_time = trader.get_last_trade_time()
        last_ob_time = time.time()
        
        while True:
            current_trade_time = trader.get_last_trade_time()
            if current_trade_time > last_trade_time:
                price = float(trader.get_last_price(ticker))
                size = trader.get_last_size(ticker)
                if size > 0:
                    trades_writer.writerow([current_trade_time, f"{price:.4f}", size])
                last_trade_time = current_trade_time
            
            #current_time = time.time()
            #if current_time - last_ob_time >= 1.0000:
                bids = trader.get_order_book(ticker, shift.OrderBookType.GLOBAL_BID)
                asks = trader.get_order_book(ticker, shift.OrderBookType.GLOBAL_ASK)
                
                row = [current_trade_time]
                
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
                        
                ob_writer.writerow(row)
                #last_ob_time = current_time
                
            time.sleep(0.0100)

if __name__ == "__main__":
    with shift.Trader("columbia-traders") as trader:
        trader.connect("initiator.cfg", "aRkkZSrj")
        
        time.sleep(3.0000)
        trader.sub_all_order_book()
        time.sleep(3.0000)
        
        try:
            record_market_data(trader, "GS")
        except KeyboardInterrupt:
            pass