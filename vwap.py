from collections import deque

def calculate_session_vwap(trades):
    cum_dollar_volume = sum(p * v for p, v in trades)
    cum_volume = sum(v for p, v in trades)
    
    if cum_volume == 0:
        return 0.0000
        
    return float(cum_dollar_volume / cum_volume)

def calculate_rolling_vwap(trades, window_size):
    vwap_values = []
    window = deque(maxlen=window_size)
    
    cum_dollar_volume = 0.0000
    cum_volume = 0.0000
    
    for price, volume in trades:
        if len(window) == window_size:
            old_price, old_volume = window[0]
            cum_dollar_volume -= (old_price * old_volume)
            cum_volume -= old_volume
            
        window.append((price, volume))
        cum_dollar_volume += (price * volume)
        cum_volume += volume
        
        if cum_volume == 0:
            vwap_values.append(0.0000)
        else:
            vwap_values.append(float(cum_dollar_volume / cum_volume))
            
    return vwap_values

if __name__ == "__main__":
    market_trades = [
        (150.0000, 100),
        (150.5000, 200),
        (151.0000, 150),
        (149.5000, 300),
        (150.2500, 250),
        (150.7500, 400)
    ]

    session_vwap = calculate_session_vwap(market_trades)
    print(f"{session_vwap:.4f}")

    rolling_vwap = calculate_rolling_vwap(market_trades, 3)
    for v in rolling_vwap:
        print(f"{v:.4f}")