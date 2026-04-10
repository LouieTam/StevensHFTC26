
import numpy as np
import math
import shift



# shift.Order.Type.LIMIT_BUY
# shift.Order.Type.LIMIT_SELL
# shift.Order.Type.MARKET_BUY
# shift.Order.Type.MARKET_SELL
# shift.Order.Type.CANCEL_BID
# shift.Order.Type.CANCEL_ASK


def sigmoid(value,k,c,s):
    return s/(1+math.exp(-k*value)) - c

def sigmoidNew(value,x_shift,y_shift,x_scale,exp_scale):
    return exp_scale/(1+math.exp(-x_scale*(value-x_shift))) - y_shift
    


def buyingPower(trader): 
    return trader.get_portfolio_summary().get_total_bp()


def exposure(trader,ticker):
    item= trader.get_portfolio_item(ticker)
    exposure = item.get_shares()* item.get_price()
    return exposure
    

def tradeCount(trader):
    counter=0
    for order in trader.get_submitted_orders():
        if order.status == shift.Order.Status.FILLED:
            counter+=1
    
    return counter
    
    
def submit_limit_order(trader, ticker, action, size, price):
    if(action.lower()=="buy"):
        order_type= shift.Order.Type.LIMIT_BUY
    else:
        order_type=shift.Order.Type.LIMIT_SELL
        
    order = shift.Order(order_type,ticker,size,price)
    trader.submit_order(order)
    return order


def submit_order(trader,ticker,type, size, price):
    
    if(type=="market"):
        if(size>0):
            order_type= shift.Order.Type.MARKET_BUY
        else:
            order_type=shift.Order.Type.MARKET_SELL
        
        order= shift.Order(order_type,ticker,abs(size)) ## order ID 
    
    else:
        if(size>0):
            order_type=shift.Order.Type.LIMIT_BUY
        else:
            order_type=shift.Order.Type.LIMIT_SELL
        order= shift.Order(order_type,ticker,abs(size),price)
        
    trader.submit_order(order)
    # print(f"Submitted {type} order for {ticker}, for quantity {size}")
    return


def tradingPosition(trader, ticker):
    item = trader.get_portfolio_item(ticker)
    return abs(item.get_shares()*item.get_price())

def z_score(values):
    values = np.asarray(values, dtype=float)
    
    if len(values) == 0:
        return 0.0
    
    std = np.std(values)
    if std == 0 or np.isnan(std):
        return 0.0
    
    last_value = values[-1]
    return (last_value - np.mean(values)) / std

def bookAnalysisScore(prices, volumes, priceTarget,k=0.5):
    
    if(priceTarget==0):
        return 0

## measure distance from curent price 
    distances = np.abs(prices - priceTarget)
    weights = 1 / (1 + np.exp(k * distances))

    return np.sum(volumes * weights)


def computeStats(prices,volumes):
## required ordered np list of prices and volume 
        cumVolume = np.sum(volumes)
        if cumVolume == 0:
            mu=0.0
            var=0.0
            skew=0.0
            kurtosis=0.0
        else:
            p_density = volumes / cumVolume
            mu = p_density @ prices
            var = p_density @ ((prices-mu)**2)
            std= math.sqrt(var)

            if std > 0:
                z_scores = (prices - mu) / std
                skew = p_density @ (z_scores**3)
                kurtosis = p_density @ (z_scores**4)
            else:
                # If std is 0, all volume is at one price; skew/kurtosis are undefined
                skew, kurtosis = 0.0, 0.0
        return {
            "mean":mu,
            "variance":var,
            "skew":skew,
            "kurtosis":kurtosis
        }

# def ema_update(prev, x, span):
#     alpha = 2.0 / (span + 1.0)
#     return x if prev is None else (alpha * x + (1 - alpha) * prev)
                
def ema(values, span):
    values = np.asarray(values, dtype=float)
    
    if len(values) == 0:
        return None
    
    alpha = 2 / (span + 1)
    ema_value = values[0]
    
    for i in range(1, len(values)):
        ema_value = alpha * values[i] + (1 - alpha) * ema_value
    
    return ema_value

            