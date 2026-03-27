import shift
import time
from collections import deque
from datetime import datetime, timedelta

# =========================
# CONFIG
# =========================
SYMBOL = "GS"

# Book / signal
LEVELS = 10
POLL_INTERVAL = 1.0
OFI_WINDOW_SECONDS = 5.0
EMA_ALPHA = 0.3
OFI_FLOOR = 20.0

LEVEL_WEIGHTS = [1.0, 0.9, 0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2, 0.1]
PERSISTENCE_LOOKBACK = 5
PERSISTENCE_REQUIRED = 4
FINAL_SCORE_THRESHOLD = 0.5

# Regime detection
REGIME_STREAK_REQUIRED = 3

# Market making
LOT_SIZE = 100
BASE_QTY_LOTS = 1
MAX_NET_POSITION = 5

MIN_SPREAD = 0.04
TICK_SIZE = 0.01
REFRESH_INTERVAL_SECONDS = 2.0

# Neutral regime quoting around mid
NEUTRAL_HALF_SPREAD_CAPTURE = 0.07

# Regime skew
UP_BID_IMPROVEMENT = 0.02
UP_ASK_WIDEN = 0.03
DOWN_ASK_IMPROVEMENT = 0.02
DOWN_BID_WIDEN = 0.03

# Inventory skew around regime-dependent target inventory
INVENTORY_SKEW_PER_LOT = 0.01
TARGET_INV_NEUTRAL = 0
TARGET_INV_UP = 2
TARGET_INV_DOWN = -2

EPS = 1e-12


# =========================
# BOOK / SIGNAL HELPERS
# =========================
def parse_shift_book(trader, symbol, levels):
    bids_obj = trader.get_order_book(symbol, shift.OrderBookType.GLOBAL_BID)
    asks_obj = trader.get_order_book(symbol, shift.OrderBookType.GLOBAL_ASK)

    bids = [(float(b.price), float(b.size)) for b in bids_obj[:levels]]
    asks = [(float(a.price), float(a.size)) for a in asks_obj[:levels]]

    return bids, asks


def ema_update(prev_ema, new_value, alpha):
    if prev_ema is None:
        return float(new_value)
    return float(alpha * new_value + (1.0 - alpha) * prev_ema)


def pad_book_side(levels_list, target_levels):
    padded = list(levels_list[:target_levels])
    while len(padded) < target_levels:
        padded.append((0.0, 0.0))
    return padded


def compute_level_ofi(prev_bid_p, prev_bid_q, prev_ask_p, prev_ask_q,
                      new_bid_p, new_bid_q, new_ask_p, new_ask_q):
    bid_term = (
        (new_bid_q if new_bid_p >= prev_bid_p else 0.0)
        - (prev_bid_q if new_bid_p <= prev_bid_p else 0.0)
    )

    ask_term = (
        -(new_ask_q if new_ask_p <= prev_ask_p else 0.0)
        + (prev_ask_q if new_ask_p >= prev_ask_p else 0.0)
    )

    return float(bid_term + ask_term)


def compute_multilevel_ofi_increment(prev_bids, prev_asks, new_bids, new_asks, levels):
    prev_bids = pad_book_side(prev_bids, levels)
    prev_asks = pad_book_side(prev_asks, levels)
    new_bids = pad_book_side(new_bids, levels)
    new_asks = pad_book_side(new_asks, levels)

    level_increments = []

    for m in range(levels):
        prev_bid_p, prev_bid_q = prev_bids[m]
        prev_ask_p, prev_ask_q = prev_asks[m]
        new_bid_p, new_bid_q = new_bids[m]
        new_ask_p, new_ask_q = new_asks[m]

        e_m = compute_level_ofi(
            prev_bid_p, prev_bid_q, prev_ask_p, prev_ask_q,
            new_bid_p, new_bid_q, new_ask_p, new_ask_q
        )
        level_increments.append(e_m)

    return level_increments


def prune_old_entries(deq, now_ts, window_seconds):
    cutoff = now_ts - window_seconds
    while deq and deq[0][0] < cutoff:
        deq.popleft()


def rolling_level_ofi(ofi_events, levels):
    totals = [0.0] * levels
    for _, vec in ofi_events:
        for m in range(levels):
            totals[m] += vec[m]
    return totals


def weighted_raw_ofi(level_ofi, weights):
    return float(sum(w * x for w, x in zip(weights, level_ofi)))


def ema_direction(ema_value, ofi_floor):
    if ema_value > ofi_floor:
        return 1
    if ema_value < -ofi_floor:
        return -1
    return 0


def persistence_stats(raw_ofi_history):
    pos = sum(1 for x in raw_ofi_history if x > 0)
    neg = sum(1 for x in raw_ofi_history if x < 0)
    zero = len(raw_ofi_history) - pos - neg
    persistence_score = (pos - neg) / max(len(raw_ofi_history), 1)
    return pos, neg, zero, persistence_score


def classify_signal(ema_dir, pos_count, neg_count, persistence_score,
                    persistence_required, final_score_threshold):
    if ema_dir == 1 and pos_count >= persistence_required:
        final_score = 0.7 * 1.0 + 0.3 * persistence_score
    elif ema_dir == -1 and neg_count >= persistence_required:
        final_score = -0.7 + 0.3 * persistence_score
    else:
        final_score = 0.3 * persistence_score

    if final_score > final_score_threshold:
        return "BUY_PRESSURE", final_score
    if final_score < -final_score_threshold:
        return "SELL_PRESSURE", final_score
    return "NEUTRAL", final_score


# =========================
# SHIFT ORDER HELPERS
# =========================
def get_position_lots(trader, symbol):
    item = trader.get_portfolio_item(symbol)
    long_lots = int(item.get_long_shares() / LOT_SIZE)
    short_lots = int(item.get_short_shares() / LOT_SIZE)
    net_lots = long_lots - short_lots
    return long_lots, short_lots, net_lots


def get_open_orders(trader, symbol):
    buy_orders = []
    sell_orders = []

    for order in trader.get_waiting_list():
        if order.symbol == symbol:
            if order.type == shift.Order.Type.LIMIT_BUY:
                buy_orders.append(order)
            elif order.type == shift.Order.Type.LIMIT_SELL:
                sell_orders.append(order)

    return buy_orders, sell_orders


def cancel_order_list(trader, orders):
    for order in orders:
        trader.submit_cancellation(order)


def cancel_all_symbol_orders(trader, symbol):
    buy_orders, sell_orders = get_open_orders(trader, symbol)
    cancel_order_list(trader, buy_orders + sell_orders)


def keep_only_one_best_order(order_list, is_buy=True):
    if not order_list:
        return None, []

    if is_buy:
        best_order = max(order_list, key=lambda o: o.price)
    else:
        best_order = min(order_list, key=lambda o: o.price)

    extras = [o for o in order_list if o.id != best_order.id]
    return best_order, extras


def needs_requote(existing_order, desired_price, desired_size, price_tol=0.001):
    if desired_size == 0:
        return existing_order is not None

    if existing_order is None:
        return True

    price_changed = abs(existing_order.price - desired_price) > price_tol
    size_changed = existing_order.size != desired_size

    return price_changed or size_changed


def submit_quote_if_needed(trader, symbol, side, existing_order, desired_price, desired_size):
    if desired_size <= 0:
        if existing_order is not None:
            trader.submit_cancellation(existing_order)
        return

    if existing_order is None:
        if side == "buy":
            order = shift.Order(shift.Order.Type.LIMIT_BUY, symbol, desired_size, desired_price)
        else:
            order = shift.Order(shift.Order.Type.LIMIT_SELL, symbol, desired_size, desired_price)
        trader.submit_order(order)
        return

    if needs_requote(existing_order, desired_price, desired_size):
        trader.submit_cancellation(existing_order)
        time.sleep(0.2)
        if side == "buy":
            order = shift.Order(shift.Order.Type.LIMIT_BUY, symbol, desired_size, desired_price)
        else:
            order = shift.Order(shift.Order.Type.LIMIT_SELL, symbol, desired_size, desired_price)
        trader.submit_order(order)


# =========================
# QUOTING LOGIC
# =========================
def round_price(px):
    return round(px, 2)


def determine_regime(buy_streak, sell_streak):
    if buy_streak >= REGIME_STREAK_REQUIRED:
        return "UP"
    if sell_streak >= REGIME_STREAK_REQUIRED:
        return "DOWN"
    return "NEUTRAL"


def get_target_inventory(regime):
    if regime == "UP":
        return TARGET_INV_UP
    if regime == "DOWN":
        return TARGET_INV_DOWN
    return TARGET_INV_NEUTRAL


def compute_regime_quotes(best_bid, best_ask, net_lots, regime,
                          base_qty_lots=BASE_QTY_LOTS):
    spread = best_ask - best_bid
    mid = 0.5 * (best_bid + best_ask)

    if spread <= 0:
        return None

    bid_px = mid - NEUTRAL_HALF_SPREAD_CAPTURE
    ask_px = mid + NEUTRAL_HALF_SPREAD_CAPTURE

    if regime == "UP":
        bid_px += UP_BID_IMPROVEMENT
        ask_px += UP_ASK_WIDEN
    elif regime == "DOWN":
        bid_px -= DOWN_BID_WIDEN
        ask_px -= DOWN_ASK_IMPROVEMENT

    target_inventory = get_target_inventory(regime)
    inventory_error = net_lots - target_inventory

    reservation_shift = inventory_error * INVENTORY_SKEW_PER_LOT
    bid_px -= reservation_shift
    ask_px -= reservation_shift

    bid_px = min(bid_px, best_ask - TICK_SIZE)
    ask_px = max(ask_px, best_bid + TICK_SIZE)

    bid_px = round_price(bid_px)
    ask_px = round_price(ask_px)

    if bid_px >= ask_px:
        bid_px = round_price(best_bid)
        ask_px = round_price(best_ask)
        if bid_px >= ask_px:
            return None

    bid_size = base_qty_lots
    ask_size = base_qty_lots

    if regime == "UP":
        if net_lots < target_inventory:
            bid_size = min(base_qty_lots + 1, MAX_NET_POSITION)
            ask_size = base_qty_lots
        elif net_lots > target_inventory:
            bid_size = max(0, base_qty_lots - 1)
            ask_size = min(base_qty_lots + 1, MAX_NET_POSITION)

    elif regime == "DOWN":
        if net_lots > target_inventory:
            bid_size = base_qty_lots
            ask_size = min(base_qty_lots + 1, MAX_NET_POSITION)
        elif net_lots < target_inventory:
            bid_size = min(base_qty_lots + 1, MAX_NET_POSITION)
            ask_size = max(0, base_qty_lots - 1)

    else:
        if net_lots > 0:
            bid_size = max(0, base_qty_lots - min(net_lots, base_qty_lots))
            ask_size = min(base_qty_lots + net_lots, MAX_NET_POSITION)
        elif net_lots < 0:
            bid_size = min(base_qty_lots + abs(net_lots), MAX_NET_POSITION)
            ask_size = max(0, base_qty_lots - min(abs(net_lots), base_qty_lots))

    if net_lots >= MAX_NET_POSITION:
        bid_size = 0
    if net_lots <= -MAX_NET_POSITION:
        ask_size = 0

    return {
        "bid_price": bid_px,
        "ask_price": ask_px,
        "bid_size": bid_size,
        "ask_size": ask_size,
        "mid": mid,
        "spread": spread,
        "target_inventory": target_inventory,
        "inventory_error": inventory_error
    }


# =========================
# MAIN LOOP
# =========================
def run_signal_regime_market_maker(trader, symbol=SYMBOL, levels=LEVELS,
                                   poll_interval=POLL_INTERVAL, end_time=None):
    prev_bids = None
    prev_asks = None

    ofi_events = deque()
    raw_ofi_history = deque(maxlen=PERSISTENCE_LOOKBACK)

    ema_t = None
    buy_streak = 0
    sell_streak = 0
    step = 0
    last_refresh_ts = None

    while datetime.now() < end_time:
        loop_start = time.time()
        now_ts = time.time()

        bids, asks = parse_shift_book(trader, symbol, levels)

        if not bids or not asks:
            time.sleep(poll_interval)
            continue

        best_bid_p, best_bid_q = bids[0]
        best_ask_p, best_ask_q = asks[0]

        if best_ask_p <= best_bid_p:
            cancel_all_symbol_orders(trader, symbol)
            time.sleep(poll_interval)
            continue

        mid = 0.5 * (best_bid_p + best_ask_p)
        spread = best_ask_p - best_bid_p

        if prev_bids is None or prev_asks is None:
            prev_bids = bids
            prev_asks = asks
            print(f"\n--- Initial snapshot @ {datetime.now()} ---")
            print(f"Best Bid:          {best_bid_p:.4f} x {best_bid_q:.4f}")
            print(f"Best Ask:          {best_ask_p:.4f} x {best_ask_q:.4f}")
            print(f"Mid:               {mid:.4f}")
            print(f"Spread:            {spread:.4f}")
            time.sleep(poll_interval)
            continue

        level_increment = compute_multilevel_ofi_increment(
            prev_bids, prev_asks, bids, asks, levels
        )

        ofi_events.append((now_ts, level_increment))
        prune_old_entries(ofi_events, now_ts, OFI_WINDOW_SECONDS)

        level_ofi_5s = rolling_level_ofi(ofi_events, levels)
        raw_ofi_t = weighted_raw_ofi(level_ofi_5s, LEVEL_WEIGHTS)
        ema_t = ema_update(ema_t, raw_ofi_t, EMA_ALPHA)

        raw_ofi_history.append(raw_ofi_t)
        pos_count, neg_count, zero_count, persistence_score = persistence_stats(raw_ofi_history)

        ema_dir = ema_direction(ema_t, OFI_FLOOR)

        signal, final_score = classify_signal(
            ema_dir=ema_dir,
            pos_count=pos_count,
            neg_count=neg_count,
            persistence_score=persistence_score,
            persistence_required=PERSISTENCE_REQUIRED,
            final_score_threshold=FINAL_SCORE_THRESHOLD
        )

        if signal == "BUY_PRESSURE":
            buy_streak += 1
            sell_streak = 0
        elif signal == "SELL_PRESSURE":
            sell_streak += 1
            buy_streak = 0
        else:
            buy_streak = 0
            sell_streak = 0

        regime = determine_regime(buy_streak, sell_streak)

        long_lots, short_lots, net_lots = get_position_lots(trader, symbol)

        buy_orders, sell_orders = get_open_orders(trader, symbol)
        my_open_buy, extra_buys = keep_only_one_best_order(buy_orders, is_buy=True)
        my_open_sell, extra_sells = keep_only_one_best_order(sell_orders, is_buy=False)

        if extra_buys:
            cancel_order_list(trader, extra_buys)
        if extra_sells:
            cancel_order_list(trader, extra_sells)

        if spread < MIN_SPREAD:
            if my_open_buy is not None:
                trader.submit_cancellation(my_open_buy)
            if my_open_sell is not None:
                trader.submit_cancellation(my_open_sell)
            action_taken = "SPREAD_TOO_TIGHT_CANCEL"
        else:
            desired = compute_regime_quotes(
                best_bid_p, best_ask_p, net_lots, regime,
                base_qty_lots=BASE_QTY_LOTS
            )

            action_taken = "NONE"

            if desired is None:
                cancel_all_symbol_orders(trader, symbol)
                action_taken = "INVALID_QUOTES_CANCEL"
            else:
                should_refresh = False
                if last_refresh_ts is None:
                    should_refresh = True
                elif (now_ts - last_refresh_ts) >= REFRESH_INTERVAL_SECONDS:
                    should_refresh = True
                elif my_open_buy is None or my_open_sell is None:
                    should_refresh = True

                if should_refresh:
                    submit_quote_if_needed(
                        trader, symbol, "buy",
                        my_open_buy, desired["bid_price"], desired["bid_size"]
                    )
                    submit_quote_if_needed(
                        trader, symbol, "sell",
                        my_open_sell, desired["ask_price"], desired["ask_size"]
                    )
                    last_refresh_ts = now_ts
                    action_taken = (
                        f'QUOTE {desired["bid_size"]} @ {desired["bid_price"]:.2f} / '
                        f'{desired["ask_size"]} @ {desired["ask_price"]:.2f}'
                    )
                else:
                    action_taken = "KEEP_QUOTES"

        step += 1

        print(f"\n--- Signal MM @ {datetime.now()} | step {step} ---")
        print(f"Best Bid:          {best_bid_p:.4f} x {best_bid_q:.4f}")
        print(f"Best Ask:          {best_ask_p:.4f} x {best_ask_q:.4f}")
        print(f"Mid:               {mid:.4f}")
        print(f"Spread:            {spread:.4f}")
        print(f"Level OFI inc 1s:  {[round(x, 2) for x in level_increment]}")
        print(f"Level OFI 5s:      {[round(x, 2) for x in level_ofi_5s]}")
        print(f"Raw OFI_t:         {raw_ofi_t:.2f}")
        print(f"EMA_t:             {ema_t:.2f}")
        print(f"Signal:            {signal}")
        print(f"Buy streak:        {buy_streak}")
        print(f"Sell streak:       {sell_streak}")
        print(f"Regime:            {regime}")
        print(f"Long lots:         {long_lots}")
        print(f"Short lots:        {short_lots}")
        print(f"Net lots:          {net_lots}")
        if spread >= MIN_SPREAD and desired is not None:
            print(f"Target inventory:  {desired['target_inventory']}")
            print(f"Inventory error:   {desired['inventory_error']}")
            print(f"Desired bid:       {desired['bid_size']} @ {desired['bid_price']:.2f}")
            print(f"Desired ask:       {desired['ask_size']} @ {desired['ask_price']:.2f}")
        print(f"Action taken:      {action_taken}")

        prev_bids = bids
        prev_asks = asks

        elapsed = time.time() - loop_start
        time.sleep(max(poll_interval - elapsed, 0.0))


if __name__ == "__main__":
    with shift.Trader("columbia-traders") as trader:
        trader.connect("initiator.cfg", "aRkkZSrj")
        time.sleep(1)

        trader.sub_all_order_book()
        time.sleep(1)

        end_time = datetime.now() + timedelta(minutes=10)

        try:
            run_signal_regime_market_maker(
                trader,
                symbol=SYMBOL,
                levels=LEVELS,
                poll_interval=POLL_INTERVAL,
                end_time=end_time
            )
        except KeyboardInterrupt:
            cancel_all_symbol_orders(trader, SYMBOL)
            trader.disconnect()