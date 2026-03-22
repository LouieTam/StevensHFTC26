import shift
import time
from collections import deque
from statistics import mean, pstdev
from datetime import datetime, timedelta

SYMBOL = "GS"
LEVELS = 20
POLL_INTERVAL = 0.2000

TOP_K = 10
GAP_WEIGHTS = [0.3000, 0.2200, 0.1400, 0.1000, 0.0800, 0.0600, 0.0400, 0.0300, 0.0300]
THICKNESS_WEIGHTS = [1.0000, 0.9500, 0.9000, 0.8000, 0.7000, 0.5500, 0.4000, 0.3000, 0.2000, 0.1000]

NORM_WINDOW_POINTS = 25
DECISION_INTERVAL_STEPS = 25

FRAGILITY_CAP = 10.0
ZSCORE_CAP = 3.0
EPS = 1e-12

UP_BREAK_THRESHOLD = 0.9000
DOWN_BREAK_THRESHOLD = -0.9000
NEUTRAL_THRESHOLD = 0.3000

EMA_SPAN = 5
EMA_ALPHA = 2.0000 / (EMA_SPAN + 1.0000)


def parse_shift_book(trader, symbol, levels):
    bids_obj = trader.get_order_book(symbol, shift.OrderBookType.GLOBAL_BID)
    asks_obj = trader.get_order_book(symbol, shift.OrderBookType.GLOBAL_ASK)

    bids = [(float(b.price), float(b.size)) for b in bids_obj[:levels]]
    asks = [(float(a.price), float(a.size)) for a in asks_obj[:levels]]

    return bids, asks


def rolling_zscore(x, hist_deque):
    if len(hist_deque) < 5:
        return 0.0000
    mu = mean(hist_deque)
    sd = pstdev(hist_deque)
    if sd < EPS:
        return 0.0000
    z = (x - mu) / (sd + EPS)
    return max(-ZSCORE_CAP, min(ZSCORE_CAP, float(z)))


def ema_update(prev_ema, new_value, alpha):
    if prev_ema is None:
        return float(new_value)
    return float(alpha * new_value + (1.0000 - alpha) * prev_ema)


def compute_adjacent_gaps(side_levels, side="ask", top_k=10):
    levels = side_levels[:top_k]
    if len(levels) < top_k:
        return None

    prices = [p for p, _ in levels]
    gaps = []

    if side == "ask":
        for i in range(top_k - 1):
            gaps.append(prices[i + 1] - prices[i])
    else:
        for i in range(top_k - 1):
            gaps.append(prices[i] - prices[i + 1])

    return gaps


def weighted_gap_score(side_levels, side="ask", top_k=10, gap_weights=None):
    if gap_weights is None:
        gap_weights = GAP_WEIGHTS

    gaps = compute_adjacent_gaps(side_levels, side=side, top_k=top_k)
    if gaps is None or len(gaps) != len(gap_weights):
        return None

    return float(sum(w * g for w, g in zip(gap_weights, gaps)))


def near_touch_thickness_score(side_levels, top_k=10, thickness_weights=None):
    if thickness_weights is None:
        thickness_weights = THICKNESS_WEIGHTS

    levels = side_levels[:top_k]
    if len(levels) < top_k or len(thickness_weights) != top_k:
        return None

    sizes = [q for _, q in levels]
    total = sum(sizes)
    if total < EPS:
        return 0.0000

    weighted = sum(a * q for a, q in zip(thickness_weights, sizes))
    return float(weighted / (total + EPS))


def fragility_score(side_levels, side="ask", top_k=10,
                    gap_weights=None, thickness_weights=None):
    g = weighted_gap_score(side_levels, side=side, top_k=top_k, gap_weights=gap_weights)
    t = near_touch_thickness_score(side_levels, top_k=top_k, thickness_weights=thickness_weights)

    if g is None or t is None:
        return None

    raw = g / (t + EPS)
    return float(min(raw, FRAGILITY_CAP))


def breakthrough_shape_signal(bids, asks, top_k=10):
    f_ask = fragility_score(
        asks, side="ask", top_k=top_k,
        gap_weights=GAP_WEIGHTS, thickness_weights=THICKNESS_WEIGHTS
    )
    f_bid = fragility_score(
        bids, side="bid", top_k=top_k,
        gap_weights=GAP_WEIGHTS, thickness_weights=THICKNESS_WEIGHTS
    )

    if f_ask is None or f_bid is None:
        return None, None, None

    shape = (f_ask - f_bid) / (f_ask + f_bid + EPS)
    return float(shape), float(f_ask), float(f_bid)


def breakthrough_action(ema_signal,
                        up_break_threshold=UP_BREAK_THRESHOLD,
                        down_break_threshold=DOWN_BREAK_THRESHOLD,
                        neutral_threshold=NEUTRAL_THRESHOLD):
    if ema_signal >= up_break_threshold:
        return "UP_BREAKTHROUGH"
    if ema_signal <= down_break_threshold:
        return "DOWN_BREAKTHROUGH"
    if abs(ema_signal) <= neutral_threshold:
        return "NO_BREAKTHROUGH"
    return "WATCH"


def run_breakthrough_detector(trader, symbol=SYMBOL, levels=LEVELS,
                              poll_interval=POLL_INTERVAL, end_time=None):
    shape_hist = deque(maxlen=NORM_WINDOW_POINTS)
    ema_sig = None
    step_count = 0

    while datetime.now() < end_time:
        loop_start = time.time()

        bids, asks = parse_shift_book(trader, symbol, levels)
        if not bids or not asks:
            time.sleep(poll_interval)
            continue

        best_bid_p, best_bid_q = bids[0]
        best_ask_p, best_ask_q = asks[0]

        if best_ask_p <= best_bid_p:
            time.sleep(poll_interval)
            continue

        mid = 0.5000 * (best_bid_p + best_ask_p)
        spread = best_ask_p - best_bid_p

        shape_raw, f_ask, f_bid = breakthrough_shape_signal(
            bids, asks, top_k=TOP_K
        )

        if shape_raw is None:
            time.sleep(poll_interval)
            continue

        z_shape = rolling_zscore(shape_raw, shape_hist)
        ema_sig = ema_update(ema_sig, z_shape, EMA_ALPHA)
        shape_hist.append(shape_raw)

        step_count += 1

        if step_count % DECISION_INTERVAL_STEPS == 0:
            action = breakthrough_action(ema_sig)

            print(f"\n--- Breakthrough Decision @ step {step_count} ---")
            print(f"Best Bid:          {best_bid_p:.4f} x {best_bid_q:.4f}")
            print(f"Best Ask:          {best_ask_p:.4f} x {best_ask_q:.4f}")
            print(f"Mid:               {mid:.4f}")
            print(f"Spread:            {spread:.4f}")
            print(f"Fragility Ask:     {f_ask:.4f}")
            print(f"Fragility Bid:     {f_bid:.4f}")
            print(f"Shape raw:         {shape_raw:.4f}")
            print(f"z_Shape:           {z_shape:.4f}")
            print(f"EMA signal:        {ema_sig:.4f}")
            print(f"Action:            {action}")

        elapsed = time.time() - loop_start
        time.sleep(max(poll_interval - elapsed, 0.0000))


if __name__ == "__main__":
    with shift.Trader("columbia-traders") as trader:
        trader.connect("initiator.cfg", "aRkkZSrj")
        time.sleep(1)

        trader.sub_all_order_book()
        time.sleep(1)

        end_time = datetime.now() + timedelta(minutes=10)

        try:
            run_breakthrough_detector(
                trader,
                symbol=SYMBOL,
                levels=LEVELS,
                poll_interval=POLL_INTERVAL,
                end_time=end_time
            )
        except KeyboardInterrupt:
            trader.disconnect()