import shift
import time
from collections import deque
from datetime import datetime, timedelta

SYMBOL = "GS"
LEVELS = 10
POLL_INTERVAL = 1.0
OFI_WINDOW_SECONDS = 5.0
EMA_ALPHA = 0.3

LEVEL_WEIGHTS = [1.0, 0.9, 0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2, 0.1]

PERSISTENCE_LOOKBACK = 5
PERSISTENCE_REQUIRED = 4
FINAL_SCORE_THRESHOLD = 0.5

OFI_FLOOR = 20.0
EPS = 1e-12


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
        final_score = 0.7 * (-1.0) + 0.3 * persistence_score
    else:
        final_score = 0.3 * persistence_score

    if final_score > final_score_threshold:
        return "BUY_PRESSURE", final_score
    if final_score < -final_score_threshold:
        return "SELL_PRESSURE", final_score
    return "NEUTRAL", final_score


def run_mlofi_pressure_signal(trader, symbol=SYMBOL, levels=LEVELS,
                              poll_interval=POLL_INTERVAL, end_time=None):
    prev_bids = None
    prev_asks = None

    ofi_events = deque()
    raw_ofi_history = deque(maxlen=PERSISTENCE_LOOKBACK)

    ema_t = None
    step = 0

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

        step += 1

        print(f"\n--- Signal @ {datetime.now()} | step {step} ---")
        print(f"Best Bid:          {best_bid_p:.4f} x {best_bid_q:.4f}")
        print(f"Best Ask:          {best_ask_p:.4f} x {best_ask_q:.4f}")
        print(f"Mid:               {mid:.4f}")
        print(f"Spread:            {spread:.4f}")

        print("Level OFI inc 1s: ", [round(x, 2) for x in level_increment])
        print("Level OFI 5s:     ", [round(x, 2) for x in level_ofi_5s])

        print(f"Raw OFI_t:         {raw_ofi_t:.2f}")
        print(f"EMA_t:             {ema_t:.2f}")
        print(f"OFI_FLOOR:         {OFI_FLOOR:.2f}")
        print(f"EMA direction:     {ema_dir}")

        print(f"Pos last 5:        {pos_count}")
        print(f"Neg last 5:        {neg_count}")
        print(f"Zero last 5:       {zero_count}")
        print(f"Persistence score: {persistence_score:.2f}")

        print(f"Final score:       {final_score:.2f}")
        print(f"Signal:            {signal}")

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
            run_mlofi_pressure_signal(
                trader,
                symbol=SYMBOL,
                levels=LEVELS,
                poll_interval=POLL_INTERVAL,
                end_time=end_time
            )
        except KeyboardInterrupt:
            trader.disconnect()