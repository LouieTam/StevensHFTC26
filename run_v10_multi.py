import shift
import time
import math
import csv
import os
from collections import deque
from datetime import datetime, timedelta
from threading import Thread, Lock

# ---------------------------------------------------------------------------
# Shared constants (not ticker-specific)
# ---------------------------------------------------------------------------

LEVELS         = 10
POLL_INTERVAL  = 1.0
TICK_SIZE      = 0.01
LOT_SIZE       = 100
TRADE_LOTS     = 2
LEVEL_WEIGHTS  = [1.0, 0.9, 0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2, 0.1]

# OFI / signal shared settings
OFI_WINDOW_SECONDS     = 5.0
EMA_ALPHA              = 0.3
PERSISTENCE_LOOKBACK   = 5
PERSISTENCE_REQUIRED   = 4
FINAL_SCORE_THRESHOLD  = 0.5
EXEC_AUDIT_INTERVAL_SECONDS = 30.0

# ---------------------------------------------------------------------------
# Per-ticker configs
# Tune these based on observed EMA range for each ticker.
# ---------------------------------------------------------------------------

TICKER_CONFIGS = {
    "AAPL": {
        "ofi_floor":            100.0,
        "ema_entry_threshold":  200.0,
        "ema_flip_threshold":   150.0,
        "hold_seconds":           7.0,
        "exit_adjust_seconds":    2.0,
        "entry_spread_frac":      0.00,
        "exit_spread_frac":       0.00,
        "entry_streak":             3,   # streak >= N to enter
    },
    "MSFT": {
        "ofi_floor":             40.0,
        "ema_entry_threshold":  130.0,
        "ema_flip_threshold":   100.0,
        "hold_seconds":           7.0,
        "exit_adjust_seconds":    1.0,
        "entry_spread_frac":      0.00,
        "exit_spread_frac":       0.00,
        "entry_streak":             3,
    },
    "AMZN": {
        "ofi_floor":             60.0,
        "ema_entry_threshold":  130.0,
        "ema_flip_threshold":    100.0,
        "hold_seconds":           7.0,
        "exit_adjust_seconds":    1.0,
        "entry_spread_frac":      0.00,
        "exit_spread_frac":       0.00,
        "entry_streak":             4,   # higher bar — AMZN signal is noisier
    },
}

# ---------------------------------------------------------------------------
# One CSV lock per ticker — prevents two threads writing the same file
# ---------------------------------------------------------------------------

_csv_locks = {ticker: Lock() for ticker in TICKER_CONFIGS}

def _submission_path(symbol):
    return f"dir_submissions_{symbol}.csv"

def _execution_path(symbol):
    return f"dir_executions_{symbol}.csv"

# ---------------------------------------------------------------------------
# Tick helpers
# ---------------------------------------------------------------------------

def round_down_to_tick(x, tick=TICK_SIZE):
    return math.floor(round(x / tick, 6)) * tick

def round_up_to_tick(x, tick=TICK_SIZE):
    return math.ceil(round(x / tick, 6)) * tick

def sanitise_price(price):
    return round(float(price), 2)

# ---------------------------------------------------------------------------
# CSV logging (per-ticker, lock-protected)
# ---------------------------------------------------------------------------

def ensure_csv_headers(symbol):
    lock = _csv_locks[symbol]
    with lock:
        sub_path = _submission_path(symbol)
        exe_path = _execution_path(symbol)
        if not os.path.exists(sub_path):
            with open(sub_path, "w", newline="") as f:
                csv.writer(f).writerow([
                    "logged_at", "order_id", "symbol", "side", "limit_price",
                    "shares", "lots", "regime", "signal", "step",
                    "position_shares_before_submit",
                ])
        if not os.path.exists(exe_path):
            with open(exe_path, "w", newline="") as f:
                csv.writer(f).writerow([
                    "logged_at", "order_id", "symbol", "side", "executed_price",
                    "executed_size", "order_size", "status", "exec_timestamp",
                ])

def append_submission_log(symbol, sim_time, order_id, side, limit_price,
                          shares, regime, signal, step, position_shares):
    with _csv_locks[symbol]:
        with open(_submission_path(symbol), "a", newline="") as f:
            csv.writer(f).writerow([
                sim_time, order_id, symbol, side, f"{float(limit_price):.4f}",
                int(shares), int(shares // LOT_SIZE), regime, signal, step,
                int(position_shares),
            ])

def append_execution_log(symbol, sim_time, order_id, side, executed_price,
                         executed_size, order_size, status, exec_timestamp):
    with _csv_locks[symbol]:
        with open(_execution_path(symbol), "a", newline="") as f:
            csv.writer(f).writerow([
                sim_time, order_id, symbol, side, f"{float(executed_price):.4f}",
                int(executed_size), int(order_size), str(status), exec_timestamp,
            ])

# ---------------------------------------------------------------------------
# Execution audit
# ---------------------------------------------------------------------------

def poll_executions(trader, symbol, tracked_orders, seen_execution_keys):
    sim_time = trader.get_last_trade_time()
    for order_id, meta in list(tracked_orders.items()):
        try:
            executed_orders = trader.get_executed_orders(order_id)
            for ex in executed_orders:
                executed_size  = int(getattr(ex,  "executed_size",  0))
                executed_price = float(getattr(ex, "executed_price", 0.0))
                if executed_size > 0:
                    exec_key = (
                        order_id,
                        str(getattr(ex, "timestamp", "")),
                        executed_size, executed_price,
                        str(getattr(ex, "status", "")),
                    )
                    if exec_key not in seen_execution_keys:
                        seen_execution_keys.add(exec_key)
                        append_execution_log(
                            symbol, sim_time, order_id, meta["side"],
                            executed_price, executed_size,
                            meta["lots"] * LOT_SIZE,
                            str(getattr(ex, "status",    "")),
                            getattr(ex,     "timestamp", ""),
                        )
            current_order = trader.get_order(order_id)
            if current_order is not None:
                status_str     = str(getattr(current_order, "status",       ""))
                total_executed = int(getattr(current_order, "executed_size", 0))
                original_size  = meta["lots"] * LOT_SIZE
                if ("FILLED"   in status_str or "CANCELED" in status_str
                        or "REJECTED" in status_str
                        or total_executed >= original_size):
                    tracked_orders[order_id]["done"] = True
        except Exception:
            pass
    for oid in [oid for oid, m in tracked_orders.items() if m.get("done")]:
        del tracked_orders[oid]

# ---------------------------------------------------------------------------
# Order book parsing
# ---------------------------------------------------------------------------

def parse_shift_book(trader, symbol, levels):
    bids_obj = trader.get_order_book(symbol, shift.OrderBookType.GLOBAL_BID)
    asks_obj = trader.get_order_book(symbol, shift.OrderBookType.GLOBAL_ASK)
    bids = [(float(b.price), float(b.size)) for b in bids_obj[:levels]]
    asks = [(float(a.price), float(a.size)) for a in asks_obj[:levels]]
    return bids, asks

# ---------------------------------------------------------------------------
# OFI computation
# ---------------------------------------------------------------------------

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
                      new_bid_p,  new_bid_q,  new_ask_p,  new_ask_q):
    bid_term = (
        (new_bid_q  if new_bid_p >= prev_bid_p else 0.0)
        - (prev_bid_q if new_bid_p <= prev_bid_p else 0.0)
    )
    ask_term = (
        -(new_ask_q  if new_ask_p <= prev_ask_p else 0.0)
        + (prev_ask_q if new_ask_p >= prev_ask_p else 0.0)
    )
    return float(bid_term + ask_term)

def compute_multilevel_ofi_increment(prev_bids, prev_asks, new_bids, new_asks, levels):
    prev_bids = pad_book_side(prev_bids, levels)
    prev_asks = pad_book_side(prev_asks, levels)
    new_bids  = pad_book_side(new_bids,  levels)
    new_asks  = pad_book_side(new_asks,  levels)
    return [
        compute_level_ofi(
            prev_bids[m][0], prev_bids[m][1], prev_asks[m][0], prev_asks[m][1],
            new_bids[m][0],  new_bids[m][1],  new_asks[m][0],  new_asks[m][1],
        )
        for m in range(levels)
    ]

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

# ---------------------------------------------------------------------------
# Signal logic
# ---------------------------------------------------------------------------

def ema_direction(ema_value, ofi_floor):
    if ema_value >  ofi_floor: return  1
    if ema_value < -ofi_floor: return -1
    return 0

def persistence_stats(raw_ofi_history):
    pos   = sum(1 for x in raw_ofi_history if x > 0)
    neg   = sum(1 for x in raw_ofi_history if x < 0)
    score = (pos - neg) / max(len(raw_ofi_history), 1)
    return pos, neg, score

def classify_signal(ema_dir, pos_count, neg_count, persistence_score):
    if ema_dir == 1 and pos_count >= PERSISTENCE_REQUIRED:
        final_score = 0.7 * 1.0 + 0.3 * persistence_score
    elif ema_dir == -1 and neg_count >= PERSISTENCE_REQUIRED:
        final_score = 0.7 * (-1.0) + 0.3 * persistence_score
    else:
        final_score = 0.3 * persistence_score
    if final_score >  FINAL_SCORE_THRESHOLD: return "BUY_PRESSURE"
    if final_score < -FINAL_SCORE_THRESHOLD: return "SELL_PRESSURE"
    return "NEUTRAL"

# ---------------------------------------------------------------------------
# Position / order helpers
# ---------------------------------------------------------------------------

def get_position_shares(trader, symbol):
    item = trader.get_portfolio_item(symbol)
    return int(item.get_long_shares()) - int(item.get_short_shares())

def cancel_all_open_orders(trader, symbol):
    for order in trader.get_waiting_list():
        if order.symbol == symbol:
            try:
                trader.submit_cancellation(order)
            except Exception:
                pass
    time.sleep(0.5)

def cancel_order_by_id(trader, oid):
    if not oid:
        return
    order = trader.get_order(oid)
    if order:
        try:
            trader.submit_cancellation(order)
            time.sleep(0.2)
        except Exception:
            pass

def submit_and_track_order(trader, symbol, side, lots, price, state, signal,
                           step, tracked_orders):
    if side == "BUY":
        order = shift.Order(shift.Order.Type.LIMIT_BUY,  symbol, int(lots), float(price))
    else:
        order = shift.Order(shift.Order.Type.LIMIT_SELL, symbol, int(lots), float(price))

    trader.submit_order(order)
    now_dt   = datetime.now()
    sim_time = trader.get_last_trade_time()

    tracked_orders[order.id] = {
        "symbol":      symbol,
        "side":        side,
        "submit_time": now_dt,
        "limit_price": float(price),
        "lots":        int(lots),
        "signal":      signal,
        "regime":      state,
        "step":        step,
        "done":        False,
    }

    pos_shares = get_position_shares(trader, symbol)
    append_submission_log(symbol, sim_time, order.id, side, price,
                          lots * LOT_SIZE, state, signal, step, pos_shares)
    return order.id

# ---------------------------------------------------------------------------
# Strategy — all per-ticker params come from config dict
# ---------------------------------------------------------------------------

def run_directional_strategy(trader, symbol, config, end_time):
    """
    Runs the directional OFI strategy for a single ticker.
    Designed to be called from a Thread — all state is local.

    config keys:
        ofi_floor, ema_entry_threshold, ema_flip_threshold,
        hold_seconds, exit_adjust_seconds,
        entry_spread_frac, exit_spread_frac, entry_streak
    """
    ensure_csv_headers(symbol)

    # Unpack config
    ofi_floor            = config["ofi_floor"]
    ema_entry_threshold  = config["ema_entry_threshold"]
    ema_flip_threshold   = config["ema_flip_threshold"]
    hold_seconds         = config["hold_seconds"]
    exit_adjust_seconds  = config["exit_adjust_seconds"]
    entry_spread_frac    = config["entry_spread_frac"]
    exit_spread_frac     = config["exit_spread_frac"]
    entry_streak_req     = config["entry_streak"]

    # OFI state
    prev_bids       = None
    prev_asks       = None
    ofi_events      = deque()
    raw_ofi_history = deque(maxlen=PERSISTENCE_LOOKBACK)
    recent_signals  = deque(maxlen=3)
    ema_t           = None

    # Signal / streak state
    prev_signal    = "NEUTRAL"
    streak_count   = 0
    streak_max_ema = 0.0

    # Trade state
    state                = "FLAT"
    entry_side           = None
    entry_lots           = TRADE_LOTS
    entry_pos_snapshot   = 0
    entry_submit_ts      = 0.0
    hold_start_ts        = 0.0
    last_adjust_ts       = 0.0
    target_hold_secs     = hold_seconds
    hold_extension_count = 0
    active_order_id      = None
    entering_neutral_streak = 0
    step                 = 0

    tracked_orders      = {}
    seen_execution_keys = set()
    next_exec_audit_ts  = time.time() + EXEC_AUDIT_INTERVAL_SECONDS

    while datetime.now() < end_time:
        loop_start = time.time()
        now_ts     = time.time()
        now_dt     = datetime.now()

        if now_ts >= next_exec_audit_ts:
            poll_executions(trader, symbol, tracked_orders, seen_execution_keys)
            next_exec_audit_ts = now_ts + EXEC_AUDIT_INTERVAL_SECONDS

        bids, asks = parse_shift_book(trader, symbol, LEVELS)

        if not bids or not asks:
            continue

        best_bid_p, _ = bids[0]
        best_ask_p, _ = asks[0]

        if best_ask_p <= best_bid_p:
            continue

        mid    = 0.5 * (best_bid_p + best_ask_p)
        spread = best_ask_p - best_bid_p

        if prev_bids is None or prev_asks is None:
            prev_bids = bids
            prev_asks = asks
            elapsed = time.time() - loop_start
            time.sleep(max(POLL_INTERVAL - elapsed, 0.0))
            continue

        # ------------------------------------------------------------------
        # OFI / signal
        # ------------------------------------------------------------------
        level_increment = compute_multilevel_ofi_increment(
            prev_bids, prev_asks, bids, asks, LEVELS)
        ofi_events.append((now_ts, level_increment))
        prune_old_entries(ofi_events, now_ts, OFI_WINDOW_SECONDS)

        level_ofi = rolling_level_ofi(ofi_events, LEVELS)
        raw_ofi_t = weighted_raw_ofi(level_ofi, LEVEL_WEIGHTS)
        ema_t     = ema_update(ema_t, raw_ofi_t, EMA_ALPHA)

        raw_ofi_history.append(raw_ofi_t)
        pos_count, neg_count, persistence_score = persistence_stats(raw_ofi_history)
        ema_dir = ema_direction(ema_t, ofi_floor)
        signal  = classify_signal(ema_dir, pos_count, neg_count, persistence_score)
        recent_signals.append(signal)

        if signal in ("BUY_PRESSURE", "SELL_PRESSURE"):
            if signal == prev_signal:
                streak_count   += 1
                streak_max_ema  = max(streak_max_ema, abs(ema_t))
            else:
                streak_count   = 1
                streak_max_ema = abs(ema_t)
        else:
            streak_count   = 0
            streak_max_ema = 0.0

        pos_shares = get_position_shares(trader, symbol)

        # ------------------------------------------------------------------
        # State machine
        # ------------------------------------------------------------------

        if state == "FLAT":
            if pos_shares != 0:
                state          = "EXITING"
                last_adjust_ts = 0.0

            elif (
                (streak_count >= entry_streak_req and streak_max_ema > ema_entry_threshold)
                or (streak_count >= 5 and streak_max_ema > 0.8 * ema_entry_threshold)
            ):
                if signal == "BUY_PRESSURE":
                    entry_side = "BUY"
                    entry_lots = TRADE_LOTS
                    entry_px   = sanitise_price(
                        round_down_to_tick(mid + entry_spread_frac * spread))
                    active_order_id = submit_and_track_order(
                        trader, symbol, entry_side, entry_lots, entry_px,
                        state, signal, step, tracked_orders)
                else:
                    entry_side = "SELL"
                    entry_lots = TRADE_LOTS
                    entry_px   = sanitise_price(
                        round_up_to_tick(mid - entry_spread_frac * spread))
                    active_order_id = submit_and_track_order(
                        trader, symbol, entry_side, entry_lots, entry_px,
                        state, signal, step, tracked_orders)

                state              = "ENTERING"
                entry_submit_ts    = now_ts
                entry_pos_snapshot = pos_shares
                streak_count       = 0
                streak_max_ema     = 0.0

        elif state == "ENTERING":
            executed_orders = (
                trader.get_executed_orders(active_order_id) if active_order_id else []
            )
            entry_filled = (
                len(executed_orders) > 0 or pos_shares != entry_pos_snapshot
            )

            if entry_filled:
                cancel_all_open_orders(trader, symbol)
                active_order_id         = None
                entering_neutral_streak = 0
                state                   = "HOLDING"
                target_hold_secs        = hold_seconds
                hold_extension_count    = 0
                hold_start_ts           = now_ts   # always wall clock

            elif (
                (entry_side == "BUY"  and signal == "SELL_PRESSURE") or
                (entry_side == "SELL" and signal == "BUY_PRESSURE")
            ):
                print(f"[{symbol}][ENTERING] Signal reversed to {signal} — aborting")
                cancel_all_open_orders(trader, symbol)
                active_order_id         = None
                entering_neutral_streak = 0
                state                   = "FLAT"

            else:
                if signal == "NEUTRAL":
                    entering_neutral_streak += 1
                else:
                    entering_neutral_streak = 0

                if entering_neutral_streak >= 4:
                    print(f"[{symbol}][ENTERING] 4 neutral ticks — aborting entry")
                    cancel_all_open_orders(trader, symbol)
                    active_order_id         = None
                    entering_neutral_streak = 0
                    state                   = "FLAT"

                elif now_ts - entry_submit_ts >= 1.0:
                    cancel_order_by_id(trader, active_order_id)
                    if entry_side == "BUY":
                        entry_px = sanitise_price(
                            round_down_to_tick(mid - entry_spread_frac * spread))
                        active_order_id = submit_and_track_order(
                            trader, symbol, "BUY", entry_lots, entry_px,
                            state, signal, step, tracked_orders)
                    else:
                        entry_px = sanitise_price(
                            round_up_to_tick(mid + entry_spread_frac * spread))
                        active_order_id = submit_and_track_order(
                            trader, symbol, "SELL", entry_lots, entry_px,
                            state, signal, step, tracked_orders)
                    entry_submit_ts = now_ts

        elif state == "HOLDING":
            if now_ts - hold_start_ts >= target_hold_secs:
                recent_list = list(recent_signals)

                # Trigger 1: same-direction streak >= 2 on current tick
                same_dir_extend = (
                    hold_extension_count < 5
                    and (
                        (pos_shares > 0 and signal == "BUY_PRESSURE"  and streak_count >= 2) or
                        (pos_shares < 0 and signal == "SELL_PRESSURE" and streak_count >= 2)
                    )
                )

                # Trigger 2: 3/3 recent signals match
                majority_extend = (
                    hold_extension_count < 5
                    and (
                        (pos_shares > 0 and recent_list.count("BUY_PRESSURE")  >= 3) or
                        (pos_shares < 0 and recent_list.count("SELL_PRESSURE") >= 3)
                    )
                )

                if same_dir_extend or majority_extend:
                    target_hold_secs     += 3.0
                    hold_extension_count += 1
                    print(
                        f"[{symbol}][HOLDING] Extended hold by 3s "
                        f"({'streak' if same_dir_extend else 'majority'}) "
                        f"— extension {hold_extension_count}/5, "
                        f"new target {target_hold_secs:.0f}s"
                    )
                else:
                    state          = "EXITING"
                    last_adjust_ts = 0.0

        elif state == "EXITING":
            if pos_shares == 0:
                cancel_all_open_orders(trader, symbol)
                active_order_id = None
                state           = "FLAT"
                # Only reset streak if it matches the direction we just exited.
                # Preserve opposite-direction streak so FLAT can re-enter immediately.
                same_direction = (
                    (entry_side == "BUY"  and signal == "BUY_PRESSURE") or
                    (entry_side == "SELL" and signal == "SELL_PRESSURE")
                )
                if same_direction:
                    streak_count   = 0
                    streak_max_ema = 0.0

            else:
                # Flip: lower threshold than fresh entry since holding an
                # opposing position is itself additional confirmation.
                valid_flip_signal = (
                    (streak_count >= 3 and streak_max_ema > ema_flip_threshold)
                    or streak_count >= 5
                )
                flip_to_short = (
                    pos_shares > 0 and signal == "SELL_PRESSURE" and valid_flip_signal
                )
                flip_to_long = (
                    pos_shares < 0 and signal == "BUY_PRESSURE" and valid_flip_signal
                )

                if flip_to_short or flip_to_long:
                    cancel_all_open_orders(trader, symbol)
                    exit_lots  = int(abs(pos_shares) / LOT_SIZE)
                    flip_lots  = exit_lots + TRADE_LOTS
                    flip_side  = "SELL" if flip_to_short else "BUY"
                    entry_side = flip_side

                    if flip_to_short:
                        flip_px = sanitise_price(
                            round_down_to_tick(mid - entry_spread_frac * spread))
                    else:
                        flip_px = sanitise_price(
                            round_up_to_tick(mid + entry_spread_frac * spread))

                    print(
                        f"[{symbol}][EXITING] Flip {flip_side} {flip_lots} lots "
                        f"(exit {exit_lots} + new {TRADE_LOTS}) @ {flip_px:.4f}"
                    )
                    active_order_id = submit_and_track_order(
                        trader, symbol, flip_side, flip_lots, flip_px,
                        state, signal, step, tracked_orders)
                    entry_lots              = flip_lots
                    state                   = "ENTERING"
                    entry_submit_ts         = now_ts
                    entry_pos_snapshot      = pos_shares
                    entering_neutral_streak = 0
                    streak_count            = 0
                    streak_max_ema          = 0.0

                elif now_ts - last_adjust_ts >= exit_adjust_seconds:
                    cancel_order_by_id(trader, active_order_id)
                    exit_lots = int(abs(pos_shares) / LOT_SIZE)
                    if exit_lots > 0:
                        if pos_shares > 0:
                            exit_px = sanitise_price(
                                round_down_to_tick(best_ask_p - exit_spread_frac * spread))
                            active_order_id = submit_and_track_order(
                                trader, symbol, "SELL", exit_lots, exit_px,
                                state, signal, step, tracked_orders)
                        else:
                            exit_px = sanitise_price(
                                round_up_to_tick(best_bid_p + exit_spread_frac * spread))
                            active_order_id = submit_and_track_order(
                                trader, symbol, "BUY", exit_lots, exit_px,
                                state, signal, step, tracked_orders)
                    last_adjust_ts = now_ts

        print(
            f"[{now_dt.strftime('%H:%M:%S')}][{symbol}] "
            f"State: {state:8s} | Pos: {pos_shares:+5d} | "
            f"Sig: {signal:14s} | Streak: {streak_count} | "
            f"NeutStr: {entering_neutral_streak} | "
            f"EMA: {ema_t:.2f} | Mid: {mid:.4f} | Sprd: {spread:.4f}"
        )

        prev_bids   = bids
        prev_asks   = asks
        prev_signal = signal
        step       += 1

        elapsed = time.time() - loop_start
        time.sleep(max(POLL_INTERVAL - elapsed, 0.0))

    # Final cleanup for this ticker
    poll_executions(trader, symbol, tracked_orders, seen_execution_keys)
    cancel_all_open_orders(trader, symbol)
    print(f"[{symbol}] Strategy thread finished.")

# ---------------------------------------------------------------------------
# Entry point — spins up one thread per ticker
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    tickers = list(TICKER_CONFIGS.keys())   # ["AAPL", "MSFT", "AMZN"]

    with shift.Trader("columbia-traders") as trader:
        trader.connect("initiator.cfg", "aRkkZSrj")
        time.sleep(1.0)

        # Cancel any leftover orders for all tickers before starting
        for ticker in tickers:
            cancel_all_open_orders(trader, ticker)

        trader.sub_all_order_book()
        time.sleep(1.0)

        end_time = datetime.now() + timedelta(minutes=500.0)

        # Spin up one thread per ticker
        threads = []
        for ticker in tickers:
            t = Thread(
                target=run_directional_strategy,
                args=(trader, ticker, TICKER_CONFIGS[ticker], end_time),
                daemon=True,           # threads die automatically if main exits
                name=f"strategy-{ticker}",
            )
            threads.append(t)

        try:
            for t in threads:
                t.start()
                time.sleep(1.0)   # stagger starts slightly to avoid burst

            # Wait for all threads to finish naturally
            for t in threads:
                t.join()

        except KeyboardInterrupt:
            print("KeyboardInterrupt — waiting for threads to finish...")
            # end_time has already passed for threads to exit cleanly;
            # set it to now so they exit on next loop iteration
            end_time = datetime.now()
            for t in threads:
                t.join(timeout=10.0)
            trader.disconnect()

        print("All strategy threads finished.")