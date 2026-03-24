import shift
import time
import math
import csv
import os
from collections import deque
from datetime import datetime, timedelta

SYMBOL = "GS"
LEVELS = 10
POLL_INTERVAL = 1.0000
OFI_WINDOW_SECONDS = 5.0000
EMA_ALPHA = 0.3000

LEVEL_WEIGHTS = [1.0000, 0.9000, 0.8000, 0.7000, 0.6000, 0.5000, 0.4000, 0.3000, 0.2000, 0.1000]

PERSISTENCE_LOOKBACK = 5
PERSISTENCE_REQUIRED = 4
FINAL_SCORE_THRESHOLD = 0.5000

OFI_FLOOR = 20.0000
EMA_ENTRY_THRESHOLD = 40.0000

TICK_SIZE = 0.0100
LOT_SIZE = 100

MAX_ABS_POSITION_LOTS = 25
BASE_BID_LOTS = 1
BASE_ASK_LOTS = 1
FAVORED_SIDE_LOTS = 2

BUY_TARGET_LOTS = 5
SELL_TARGET_LOTS = -3

INVENTORY_SKEW_PER_LOT = 0.0020
MAX_INVENTORY_SKEW = 0.0800

BUY_CENTER_SHIFT_FRAC = 0.0600
SELL_CENTER_SHIFT_FRAC = -0.0400

NEUTRAL_BID_OFFSET_FRAC = 0.2500
NEUTRAL_ASK_OFFSET_FRAC = 0.2500

BUY_BID_OFFSET_FRAC = 0.1800
BUY_ASK_OFFSET_FRAC = 0.3200

SELL_BID_OFFSET_FRAC = 0.3200
SELL_ASK_OFFSET_FRAC = 0.1800

REGIME_HOLD_SECONDS = 10.0000
POST_FILL_RECOVERY_SECONDS = 2.0000

EXEC_AUDIT_INTERVAL_SECONDS = 300.0000
CANCEL_WAIT_SECONDS = 0.5000
MIN_REST_SECONDS = 5.0000

SUBMISSION_LOG_PATH = "mm_order_submissions.csv"
EXECUTION_LOG_PATH = "mm_order_executions.csv"

def round_down_to_tick(x, tick=TICK_SIZE):
    return math.floor(round(x / tick, 6)) * tick

def round_up_to_tick(x, tick=TICK_SIZE):
    return math.ceil(round(x / tick, 6)) * tick

def lots_to_shares(lots):
    return int(lots * LOT_SIZE)

def ensure_csv_headers():
    if not os.path.exists(SUBMISSION_LOG_PATH):
        with open(SUBMISSION_LOG_PATH, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                "logged_at",
                "order_id",
                "symbol",
                "side",
                "limit_price",
                "shares",
                "lots",
                "regime",
                "signal",
                "step",
                "position_shares_before_submit",
            ])

    if not os.path.exists(EXECUTION_LOG_PATH):
        with open(EXECUTION_LOG_PATH, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                "logged_at",
                "order_id",
                "symbol",
                "side",
                "executed_price",
                "executed_size",
                "order_size",
                "status",
                "exec_timestamp",
            ])

def append_submission_log(sim_time, order_id, symbol, side, limit_price, shares, regime, signal, step, position_shares):
    with open(SUBMISSION_LOG_PATH, "a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            sim_time,
            order_id,
            symbol,
            side,
            f"{float(limit_price):.4f}",
            int(shares),
            int(shares // LOT_SIZE),
            regime,
            signal,
            step,
            int(position_shares),
        ])

def append_execution_log(sim_time, order_id, symbol, side, executed_price, executed_size, order_size, status, exec_timestamp):
    with open(EXECUTION_LOG_PATH, "a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            sim_time,
            order_id,
            symbol,
            side,
            f"{float(executed_price):.4f}",
            int(executed_size),
            int(order_size),
            str(status),
            exec_timestamp,
        ])

def parse_shift_book(trader, symbol, levels):
    bids_obj = trader.get_order_book(symbol, shift.OrderBookType.GLOBAL_BID)
    asks_obj = trader.get_order_book(symbol, shift.OrderBookType.GLOBAL_ASK)

    bids = [(float(b.price), float(b.size)) for b in bids_obj[:levels]]
    asks = [(float(a.price), float(a.size)) for a in asks_obj[:levels]]

    return bids, asks

def ema_update(prev_ema, new_value, alpha):
    if prev_ema is None:
        return float(new_value)
    return float(alpha * new_value + (1.0000 - alpha) * prev_ema)

def pad_book_side(levels_list, target_levels):
    padded = list(levels_list[:target_levels])
    while len(padded) < target_levels:
        padded.append((0.0000, 0.0000))
    return padded

def compute_level_ofi(prev_bid_p, prev_bid_q, prev_ask_p, prev_ask_q,
                      new_bid_p, new_bid_q, new_ask_p, new_ask_q):
    bid_term = (
        (new_bid_q if new_bid_p >= prev_bid_p else 0.0000)
        - (prev_bid_q if new_bid_p <= prev_bid_p else 0.0000)
    )

    ask_term = (
        -(new_ask_q if new_ask_p <= prev_ask_p else 0.0000)
        + (prev_ask_q if new_ask_p >= prev_ask_p else 0.0000)
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
    totals = [0.0000] * levels
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
        final_score = 0.7000 * 1.0000 + 0.3000 * persistence_score
    elif ema_dir == -1 and neg_count >= persistence_required:
        final_score = 0.7000 * (-1.0000) + 0.3000 * persistence_score
    else:
        final_score = 0.3000 * persistence_score

    if final_score > final_score_threshold:
        return "BUY_PRESSURE", final_score
    if final_score < -final_score_threshold:
        return "SELL_PRESSURE", final_score
    return "NEUTRAL", final_score

def update_signal_run(signal, prev_signal, current_run_id, current_run_len):
    if signal in ("BUY_PRESSURE", "SELL_PRESSURE"):
        if signal == prev_signal:
            current_run_len += 1
        else:
            current_run_id += 1
            current_run_len = 1
    else:
        current_run_len = 0
    return current_run_id, current_run_len

def maybe_activate_regime(signal, ema_t, run_id, run_len, last_triggered_run_id,
                          regime, regime_expiry, now_dt):
    if signal == "BUY_PRESSURE" and run_len >= 3 and abs(ema_t) > EMA_ENTRY_THRESHOLD:
        if run_id != last_triggered_run_id or regime != "BUY":
            regime = "BUY"
            regime_expiry = now_dt + timedelta(seconds=REGIME_HOLD_SECONDS)
            last_triggered_run_id = run_id
        else:
            regime_expiry = max(regime_expiry, now_dt + timedelta(seconds=REGIME_HOLD_SECONDS))

    elif signal == "SELL_PRESSURE" and run_len >= 3 and abs(ema_t) > EMA_ENTRY_THRESHOLD:
        if run_id != last_triggered_run_id or regime != "SELL":
            regime = "SELL"
            regime_expiry = now_dt + timedelta(seconds=REGIME_HOLD_SECONDS)
            last_triggered_run_id = run_id
        else:
            regime_expiry = max(regime_expiry, now_dt + timedelta(seconds=REGIME_HOLD_SECONDS))

    if regime_expiry is not None and now_dt >= regime_expiry:
        regime = "NEUTRAL"
        regime_expiry = None

    return regime, regime_expiry, last_triggered_run_id

def get_position_shares(trader, symbol):
    item = trader.get_portfolio_item(symbol)
    long_shares = int(item.get_long_shares())
    short_shares = int(item.get_short_shares())
    return long_shares - short_shares

def cancel_order_safe(trader, order):
    if order is None:
        return
    try:
        trader.submit_cancellation(order)
        time.sleep(CANCEL_WAIT_SECONDS)
    except Exception as e:
        pass

def get_regime_params(regime):
    if regime == "BUY":
        return {
            "target_inventory_shares": lots_to_shares(BUY_TARGET_LOTS),
            "center_shift_frac": BUY_CENTER_SHIFT_FRAC,
            "bid_offset_frac": BUY_BID_OFFSET_FRAC,
            "ask_offset_frac": BUY_ASK_OFFSET_FRAC,
            "bid_lots": FAVORED_SIDE_LOTS,
            "ask_lots": BASE_ASK_LOTS,
        }
    if regime == "SELL":
        return {
            "target_inventory_shares": lots_to_shares(SELL_TARGET_LOTS),
            "center_shift_frac": SELL_CENTER_SHIFT_FRAC,
            "bid_offset_frac": SELL_BID_OFFSET_FRAC,
            "ask_offset_frac": SELL_ASK_OFFSET_FRAC,
            "bid_lots": BASE_BID_LOTS,
            "ask_lots": FAVORED_SIDE_LOTS,
        }
    return {
        "target_inventory_shares": 0,
        "center_shift_frac": 0.0000,
        "bid_offset_frac": NEUTRAL_BID_OFFSET_FRAC,
        "ask_offset_frac": NEUTRAL_ASK_OFFSET_FRAC,
        "bid_lots": BASE_BID_LOTS,
        "ask_lots": BASE_ASK_LOTS,
    }

def apply_post_fill_adjustments(regime_params, position_delta_shares, post_fill_until, now_dt):
    params = dict(regime_params)

    if post_fill_until is None or now_dt >= post_fill_until or position_delta_shares == 0:
        return params

    if position_delta_shares > 0:
        params["bid_lots"] = 0
        params["ask_lots"] = max(params["ask_lots"], FAVORED_SIDE_LOTS)
        params["center_shift_frac"] -= 0.0300
    else:
        params["ask_lots"] = 0
        params["bid_lots"] = max(params["bid_lots"], FAVORED_SIDE_LOTS)
        params["center_shift_frac"] += 0.0300

    return params

def compute_quote_prices(best_bid, best_ask, mid, spread, regime_params, position_shares):
    target_inventory_shares = regime_params["target_inventory_shares"]
    inv_diff_lots = (position_shares - target_inventory_shares) / LOT_SIZE
    inventory_adjustment = -INVENTORY_SKEW_PER_LOT * inv_diff_lots
    inventory_adjustment = max(-MAX_INVENTORY_SKEW, min(MAX_INVENTORY_SKEW, inventory_adjustment))

    center = mid + regime_params["center_shift_frac"] * spread + inventory_adjustment

    desired_bid = center - regime_params["bid_offset_frac"] * spread
    desired_ask = center + regime_params["ask_offset_frac"] * spread

    bid_price = round_down_to_tick(desired_bid)
    ask_price = round_up_to_tick(desired_ask)

    bid_price = max(best_bid, bid_price)
    ask_price = min(best_ask, ask_price)

    bid_price = min(bid_price, best_ask - TICK_SIZE)
    ask_price = max(ask_price, best_bid + TICK_SIZE)

    bid_price = round_down_to_tick(bid_price)
    ask_price = round_up_to_tick(ask_price)

    if ask_price - bid_price < TICK_SIZE:
        return None, None

    if bid_price >= ask_price:
        return None, None

    return bid_price, ask_price

def submit_quote_and_track(trader, symbol, side, lots, price, tracked_orders, signal, regime, step, position_shares):
    if lots <= 0:
        return None, None

    if side == "BUY":
        order = shift.Order(shift.Order.Type.LIMIT_BUY, symbol, int(lots), float(price))
    else:
        order = shift.Order(shift.Order.Type.LIMIT_SELL, symbol, int(lots), float(price))

    trader.submit_order(order)

    now_dt = datetime.now()
    sim_time = trader.get_last_trade_time()

    tracked_orders[order.id] = {
        "symbol": symbol,
        "side": side,
        "submit_time": now_dt,
        "limit_price": float(price),
        "lots": int(lots),
        "signal": signal,
        "regime": regime,
        "step": step,
        "done": False,
    }

    append_submission_log(
        sim_time,
        order.id,
        symbol,
        side,
        price,
        lots * LOT_SIZE,
        regime,
        signal,
        step,
        position_shares,
    )

    return order, now_dt

def poll_executions(trader, tracked_orders, seen_execution_keys):
    sim_time = trader.get_last_trade_time()
    for order_id, meta in list(tracked_orders.items()):
        try:
            executed_orders = trader.get_executed_orders(order_id)
            
            for ex in executed_orders:
                executed_size = int(getattr(ex, "executed_size", 0))
                executed_price = float(getattr(ex, "executed_price", 0.0000))
                
                if executed_size > 0:
                    exec_key = (
                        order_id,
                        str(getattr(ex, "timestamp", "")),
                        executed_size,
                        executed_price,
                        str(getattr(ex, "status", "")),
                    )

                    if exec_key not in seen_execution_keys:
                        seen_execution_keys.add(exec_key)

                        append_execution_log(
                            sim_time,
                            order_id,
                            getattr(ex, "symbol", meta["symbol"]),
                            meta["side"],
                            executed_price,
                            executed_size,
                            meta["lots"] * LOT_SIZE,
                            str(getattr(ex, "status", "")),
                            getattr(ex, "timestamp", ""),
                        )

                        print(
                            f"[EXEC AUDIT] order_id={order_id} side={meta['side']} "
                            f"exec_px={executed_price:.4f} "
                            f"exec_sz={executed_size} "
                            f"status={getattr(ex, 'status', '')} "
                            f"time={getattr(ex, 'timestamp', '')}"
                        )
            
            current_order = trader.get_order(order_id)
            if current_order is not None:
                status_str = str(getattr(current_order, "status", ""))
                total_executed = int(getattr(current_order, "executed_size", 0))
                original_size = meta["lots"] * LOT_SIZE

                if "FILLED" in status_str or "CANCELED" in status_str or "REJECTED" in status_str or total_executed >= original_size:
                    tracked_orders[order_id]["done"] = True
            
        except Exception as e:
            pass

    to_delete = [oid for oid, meta in tracked_orders.items() if meta.get("done")]
    for oid in to_delete:
        del tracked_orders[oid]

def check_live_order_status(trader, live_order):
    if live_order is None:
        return None
        
    try:
        updated_order = trader.get_order(live_order.id)
        if updated_order is None:
            return live_order
            
        status = str(updated_order.status)
        
        if "REJECTED" in status:
            return None
            
        if "FILLED" in status or "CANCELED" in status:
            return None
            
        return updated_order
    except Exception:
        return live_order

def order_age_seconds(submit_time, now_dt):
    if submit_time is None:
        return 1000000000.0000
    return (now_dt - submit_time).total_seconds()

def should_replace(existing_order, existing_submit_time, desired_price, desired_lots, force_refresh, now_dt):
    if existing_order is None:
        return True

    if force_refresh:
        return True

    existing_price = float(getattr(existing_order, "price", 0.0000))
    existing_size = int(getattr(existing_order, "size", 0))

    if existing_price == float(desired_price) and existing_size == int(desired_lots):
        return False

    age = order_age_seconds(existing_submit_time, now_dt)
    if age < MIN_REST_SECONDS:
        return False

    return True

def cancel_all_open_orders(trader, symbol):
    for order in trader.get_waiting_list():
        trader.submit_cancellation(order)
    time.sleep(1.0000)

def run_mlofi_market_maker(trader, symbol=SYMBOL, levels=LEVELS,
                           poll_interval=POLL_INTERVAL, end_time=None):
    ensure_csv_headers()

    prev_bids = None
    prev_asks = None

    ofi_events = deque()
    raw_ofi_history = deque(maxlen=PERSISTENCE_LOOKBACK)

    ema_t = None
    step = 0

    regime = "NEUTRAL"
    regime_expiry = None
    previous_regime = "NEUTRAL"

    prev_signal = None
    current_run_id = 0
    current_run_len = 0
    last_triggered_run_id = -1

    tracked_orders = {}
    seen_execution_keys = set()
    next_exec_audit_ts = time.time() + EXEC_AUDIT_INTERVAL_SECONDS

    last_position_shares = 0
    post_fill_until = None
    latest_position_delta = 0

    live_bid_order = None
    live_bid_submit_time = None
    live_ask_order = None
    live_ask_submit_time = None

    while datetime.now() < end_time:
        loop_start = time.time()
        now_dt = datetime.now()
        now_ts = time.time()

        if now_ts >= next_exec_audit_ts:
            poll_executions(trader, tracked_orders, seen_execution_keys)
            next_exec_audit_ts = now_ts + EXEC_AUDIT_INTERVAL_SECONDS

        live_bid_order = check_live_order_status(trader, live_bid_order)
        if live_bid_order is None:
            live_bid_submit_time = None

        live_ask_order = check_live_order_status(trader, live_ask_order)
        if live_ask_order is None:
            live_ask_submit_time = None

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

        if prev_bids is None or prev_asks is None:
            prev_bids = bids
            prev_asks = asks
            last_position_shares = get_position_shares(trader, symbol)
            print(f"\n--- Initial snapshot @ {now_dt} ---")
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

        current_run_id, current_run_len = update_signal_run(
            signal, prev_signal, current_run_id, current_run_len
        )

        regime, regime_expiry, last_triggered_run_id = maybe_activate_regime(
            signal=signal,
            ema_t=ema_t,
            run_id=current_run_id,
            run_len=current_run_len,
            last_triggered_run_id=last_triggered_run_id,
            regime=regime,
            regime_expiry=regime_expiry,
            now_dt=now_dt
        )

        regime_shift = regime != previous_regime

        position_shares = get_position_shares(trader, symbol)
        position_delta = position_shares - last_position_shares
        fill_detected = position_delta != 0

        print("regime_shift:", regime_shift)
        print("fill_detected:", fill_detected)
        print("current regime:", regime)
        print("previous regime:", previous_regime)
        print("position_shares:", position_shares)
        print("last_position_shares:", last_position_shares)
        print("position_delta:", position_delta)

        if fill_detected:
            latest_position_delta = position_delta
            post_fill_until = now_dt + timedelta(seconds=POST_FILL_RECOVERY_SECONDS)

            if live_bid_order is not None:
                cancel_order_safe(trader, live_bid_order)
                live_bid_order = None
                live_bid_submit_time = None

            if live_ask_order is not None:
                cancel_order_safe(trader, live_ask_order)
                live_ask_order = None
                live_ask_submit_time = None

        base_params = get_regime_params(regime)
        active_params = apply_post_fill_adjustments(
            regime_params=base_params,
            position_delta_shares=latest_position_delta,
            post_fill_until=post_fill_until,
            now_dt=now_dt
        )

        bid_price, ask_price = compute_quote_prices(
            best_bid=best_bid_p,
            best_ask=best_ask_p,
            mid=mid,
            spread=spread,
            regime_params=active_params,
            position_shares=position_shares
        )

        max_abs_shares = lots_to_shares(MAX_ABS_POSITION_LOTS)
        bid_lots = active_params["bid_lots"]
        ask_lots = active_params["ask_lots"]

        if position_shares >= max_abs_shares:
            bid_lots = 0
        if position_shares <= -max_abs_shares:
            ask_lots = 0

        desired_bid_lots = bid_lots if (bid_lots > 0 and bid_price is not None) else 0
        desired_ask_lots = ask_lots if (ask_lots > 0 and ask_price is not None) else 0

        print("live_bid_order exists:", live_bid_order is not None)
        print("live_ask_order exists:", live_ask_order is not None)
        print("desired_bid:", bid_price, desired_bid_lots)
        print("desired_ask:", ask_price, desired_ask_lots)

        if live_bid_order is not None:
            print(
                "live_bid_details:",
                live_bid_order.id,
                float(live_bid_order.price),
                int(live_bid_order.size),
                order_age_seconds(live_bid_submit_time, now_dt)
            )
        if live_ask_order is not None:
            print(
                "live_ask_details:",
                live_ask_order.id,
                float(live_ask_order.price),
                int(live_ask_order.size),
                order_age_seconds(live_ask_submit_time, now_dt)
            )

        force_refresh = regime_shift or fill_detected
        print("force_refresh:", force_refresh)

        if desired_bid_lots == 0 or bid_price is None:
            if live_bid_order is not None:
                print("canceling live bid because desired bid is none/zero")
                cancel_order_safe(trader, live_bid_order)
                live_bid_order = None
                live_bid_submit_time = None
        else:
            replace_bid = should_replace(
                existing_order=live_bid_order,
                existing_submit_time=live_bid_submit_time,
                desired_price=bid_price,
                desired_lots=desired_bid_lots,
                force_refresh=force_refresh,
                now_dt=now_dt
            )
            print("replace_bid:", replace_bid)

            if replace_bid:
                if live_bid_order is not None:
                    print(f"replacing bid {live_bid_order.id}")
                    cancel_order_safe(trader, live_bid_order)
                live_bid_order, live_bid_submit_time = submit_quote_and_track(
                    trader=trader,
                    symbol=symbol,
                    side="BUY",
                    lots=desired_bid_lots,
                    price=bid_price,
                    tracked_orders=tracked_orders,
                    signal=signal,
                    regime=regime,
                    step=step,
                    position_shares=position_shares
                )
                print(f"submitted new bid {live_bid_order.id}")

        if desired_ask_lots == 0 or ask_price is None:
            if live_ask_order is not None:
                print("canceling live ask because desired ask is none/zero")
                cancel_order_safe(trader, live_ask_order)
                live_ask_order = None
                live_ask_submit_time = None
        else:
            replace_ask = should_replace(
                existing_order=live_ask_order,
                existing_submit_time=live_ask_submit_time,
                desired_price=ask_price,
                desired_lots=desired_ask_lots,
                force_refresh=force_refresh,
                now_dt=now_dt
            )
            print("replace_ask:", replace_ask)

            if replace_ask:
                if live_ask_order is not None:
                    print(f"replacing ask {live_ask_order.id}")
                    cancel_order_safe(trader, live_ask_order)
                live_ask_order, live_ask_submit_time = submit_quote_and_track(
                    trader=trader,
                    symbol=symbol,
                    side="SELL",
                    lots=desired_ask_lots,
                    price=ask_price,
                    tracked_orders=tracked_orders,
                    signal=signal,
                    regime=regime,
                    step=step,
                    position_shares=position_shares
                )
                print(f"submitted new ask {live_ask_order.id}")

        step += 1

        live_bid_price = float(getattr(live_bid_order, "price", 0.0000)) if live_bid_order is not None else None
        live_ask_price = float(getattr(live_ask_order, "price", 0.0000)) if live_ask_order is not None else None
        live_bid_age = order_age_seconds(live_bid_submit_time, now_dt) if live_bid_submit_time is not None else None
        live_ask_age = order_age_seconds(live_ask_submit_time, now_dt) if live_ask_submit_time is not None else None

        expiry_str = regime_expiry.strftime("%H:%M:%S") if regime_expiry is not None else "None"
        post_fill_str = post_fill_until.strftime("%H:%M:%S") if post_fill_until is not None else "None"

        sim_time_now = trader.get_last_trade_time()
        print(f"\n--- MM Loop @ {now_dt} | step {step} ---")
        print(f"Best Bid:          {best_bid_p:.4f} x {best_bid_q:.4f}")
        print(f"Best Ask:          {best_ask_p:.4f} x {best_ask_q:.4f}")
        print(f"Mid:               {mid:.4f}")
        print(f"Spread:            {spread:.4f}")
        print("Level OFI inc 1s: ", [round(x, 4) for x in level_increment])
        print("Level OFI 5s:     ", [round(x, 4) for x in level_ofi_5s])
        print(f"Raw OFI_t:         {raw_ofi_t:.4f}")
        print(f"EMA_t:             {ema_t:.4f}")
        print(f"Signal:            {signal}")
        print(f"Regime:            {regime}")
        print(f"Regime shift:      {regime_shift}")
        print(f"Regime expiry:     {expiry_str}")
        print(f"Position shares:   {position_shares}")
        print(f"Position delta:    {position_delta}")
        print(f"Post-fill until:   {post_fill_str}")
        print(f"Desired bid:       {bid_price} / lots {desired_bid_lots}")
        print(f"Desired ask:       {ask_price} / lots {desired_ask_lots}")
        print(f"Live bid:          {live_bid_price} / age {live_bid_age}")
        print(f"Live ask:          {live_ask_price} / age {live_ask_age}")
        print(f"Tracked order ids: {len(tracked_orders)}")
        print(f"Sim time now: {sim_time_now}")
        
        bp = trader.get_portfolio_summary().get_total_bp()
        print(f"Available Buying Power: {bp:.4f}")

        prev_bids = bids
        prev_asks = asks
        prev_signal = signal
        previous_regime = regime
        last_position_shares = position_shares

        if post_fill_until is not None and now_dt >= post_fill_until:
            latest_position_delta = 0
            post_fill_until = None

        elapsed = time.time() - loop_start
        time.sleep(max(poll_interval - elapsed, 0.0000))

    poll_executions(trader, tracked_orders, seen_execution_keys)

    if live_bid_order is not None:
        cancel_order_safe(trader, live_bid_order)
    if live_ask_order is not None:
        cancel_order_safe(trader, live_ask_order)

if __name__ == "__main__":
    with shift.Trader("columbia-traders") as trader:
        trader.connect("initiator.cfg", "aRkkZSrj")
        time.sleep(1.0000)
        
        cancel_all_open_orders(trader, SYMBOL)

        trader.sub_all_order_book()
        time.sleep(1.0000)

        end_time = datetime.now() + timedelta(minutes=500.0000)

        try:
            run_mlofi_market_maker(
                trader,
                symbol=SYMBOL,
                levels=LEVELS,
                poll_interval=POLL_INTERVAL,
                end_time=end_time
            )
        except KeyboardInterrupt:
            trader.disconnect()