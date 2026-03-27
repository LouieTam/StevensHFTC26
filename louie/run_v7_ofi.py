import shift
import time
import math
import csv
import os
from collections import deque
from datetime import datetime, timedelta

# Claude version

SYMBOL = "GS"
LEVELS = 10
POLL_INTERVAL = 1.0000
# OFI_WINDOW_SECONDS removed — EMA now runs directly on per-tick increments (single smoothing layer)
EMA_ALPHA = 0.1500                       # reduced from 0.30; per-tick increments are noisier than
                                         # a rolling sum, so we smooth more aggressively

LEVEL_WEIGHTS = [1.0000, 0.9000, 0.8000, 0.7000, 0.6000, 0.5000, 0.4000, 0.3000, 0.2000, 0.1000]

# Single entry threshold — OFI_FLOOR and FINAL_SCORE_THRESHOLD collapsed into one value.
# Signal is BUY_PRESSURE / SELL_PRESSURE when abs(ema_t) > EMA_ENTRY_THRESHOLD.
# Regime activates on that same condition — no redundant intermediate gate.
EMA_ENTRY_THRESHOLD = 3.0000

# Dynamic regime decay — exit early if EMA falls to this fraction of its activation value
REGIME_DECAY_THRESHOLD = 0.4000         # exit if EMA lost 60% of entry strength

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

NEUTRAL_BID_OFFSET_FRAC = 0.2000
NEUTRAL_ASK_OFFSET_FRAC = 0.2000

# BUY regime — bid-only, no ask posted
BUY_BID_OFFSET_FRAC = 0.1800          # tight bid, close to mid to maximise fill probability

# SELL regime — ask-only, no bid posted
SELL_ASK_OFFSET_FRAC = 0.1800         # tight ask, close to mid to maximise fill probability

NEUTRAL_UNWIND_CENTER_SHIFT_PER_LOT = 0.0100
NEUTRAL_UNWIND_MAX_CENTER_SHIFT = 0.0800

NEUTRAL_LONG_BID_OFFSET_FRAC = 0.3500
NEUTRAL_LONG_ASK_OFFSET_FRAC = 0.1500

NEUTRAL_SHORT_BID_OFFSET_FRAC = 0.1500
NEUTRAL_SHORT_ASK_OFFSET_FRAC = 0.3500

NEUTRAL_UNWIND_SIZE_THRESHOLD_LOTS = 1
NEUTRAL_UNWIND_BASE_LOTS = 2
NEUTRAL_UNWIND_MAX_LOTS = 4
NEUTRAL_UNWIND_STEP_SHARES = 500

REGIME_HOLD_SECONDS = 10.0000
POST_FILL_RECOVERY_SECONDS = 2.0000

EXEC_AUDIT_INTERVAL_SECONDS = 30.0000   # FIX: was 300s — reduced so fills log promptly
RECONCILE_INTERVAL_SECONDS = 15.0000    # FIX: new — periodic orphan-order sweep
CANCEL_WAIT_SECONDS = 0.5000
MIN_REST_SECONDS = 5.0000

SUBMISSION_LOG_PATH = "mm_order_submissions.csv"
EXECUTION_LOG_PATH = "mm_order_executions.csv"

# ---------------------------------------------------------------------------
# Tick helpers
# ---------------------------------------------------------------------------

def round_down_to_tick(x, tick=TICK_SIZE):
    return math.floor(round(x / tick, 6)) * tick

def round_up_to_tick(x, tick=TICK_SIZE):
    return math.ceil(round(x / tick, 6)) * tick

def lots_to_shares(lots):
    return int(lots * LOT_SIZE)

# ---------------------------------------------------------------------------
# CSV logging
# ---------------------------------------------------------------------------

def ensure_csv_headers():
    if not os.path.exists(SUBMISSION_LOG_PATH):
        with open(SUBMISSION_LOG_PATH, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                "logged_at", "order_id", "symbol", "side", "limit_price",
                "shares", "lots", "regime", "signal", "step",
                "position_shares_before_submit",
            ])

    if not os.path.exists(EXECUTION_LOG_PATH):
        with open(EXECUTION_LOG_PATH, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                "logged_at", "order_id", "symbol", "side", "executed_price",
                "executed_size", "order_size", "status", "exec_timestamp",
            ])

def append_submission_log(sim_time, order_id, symbol, side, limit_price,
                          shares, regime, signal, step, position_shares):
    with open(SUBMISSION_LOG_PATH, "a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            sim_time, order_id, symbol, side,
            f"{float(limit_price):.4f}", int(shares), int(shares // LOT_SIZE),
            regime, signal, step, int(position_shares),
        ])

def append_execution_log(sim_time, order_id, symbol, side, executed_price,
                         executed_size, order_size, status, exec_timestamp):
    with open(EXECUTION_LOG_PATH, "a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            sim_time, order_id, symbol, side,
            f"{float(executed_price):.4f}", int(executed_size),
            int(order_size), str(status), exec_timestamp,
        ])

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
    new_bids  = pad_book_side(new_bids,  levels)
    new_asks  = pad_book_side(new_asks,  levels)

    level_increments = []
    for m in range(levels):
        e_m = compute_level_ofi(
            prev_bids[m][0], prev_bids[m][1], prev_asks[m][0], prev_asks[m][1],
            new_bids[m][0],  new_bids[m][1],  new_asks[m][0],  new_asks[m][1],
        )
        level_increments.append(e_m)
    return level_increments

def weighted_raw_ofi(level_ofi, weights):
    return float(sum(w * x for w, x in zip(weights, level_ofi)))

# ---------------------------------------------------------------------------
# Signal / regime logic
# ---------------------------------------------------------------------------

def classify_signal(ema_t, entry_threshold):
    """
    Single-gate signal classification.

    The EMA now runs on per-tick weighted OFI increments directly (no rolling
    window pre-smoothing), so there is only one smoothing layer.  The three
    previous thresholds (OFI_FLOOR, FINAL_SCORE_THRESHOLD, EMA_ENTRY_THRESHOLD)
    have been collapsed into a single EMA_ENTRY_THRESHOLD check.

    Returns (signal_str, normalised_score).
    normalised_score = abs(ema_t) / entry_threshold, clamped to [0, 1].
    Used downstream for regime decay ratio tracking.
    """
    abs_ema    = abs(ema_t)
    normalised = min(abs_ema / entry_threshold, 1.0)

    if ema_t > 0 and abs_ema > entry_threshold:
        return "BUY_PRESSURE", normalised
    if ema_t < 0 and abs_ema > entry_threshold:
        return "SELL_PRESSURE", -normalised
    return "NEUTRAL", 0.0


def update_signal_run(signal, prev_signal, current_run_id):
    """
    Track run identity only — run_len gate removed.
    A new run_id is issued whenever the directional signal flips, which is
    enough to prevent re-triggering the same momentum burst.
    """
    if signal in ("BUY_PRESSURE", "SELL_PRESSURE") and signal != prev_signal:
        current_run_id += 1
    return current_run_id


def maybe_activate_regime(signal, ema_t, run_id, last_triggered_run_id,
                          regime, regime_expiry, regime_entry_ema, now_dt):
    """
    Activate / maintain / decay a directional regime.

    Entry:  signal crosses EMA_ENTRY_THRESHOLD — no run-length gate needed
            because the 15s OFI window + EMA already smooth out single spikes.

    Decay:  if the EMA falls to REGIME_DECAY_THRESHOLD * entry_ema, exit early.
            The hard expiry (REGIME_HOLD_SECONDS) is a safety ceiling only.
    """
    target_regime = "BUY" if signal == "BUY_PRESSURE" else (
                    "SELL" if signal == "SELL_PRESSURE" else None)

    # --- Entry / re-entry ---
    if target_regime is not None and abs(ema_t) > EMA_ENTRY_THRESHOLD:
        if run_id != last_triggered_run_id or regime != target_regime:
            regime            = target_regime
            regime_expiry     = now_dt + timedelta(seconds=REGIME_HOLD_SECONDS)
            regime_entry_ema  = abs(ema_t)
            last_triggered_run_id = run_id
        else:
            # Same run still going — push out the safety ceiling
            regime_expiry = max(regime_expiry, now_dt + timedelta(seconds=REGIME_HOLD_SECONDS))
            # Update entry EMA upward only (track the peak of the current burst)
            if abs(ema_t) > regime_entry_ema:
                regime_entry_ema = abs(ema_t)

    # --- Dynamic decay exit ---
    if regime in ("BUY", "SELL") and regime_entry_ema is not None:
        decay_ratio = abs(ema_t) / regime_entry_ema
        if decay_ratio < REGIME_DECAY_THRESHOLD:
            print(f"[REGIME DECAY] {regime} → NEUTRAL  "
                  f"ema={ema_t:.2f}  entry_ema={regime_entry_ema:.2f}  "
                  f"decay_ratio={decay_ratio:.3f}")
            regime           = "NEUTRAL"
            regime_expiry    = None
            regime_entry_ema = None

    # --- Hard expiry ceiling ---
    if regime_expiry is not None and now_dt >= regime_expiry:
        print(f"[REGIME EXPIRY] {regime} → NEUTRAL (hard expiry)")
        regime           = "NEUTRAL"
        regime_expiry    = None
        regime_entry_ema = None

    return regime, regime_expiry, regime_entry_ema, last_triggered_run_id

# ---------------------------------------------------------------------------
# Position
# ---------------------------------------------------------------------------

def get_position_shares(trader, symbol):
    item = trader.get_portfolio_item(symbol)
    long_shares  = int(item.get_long_shares())
    short_shares = int(item.get_short_shares())
    return long_shares - short_shares

# ---------------------------------------------------------------------------
# Order cancellation
# ---------------------------------------------------------------------------

def cancel_order_safe(trader, order):
    """
    Submit a cancellation and return True if the call succeeded.
    Callers must NOT assume the order is gone just because this returns True —
    the exchange may still reject the cancel.  Always verify via get_order().
    """
    if order is None:
        return False
    try:
        trader.submit_cancellation(order)
        time.sleep(CANCEL_WAIT_SECONDS)
        return True
    except Exception as e:
        print(f"[WARN] cancel_order_safe failed for {order.id}: {e}")
        return False

def cancel_all_open_orders(trader, symbol):
    """Cancel everything in the waiting list and wait for confirmation."""
    orders = trader.get_waiting_list()
    for order in orders:
        try:
            trader.submit_cancellation(order)
        except Exception:
            pass
    if orders:
        time.sleep(2.0000)   # give exchange time to process

    # Verify they're gone; retry any survivors once
    survivors = [o for o in trader.get_waiting_list() if o.symbol == symbol]
    for order in survivors:
        try:
            trader.submit_cancellation(order)
        except Exception:
            pass
    if survivors:
        time.sleep(1.0000)

# ---------------------------------------------------------------------------
# FIX: Orphan-order reconciliation
# ---------------------------------------------------------------------------

def reconcile_open_orders(trader, symbol, live_bid_order, live_ask_order):
    """
    Cancel any resting orders for `symbol` that are NOT our current live quotes.
    This cleans up orphans from lost references, failed cancels, or restarts.
    """
    expected_ids = set()
    if live_bid_order is not None:
        expected_ids.add(live_bid_order.id)
    if live_ask_order is not None:
        expected_ids.add(live_ask_order.id)

    cancelled = 0
    for order in trader.get_waiting_list():
        if order.symbol == symbol and order.id not in expected_ids:
            print(f"[RECONCILE] Cancelling orphaned order {order.id} "
                  f"side={order.type} price={float(order.price):.4f} size={int(order.size)}")
            try:
                trader.submit_cancellation(order)
                cancelled += 1
            except Exception as e:
                print(f"[RECONCILE] Failed to cancel {order.id}: {e}")

    if cancelled:
        time.sleep(CANCEL_WAIT_SECONDS * cancelled)

# ---------------------------------------------------------------------------
# Regime / quote params
# ---------------------------------------------------------------------------

def get_regime_params(regime):
    """
    BUY regime  — bid only, no ask.  We believe price is moving up; posting an
                  ask would mean selling cheap into the momentum we just detected.
                  Shift center slightly upward to lean into the move.

    SELL regime — ask only, no bid.  Mirror of BUY.

    NEUTRAL     — symmetric two-sided market making.
    """
    if regime == "BUY":
        return {
            "target_inventory_shares": lots_to_shares(BUY_TARGET_LOTS),
            "center_shift_frac":       BUY_CENTER_SHIFT_FRAC,
            "bid_offset_frac":         BUY_BID_OFFSET_FRAC,
            "ask_offset_frac":         0.0,       # unused — no ask posted in BUY
            "bid_lots":                FAVORED_SIDE_LOTS,
            "ask_lots":                0,          # no ask in BUY regime
        }
    if regime == "SELL":
        return {
            "target_inventory_shares": lots_to_shares(SELL_TARGET_LOTS),
            "center_shift_frac":       SELL_CENTER_SHIFT_FRAC,
            "bid_offset_frac":         0.0,        # unused — no bid posted in SELL
            "ask_offset_frac":         SELL_ASK_OFFSET_FRAC,
            "bid_lots":                0,           # no bid in SELL regime
            "ask_lots":                FAVORED_SIDE_LOTS,
        }
    return {
        "target_inventory_shares": 0,
        "center_shift_frac":       0.0000,
        "bid_offset_frac":         NEUTRAL_BID_OFFSET_FRAC,
        "ask_offset_frac":         NEUTRAL_ASK_OFFSET_FRAC,
        "bid_lots":                BASE_BID_LOTS,
        "ask_lots":                BASE_ASK_LOTS,
    }

def compute_neutral_unwind_lots(position_shares):
    abs_shares  = abs(int(position_shares))
    extra_lots  = abs_shares // NEUTRAL_UNWIND_STEP_SHARES
    unwind_lots = NEUTRAL_UNWIND_BASE_LOTS + extra_lots
    return int(min(unwind_lots, NEUTRAL_UNWIND_MAX_LOTS))

def apply_neutral_inventory_unwind(regime_params, regime, position_shares):
    params = dict(regime_params)
    if regime != "NEUTRAL":
        return params

    position_lots = position_shares / LOT_SIZE
    if abs(position_lots) < NEUTRAL_UNWIND_SIZE_THRESHOLD_LOTS:
        return params

    unwind_shift = NEUTRAL_UNWIND_CENTER_SHIFT_PER_LOT * abs(position_lots)
    unwind_shift = min(unwind_shift, NEUTRAL_UNWIND_MAX_CENTER_SHIFT)
    unwind_lots  = compute_neutral_unwind_lots(position_shares)

    if position_shares > 0:
        params["center_shift_frac"] -= unwind_shift
        params["bid_offset_frac"]    = NEUTRAL_LONG_BID_OFFSET_FRAC
        params["ask_offset_frac"]    = NEUTRAL_LONG_ASK_OFFSET_FRAC
        params["bid_lots"]           = 0
        params["ask_lots"]           = unwind_lots
    elif position_shares < 0:
        params["center_shift_frac"] += unwind_shift
        params["bid_offset_frac"]    = NEUTRAL_SHORT_BID_OFFSET_FRAC
        params["ask_offset_frac"]    = NEUTRAL_SHORT_ASK_OFFSET_FRAC
        params["bid_lots"]           = unwind_lots
        params["ask_lots"]           = 0

    return params

def apply_post_fill_adjustments(regime_params, position_delta_shares,
                                post_fill_until, now_dt):
    params = dict(regime_params)
    if post_fill_until is None or now_dt >= post_fill_until or position_delta_shares == 0:
        return params

    if position_delta_shares > 0:
        params["bid_lots"]           = 0
        params["ask_lots"]           = max(params["ask_lots"], FAVORED_SIDE_LOTS)
        params["center_shift_frac"] -= 0.0300
    else:
        params["ask_lots"]           = 0
        params["bid_lots"]           = max(params["bid_lots"], FAVORED_SIDE_LOTS)
        params["center_shift_frac"] += 0.0300

    return params

# ---------------------------------------------------------------------------
# Quote price computation
# ---------------------------------------------------------------------------

def compute_quote_prices(best_bid, best_ask, mid, spread, regime_params, position_shares):
    """
    In BUY regime ask_lots==0, so ask_price=None is returned (and vice versa for SELL).
    Spread validation only applies when both sides are active (NEUTRAL).
    """
    target_inventory_shares = regime_params["target_inventory_shares"]
    inv_diff_lots        = (position_shares - target_inventory_shares) / LOT_SIZE
    inventory_adjustment = -INVENTORY_SKEW_PER_LOT * inv_diff_lots
    inventory_adjustment = max(-MAX_INVENTORY_SKEW, min(MAX_INVENTORY_SKEW, inventory_adjustment))

    center   = mid + regime_params["center_shift_frac"] * spread + inventory_adjustment
    want_bid = regime_params["bid_lots"] > 0
    want_ask = regime_params["ask_lots"] > 0

    bid_price = None
    ask_price = None

    if want_bid:
        desired_bid = center - regime_params["bid_offset_frac"] * spread
        bid_price   = round_down_to_tick(desired_bid)
        bid_price   = max(best_bid, bid_price)
        bid_price   = min(bid_price, best_ask - TICK_SIZE)
        bid_price   = round_down_to_tick(bid_price)

    if want_ask:
        desired_ask = center + regime_params["ask_offset_frac"] * spread
        ask_price   = round_up_to_tick(desired_ask)
        ask_price   = min(best_ask, ask_price)
        ask_price   = max(ask_price, best_bid + TICK_SIZE)
        ask_price   = round_up_to_tick(ask_price)

    if want_bid and want_ask:
        if ask_price <= bid_price:
            bid_price = round_down_to_tick(mid - TICK_SIZE)
            ask_price = round_up_to_tick(mid + TICK_SIZE)
            bid_price = min(bid_price, best_ask - TICK_SIZE)
            ask_price = max(ask_price, best_bid + TICK_SIZE)
            bid_price = round_down_to_tick(bid_price)
            ask_price = round_up_to_tick(ask_price)
        if ask_price - bid_price < TICK_SIZE or bid_price >= ask_price:
            return None, None

    return bid_price, ask_price

# ---------------------------------------------------------------------------
# Order submission / tracking
# ---------------------------------------------------------------------------

def submit_quote_and_track(trader, symbol, side, lots, price, tracked_orders,
                           signal, regime, step, position_shares):
    if lots <= 0:
        return None, None

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
        "regime":      regime,
        "step":        step,
        "done":        False,
    }

    append_submission_log(
        sim_time, order.id, symbol, side, price,
        lots * LOT_SIZE, regime, signal, step, position_shares,
    )

    return order, now_dt

# ---------------------------------------------------------------------------
# Execution audit
# ---------------------------------------------------------------------------

def poll_executions(trader, tracked_orders, seen_execution_keys):
    sim_time = trader.get_last_trade_time()
    for order_id, meta in list(tracked_orders.items()):
        try:
            executed_orders = trader.get_executed_orders(order_id)
            for ex in executed_orders:
                executed_size  = int(getattr(ex,  "executed_size",  0))
                executed_price = float(getattr(ex, "executed_price", 0.0000))

                if executed_size > 0:
                    exec_key = (
                        order_id,
                        str(getattr(ex,  "timestamp", "")),
                        executed_size,
                        executed_price,
                        str(getattr(ex,  "status",    "")),
                    )
                    if exec_key not in seen_execution_keys:
                        seen_execution_keys.add(exec_key)
                        append_execution_log(
                            sim_time, order_id,
                            getattr(ex, "symbol", meta["symbol"]),
                            meta["side"], executed_price, executed_size,
                            meta["lots"] * LOT_SIZE,
                            str(getattr(ex, "status",    "")),
                            getattr(ex,     "timestamp", ""),
                        )
                        print(
                            f"[EXEC AUDIT] order_id={order_id} side={meta['side']} "
                            f"exec_px={executed_price:.4f} exec_sz={executed_size} "
                            f"status={getattr(ex, 'status', '')} "
                            f"time={getattr(ex, 'timestamp', '')}"
                        )

            current_order = trader.get_order(order_id)
            if current_order is not None:
                status_str     = str(getattr(current_order, "status",        ""))
                total_executed = int(getattr(current_order, "executed_size",  0))
                original_size  = meta["lots"] * LOT_SIZE
                if ("FILLED" in status_str or "CANCELED" in status_str
                        or "REJECTED" in status_str
                        or total_executed >= original_size):
                    tracked_orders[order_id]["done"] = True

        except Exception:
            pass

    to_delete = [oid for oid, m in tracked_orders.items() if m.get("done")]
    for oid in to_delete:
        del tracked_orders[oid]

# ---------------------------------------------------------------------------
# FIX: Live-order status check — explicit treatment of None return value
# ---------------------------------------------------------------------------

def check_live_order_status(trader, live_order):
    """
    Returns the (possibly updated) order object, or None if it is done/gone.

    IMPORTANT: get_order() returning None is ambiguous — it can mean the order
    was filled OR that the exchange hasn't acknowledged it yet.  We keep the
    stale reference in that case so the position-delta check can still detect
    the fill on the next loop.  Only a confirmed terminal status clears the ref.
    """
    if live_order is None:
        return None
    try:
        updated_order = trader.get_order(live_order.id)
        if updated_order is None:
            # Ambiguous — do NOT treat as gone; keep reference until we see
            # a position delta that confirms the fill.
            return live_order

        status = str(updated_order.status)
        if "REJECTED" in status or "FILLED" in status or "CANCELED" in status:
            return None

        return updated_order
    except Exception:
        return live_order

# ---------------------------------------------------------------------------
# FIX: should_replace — compare sizes in shares, not lots
# ---------------------------------------------------------------------------

def order_age_seconds(submit_time, now_dt):
    if submit_time is None:
        return 1_000_000_000.0
    return (now_dt - submit_time).total_seconds()

def should_replace(existing_order, existing_submit_time, desired_price,
                   desired_lots, force_refresh, now_dt):
    if existing_order is None:
        return True
    if force_refresh:
        return True

    existing_price  = float(getattr(existing_order, "price", 0.0000))
    # FIX: existing_order.size is in SHARES; convert desired_lots to shares for comparison
    existing_shares = int(getattr(existing_order,   "size",  0))
    desired_shares  = int(desired_lots * LOT_SIZE)

    if existing_price == float(desired_price) and existing_shares == desired_shares:
        return False

    age = order_age_seconds(existing_submit_time, now_dt)
    if age < MIN_REST_SECONDS:
        return False

    return True

# ---------------------------------------------------------------------------
# Main market-making loop
# ---------------------------------------------------------------------------

def run_mlofi_market_maker(trader, symbol=SYMBOL, levels=LEVELS,
                           poll_interval=POLL_INTERVAL, end_time=None):
    ensure_csv_headers()

    prev_bids = None
    prev_asks = None

    ema_t = None
    step  = 0

    regime            = "NEUTRAL"
    regime_expiry     = None
    regime_entry_ema  = None          # EMA magnitude at the moment of regime activation
    previous_regime   = "NEUTRAL"

    prev_signal           = None
    current_run_id        = 0
    last_triggered_run_id = -1

    tracked_orders      = {}
    seen_execution_keys = set()
    next_exec_audit_ts  = time.time() + EXEC_AUDIT_INTERVAL_SECONDS
    next_reconcile_ts   = time.time() + RECONCILE_INTERVAL_SECONDS  # FIX: new

    last_position_shares  = 0
    post_fill_until       = None
    latest_position_delta = 0

    live_bid_order      = None
    live_bid_submit_time = None
    live_ask_order      = None
    live_ask_submit_time = None

    while datetime.now() < end_time:
        loop_start = time.time()
        now_dt     = datetime.now()
        now_ts     = time.time()

        # ------------------------------------------------------------------
        # FIX: Expire post_fill at the TOP of the loop so the rest of this
        # iteration uses clean state (was at the bottom — one tick stale).
        # ------------------------------------------------------------------
        if post_fill_until is not None and now_dt >= post_fill_until:
            latest_position_delta = 0
            post_fill_until       = None

        # Periodic execution audit
        if now_ts >= next_exec_audit_ts:
            poll_executions(trader, tracked_orders, seen_execution_keys)
            next_exec_audit_ts = now_ts + EXEC_AUDIT_INTERVAL_SECONDS

        # FIX: Periodic orphan-order reconciliation
        if now_ts >= next_reconcile_ts:
            reconcile_open_orders(trader, symbol, live_bid_order, live_ask_order)
            next_reconcile_ts = now_ts + RECONCILE_INTERVAL_SECONDS

        # Check live order statuses
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

        mid    = 0.5000 * (best_bid_p + best_ask_p)
        spread = best_ask_p - best_bid_p

        if prev_bids is None or prev_asks is None:
            prev_bids = bids
            prev_asks = asks
            last_position_shares = get_position_shares(trader, symbol)
            print(f"\n--- Initial snapshot @ {now_dt} ---")
            print(f"Best Bid: {best_bid_p:.4f} x {best_bid_q:.4f}")
            print(f"Best Ask: {best_ask_p:.4f} x {best_ask_q:.4f}")
            print(f"Mid:      {mid:.4f}")
            print(f"Spread:   {spread:.4f}")
            time.sleep(poll_interval)
            continue

        # OFI computation — EMA runs directly on the per-tick weighted increment
        level_increment = compute_multilevel_ofi_increment(
            prev_bids, prev_asks, bids, asks, levels
        )

        raw_ofi_t = weighted_raw_ofi(level_increment, LEVEL_WEIGHTS)
        ema_t     = ema_update(ema_t, raw_ofi_t, EMA_ALPHA)

        signal, final_score = classify_signal(
            ema_t=ema_t,
            entry_threshold=EMA_ENTRY_THRESHOLD,
        )

        current_run_id = update_signal_run(signal, prev_signal, current_run_id)

        # Capture previous regime BEFORE updating, so regime_shift is correct
        # when used for force_refresh below.
        previous_regime_this_tick = previous_regime

        regime, regime_expiry, regime_entry_ema, last_triggered_run_id = maybe_activate_regime(
            signal=signal,
            ema_t=ema_t,
            run_id=current_run_id,
            last_triggered_run_id=last_triggered_run_id,
            regime=regime,
            regime_expiry=regime_expiry,
            regime_entry_ema=regime_entry_ema,
            now_dt=now_dt,
        )

        # FIX: compare against the value from the START of this tick
        regime_shift = regime != previous_regime_this_tick

        position_shares = get_position_shares(trader, symbol)
        position_delta  = position_shares - last_position_shares
        fill_detected   = position_delta != 0

        print("regime_shift:",         regime_shift)
        print("fill_detected:",        fill_detected)
        print("current regime:",       regime)
        print("previous regime:",      previous_regime_this_tick)
        print("regime_entry_ema:",     regime_entry_ema)
        print("position_shares:",      position_shares)
        print("last_position_shares:", last_position_shares)
        print("position_delta:",       position_delta)

        if fill_detected:
            latest_position_delta = position_delta
            post_fill_until       = now_dt + timedelta(seconds=POST_FILL_RECOVERY_SECONDS)

            # Cancel live quotes immediately on fill
            if live_bid_order is not None:
                cancel_order_safe(trader, live_bid_order)
                live_bid_order      = None
                live_bid_submit_time = None

            if live_ask_order is not None:
                cancel_order_safe(trader, live_ask_order)
                live_ask_order      = None
                live_ask_submit_time = None

            # FIX: trigger an immediate exec audit and reconciliation on fill
            poll_executions(trader, tracked_orders, seen_execution_keys)
            reconcile_open_orders(trader, symbol, live_bid_order, live_ask_order)

        base_params  = get_regime_params(regime)
        base_params  = apply_neutral_inventory_unwind(
            regime_params=base_params, regime=regime, position_shares=position_shares
        )
        active_params = apply_post_fill_adjustments(
            regime_params=base_params,
            position_delta_shares=latest_position_delta,
            post_fill_until=post_fill_until,
            now_dt=now_dt,
        )

        neutral_unwind_active = (
            regime == "NEUTRAL"
            and abs(position_shares / LOT_SIZE) >= NEUTRAL_UNWIND_SIZE_THRESHOLD_LOTS
        )

        print("neutral_unwind_active:",    neutral_unwind_active)
        print("active center_shift_frac:", active_params["center_shift_frac"])
        print("active bid_offset_frac:",   active_params["bid_offset_frac"])
        print("active ask_offset_frac:",   active_params["ask_offset_frac"])
        print("active bid_lots:",          active_params["bid_lots"])
        print("active ask_lots:",          active_params["ask_lots"])

        bid_price, ask_price = compute_quote_prices(
            best_bid=best_bid_p,
            best_ask=best_ask_p,
            mid=mid,
            spread=spread,
            regime_params=active_params,
            position_shares=position_shares,
        )

        max_abs_shares = lots_to_shares(MAX_ABS_POSITION_LOTS)
        bid_lots       = active_params["bid_lots"]
        ask_lots       = active_params["ask_lots"]

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
                order_age_seconds(live_bid_submit_time, now_dt),
            )
        if live_ask_order is not None:
            print(
                "live_ask_details:",
                live_ask_order.id,
                float(live_ask_order.price),
                int(live_ask_order.size),
                order_age_seconds(live_ask_submit_time, now_dt),
            )

        # FIX: force_refresh only combines fill_detected with regime_shift.
        # Both cancellations from fill_detected already happened above, so
        # the live_*_order refs are already None — force_refresh here only
        # drives the replace logic for cases where orders survived.
        force_refresh = regime_shift  # fill_detected already cancelled above

        print("force_refresh:", force_refresh)

        # --- BID side ---
        if desired_bid_lots == 0 or bid_price is None:
            if live_bid_order is not None:
                print("canceling live bid because desired bid is none/zero")
                cancel_order_safe(trader, live_bid_order)
                live_bid_order       = None
                live_bid_submit_time = None
        else:
            replace_bid = should_replace(
                existing_order=live_bid_order,
                existing_submit_time=live_bid_submit_time,
                desired_price=bid_price,
                desired_lots=desired_bid_lots,
                force_refresh=force_refresh,
                now_dt=now_dt,
            )
            print("replace_bid:", replace_bid)

            if replace_bid:
                if live_bid_order is not None:
                    print(f"replacing bid {live_bid_order.id}")
                    cancel_order_safe(trader, live_bid_order)
                live_bid_order, live_bid_submit_time = submit_quote_and_track(
                    trader=trader, symbol=symbol, side="BUY",
                    lots=desired_bid_lots, price=bid_price,
                    tracked_orders=tracked_orders, signal=signal,
                    regime=regime, step=step, position_shares=position_shares,
                )
                print(f"submitted new bid {live_bid_order.id}")

        # --- ASK side ---
        if desired_ask_lots == 0 or ask_price is None:
            if live_ask_order is not None:
                print("canceling live ask because desired ask is none/zero")
                cancel_order_safe(trader, live_ask_order)
                live_ask_order       = None
                live_ask_submit_time = None
        else:
            replace_ask = should_replace(
                existing_order=live_ask_order,
                existing_submit_time=live_ask_submit_time,
                desired_price=ask_price,
                desired_lots=desired_ask_lots,
                force_refresh=force_refresh,
                now_dt=now_dt,
            )
            print("replace_ask:", replace_ask)

            if replace_ask:
                if live_ask_order is not None:
                    print(f"replacing ask {live_ask_order.id}")
                    cancel_order_safe(trader, live_ask_order)
                live_ask_order, live_ask_submit_time = submit_quote_and_track(
                    trader=trader, symbol=symbol, side="SELL",
                    lots=desired_ask_lots, price=ask_price,
                    tracked_orders=tracked_orders, signal=signal,
                    regime=regime, step=step, position_shares=position_shares,
                )
                print(f"submitted new ask {live_ask_order.id}")

        step += 1

        live_bid_price = float(getattr(live_bid_order, "price", 0.0000)) if live_bid_order else None
        live_ask_price = float(getattr(live_ask_order, "price", 0.0000)) if live_ask_order else None
        live_bid_age   = order_age_seconds(live_bid_submit_time, now_dt)  if live_bid_submit_time else None
        live_ask_age   = order_age_seconds(live_ask_submit_time, now_dt)  if live_ask_submit_time else None

        expiry_str    = regime_expiry.strftime("%H:%M:%S")   if regime_expiry   else "None"
        post_fill_str = post_fill_until.strftime("%H:%M:%S") if post_fill_until else "None"
        sim_time_now  = trader.get_last_trade_time()

        print(f"\n--- MM Loop @ {now_dt} | step {step} ---")
        print(f"Best Bid:          {best_bid_p:.4f} x {best_bid_q:.4f}")
        print(f"Best Ask:          {best_ask_p:.4f} x {best_ask_q:.4f}")
        print(f"Mid:               {mid:.4f}")
        print(f"Spread:            {spread:.4f}")
        print("Level OFI inc:    ", [round(x, 4) for x in level_increment])
        print(f"Raw OFI_t (tick):  {raw_ofi_t:.4f}")
        print(f"EMA_t:             {ema_t:.4f}")
        print(f"Final score:       {final_score:.4f}")
        print(f"Signal:            {signal}")
        print(f"Regime:            {regime}")
        print(f"Regime entry EMA:  {regime_entry_ema}")
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
        print(f"Sim time now:      {sim_time_now}")

        bp = trader.get_portfolio_summary().get_total_bp()
        print(f"Available Buying Power: {bp:.4f}")

        # Roll state forward
        prev_bids            = bids
        prev_asks            = asks
        prev_signal          = signal
        previous_regime      = regime
        last_position_shares = position_shares

        elapsed = time.time() - loop_start
        time.sleep(max(poll_interval - elapsed, 0.0000))

    # Shutdown
    poll_executions(trader, tracked_orders, seen_execution_keys)

    if live_bid_order is not None:
        cancel_order_safe(trader, live_bid_order)
    if live_ask_order is not None:
        cancel_order_safe(trader, live_ask_order)

    # Final sweep for any remaining orphans
    reconcile_open_orders(trader, symbol, None, None)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

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
                end_time=end_time,
            )
        except KeyboardInterrupt:
            trader.disconnect()