import shift
import time
import math
import csv
import os
from collections import deque
from datetime import datetime, timedelta

SYMBOL = "AAPL"
LEVELS = 10
POLL_INTERVAL = 1.0
OFI_WINDOW_SECONDS = 5.0
EMA_ALPHA = 0.3

LEVEL_WEIGHTS = [1.0, 0.9, 0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2, 0.1]

PERSISTENCE_LOOKBACK   = 5
PERSISTENCE_REQUIRED   = 4
FINAL_SCORE_THRESHOLD  = 0.5
OFI_FLOOR              = 100.0

# ── Sizing thresholds ────────────────────────────────────────────────────────
BUY_TIER1_EMA   = 150     # EMA ≥ 150  → target long 1 lot
BUY_TIER2_EMA   = 200     # EMA ≥ 200  → target long 3 lots
SELL_TIER1_EMA  = 130     # abs(EMA) ≥ 130 → target short 1 lot
SELL_TIER2_EMA  = 200     # abs(EMA) ≥ 200 → target short 2 lots
OVERLAP_MIN_EMA = 150     # minimum abs(EMA) to flip direction

TICK_SIZE  = 0.01
LOT_SIZE   = 100

EXIT_REPRICE_SECONDS = 2.0   # reprice limit exit orders every N seconds

SUBMISSION_LOG_PATH = "ofi_pyramid_submissions.csv"
EXECUTION_LOG_PATH  = "ofi_pyramid_executions.csv"

# ---------------------------------------------------------------------------
# Tick helpers
# ---------------------------------------------------------------------------

def round_to_tick(x):
    return round(round(x / TICK_SIZE, 6) * TICK_SIZE, 2)

def sanitise_price(p):
    return round(float(p), 2)

# ---------------------------------------------------------------------------
# CSV logging
# ---------------------------------------------------------------------------

def ensure_csv_headers():
    if not os.path.exists(SUBMISSION_LOG_PATH):
        with open(SUBMISSION_LOG_PATH, "w", newline="") as f:
            csv.writer(f).writerow([
                "logged_at", "order_id", "symbol", "side", "limit_price",
                "shares", "lots", "reason", "step", "pos_before",
            ])
    if not os.path.exists(EXECUTION_LOG_PATH):
        with open(EXECUTION_LOG_PATH, "w", newline="") as f:
            csv.writer(f).writerow([
                "logged_at", "order_id", "symbol", "side", "executed_price",
                "executed_size", "order_size", "status", "exec_timestamp",
            ])

def log_submission(sim_time, order_id, symbol, side, price_label, shares, reason, step, pos):
    with open(SUBMISSION_LOG_PATH, "a", newline="") as f:
        csv.writer(f).writerow([
            sim_time, order_id, symbol, side, price_label,
            int(shares), int(shares // LOT_SIZE), reason, step, int(pos),
        ])

def log_execution(sim_time, order_id, symbol, side, exec_price, exec_size,
                  order_size, status, exec_ts):
    with open(EXECUTION_LOG_PATH, "a", newline="") as f:
        csv.writer(f).writerow([
            sim_time, order_id, symbol, side, f"{float(exec_price):.4f}",
            int(exec_size), int(order_size), str(status), exec_ts,
        ])

# ---------------------------------------------------------------------------
# Execution audit
# ---------------------------------------------------------------------------

def poll_executions(trader, tracked_orders, seen_keys):
    sim_time = trader.get_last_trade_time()
    for oid, meta in list(tracked_orders.items()):
        try:
            for ex in trader.get_executed_orders(oid):
                sz  = int(getattr(ex,  "executed_size",  0))
                px  = float(getattr(ex, "executed_price", 0.0))
                if sz > 0:
                    key = (oid, str(getattr(ex,"timestamp","")), sz, px,
                           str(getattr(ex,"status","")))
                    if key not in seen_keys:
                        seen_keys.add(key)
                        log_execution(sim_time, oid,
                            getattr(ex,"symbol",meta["symbol"]), meta["side"],
                            px, sz, meta["lots"]*LOT_SIZE,
                            str(getattr(ex,"status","")),
                            getattr(ex,"timestamp",""))
            cur = trader.get_order(oid)
            if cur is not None:
                s = str(getattr(cur,"status",""))
                done = ("FILLED" in s or "CANCELED" in s or "REJECTED" in s
                        or int(getattr(cur,"executed_size",0)) >= meta["lots"]*LOT_SIZE)
                if done:
                    tracked_orders[oid]["done"] = True
        except Exception:
            pass
    for oid in [k for k,v in tracked_orders.items() if v.get("done")]:
        del tracked_orders[oid]

# ---------------------------------------------------------------------------
# Order book / OFI
# ---------------------------------------------------------------------------

def parse_book(trader, symbol, levels):
    bo = trader.get_order_book(symbol, shift.OrderBookType.GLOBAL_BID)
    ao = trader.get_order_book(symbol, shift.OrderBookType.GLOBAL_ASK)
    bids = [(float(b.price), float(b.size)) for b in bo[:levels]]
    asks = [(float(a.price), float(a.size)) for a in ao[:levels]]
    return bids, asks

def ema_update(prev, val, alpha):
    return float(val) if prev is None else float(alpha*val + (1-alpha)*prev)

def pad(lst, n):
    lst = list(lst[:n])
    while len(lst) < n: lst.append((0.,0.))
    return lst

def level_ofi(pb, pb_q, pa, pa_q, nb, nb_q, na, na_q):
    b = (nb_q if nb>=pb else 0.) - (pb_q if nb<=pb else 0.)
    a = -(na_q if na<=pa else 0.) + (pa_q if na>=pa else 0.)
    return float(b+a)

def multilevel_ofi(prev_bids, prev_asks, new_bids, new_asks, levels):
    pb = pad(prev_bids, levels); pa = pad(prev_asks, levels)
    nb = pad(new_bids,  levels); na = pad(new_asks,  levels)
    return [level_ofi(pb[m][0],pb[m][1],pa[m][0],pa[m][1],
                      nb[m][0],nb[m][1],na[m][0],na[m][1]) for m in range(levels)]

def prune(dq, now_ts, window):
    cut = now_ts - window
    while dq and dq[0][0] < cut: dq.popleft()

def rolling_ofi(events, levels):
    t = [0.]*levels
    for _, v in events:
        for m in range(levels): t[m] += v[m]
    return t

def weighted_ofi(level_ofi_vec, weights):
    return float(sum(w*x for w,x in zip(weights, level_ofi_vec)))

# ---------------------------------------------------------------------------
# Signal logic
# ---------------------------------------------------------------------------

def ema_direction(ema, floor):
    if ema >  floor: return  1
    if ema < -floor: return -1
    return 0

def persistence_stats(history):
    pos = sum(1 for x in history if x > 0)
    neg = sum(1 for x in history if x < 0)
    return pos, neg, (pos-neg)/max(len(history),1)

def classify_signal(ema_dir, pos, neg, pscore, req, thresh):
    if ema_dir==1 and pos>=req:
        fs = 0.7*1.0 + 0.3*pscore
    elif ema_dir==-1 and neg>=req:
        fs = 0.7*(-1.0) + 0.3*pscore
    else:
        fs = 0.3*pscore
    if fs >  thresh: return "BUY_PRESSURE"
    if fs < -thresh: return "SELL_PRESSURE"
    return "NEUTRAL"

# ---------------------------------------------------------------------------
# Position / order helpers
# ---------------------------------------------------------------------------

def get_pos(trader, symbol):
    item = trader.get_portfolio_item(symbol)
    return int(item.get_long_shares()) - int(item.get_short_shares())

def get_bp(trader):
    return float(trader.get_portfolio_summary().get_total_bp())

def cancel_all(trader, symbol, max_wait=2.0, retry_interval=0.2):
    """
    Cancel all resting orders for symbol and verify they are gone.
    Returns True if book confirmed clean, False otherwise.
    """
    deadline = time.time() + max_wait
    while time.time() < deadline:
        pending = [o for o in trader.get_waiting_list() if o.symbol == symbol]
        if not pending:
            return True
        for o in pending:
            try: trader.submit_cancellation(o)
            except Exception: pass
        time.sleep(retry_interval)
    remaining = [o for o in trader.get_waiting_list() if o.symbol == symbol]
    if remaining:
        print(f"[WARN] cancel_all: {len(remaining)} orders still pending "
              f"after {max_wait}s for {symbol}")
        return False
    return True

def submit_market_order(trader, symbol, side, lots, reason, step,
                        tracked_orders, pos_before):
    """
    Submit a MARKET order (entry, pyramid add, or flip).
    Market orders execute immediately — no reprice loop needed.
    """
    if lots <= 0:
        return None
    if side == "BUY":
        order = shift.Order(shift.Order.Type.MARKET_BUY,  symbol, int(lots))
    else:
        order = shift.Order(shift.Order.Type.MARKET_SELL, symbol, int(lots))
    trader.submit_order(order)
    sim_time = trader.get_last_trade_time()
    tracked_orders[order.id] = {
        "symbol": symbol, "side": side, "lots": int(lots),
        "limit_price": "MARKET", "done": False,
    }
    log_submission(sim_time, order.id, symbol, side, "MARKET",
                   lots * LOT_SIZE, reason, step, pos_before)
    return order.id

def submit_limit_order(trader, symbol, side, lots, price, reason, step,
                       tracked_orders, pos_before):
    """
    Submit a LIMIT order (exit to flat only).
    Price is always mid — no spread fraction applied to exits.
    """
    if lots <= 0:
        return None
    if side == "BUY":
        order = shift.Order(shift.Order.Type.LIMIT_BUY,  symbol, int(lots), float(price))
    else:
        order = shift.Order(shift.Order.Type.LIMIT_SELL, symbol, int(lots), float(price))
    trader.submit_order(order)
    sim_time = trader.get_last_trade_time()
    tracked_orders[order.id] = {
        "symbol": symbol, "side": side, "lots": int(lots),
        "limit_price": float(price), "done": False,
    }
    log_submission(sim_time, order.id, symbol, side, f"{float(price):.4f}",
                   lots * LOT_SIZE, reason, step, pos_before)
    return order.id

def has_enough_bp(trader, side, lots, price_est, current_pos_shares):
    """
    BP check before submitting.
    For market orders, pass mid as price_est.
    """
    bp     = get_bp(trader)
    shares = lots * LOT_SIZE
    if side == "BUY":
        required = price_est * shares
    else:
        resulting = current_pos_shares - shares
        if resulting < 0:
            short_shares = abs(resulting)
            required = 2.0 * price_est * short_shares
        else:
            required = 0.0
    if bp >= required:
        return True
    print(f"[BP] Insufficient: need ${required:.0f}, have ${bp:.0f} — skipping")
    return False

# ---------------------------------------------------------------------------
# Target accumulator
# ---------------------------------------------------------------------------

def update_target_accumulator(signal, ema_t, current_lots,
                               buy_acc, sell_acc, neutral_streak):
    """
    Accumulates target lots over consecutive qualifying ticks.
    Returns (target_lots, new_buy_acc, new_sell_acc, new_neutral_streak).
    """
    if signal == "BUY_PRESSURE":
        neutral_streak = 0
        sell_acc = 0

        if current_lots < 0 and abs(ema_t) < OVERLAP_MIN_EMA:
            return 0, 0, 0, 0

        if ema_t >= BUY_TIER2_EMA:
            buy_acc = max(buy_acc + 1, 3)
        elif ema_t >= BUY_TIER1_EMA:
            buy_acc = buy_acc + 1
        else:
            buy_acc = 0

        return buy_acc, buy_acc, 0, 0

    elif signal == "SELL_PRESSURE":
        neutral_streak = 0
        buy_acc = 0

        if current_lots > 0 and abs(ema_t) < OVERLAP_MIN_EMA:
            return 0, 0, 0, 0

        abs_ema = abs(ema_t)
        if abs_ema >= SELL_TIER2_EMA:
            sell_acc = max(sell_acc + 1, 2)
        elif abs_ema >= SELL_TIER1_EMA:
            sell_acc = sell_acc + 1
        else:
            sell_acc = 0

        return -sell_acc, 0, sell_acc, 0

    else:  # NEUTRAL — 2-tick grace period
        neutral_streak += 1
        if neutral_streak >= 2:
            return 0, 0, 0, neutral_streak
        else:
            current_target = buy_acc if buy_acc > 0 else -sell_acc
            return current_target, buy_acc, sell_acc, neutral_streak

# ---------------------------------------------------------------------------
# Main strategy loop
# ---------------------------------------------------------------------------

def run_strategy(trader, symbol=SYMBOL, end_time=None):
    ensure_csv_headers()
    cancel_all(trader, symbol)

    # OFI state
    prev_bids       = None
    prev_asks       = None
    ofi_events      = deque()
    raw_ofi_history = deque(maxlen=PERSISTENCE_LOOKBACK)
    ema_t           = None

    # Target accumulators
    buy_acc        = 0
    sell_acc       = 0
    neutral_streak = 0

    # Order state — only used for LIMIT exit orders
    # Market orders (entry/flip/pyramid) execute immediately so we don't
    # need to track them for repricing.
    exit_oid      = None   # active limit exit order id
    exit_side     = None
    exit_lots     = 0
    exit_submit_ts = 0.0

    last_known_pos = 0
    tracked_orders = {}
    seen_keys      = set()
    next_audit_ts  = time.time() + 30.0
    step = 0

    while datetime.now() < end_time:
        loop_start = time.time()
        now_ts     = time.time()

        if now_ts >= next_audit_ts:
            poll_executions(trader, tracked_orders, seen_keys)
            next_audit_ts = now_ts + 30.0

        # ── Book parse ───────────────────────────────────────────────────
        bids, asks = parse_book(trader, symbol, LEVELS)
        if not bids or not asks:
            elapsed = time.time() - loop_start
            time.sleep(max(POLL_INTERVAL - elapsed, 0.))
            continue

        best_bid, _ = bids[0]
        best_ask, _ = asks[0]
        if best_ask <= best_bid:
            elapsed = time.time() - loop_start
            time.sleep(max(POLL_INTERVAL - elapsed, 0.))
            continue

        mid    = 0.5 * (best_bid + best_ask)
        spread = best_ask - best_bid

        if prev_bids is None or prev_asks is None:
            prev_bids, prev_asks = bids, asks
            elapsed = time.time() - loop_start
            time.sleep(max(POLL_INTERVAL - elapsed, 0.))
            continue

        # ── OFI / signal ─────────────────────────────────────────────────
        inc = multilevel_ofi(prev_bids, prev_asks, bids, asks, LEVELS)
        ofi_events.append((now_ts, inc))
        prune(ofi_events, now_ts, OFI_WINDOW_SECONDS)

        raw_ofi  = weighted_ofi(rolling_ofi(ofi_events, LEVELS), LEVEL_WEIGHTS)
        ema_t    = ema_update(ema_t, raw_ofi, EMA_ALPHA)
        raw_ofi_history.append(raw_ofi)

        pos_count, neg_count, pscore = persistence_stats(raw_ofi_history)
        ema_dir = ema_direction(ema_t, OFI_FLOOR)
        signal  = classify_signal(ema_dir, pos_count, neg_count, pscore,
                                  PERSISTENCE_REQUIRED, FINAL_SCORE_THRESHOLD)

        # ── Position ─────────────────────────────────────────────────────
        pos_shares = get_pos(trader, symbol)
        pos_lots   = pos_shares // LOT_SIZE

        if pos_lots != last_known_pos:
            delta = pos_lots - last_known_pos
            print(f"[FILL] Pos {last_known_pos:+d} → {pos_lots:+d} ({delta:+d}L)")
        last_known_pos = pos_lots

        # ── Target accumulator ────────────────────────────────────────────
        target_lots, buy_acc, sell_acc, neutral_streak = update_target_accumulator(
            signal, ema_t, pos_lots, buy_acc, sell_acc, neutral_streak
        )
        delta_lots = target_lots - pos_lots

        # ── Classify the action needed ────────────────────────────────────
        # is_exit : signal gone neutral / weak, closing to flat
        # is_entry: was flat, opening a new position
        # is_flip : was long, now need to go short (or vice versa)
        # is_add  : adding to existing position in same direction
        is_exit  = (target_lots == 0 and pos_lots != 0)
        is_entry = (target_lots != 0 and pos_lots == 0)
        is_flip  = (target_lots != 0 and pos_lots != 0
                    and (target_lots > 0) != (pos_lots > 0))
        is_add   = (target_lots != 0 and pos_lots != 0
                    and (target_lots > 0) == (pos_lots > 0)
                    and abs(target_lots) > abs(pos_lots))

        # ── Check exit order status ───────────────────────────────────────
        if exit_oid:
            o = trader.get_order(exit_oid)
            if o is None:
                exit_oid = None; exit_side = None; exit_lots = 0
            else:
                s = str(getattr(o, "status", ""))
                exec_sz = int(getattr(o, "executed_size", 0))
                if ("FILLED" in s or "CANCELED" in s or "REJECTED" in s
                        or exec_sz >= exit_lots * LOT_SIZE):
                    exit_oid = None; exit_side = None; exit_lots = 0

        # ── Order management ──────────────────────────────────────────────

        if delta_lots == 0:
            # Already at target — sweep any lingering orders
            if exit_oid:
                cancel_all(trader, symbol)
                exit_oid = None; exit_side = None; exit_lots = 0

        elif is_exit:
            # ── EXIT: limit order at mid, reprice every EXIT_REPRICE_SECONDS ──
            # Cancel any pending market orders that might have snuck in
            # (shouldn't happen but guard anyway), then manage the limit exit.
            order_side  = "BUY" if delta_lots > 0 else "SELL"
            order_lots  = abs(delta_lots)
            order_price = sanitise_price(round_to_tick(mid))

            same_exit = (exit_oid is not None
                         and exit_side == order_side
                         and exit_lots == order_lots)
            age = now_ts - exit_submit_ts
            should_reprice = (not same_exit) or (age >= EXIT_REPRICE_SECONDS)

            if should_reprice:
                if cancel_all(trader, symbol):
                    exit_oid = None
                    pos_shares = get_pos(trader, symbol)
                    pos_lots   = pos_shares // LOT_SIZE
                    order_lots = abs(target_lots - pos_lots)
                    if order_lots > 0:
                        reason = (f"EXIT target=0 cur={pos_lots} "
                                  f"sig={signal} ema={ema_t:.1f}")
                        exit_oid = submit_limit_order(
                            trader, symbol, order_side, order_lots, order_price,
                            reason, step, tracked_orders, pos_shares
                        )
                        if exit_oid:
                            exit_side      = order_side
                            exit_lots      = order_lots
                            exit_submit_ts = now_ts

        elif is_entry or is_add or is_flip:
            # ── MARKET ORDER: entry, pyramid add, or sign flip ────────────
            # Cancel any resting limit exit orders first.
            # Market orders don't need a reprice loop — just submit and done.
            order_side = "BUY" if delta_lots > 0 else "SELL"
            order_lots = abs(delta_lots)

            if not has_enough_bp(trader, order_side, order_lots, mid, pos_shares):
                pass  # skip this tick
            else:
                # Clear the book before firing a market order.
                # For is_flip, there may be a pending limit exit we're abandoning.
                # For is_entry/is_add, the book should already be clean.
                if exit_oid:
                    cancel_all(trader, symbol)
                    exit_oid = None; exit_side = None; exit_lots = 0

                action = ("ENTRY" if is_entry else
                          "FLIP"  if is_flip  else "ADD")
                reason = (f"MKT-{action} target={target_lots} cur={pos_lots} "
                          f"sig={signal} ema={ema_t:.1f}")
                print(f"[{action}] {order_side} {order_lots}L @ MARKET "
                      f"(pos={pos_lots:+d} → target={target_lots:+d})")
                submit_market_order(
                    trader, symbol, order_side, order_lots,
                    reason, step, tracked_orders, pos_shares
                )
                # No active_oid to track — market orders fill immediately.
                # next tick's pos_shares will reflect the fill.

        sim_now = trader.get_last_trade_time()
        print(
            f"[{sim_now}][{symbol}] "
            f"Sig: {signal:14s} | EMA: {ema_t:8.2f} | "
            f"Pos: {pos_lots:+3d}L | Target: {target_lots:+3d}L | "
            f"Delta: {delta_lots:+3d}L | Acc: B{buy_acc}/S{sell_acc} | "
            f"Mid: {mid:.4f} | ExitOrder: {'YES' if exit_oid else 'no':3s} | "
            f"BP: {get_bp(trader):.0f}"
        )

        prev_bids   = bids
        prev_asks   = asks
        step       += 1

        elapsed = time.time() - loop_start
        time.sleep(max(POLL_INTERVAL - elapsed, 0.))

    poll_executions(trader, tracked_orders, seen_keys)
    cancel_all(trader, symbol)
    print(f"[{symbol}] Strategy finished. Final pos: {get_pos(trader, symbol)} shares")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    with shift.Trader("columbia-traders") as trader:
        trader.connect("initiator.cfg", "aRkkZSrj")
        time.sleep(1.0)
        cancel_all(trader, SYMBOL)
        trader.sub_all_order_book()
        time.sleep(1.0)
        end_time = datetime.now() + timedelta(minutes=500.0)
        try:
            run_strategy(trader, symbol=SYMBOL, end_time=end_time)
        except KeyboardInterrupt:
            trader.disconnect()