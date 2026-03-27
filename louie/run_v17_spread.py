import shift
import time
import csv
import os
from collections import deque
from datetime import timedelta

SYMBOL = "NVDA"
LEVELS = 10
POLL_INTERVAL      = 1.0
OFI_WINDOW_SECONDS = 10.0
EMA_ALPHA          = 0.2

LEVEL_WEIGHTS = [1.0, 0.9, 0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2, 0.1]

PERSISTENCE_LOOKBACK  = 6
PERSISTENCE_REQUIRED  = 5
FINAL_SCORE_THRESHOLD = 0.5
OFI_FLOOR             = 30.0

# ── Strategy parameters ───────────────────────────────────────────────────────
SIGNAL_WINDOW       = 15   # rolling window of ticks for signal voting
SIGNAL_THRESHOLD    = 13    # min ticks in window that must agree to open a pair

BID_LOTS            = 5    # total lots to buy/sell on entry leg
ASK_LOTS_NEAR       = 4    # lots posted at fill_price + 1 * spread
ASK_LOTS_FAR        = 1    # lots posted at fill_price + 2 * spread

BID_REPRICE_SECONDS = 1.0  # reprice entry limit every N seconds
FILL_HOLD_SECONDS   = 40   # seconds to wait for asks after first fill before liquidating

TICK_SIZE = 0.01
LOT_SIZE  = 100

SUBMISSION_LOG_PATH = "spread_submissions.csv"
EXECUTION_LOG_PATH  = "spread_executions.csv"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def round_to_tick(x):
    return round(round(x / TICK_SIZE, 6) * TICK_SIZE, 2)

def sanitise_price(p):
    return round(float(p), 2)

def ensure_csv_headers():
    if not os.path.exists(SUBMISSION_LOG_PATH):
        with open(SUBMISSION_LOG_PATH, "w", newline="") as f:
            csv.writer(f).writerow([
                "sim_time", "order_id", "symbol", "side", "price",
                "lots", "reason", "step",
            ])
    if not os.path.exists(EXECUTION_LOG_PATH):
        with open(EXECUTION_LOG_PATH, "w", newline="") as f:
            csv.writer(f).writerow([
                "sim_time", "order_id", "symbol", "side",
                "executed_price", "executed_size", "status", "exec_timestamp",
            ])

def log_submission(sim_time, order_id, symbol, side, price, lots, reason, step):
    with open(SUBMISSION_LOG_PATH, "a", newline="") as f:
        csv.writer(f).writerow([sim_time, order_id, symbol, side,
                                 price, lots, reason, step])

def log_execution(sim_time, order_id, symbol, side, exec_price,
                  exec_size, status, exec_ts):
    with open(EXECUTION_LOG_PATH, "a", newline="") as f:
        csv.writer(f).writerow([sim_time, order_id, symbol, side,
                                 f"{float(exec_price):.4f}", int(exec_size),
                                 str(status), exec_ts])

def poll_executions(trader, tracked_orders, seen_keys):
    sim_time = trader.get_last_trade_time()
    for oid, meta in list(tracked_orders.items()):
        try:
            for ex in trader.get_executed_orders(oid):
                sz = int(getattr(ex, "executed_size", 0))
                px = float(getattr(ex, "executed_price", 0.0))
                if sz > 0:
                    key = (oid, str(getattr(ex, "timestamp", "")), sz, px,
                           str(getattr(ex, "status", "")))
                    if key not in seen_keys:
                        seen_keys.add(key)
                        exec_ts = getattr(ex, "timestamp", "")
                        print(f"[FILL] {meta['side']} {sz} {meta['symbol']} "
                              f"@ {px:.4f} | status={getattr(ex,'status','')} "
                              f"| sim={exec_ts}", flush=True)
                        log_execution(sim_time, oid,
                                      getattr(ex, "symbol", meta["symbol"]),
                                      meta["side"], px, sz,
                                      str(getattr(ex, "status", "")), exec_ts)
            cur = trader.get_order(oid)
            if cur is not None:
                s = str(getattr(cur, "status", ""))
                if ("FILLED" in s or "CANCELED" in s or "REJECTED" in s
                        or int(getattr(cur, "executed_size", 0))
                        >= meta["lots"]):
                    tracked_orders[oid]["done"] = True
        except Exception:
            pass
    for oid in [k for k, v in tracked_orders.items() if v.get("done")]:
        del tracked_orders[oid]

# ---------------------------------------------------------------------------
# Order book / OFI
# ---------------------------------------------------------------------------

def parse_book(trader, symbol, levels):
    bo = trader.get_order_book(symbol, shift.OrderBookType.GLOBAL_BID)
    ao = trader.get_order_book(symbol, shift.OrderBookType.GLOBAL_ASK)
    return ([(float(b.price), float(b.size)) for b in bo[:levels]],
            [(float(a.price), float(a.size)) for a in ao[:levels]])

def ema_update(prev, val, alpha):
    return float(val) if prev is None else float(alpha * val + (1 - alpha) * prev)

def pad(lst, n):
    lst = list(lst[:n])
    while len(lst) < n: lst.append((0., 0.))
    return lst

def level_ofi(pb, pb_q, pa, pa_q, nb, nb_q, na, na_q):
    b = (nb_q if nb >= pb else 0.) - (pb_q if nb <= pb else 0.)
    a = -(na_q if na <= pa else 0.) + (pa_q if na >= pa else 0.)
    return float(b + a)

def multilevel_ofi(prev_bids, prev_asks, new_bids, new_asks, levels):
    pb = pad(prev_bids, levels); pa = pad(prev_asks, levels)
    nb = pad(new_bids,  levels); na = pad(new_asks,  levels)
    return [level_ofi(pb[m][0], pb[m][1], pa[m][0], pa[m][1],
                      nb[m][0], nb[m][1], na[m][0], na[m][1])
            for m in range(levels)]

def prune(dq, now_ts, window):
    cut = now_ts - window
    while dq and dq[0][0] < cut: dq.popleft()

def rolling_ofi(events, levels):
    t = [0.] * levels
    for _, v in events:
        for m in range(levels): t[m] += v[m]
    return t

def weighted_ofi(level_ofi_vec, weights):
    return float(sum(w * x for w, x in zip(weights, level_ofi_vec)))

def ema_direction(ema, floor):
    if ema >  floor: return  1
    if ema < -floor: return -1
    return 0

def persistence_stats(history):
    pos = sum(1 for x in history if x > 0)
    neg = sum(1 for x in history if x < 0)
    return pos, neg, (pos - neg) / max(len(history), 1)

def classify_signal(ema_dir, pos, neg, pscore, req, thresh):
    if ema_dir == 1 and pos >= req:
        fs = 0.7 * 1.0 + 0.3 * pscore
    elif ema_dir == -1 and neg >= req:
        fs = 0.7 * (-1.0) + 0.3 * pscore
    else:
        fs = 0.3 * pscore
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

def submit_limit_order(trader, symbol, side, lots, price, reason, step,
                       tracked_orders):
    if lots <= 0:
        return None
    sim_time = trader.get_last_trade_time()
    order = shift.Order(
        shift.Order.Type.LIMIT_BUY if side == "BUY" else shift.Order.Type.LIMIT_SELL,
        symbol, int(lots), float(price)
    )
    trader.submit_order(order)
    tracked_orders[order.id] = {"symbol": symbol, "side": side,
                                "lots": int(lots), "done": False}
    log_submission(sim_time, order.id, symbol, side,
                   f"{float(price):.4f}", lots, reason, step)
    print(f"[ORDER] {side} {lots}L LIMIT @ {price:.4f} {symbol} | {reason}",
          flush=True)
    return order.id

def get_filled_lots(trader, oid):
    """Return total lots filled so far for an order (executed_size is in lots)."""
    try:
        cur = trader.get_order(oid)
        if cur is None:
            return 0
        return int(getattr(cur, "executed_size", 0))
    except Exception:
        return 0

def get_last_fill_price(trader, oid):
    """Return the most recent execution price for an order, or None."""
    try:
        execs = trader.get_executed_orders(oid)
        prices = [float(getattr(e, "executed_price", 0.0))
                  for e in execs
                  if int(getattr(e, "executed_size", 0)) > 0
                  and float(getattr(e, "executed_price", 0.0)) > 0]
        return prices[-1] if prices else None
    except Exception:
        return None

def is_order_done(trader, oid, lots):
    """True if order is fully filled, canceled, or rejected."""
    try:
        cur = trader.get_order(oid)
        if cur is None:
            return True
        s = str(getattr(cur, "status", ""))
        if "CANCELED" in s or "REJECTED" in s:
            return True
        if "FILLED" in s and "PARTIALLY" not in s:
            return True
        if int(getattr(cur, "executed_size", 0)) >= lots:
            return True
    except Exception:
        return True
    return False

# ---------------------------------------------------------------------------
# Signal voting window
# ---------------------------------------------------------------------------

def vote(signal_history):
    """
    Return 'BUY', 'SELL', or None based on rolling vote.
    Fires if >= SIGNAL_THRESHOLD of last SIGNAL_WINDOW ticks agree.
    """
    if len(signal_history) < SIGNAL_WINDOW:
        return None
    window = list(signal_history)[-SIGNAL_WINDOW:]
    buys  = sum(1 for s in window if s == "BUY_PRESSURE")
    sells = sum(1 for s in window if s == "SELL_PRESSURE")
    if buys  >= SIGNAL_THRESHOLD: return "BUY"
    if sells >= SIGNAL_THRESHOLD: return "SELL"
    return None

# ---------------------------------------------------------------------------
# Main strategy loop
# ---------------------------------------------------------------------------

def run_strategy(trader, symbol=SYMBOL, end_time=None):
    ensure_csv_headers()
    trader.cancel_all_pending_orders()

    # OFI state
    prev_bids       = None
    prev_asks       = None
    ofi_events      = deque()
    raw_ofi_history = deque(maxlen=PERSISTENCE_LOOKBACK)
    ema_t           = None
    signal_history  = deque(maxlen=SIGNAL_WINDOW)

    # ── Pair state machine ────────────────────────────────────────────────────
    # States: IDLE → BIDDING → FILLING → DONE
    #   IDLE:    no open pair
    #   BIDDING: bid submitted, waiting for first fill
    #   FILLING: at least one fill received, asks posted, waiting for asks
    #   DONE:    cleanup / liquidation in progress
    state = "IDLE"

    # Bid leg
    bid_oid         = None
    bid_side        = None       # "BUY" or "SELL"
    bid_lots_total  = BID_LOTS   # lots we want to buy/sell in total
    bid_lots_filled = 0          # lots filled so far
    bid_submit_ts   = 0.0

    # Ask legs (fixed price, submitted once on first fill)
    ask_near_oid    = None
    ask_far_oid     = None
    ask_near_side   = None
    ask_far_side    = None
    ask_near_lots   = ASK_LOTS_NEAR
    ask_far_lots    = ASK_LOTS_FAR
    first_fill_ts   = None       # wall-clock time of first fill

    tracked_orders  = {}
    seen_keys       = set()
    step            = 0

    while trader.get_last_trade_time() < end_time:

        sim_time = trader.get_last_trade_time()
        poll_executions(trader, tracked_orders, seen_keys)

        # ── Book parse ────────────────────────────────────────────────────────
        bids, asks = parse_book(trader, symbol, LEVELS)
        if not bids or not asks:
            time.sleep(POLL_INTERVAL); continue
        best_bid, _ = bids[0]
        best_ask, _ = asks[0]
        if best_ask <= best_bid:
            time.sleep(POLL_INTERVAL); continue
        mid    = 0.5 * (best_bid + best_ask)
        spread = best_ask - best_bid

        if prev_bids is None or prev_asks is None:
            prev_bids, prev_asks = bids, asks
            time.sleep(POLL_INTERVAL); continue

        # ── OFI / signal ──────────────────────────────────────────────────────
        inc = multilevel_ofi(prev_bids, prev_asks, bids, asks, LEVELS)
        ofi_events.append((time.time(), inc))
        prune(ofi_events, time.time(), OFI_WINDOW_SECONDS)
        raw_ofi = weighted_ofi(rolling_ofi(ofi_events, LEVELS), LEVEL_WEIGHTS)
        ema_t   = ema_update(ema_t, raw_ofi, EMA_ALPHA)
        raw_ofi_history.append(raw_ofi)
        pos_count, neg_count, pscore = persistence_stats(raw_ofi_history)
        ema_dir = ema_direction(ema_t, OFI_FLOOR)
        signal  = classify_signal(ema_dir, pos_count, neg_count, pscore,
                                  PERSISTENCE_REQUIRED, FINAL_SCORE_THRESHOLD)
        signal_history.append(signal)

        # Current vote
        vote_result = vote(signal_history)

        # ── State machine ─────────────────────────────────────────────────────

        if state == "IDLE":
            # ── Open a new pair if signal is strong enough ────────────────────
            if vote_result is not None:
                bid_side       = "BUY" if vote_result == "BUY" else "SELL"
                bid_lots_total  = BID_LOTS
                bid_lots_filled = 0
                bid_price       = sanitise_price(round_to_tick(mid))
                reason          = (f"PAIR-BID vote={vote_result} "
                                   f"ema={ema_t:.1f}")
                bid_oid = submit_limit_order(trader, symbol, bid_side,
                                             bid_lots_total, bid_price,
                                             reason, step, tracked_orders)
                if bid_oid:
                    bid_submit_ts   = time.time()
                    first_fill_ts   = None
                    ask_near_oid    = None
                    ask_far_oid     = None
                    state           = "BIDDING"
                    print(f"[STATE] IDLE → BIDDING ({vote_result})", flush=True)

        elif state == "BIDDING":
            # ── Cancel if vote no longer supports current direction ────────
            # This covers both full flips and signal decay (vote drops below threshold)
            if vote_result != ("BUY" if bid_side == "BUY" else "SELL"):
                print(f"[CANCEL] Vote no longer supports {bid_side} "
                      f"(vote={vote_result}) — cancelling pair", flush=True)
                trader.cancel_all_pending_orders()
                bid_oid = None; bid_lots_filled = 0; state = "IDLE"
                time.sleep(POLL_INTERVAL); continue


            # ── Check fills on current bid order ──────────────────────────────
            if bid_oid:
                current_filled = get_filled_lots(trader, bid_oid)
                if current_filled > 0 and first_fill_ts is None:
                    # Partial fill detected — record and move to FILLING
                    fill_px = get_last_fill_price(trader, bid_oid)
                    if fill_px is None:
                        fill_px = mid
                    bid_lots_filled += current_filled
                    print(f"[BID FILL] {current_filled}L @ {fill_px:.4f} "
                          f"(total: {bid_lots_filled}/{bid_lots_total}L)", flush=True)
                    first_fill_ts = time.time()
                    ask_near_side = "SELL" if bid_side == "BUY" else "BUY"
                    ask_far_side  = ask_near_side
                    near_price = sanitise_price(round_to_tick(
                        fill_px + spread if bid_side == "BUY" else fill_px - spread))
                    far_price  = sanitise_price(round_to_tick(
                        fill_px + 2 * spread if bid_side == "BUY" else fill_px - 2 * spread))
                    ask_near_oid = submit_limit_order(
                        trader, symbol, ask_near_side, ASK_LOTS_NEAR, near_price,
                        f"ASK-NEAR {ask_near_side} fill={fill_px:.4f}", step, tracked_orders)
                    ask_far_oid = submit_limit_order(
                        trader, symbol, ask_far_side, ASK_LOTS_FAR, far_price,
                        f"ASK-FAR {ask_far_side} fill={fill_px:.4f}", step, tracked_orders)
                    print(
                        f"[PAIR OPEN] BID filled @ {fill_px:.4f} | "
                        f"Spread: {spread:.4f} | "
                        f"ASK-NEAR {ask_near_side} {ASK_LOTS_NEAR}L @ {near_price:.4f} | "
                        f"ASK-FAR {ask_far_side} {ASK_LOTS_FAR}L @ {far_price:.4f}",
                        flush=True)
                    state = "FILLING"
                    prev_bids = bids; prev_asks = asks
                    step += 1
                    time.sleep(POLL_INTERVAL); continue

            # ── Reprice bid every BID_REPRICE_SECONDS ─────────────────────────
            if bid_oid and first_fill_ts is None:
                age = time.time() - bid_submit_ts
                if age >= BID_REPRICE_SECONDS:
                    # Accumulate any fills from the order being cancelled
                    filled_on_cancel = get_filled_lots(trader, bid_oid)
                    trader.cancel_all_pending_orders()
                    bid_lots_filled += filled_on_cancel
                    bid_oid = None
                    remaining = bid_lots_total - bid_lots_filled
                    if remaining <= 0:
                        state = "IDLE"
                    else:
                        bid_price = sanitise_price(round_to_tick(mid))
                        reason    = (f"PAIR-BID-REPRICE vote={vote_result} "
                                     f"ema={ema_t:.1f}")
                        bid_oid = submit_limit_order(
                            trader, symbol, bid_side, remaining,
                            bid_price, reason, step, tracked_orders)
                        if bid_oid:
                            bid_submit_ts = time.time()

        elif state == "FILLING":
            # ── Check for signal flip ─────────────────────────────────────────
            opposite = "SELL" if bid_side == "BUY" else "BUY"
            if vote_result == opposite:
                print(f"[FLIP] Signal flipped to {opposite} — liquidating",
                      flush=True)
                trader.cancel_all_pending_orders()
                # Liquidate remaining position at mid
                pos_shares = get_pos(trader, symbol)
                pos_lots   = pos_shares // LOT_SIZE
                if pos_lots != 0:
                    liq_side  = "SELL" if pos_lots > 0 else "BUY"
                    liq_price = sanitise_price(round_to_tick(mid))
                    submit_limit_order(trader, symbol, liq_side, abs(pos_lots),
                                       liq_price, "LIQUIDATE-FLIP",
                                       step, tracked_orders)
                state = "IDLE"
                time.sleep(POLL_INTERVAL); continue

            # ── Continue repricing remaining bid lots ─────────────────────────
            if bid_oid and bid_lots_filled < bid_lots_total:
                current_filled = get_filled_lots(trader, bid_oid)
                if current_filled > bid_lots_filled:
                    bid_lots_filled = current_filled
                    print(f"[BID FILL] total filled: "
                          f"{bid_lots_filled}/{bid_lots_total}L", flush=True)

                if not is_order_done(trader, bid_oid,
                                     bid_lots_total - bid_lots_filled):
                    age = time.time() - bid_submit_ts
                    if age >= BID_REPRICE_SECONDS:
                        remaining = bid_lots_total - bid_lots_filled
                        if remaining > 0:
                            trader.cancel_all_pending_orders()
                            # Re-submit remaining bid at new mid
                            # (do NOT cancel asks — they are fixed)
                            bid_price = sanitise_price(round_to_tick(mid))
                            reason    = (f"PAIR-BID-REPRICE-FILLING "
                                         f"rem={remaining}L")
                            bid_oid = submit_limit_order(
                                trader, symbol, bid_side, remaining,
                                bid_price, reason, step, tracked_orders
                            )
                            if bid_oid:
                                bid_submit_ts = time.time()
                            # Re-post asks since cancel_all removed them
                            if ask_near_oid is not None:
                                ask_near_oid = submit_limit_order(
                                    trader, symbol, ask_near_side,
                                    ASK_LOTS_NEAR,
                                    sanitise_price(round_to_tick(
                                        float(tracked_orders.get(
                                            ask_near_oid, {}).get(
                                            "price", mid + spread)))),
                                    "ASK-NEAR-REPOST", step, tracked_orders
                                )
                            if ask_far_oid is not None:
                                ask_far_oid = submit_limit_order(
                                    trader, symbol, ask_far_side,
                                    ASK_LOTS_FAR,
                                    sanitise_price(round_to_tick(
                                        float(tracked_orders.get(
                                            ask_far_oid, {}).get(
                                            "price", mid + 2 * spread)))),
                                    "ASK-FAR-REPOST", step, tracked_orders
                                )

            # ── Check if both asks are done ───────────────────────────────────
            near_done = (ask_near_oid is None
                         or is_order_done(trader, ask_near_oid, ASK_LOTS_NEAR))
            far_done  = (ask_far_oid  is None
                         or is_order_done(trader, ask_far_oid,  ASK_LOTS_FAR))
            bid_done  = (bid_lots_filled >= bid_lots_total
                         or bid_oid is None
                         or is_order_done(trader, bid_oid,
                                          bid_lots_total - bid_lots_filled))

            if near_done and far_done and bid_done:
                print(f"[DONE] All legs filled — pair complete", flush=True)
                state = "IDLE"
                time.sleep(POLL_INTERVAL); continue

            # ── 40s liquidation timer ─────────────────────────────────────────
            if first_fill_ts and (time.time() - first_fill_ts) >= FILL_HOLD_SECONDS:
                print(f"[TIMEOUT] 40s elapsed — liquidating remaining position",
                      flush=True)
                trader.cancel_all_pending_orders()
                pos_shares = get_pos(trader, symbol)
                pos_lots   = pos_shares // LOT_SIZE
                if pos_lots != 0:
                    liq_side  = "SELL" if pos_lots > 0 else "BUY"
                    liq_price = sanitise_price(round_to_tick(mid))
                    submit_limit_order(trader, symbol, liq_side, abs(pos_lots),
                                       liq_price, "LIQUIDATE-TIMEOUT",
                                       step, tracked_orders)
                state = "IDLE"

        # ── Log line ──────────────────────────────────────────────────────────
        window    = list(signal_history)[-SIGNAL_WINDOW:] if signal_history else []
        buy_votes = sum(1 for s in window if s == "BUY_PRESSURE")
        sel_votes = sum(1 for s in window if s == "SELL_PRESSURE")
        hold_age  = (f"{time.time()-first_fill_ts:.0f}s"
                     if first_fill_ts else "-")
        print(
            f"[{sim_time}][{symbol}] "
            f"EMA: {ema_t:8.2f} | "
            f"Sig: {signal:14s} | Vote: B{buy_votes}/S{sel_votes} | "
            f"State: {state:8s} | Hold: {hold_age:>4s} | "
            f"Mid: {mid:.4f} | BP: {get_bp(trader):.0f}",
            flush=True
        )

        prev_bids = bids
        prev_asks = asks
        step += 1
        time.sleep(POLL_INTERVAL)

    # ── Shutdown ──────────────────────────────────────────────────────────────
    poll_executions(trader, tracked_orders, seen_keys)
    trader.cancel_all_pending_orders()
    pos_shares = get_pos(trader, symbol)
    pos_lots   = pos_shares // LOT_SIZE
    if pos_lots != 0:
        bids, asks = parse_book(trader, symbol, LEVELS)
        if bids and asks:
            mid = 0.5 * (bids[0][0] + asks[0][0])
            liq_side  = "SELL" if pos_lots > 0 else "BUY"
            liq_price = sanitise_price(round_to_tick(mid))
            submit_limit_order(trader, symbol, liq_side, abs(pos_lots),
                               liq_price, "SHUTDOWN-LIQUIDATE", 0, tracked_orders)
    print(f"[{symbol}] Strategy finished. Final pos: {pos_lots}L", flush=True)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    with shift.Trader("columbia-traders") as trader:
        trader.connect("initiator.cfg", "aRkkZSrj")
        time.sleep(1.0)
        trader.sub_all_order_book()
        time.sleep(1.0)
        end_time = trader.get_last_trade_time() + timedelta(minutes=500.0)
        try:
            run_strategy(trader, symbol=SYMBOL, end_time=end_time)
        except KeyboardInterrupt:
            trader.disconnect()