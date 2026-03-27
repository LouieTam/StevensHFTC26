import shift
import time
import math
import csv
import os
from collections import deque
from datetime import datetime, timedelta

SYMBOL = "NVDA"
LEVELS = 10
POLL_INTERVAL = 1.0
OFI_WINDOW_SECONDS = 10.0
EMA_ALPHA = 0.2

LEVEL_WEIGHTS = [1.0, 0.9, 0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2, 0.1]

PERSISTENCE_LOOKBACK   = 5
PERSISTENCE_REQUIRED   = 3
FINAL_SCORE_THRESHOLD  = 0.5
OFI_FLOOR              = 30.0

# ── Sizing thresholds ────────────────────────────────────────────────────────
BUY_TIER1_EMA   = 70
BUY_TIER2_EMA   = 100
SELL_TIER1_EMA  = 80
SELL_TIER2_EMA  = 110
OVERLAP_MIN_EMA = 70

# ── Streak rules ─────────────────────────────────────────────────────────────
# If buy_signal_streak  >= BUY_STREAK_TRIGGER,  replace BUY_TIER1_EMA  with
# STREAK_BUY_THRESHOLD  on the next accumulator tick.
# If sell_signal_streak >= SELL_STREAK_TRIGGER, replace SELL_TIER1_EMA with
# STREAK_SELL_THRESHOLD on the next accumulator tick.
BUY_STREAK_TRIGGER    = 5
STREAK_BUY_THRESHOLD  = 70.0   # more lenient tier-1 after a buy streak

SELL_STREAK_TRIGGER   = 5
STREAK_SELL_THRESHOLD = 70.0   # more lenient tier-1 after a sell streak

TICK_SIZE  = 0.01
LOT_SIZE   = 100

EXIT_REPRICE_SECONDS = 2.0

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
                "sim_time", "order_id", "symbol", "side", "price",
                "shares", "lots", "reason", "step", "pos_before",
            ])
    if not os.path.exists(EXECUTION_LOG_PATH):
        with open(EXECUTION_LOG_PATH, "w", newline="") as f:
            csv.writer(f).writerow([
                "sim_time", "order_id", "symbol", "side",
                "executed_price", "executed_size", "order_size", "status", "exec_timestamp",
            ])

def log_submission(sim_time, order_id, symbol, side, price, shares, reason, step, pos):
    with open(SUBMISSION_LOG_PATH, "a", newline="") as f:
        csv.writer(f).writerow([
            sim_time, order_id, symbol, side, price,
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
                                      meta["lots"] * LOT_SIZE,
                                      str(getattr(ex, "status", "")),
                                      exec_ts)
            cur = trader.get_order(oid)
            if cur is not None:
                s = str(getattr(cur, "status", ""))
                if ("FILLED" in s or "CANCELED" in s or "REJECTED" in s
                        or int(getattr(cur, "executed_size", 0))
                        >= meta["lots"] * LOT_SIZE):
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

def cancel_all(trader, symbol):
    for order in trader.get_waiting_list():
        if order.symbol == symbol:
            trader.submit_cancellation(order)
            time.sleep(1.0)

def submit_market_order(trader, symbol, side, lots, reason, step,
                        tracked_orders, pos_before):
    if lots <= 0:
        return None
    sim_time = trader.get_last_trade_time()
    if side == "BUY":
        order = shift.Order(shift.Order.Type.MARKET_BUY,  symbol, int(lots))
    else:
        order = shift.Order(shift.Order.Type.MARKET_SELL, symbol, int(lots))
    trader.submit_order(order)
    tracked_orders[order.id] = {
        "symbol": symbol, "side": side, "lots": int(lots), "done": False,
    }
    log_submission(sim_time, order.id, symbol, side, "MARKET",
                   lots * LOT_SIZE, reason, step, pos_before)
    print(f"[ORDER_SUBMITTED] {side} {lots}L MARKET {symbol} | {reason}", flush=True)
    return order.id

def submit_limit_order(trader, symbol, side, lots, price, reason, step,
                       tracked_orders, pos_before):
    if lots <= 0:
        return None
    sim_time = trader.get_last_trade_time()
    if side == "BUY":
        order = shift.Order(shift.Order.Type.LIMIT_BUY,  symbol, int(lots), float(price))
    else:
        order = shift.Order(shift.Order.Type.LIMIT_SELL, symbol, int(lots), float(price))
    trader.submit_order(order)
    tracked_orders[order.id] = {
        "symbol": symbol, "side": side, "lots": int(lots), "done": False,
    }
    log_submission(sim_time, order.id, symbol, side, f"{float(price):.4f}",
                   lots * LOT_SIZE, reason, step, pos_before)
    return order.id

def has_enough_bp(trader, side, lots, price_est, current_pos_shares):
    bp     = get_bp(trader)
    shares = lots * LOT_SIZE
    if side == "BUY":
        required = price_est * shares
    else:
        resulting = current_pos_shares - shares
        if resulting < 0:
            required = 2.0 * price_est * abs(resulting)
        else:
            required = 0.0
    if bp >= required:
        return True
    print(f"[BP] Insufficient: need ${required:.0f}, have ${bp:.0f} — skipping", flush=True)
    return False

# ---------------------------------------------------------------------------
# Target accumulator  (with streak rules)
# ---------------------------------------------------------------------------

def update_target_accumulator(signal, ema_t, current_lots,
                               buy_acc, sell_acc, neutral_streak,
                               buy_signal_streak, sell_signal_streak):
    """
    Streak rules:
      BUY  — if buy_signal_streak  >= BUY_STREAK_TRIGGER,
              effective tier-1 buy  threshold -> STREAK_BUY_THRESHOLD
      SELL — if sell_signal_streak >= SELL_STREAK_TRIGGER,
              effective tier-1 sell threshold -> STREAK_SELL_THRESHOLD
    """
    effective_buy_tier1  = (STREAK_BUY_THRESHOLD
                            if buy_signal_streak  >= BUY_STREAK_TRIGGER
                            else BUY_TIER1_EMA)
    effective_sell_tier1 = (STREAK_SELL_THRESHOLD
                            if sell_signal_streak >= SELL_STREAK_TRIGGER
                            else SELL_TIER1_EMA)

    if signal == "BUY_PRESSURE":
        neutral_streak = 0
        sell_acc = 0
        if current_lots < 0 and abs(ema_t) < OVERLAP_MIN_EMA:
            return 0, 0, 0, 0
        if ema_t >= BUY_TIER2_EMA:
            buy_acc = max(buy_acc + 2, 3)
        elif ema_t >= effective_buy_tier1:
            buy_acc = buy_acc + 2
        else:
            # EMA below tier1 but signal still bullish — hold position,
            # don't exit. Only exit on an actual signal flip to NEUTRAL/SELL.
            buy_acc = 0
            return current_lots, 0, 0, 0
        return buy_acc, buy_acc, 0, 0

    elif signal == "SELL_PRESSURE":
        neutral_streak = 0
        buy_acc = 0
        if current_lots > 0 and abs(ema_t) < OVERLAP_MIN_EMA:
            return 0, 0, 0, 0
        abs_ema = abs(ema_t)
        if abs_ema >= SELL_TIER2_EMA:
            sell_acc = max(sell_acc + 1, 2)
        elif abs_ema >= effective_sell_tier1:
            sell_acc = sell_acc + 1
        else:
            # EMA below tier1 but signal still bearish — hold position,
            # don't exit. Only exit on an actual signal flip to NEUTRAL/BUY.
            sell_acc = 0
            return current_lots, 0, 0, 0
        return -sell_acc, 0, sell_acc, 0

    else:  # NEUTRAL — 2-tick grace period
        neutral_streak += 1
        if neutral_streak >= 5:
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
    buy_acc           = 0
    sell_acc          = 0
    neutral_streak    = 0
    buy_signal_streak  = 0   # consecutive BUY_PRESSURE ticks
    sell_signal_streak = 0   # consecutive SELL_PRESSURE ticks

    # Exit order state (limit orders only)
    exit_oid       = None
    exit_side      = None
    exit_lots      = 0
    exit_submit_ts = 0.0

    last_known_pos = 0
    market_oid     = None
    tracked_orders = {}
    seen_keys      = set()
    step           = 0

    while trader.get_last_trade_time() < end_time:

        sim_time = trader.get_last_trade_time()

        poll_executions(trader, tracked_orders, seen_keys)

        # ── Book parse ────────────────────────────────────────────────────────
        bids, asks = parse_book(trader, symbol, LEVELS)
        if not bids or not asks:
            time.sleep(POLL_INTERVAL)
            continue

        best_bid, _ = bids[0]
        best_ask, _ = asks[0]
        if best_ask <= best_bid:
            time.sleep(POLL_INTERVAL)
            continue

        mid    = 0.5 * (best_bid + best_ask)
        spread = best_ask - best_bid

        if prev_bids is None or prev_asks is None:
            prev_bids, prev_asks = bids, asks
            time.sleep(POLL_INTERVAL)
            continue

        # ── OFI / signal ──────────────────────────────────────────────────────
        inc = multilevel_ofi(prev_bids, prev_asks, bids, asks, LEVELS)
        ofi_events.append((time.time(), inc))
        prune(ofi_events, time.time(), OFI_WINDOW_SECONDS)

        raw_ofi  = weighted_ofi(rolling_ofi(ofi_events, LEVELS), LEVEL_WEIGHTS)
        ema_t    = ema_update(ema_t, raw_ofi, EMA_ALPHA)
        raw_ofi_history.append(raw_ofi)

        pos_count, neg_count, pscore = persistence_stats(raw_ofi_history)
        ema_dir = ema_direction(ema_t, OFI_FLOOR)
        signal  = classify_signal(ema_dir, pos_count, neg_count, pscore,
                                  PERSISTENCE_REQUIRED, FINAL_SCORE_THRESHOLD)

        # ── Update streak counters BEFORE accumulator ─────────────────────────
        if signal == "BUY_PRESSURE":
            buy_signal_streak  += 1
            sell_signal_streak  = 0
        elif signal == "SELL_PRESSURE":
            sell_signal_streak += 1
            buy_signal_streak   = 0
        else:
            buy_signal_streak  = 0
            sell_signal_streak = 0

        # ── Position ──────────────────────────────────────────────────────────
        pos_shares = get_pos(trader, symbol)
        pos_lots   = pos_shares // LOT_SIZE

        if pos_lots != last_known_pos:
            print(f"[FILL CONFIRMED] Pos {last_known_pos:+d} → {pos_lots:+d}", flush=True)
            last_known_pos = pos_lots
            market_oid     = None

        # ── Target accumulator ────────────────────────────────────────────────
        # Always run so accumulators keep building even while a market order is
        # in flight. Submission is still gated on market_oid below.
        target_lots, buy_acc, sell_acc, neutral_streak = update_target_accumulator(
            signal, ema_t, pos_lots, buy_acc, sell_acc, neutral_streak,
            buy_signal_streak, sell_signal_streak
        )
        if market_oid is not None:
            print(f"[WAITING] market order in flight, pos={pos_lots:+d}, "
                  f"target={target_lots:+d}, acc=B{buy_acc}/S{sell_acc}", flush=True)

        delta_lots = target_lots - pos_lots

        # ── Classify action ───────────────────────────────────────────────────
        is_exit  = (target_lots == 0 and pos_lots != 0)
        is_entry = (target_lots != 0 and pos_lots == 0)
        is_flip  = (target_lots != 0 and pos_lots != 0
                    and (target_lots > 0) != (pos_lots > 0))
        is_add   = (target_lots != 0 and pos_lots != 0
                    and (target_lots > 0) == (pos_lots > 0)
                    and abs(target_lots) > abs(pos_lots))

        # ── Check exit order status ───────────────────────────────────────────
        if exit_oid:
            cur = trader.get_order(exit_oid)
            if cur is None:
                exit_oid = None; exit_side = None; exit_lots = 0
            else:
                s = str(getattr(cur, "status", ""))
                exec_sz = int(getattr(cur, "executed_size", 0))
                if ("FILLED" in s or "CANCELED" in s or "REJECTED" in s
                        or exec_sz >= exit_lots * LOT_SIZE):
                    exit_oid = None; exit_side = None; exit_lots = 0

        # ── Order management ──────────────────────────────────────────────────

        if delta_lots == 0:
            if exit_oid:
                cancel_all(trader, symbol)
                exit_oid = None; exit_side = None; exit_lots = 0

        elif is_exit:
            order_side  = "BUY" if delta_lots > 0 else "SELL"
            order_lots  = abs(delta_lots)
            order_price = sanitise_price(round_to_tick(mid))

            same_exit      = (exit_oid is not None
                              and exit_side == order_side
                              and exit_lots == order_lots)
            age            = time.time() - exit_submit_ts
            should_reprice = (not same_exit) or (age >= EXIT_REPRICE_SECONDS)

            if should_reprice:
                cancel_all(trader, symbol)
                exit_oid   = None
                pos_shares = get_pos(trader, symbol)
                pos_lots   = pos_shares // LOT_SIZE
                order_lots = abs(target_lots - pos_lots)
                if order_lots > 0:
                    reason   = (f"EXIT target=0 cur={pos_lots} "
                                f"sig={signal} ema={ema_t:.1f}")
                    exit_oid = submit_limit_order(
                        trader, symbol, order_side, order_lots, order_price,
                        reason, step, tracked_orders, pos_shares
                    )
                    if exit_oid:
                        exit_side      = order_side
                        exit_lots      = order_lots
                        exit_submit_ts = time.time()

        elif is_entry or is_add or is_flip:
            order_side = "BUY" if delta_lots > 0 else "SELL"
            order_lots = abs(delta_lots)

            if market_oid is not None:
                print(f"[SKIP] market order {market_oid[:8]}… still in flight", flush=True)
            elif has_enough_bp(trader, order_side, order_lots, mid, pos_shares):
                if exit_oid:
                    cancel_all(trader, symbol)
                    exit_oid = None; exit_side = None; exit_lots = 0

                action = ("ENTRY" if is_entry else
                          "FLIP"  if is_flip  else "ADD")
                if buy_signal_streak >= BUY_STREAK_TRIGGER:
                    streak_tag = f" [BUY_STREAK={buy_signal_streak}->tier1={STREAK_BUY_THRESHOLD}]"
                elif sell_signal_streak >= SELL_STREAK_TRIGGER:
                    streak_tag = f" [SELL_STREAK={sell_signal_streak}->tier1={STREAK_SELL_THRESHOLD}]"
                else:
                    streak_tag = ""
                reason = (f"MKT-{action}{streak_tag} target={target_lots} cur={pos_lots} "
                          f"sig={signal} ema={ema_t:.1f}")
                oid = submit_market_order(
                    trader, symbol, order_side, order_lots,
                    reason, step, tracked_orders, pos_shares
                )
                if oid:
                    market_oid = oid

        # ── Streak indicator for log line ─────────────────────────────────────
        if buy_signal_streak >= BUY_STREAK_TRIGGER:
            streak_indicator = f"*BSTREAK{buy_signal_streak}*"
        elif sell_signal_streak >= SELL_STREAK_TRIGGER:
            streak_indicator = f"*SSTREAK{sell_signal_streak}*"
        else:
            streak_indicator = f"Bstreak={buy_signal_streak}/Sstreak={sell_signal_streak}"

        print(
            f"[{sim_time}][{symbol}] "
            f"Sig: {signal:14s} | EMA: {ema_t:8.2f} | "
            f"Pos: {pos_lots:+3d}L | Target: {target_lots:+3d}L | "
            f"Delta: {delta_lots:+3d}L | Acc: B{buy_acc}/S{sell_acc} | "
            f"Mid: {mid:.4f} | {streak_indicator} | "
            f"ExitOrder: {'YES' if exit_oid else 'no':3s} | "
            f"BP: {get_bp(trader):.0f}",
            flush=True
        )

        prev_bids = bids
        prev_asks = asks
        step += 1

        time.sleep(POLL_INTERVAL)

    # Shutdown
    poll_executions(trader, tracked_orders, seen_keys)
    cancel_all(trader, symbol)
    print(f"[{symbol}] Strategy finished. Final pos: {get_pos(trader, symbol)} shares",
          flush=True)


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