import shift
import time
import csv
import os
from collections import deque
from datetime import timedelta

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
BUY_TIER1_EMA   = 80
BUY_TIER2_EMA   = 110
SELL_TIER1_EMA  = 80
SELL_TIER2_EMA  = 110
OVERLAP_MIN_EMA = 70

# ── Streak rules ──────────────────────────────────────────────────────────────
BUY_STREAK_TRIGGER    = 5
STREAK_BUY_THRESHOLD  = 60.0
SELL_STREAK_TRIGGER   = 5
STREAK_SELL_THRESHOLD = 70.0

TICK_SIZE  = 0.01
LOT_SIZE   = 100

# ── Entry limit order settings ────────────────────────────────────────────────
# Entry orders are limit orders at mid. They reprice every ENTRY_REPRICE_SECONDS
# to the current mid. If the signal changes direction, they are cancelled.
# There is no aggressive mode — if signal is gone, cancel and move on.
ENTRY_REPRICE_SECONDS = 2.0

# ── EMA drop exit rule ───────────────────────────────────────────────────────
# While in a position, we track the EMA high watermark (LONG) or low watermark
# (SHORT). If EMA drops (rises) for EMA_DROP_TICKS consecutive ticks from the
# previous tick value, we submit a limit exit at mid.
# Re-entry fires if EMA recovers above the watermark while still in signal.
# Sign change (EMA crosses zero) triggers a flip regardless of drop counter.
EMA_DROP_TICKS = 3   # consecutive ticks of EMA moving against position before exit

# ── Exit limit order settings ─────────────────────────────────────────────────
EXIT_REPRICE_SECONDS  = 2.0
EXIT_REPRICE_MAX      = 3
EXIT_AGGR_FRAC        = 0.5

SUBMISSION_LOG_PATH = "ofi_limit_entry_submissions.csv"
EXECUTION_LOG_PATH  = "ofi_limit_entry_executions.csv"

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
                                      str(getattr(ex, "status", "")), exec_ts)
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
                       tracked_orders, pos_before):
    if lots <= 0:
        return None
    sim_time = trader.get_last_trade_time()
    if side == "BUY":
        order = shift.Order(shift.Order.Type.LIMIT_BUY,  symbol, 5, float(price))
    else:
        order = shift.Order(shift.Order.Type.LIMIT_SELL, symbol, int(lots), float(price))
    trader.submit_order(order)
    tracked_orders[order.id] = {"symbol": symbol, "side": side,
                                "lots": int(lots), "done": False}
    log_submission(sim_time, order.id, symbol, side, f"{float(price):.4f}",
                   lots * LOT_SIZE, reason, step, pos_before)
    print(f"[ORDER] {side} {lots}L LIMIT @ {price:.4f} {symbol} | {reason}", flush=True)
    return order.id

def has_enough_bp(trader, side, lots, price_est, current_pos_shares):
    bp     = get_bp(trader)
    shares = lots * LOT_SIZE
    if side == "BUY":
        existing_short  = max(-current_pos_shares, 0)
        new_long_shares = max(shares - existing_short, 0)
        required = price_est * new_long_shares
    else:
        existing_long    = max(current_pos_shares, 0)
        new_short_shares = max(shares - existing_long, 0)
        required = 2.0 * price_est * new_short_shares
    if bp >= required:
        return True
    print(f"[BP] Need ${required:.0f}, have ${bp:.0f} — skipping", flush=True)
    return False

# ---------------------------------------------------------------------------
# Target accumulator
# ---------------------------------------------------------------------------

def update_target_accumulator(signal, ema_t, current_lots,
                               buy_acc, sell_acc,
                               buy_signal_streak, sell_signal_streak):
    effective_buy_tier1  = (STREAK_BUY_THRESHOLD
                            if buy_signal_streak  >= BUY_STREAK_TRIGGER
                            else BUY_TIER1_EMA)
    effective_sell_tier1 = (STREAK_SELL_THRESHOLD
                            if sell_signal_streak >= SELL_STREAK_TRIGGER
                            else SELL_TIER1_EMA)

    if signal == "BUY_PRESSURE":
        sell_acc = 0
        if current_lots < 0 and abs(ema_t) < OVERLAP_MIN_EMA:
            return 0, 0, 0
        if ema_t >= BUY_TIER2_EMA:
            buy_acc = max(buy_acc + 2, 3)
        elif ema_t >= effective_buy_tier1:
            buy_acc = buy_acc + 2
        else:
            return current_lots, buy_acc, 0  # hold
        return buy_acc, buy_acc, 0

    elif signal == "SELL_PRESSURE":
        buy_acc = 0
        if current_lots > 0 and abs(ema_t) < OVERLAP_MIN_EMA:
            return 0, 0, 0
        abs_ema = abs(ema_t)
        if abs_ema >= SELL_TIER2_EMA:
            sell_acc = max(sell_acc + 1, 2)
        elif abs_ema >= effective_sell_tier1:
            sell_acc = sell_acc + 1
        else:
            return current_lots, 0, sell_acc  # hold
        return -sell_acc, 0, sell_acc

    else:  # NEUTRAL — hold position, exit is EMA-drop driven not signal driven
        current_target = buy_acc if buy_acc > 0 else -sell_acc
        return current_target, buy_acc, sell_acc

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

    # Accumulators
    buy_acc            = 0
    sell_acc           = 0
    buy_signal_streak  = 0
    sell_signal_streak = 0

    # EMA watermark and drop tracking
    ema_watermark  = None   # high watermark (LONG) or low watermark (SHORT)
    ema_drop_count = 0      # consecutive ticks EMA moved against position
    prev_ema       = None   # EMA from previous tick

    # Entry limit order state
    # Tracks a single pending limit order for entry/add/flip.
    # Repriced every ENTRY_REPRICE_SECONDS. Cancelled on signal flip or delta=0.
    entry_oid        = None
    entry_side       = None
    entry_lots       = 0
    entry_submit_ts  = 0.0
    entry_target     = 0    # target_lots at time of submission — used to detect stale orders

    # Exit limit order state
    exit_oid           = None
    exit_side          = None
    exit_lots          = 0
    exit_submit_ts     = 0.0
    exit_reprice_count = 0

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

        # ── Streak counters ───────────────────────────────────────────────────
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

        # ── Target accumulator ────────────────────────────────────────────────
        target_lots, buy_acc, sell_acc = update_target_accumulator(
            signal, ema_t, pos_lots, buy_acc, sell_acc,
            buy_signal_streak, sell_signal_streak
        )

        delta_lots = target_lots - pos_lots

        # ── EMA watermark and drop tracking ──────────────────────────────────
        # Maintain a high watermark (LONG) / low watermark (SHORT) per position.
        # Consecutive ticks where EMA moves against the position increment
        # ema_drop_count. On EMA_DROP_TICKS consecutive drops we exit.
        # If EMA recovers above watermark while signal still valid, cancel exit
        # and re-enter. Sign change (zero cross) always triggers a flip.
        if pos_lots != 0 and prev_ema is not None:
            if pos_lots > 0:  # LONG
                if ema_t > prev_ema and ema_t > 0:
                    # EMA rising in positive territory — update watermark
                    ema_watermark  = ema_t if ema_watermark is None else max(ema_watermark, ema_t)
                    ema_drop_count = 0
                    if exit_oid:
                        trader.cancel_all_pending_orders()
                        exit_oid = None; exit_side = None; exit_lots = 0
                        exit_reprice_count = 0
                        print(f"[EXIT CANCELLED] EMA new high {ema_t:.2f} (prev {prev_ema:.2f})",
                              flush=True)
                else:
                    ema_drop_count += 1
            else:  # SHORT
                if ema_t < prev_ema and ema_t < 0:
                    # EMA falling in negative territory — update watermark
                    ema_watermark  = ema_t if ema_watermark is None else min(ema_watermark, ema_t)
                    ema_drop_count = 0
                    if exit_oid:
                        trader.cancel_all_pending_orders()
                        exit_oid = None; exit_side = None; exit_lots = 0
                        exit_reprice_count = 0
                        print(f"[EXIT CANCELLED] EMA new low {ema_t:.2f} (prev {prev_ema:.2f})",
                              flush=True)
                else:
                    ema_drop_count += 1
        elif pos_lots == 0:
            # Flat — reset watermark and drop counter
            ema_watermark  = None
            ema_drop_count = 0

        # ── Sign change detection (zero cross) ───────────────────────────────
        sign_change = (
            prev_ema is not None
            and prev_ema != 0.0
            and ema_t != 0.0
            and ((prev_ema < 0 < ema_t) or (prev_ema > 0 > ema_t))
        )

        # ── Classify action ───────────────────────────────────────────────────
        # is_exit is now driven by EMA drop or sign change, not accumulator
        ema_drop_exit = (pos_lots != 0 and ema_drop_count >= EMA_DROP_TICKS)
        is_flip       = sign_change and pos_lots != 0
        is_exit       = ema_drop_exit and not is_flip
        is_entry      = (target_lots != 0 and pos_lots == 0
                         and not sign_change)
        is_add        = (target_lots != 0 and pos_lots != 0
                         and (target_lots > 0) == (pos_lots > 0)
                         and abs(target_lots) > abs(pos_lots)
                         and not ema_drop_exit)
        wants_entry   = is_entry or is_add or is_flip

        # ── Entry order status check ──────────────────────────────────────────
        if entry_oid:
            cur = trader.get_order(entry_oid)
            if cur is None:
                entry_oid = None; entry_side = None; entry_lots = 0
            else:
                s = str(getattr(cur, "status", ""))
                if "FILLED" in s and "PARTIALLY" not in s:
                    print(f"[ENTRY FILLED] {entry_side} {entry_lots}L", flush=True)
                    entry_oid = None; entry_side = None; entry_lots = 0
                    # Seed watermark from current EMA on fill
                    ema_watermark  = ema_t
                    ema_drop_count = 0
                elif "CANCELED" in s or "REJECTED" in s:
                    entry_oid = None; entry_side = None; entry_lots = 0

        # ── Exit order status check ───────────────────────────────────────────
        if exit_oid:
            cur = trader.get_order(exit_oid)
            if cur is None:
                exit_oid = None; exit_side = None; exit_lots = 0
            else:
                s = str(getattr(cur, "status", ""))
                if "FILLED" in s and "PARTIALLY" not in s:
                    exit_oid = None; exit_side = None; exit_lots = 0
                    exit_reprice_count = 0
                elif "CANCELED" in s or "REJECTED" in s:
                    exit_oid = None; exit_side = None; exit_lots = 0

        # ── Order management ──────────────────────────────────────────────────

        if not wants_entry and not is_exit:
            # No action needed — cancel stale entry if signal weakened
            if entry_oid and delta_lots == 0:
                trader.cancel_all_pending_orders()
                entry_oid = None; entry_side = None; entry_lots = 0
                print(f"[ENTRY CANCELLED] delta=0, target={target_lots}", flush=True)

        elif is_flip:
            # ── Sign change: close position + open opposite ───────────────────
            order_side = "BUY" if ema_t > 0 else "SELL"
            order_lots = abs(pos_lots)   # close existing
            # Cancel everything outstanding
            trader.cancel_all_pending_orders()
            entry_oid = None; entry_side = None; entry_lots = 0
            exit_oid  = None; exit_side  = None; exit_lots  = 0
            exit_reprice_count = 0
            ema_drop_count     = 0

            order_price = sanitise_price(round_to_tick(mid))
            reason = (f"LMT-FLIP ema {prev_ema:.1f}→{ema_t:.1f} "
                      f"cur={pos_lots} sig={signal}")
            if has_enough_bp(trader, order_side, order_lots, mid, pos_shares):
                oid = submit_limit_order(trader, symbol, order_side, order_lots,
                                         order_price, reason, step,
                                         tracked_orders, pos_shares)
                if oid:
                    entry_oid       = oid
                    entry_side      = order_side
                    entry_lots      = order_lots
                    entry_submit_ts = time.time()
                    entry_target    = target_lots
                    ema_watermark   = ema_t

        elif is_exit:
            # ── EMA drop exit: limit at mid, reprice/escalate ─────────────────
            # Reset accumulators so target becomes 0 and delta = full close
            buy_acc  = 0
            sell_acc = 0
            order_side = "BUY" if pos_lots < 0 else "SELL"
            if entry_oid:
                trader.cancel_all_pending_orders()
                entry_oid = None; entry_side = None; entry_lots = 0

            age          = time.time() - exit_submit_ts
            first_submit = (exit_reprice_count == 0)
            should       = first_submit or (age >= EXIT_REPRICE_SECONDS)

            if should:
                if exit_oid:
                    trader.cancel_all_pending_orders()
                    exit_oid = None

                pos_shares = get_pos(trader, symbol)
                pos_lots   = pos_shares // LOT_SIZE
                order_lots = abs(pos_lots)
                if order_lots > 0:
                    if exit_reprice_count >= EXIT_REPRICE_MAX:
                        order_price = sanitise_price(round_to_tick(
                            mid + EXIT_AGGR_FRAC * spread if order_side == "BUY"
                            else mid - EXIT_AGGR_FRAC * spread
                        ))
                        price_tag = "AGGR"
                    else:
                        order_price = sanitise_price(round_to_tick(mid))
                        price_tag   = f"MID({exit_reprice_count + 1}/{EXIT_REPRICE_MAX})"
                    reason   = (f"EXIT-{price_tag} drop={ema_drop_count} "
                                f"cur={pos_lots} ema={ema_t:.1f} wm={(ema_watermark if ema_watermark is not None else 0.0):.1f}")
                    exit_oid = submit_limit_order(
                        trader, symbol, order_side, order_lots, order_price,
                        reason, step, tracked_orders, pos_shares
                    )
                    if exit_oid:
                        exit_side          = order_side
                        exit_lots          = order_lots
                        exit_submit_ts     = time.time()
                        exit_reprice_count += 1

        elif wants_entry:
            # ── Entry / Add: limit at mid, reprice every ENTRY_REPRICE_SECONDS ─
            order_side = "BUY" if delta_lots > 0 else "SELL"
            order_lots = abs(delta_lots)

            if exit_oid:
                trader.cancel_all_pending_orders()
                exit_oid = None; exit_side = None; exit_lots = 0
                exit_reprice_count = 0

            signal_flipped = (entry_oid is not None and entry_side != order_side)
            target_changed = (entry_oid is not None and entry_target != target_lots)
            age            = time.time() - entry_submit_ts
            first_submit   = (entry_oid is None)
            should_reprice = (first_submit or signal_flipped or target_changed
                              or age >= ENTRY_REPRICE_SECONDS)

            if signal_flipped:
                trader.cancel_all_pending_orders()
                entry_oid = None; entry_side = None; entry_lots = 0
                print(f"[ENTRY CANCELLED] signal flipped to {signal}", flush=True)

            if should_reprice and has_enough_bp(trader, order_side, order_lots,
                                                mid, pos_shares):
                if entry_oid:
                    trader.cancel_all_pending_orders()
                    entry_oid = None

                order_price = sanitise_price(round_to_tick(mid))
                action = "ENTRY" if is_entry else "ADD"
                if buy_signal_streak >= BUY_STREAK_TRIGGER:
                    streak_tag = f" [BUY_STREAK={buy_signal_streak}]"
                elif sell_signal_streak >= SELL_STREAK_TRIGGER:
                    streak_tag = f" [SELL_STREAK={sell_signal_streak}]"
                else:
                    streak_tag = ""
                reason = (f"LMT-{action}{streak_tag} target={target_lots} "
                          f"cur={pos_lots} sig={signal} ema={ema_t:.1f} "
                          f"wm={(ema_watermark if ema_watermark is not None else 0.0):.1f}")
                oid = submit_limit_order(
                    trader, symbol, order_side, order_lots, order_price,
                    reason, step, tracked_orders, pos_shares
                )
                if oid:
                    entry_oid       = oid
                    entry_side      = order_side
                    entry_lots      = order_lots
                    entry_submit_ts = time.time()
                    entry_target    = target_lots

        # ── Log line ──────────────────────────────────────────────────────────
        if buy_signal_streak >= BUY_STREAK_TRIGGER:
            streak_indicator = f"*BSTREAK{buy_signal_streak}*"
        elif sell_signal_streak >= SELL_STREAK_TRIGGER:
            streak_indicator = f"*SSTREAK{sell_signal_streak}*"
        else:
            streak_indicator = f"Bstreak={buy_signal_streak}/Sstreak={sell_signal_streak}"

        wm_str = f"{ema_watermark:.2f}" if ema_watermark is not None else "  -  "
        print(
            f"[{sim_time}][{symbol}] "
            f"Sig: {signal:14s} | EMA: {ema_t:8.2f} | WM: {wm_str} | "
            f"Drop: {ema_drop_count} | "
            f"Pos: {pos_lots:+3d}L | Target: {target_lots:+3d}L | "
            f"Delta: {delta_lots:+3d}L | Acc: B{buy_acc}/S{sell_acc} | "
            f"Mid: {mid:.4f} | {streak_indicator} | "
            f"Entry: {'YES' if entry_oid else 'no ':3s} | "
            f"Exit: {'YES' if exit_oid else 'no ':3s} | "
            f"BP: {get_bp(trader):.0f}",
            flush=True
        )

        prev_bids = bids
        prev_asks = asks
        prev_ema  = ema_t
        step += 1
        time.sleep(POLL_INTERVAL)

    # Shutdown
    poll_executions(trader, tracked_orders, seen_keys)
    trader.cancel_all_pending_orders()
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