import shift
import time
import math
import csv
import os
import statistics
from collections import deque
from datetime import datetime, timedelta

SYMBOL = "MSFT"
LEVELS = 10
POLL_INTERVAL = 1.0
OFI_WINDOW_SECONDS = 10.0
EMA_ALPHA = 0.2

LEVEL_WEIGHTS = [1.0, 0.9, 0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2, 0.1]

PERSISTENCE_LOOKBACK   = 6
PERSISTENCE_REQUIRED   = 5
FINAL_SCORE_THRESHOLD  = 0.5
OFI_FLOOR_Z            = 0.5

ZSCORE_WINDOW   = 60
ZSCORE_WARMUP   = 30

BUY_TIER1_Z    = 2.0
BUY_TIER2_Z    = 2.5
SELL_TIER1_Z   = 2.0
SELL_TIER2_Z   = 2.5
OVERLAP_MIN_Z  = 1.8

BUY_STREAK_TRIGGER    = 5
STREAK_BUY_Z          = 1.8
SELL_STREAK_TRIGGER   = 5
STREAK_SELL_Z         = 1.8

# ── Position caps ─────────────────────────────────────────────────────────────
MAX_LONG_LOTS  = 10   # maximum long position in lots
MAX_SHORT_LOTS = 10   # maximum short position in lots (stored as positive)

TICK_SIZE = 0.01
LOT_SIZE  = 100

# ── Entry: market orders (fast execution on volatile moves) ───────────────────
# Market orders cross the spread immediately — better for catching directional
# moves before they pass. We use market_oid guard to prevent stacked orders.

# ── Exit limit order settings ─────────────────────────────────────────────────
EXIT_REPRICE_SECONDS = 2.0
EXIT_AGGR_FRAC       = 0.2

# ── Competition rules ─────────────────────────────────────────────────────────
PROFIT_TARGET_USD      = 5000.0   # stop trading once realized P&L exceeds this
MIN_TRADES_REQUIRED    = 200      # minimum unique executed trades required
TRADE_PUSH_MINUTES     = 30       # minutes before end_time to start pushing trades

# ── Order chunking ────────────────────────────────────────────────────────────
# Orders larger than CHUNK_THRESHOLD lots are split into 2 equal chunks.
# The first chunk fires immediately; the second fires automatically on the
# next tick once the first fill is confirmed (via market_oid / exit reprice).
CHUNK_THRESHOLD = 8

SUBMISSION_LOG_PATH = "ofi_pyramid_submissions.csv"
EXECUTION_LOG_PATH  = "ofi_pyramid_executions.csv"

# ---------------------------------------------------------------------------
# Tick helpers
# ---------------------------------------------------------------------------

def round_to_tick(x):
    return round(round(x / TICK_SIZE, 6) * TICK_SIZE, 2)

def sanitise_price(p):
    return round(float(p), 2)

def compute_zscore(ema_t, ema_history):
    n = len(ema_history)
    if n < ZSCORE_WARMUP:
        return 0.0, False
    mu  = statistics.mean(ema_history)
    std = statistics.pstdev(ema_history)
    if std < 1e-9:
        return 0.0, True
    return (ema_t - mu) / std, True

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
    """Returns the number of new execution records logged this call."""
    new_fills = 0
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
                        new_fills += 1
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
    return new_fills

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
    trader.cancel_all_pending_orders()

def submit_market_order(trader, symbol, side, lots, reason, step,
                        tracked_orders, pos_before):
    """Submit a market order for entries. Fast fill, no reprice needed."""
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
    print(f"[ORDER] {side} {lots}L MARKET {symbol} | {reason}", flush=True)
    return order.id

def submit_limit_order(trader, symbol, side, lots, price, reason, step,
                       tracked_orders, pos_before):
    """Submit a limit order for exits."""
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
    print(f"[ORDER] {side} {lots}L LIMIT @ {price:.4f} {symbol} | {reason}", flush=True)
    return order.id

def has_enough_bp(trader, side, lots, price_est, current_pos_shares):
    bp     = get_bp(trader)
    shares = lots * LOT_SIZE
    if side == "BUY":
        existing_short  = max(-current_pos_shares, 0)
        new_long_shares = max(shares - existing_short, 0)
        open_required   = price_est * new_long_shares
        exit_required   = 0.0
    else:
        existing_long    = max(current_pos_shares, 0)
        new_short_shares = max(shares - existing_long, 0)
        open_required    = 2.0 * price_est * new_short_shares
        resulting_short  = max(-current_pos_shares, 0) + new_short_shares
        exit_required    = price_est * resulting_short
    required = max(open_required, exit_required)
    if bp >= required:
        return True
    print(f"[BP] Need ${required:.0f} (open=${open_required:.0f} "
          f"exit=${exit_required:.0f}), have ${bp:.0f} — skipping", flush=True)
    return False

# ---------------------------------------------------------------------------
# Target accumulator — with MAX_LONG_LOTS and MAX_SHORT_LOTS caps
# ---------------------------------------------------------------------------

def update_target_accumulator(signal, ema_z, zscore_valid, current_lots,
                               buy_acc, sell_acc, neutral_streak,
                               buy_signal_streak, sell_signal_streak):
    if not zscore_valid:
        return current_lots, buy_acc, sell_acc, neutral_streak

    effective_buy_tier1  = (STREAK_BUY_Z
                            if buy_signal_streak  >= BUY_STREAK_TRIGGER
                            else BUY_TIER1_Z)
    effective_sell_tier1 = (STREAK_SELL_Z
                            if sell_signal_streak >= SELL_STREAK_TRIGGER
                            else SELL_TIER1_Z)

    if signal == "BUY_PRESSURE":
        neutral_streak = 0
        sell_acc = 0
        if current_lots < 0 and ema_z < OVERLAP_MIN_Z:
            return 0, 0, 0, 0
        if ema_z >= BUY_TIER2_Z:
            buy_acc = max(buy_acc + 2, 3)
        elif ema_z >= effective_buy_tier1:
            buy_acc = buy_acc + 2
        else:
            return current_lots, buy_acc, 0, 0

        # ── Cap long position at MAX_LONG_LOTS ────────────────────────────
        buy_acc = min(buy_acc, MAX_LONG_LOTS)
        return buy_acc, buy_acc, 0, 0

    elif signal == "SELL_PRESSURE":
        neutral_streak = 0
        buy_acc = 0
        if current_lots > 0 and -ema_z < OVERLAP_MIN_Z:
            return 0, 0, 0, 0
        abs_z = abs(ema_z)
        if abs_z >= SELL_TIER2_Z:
            sell_acc = max(sell_acc + 1, 2)
        elif abs_z >= effective_sell_tier1:
            sell_acc = sell_acc + 1
        else:
            return current_lots, 0, sell_acc, 0

        # ── Cap short position at MAX_SHORT_LOTS ──────────────────────────
        sell_acc = min(sell_acc, MAX_SHORT_LOTS)
        print_cap = sell_acc == MAX_SHORT_LOTS
        if print_cap:
            print(f"[CAP] Short position capped at {MAX_SHORT_LOTS}L", flush=True)
        return -sell_acc, 0, sell_acc, 0

    else:  # NEUTRAL — grace period
        neutral_streak += 1
        if neutral_streak >= 7:
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

    prev_bids       = None
    prev_asks       = None
    ofi_events      = deque()
    raw_ofi_history = deque(maxlen=PERSISTENCE_LOOKBACK)
    ema_t           = None
    ema_history     = deque(maxlen=ZSCORE_WINDOW)

    buy_acc            = 0
    sell_acc           = 0
    neutral_streak     = 0
    buy_signal_streak  = 0
    sell_signal_streak = 0

    # market_oid guard — set after submitting a market entry order.
    # Cleared when pos_lots changes. Prevents stacked market orders.
    market_oid     = None
    last_known_pos = 0

    # Exit limit order state
    exit_oid       = None
    exit_side      = None
    exit_lots      = 0
    exit_submit_ts = 0.0

    tracked_orders  = {}
    seen_keys       = set()
    unique_trades   = 0   # count of unique execution records — for competition minimum
    step            = 0

    profit_target_hit = False
    trade_push_time   = end_time - timedelta(minutes=TRADE_PUSH_MINUTES)

    while trader.get_last_trade_time() < end_time:

        sim_time = trader.get_last_trade_time()
        unique_trades += poll_executions(trader, tracked_orders, seen_keys)

        # ── Competition rule 1: stop trading once P&L target is hit ──────────
        realized_pl = trader.get_portfolio_summary().get_total_realized_pl()
        if realized_pl >= PROFIT_TARGET_USD and not profit_target_hit:
            profit_target_hit = True
            print(f"[PROFIT TARGET] Realized P&L ${realized_pl:.2f} >= "
                  f"${PROFIT_TARGET_USD:.0f} — stopping new entries", flush=True)

        if profit_target_hit:
            # Still manage exits to flatten any open position, but no new entries
            pos_shares = get_pos(trader, symbol)
            if pos_shares == 0:
                print(f"[PROFIT TARGET] Flat — waiting for end. "
                      f"Trades: {unique_trades}/{MIN_TRADES_REQUIRED}", flush=True)
                time.sleep(POLL_INTERVAL)
                continue

        # ── Competition rule 2: push trades if < 200 with 30 min to go ───────
        if (sim_time >= trade_push_time
                and unique_trades < MIN_TRADES_REQUIRED):
            needed = MIN_TRADES_REQUIRED - unique_trades
            print(f"[TRADE PUSH] {unique_trades}/{MIN_TRADES_REQUIRED} trades — "
                  f"need {needed} more. Submitting 1L market buy+sell.", flush=True)
            cancel_all(trader, symbol)
            # Submit a 1-lot buy and a 1-lot sell — two market orders, two fills,
            # each counts as a unique execution record toward the minimum.
            buy_order  = shift.Order(shift.Order.Type.MARKET_BUY,  symbol, 1)
            sell_order = shift.Order(shift.Order.Type.MARKET_SELL, symbol, 1)
            trader.submit_order(buy_order)
            trader.submit_order(sell_order)
            tracked_orders[buy_order.id]  = {"symbol": symbol, "side": "BUY",
                                              "lots": 1, "done": False}
            tracked_orders[sell_order.id] = {"symbol": symbol, "side": "SELL",
                                             "lots": 1, "done": False}
            log_submission(sim_time, buy_order.id,  symbol, "BUY",  "MARKET",
                           LOT_SIZE, "trade_push", step, get_pos(trader, symbol))
            log_submission(sim_time, sell_order.id, symbol, "SELL", "MARKET",
                           LOT_SIZE, "trade_push", step, get_pos(trader, symbol))
            time.sleep(1.0)   # let them fill before next tick
            continue

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

        ema_history.append(ema_t)
        ema_z, zscore_valid = compute_zscore(ema_t, ema_history)

        pos_count, neg_count, pscore = persistence_stats(raw_ofi_history)
        ema_dir = ema_direction(ema_z, OFI_FLOOR_Z)
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

        # ── market_oid guard: detect fill via position change ─────────────────
        if market_oid is not None:
            if pos_lots != last_known_pos:
                print(f"[FILL CONFIRMED] Pos {last_known_pos:+d} → {pos_lots:+d}L",
                      flush=True)
                last_known_pos = pos_lots
                market_oid     = None
            else:
                # Safety: clear if order was rejected/cancelled
                try:
                    o = trader.get_order(market_oid)
                    if o is not None:
                        s = str(getattr(o, "status", ""))
                        if "CANCELED" in s or "REJECTED" in s:
                            print(f"[WARN] Market order {market_oid[:8]}… {s} — clearing",
                                  flush=True)
                            market_oid = None
                except Exception:
                    pass
        else:
            last_known_pos = pos_lots

        # ── Target accumulator ────────────────────────────────────────────────
        # Freeze accumulator while market order is in flight — prevents
        # inflating target before the previous fill is confirmed.
        if market_oid is None:
            target_lots, buy_acc, sell_acc, neutral_streak = update_target_accumulator(
                signal, ema_z, zscore_valid, pos_lots, buy_acc, sell_acc, neutral_streak,
                buy_signal_streak, sell_signal_streak
            )
        else:
            # Recompute target from frozen accumulators without incrementing
            if signal == "BUY_PRESSURE":
                target_lots = buy_acc
            elif signal == "SELL_PRESSURE":
                target_lots = -sell_acc
            else:
                neutral_streak += 1
                if neutral_streak >= 7:
                    target_lots = 0; buy_acc = 0; sell_acc = 0
                else:
                    target_lots = buy_acc if buy_acc > 0 else -sell_acc
            print(f"[WAITING] market order in flight | pos={pos_lots:+d} "
                  f"target={target_lots:+d}", flush=True)

        delta_lots = target_lots - pos_lots

        # ── Classify action ───────────────────────────────────────────────────
        is_exit    = (target_lots == 0 and pos_lots != 0)
        is_entry   = (target_lots != 0 and pos_lots == 0)
        is_flip    = (target_lots != 0 and pos_lots != 0
                      and (target_lots > 0) != (pos_lots > 0))
        is_add     = (target_lots != 0 and pos_lots != 0
                      and (target_lots > 0) == (pos_lots > 0)
                      and abs(target_lots) > abs(pos_lots))
        wants_entry = is_entry or is_add or is_flip

        # ── Exit order status check ───────────────────────────────────────────
        if exit_oid:
            cur = trader.get_order(exit_oid)
            if cur is None:
                exit_oid = None; exit_side = None; exit_lots = 0
            else:
                s       = str(getattr(cur, "status", ""))
                exec_sz = int(getattr(cur, "executed_size", 0))
                if ("FILLED" in s or "CANCELED" in s or "REJECTED" in s
                        or exec_sz >= exit_lots * LOT_SIZE):
                    exit_oid = None; exit_side = None; exit_lots = 0

        # ── Order management ──────────────────────────────────────────────────

        if delta_lots == 0 and not is_exit:
            # Nothing to do — cancel any stale exit order
            if exit_oid:
                cancel_all(trader, symbol)
                exit_oid = None; exit_side = None; exit_lots = 0

        elif is_exit:
            # ── EXIT: limit order, repriced every EXIT_REPRICE_SECONDS ───────
            order_side  = "BUY" if delta_lots > 0 else "SELL"
            order_lots  = abs(delta_lots)
            if order_side == "BUY":
                order_price = sanitise_price(round_to_tick(mid + EXIT_AGGR_FRAC * spread))
            else:
                order_price = sanitise_price(round_to_tick(mid - EXIT_AGGR_FRAC * spread))

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
                    # ── Chunk: split exits > CHUNK_THRESHOLD into 2 halves ────
                    # Submit only the first chunk now. After it fills, pos_lots
                    # updates and the exit reprice fires the second chunk.
                    if order_lots > CHUNK_THRESHOLD:
                        chunk_lots = math.ceil(order_lots / 2)
                        print(f"[CHUNK] EXIT {order_lots}L → submitting chunk 1 of 2 "
                              f"({chunk_lots}L), remainder on reprice", flush=True)
                        order_lots = chunk_lots

                    reason   = (f"EXIT target=0 cur={pos_lots} "
                                f"sig={signal} ema_z={ema_z:.2f}")
                    exit_oid = submit_limit_order(
                        trader, symbol, order_side, order_lots, order_price,
                        reason, step, tracked_orders, pos_shares
                    )
                    if exit_oid:
                        exit_side      = order_side
                        exit_lots      = order_lots
                        exit_submit_ts = time.time()

        elif wants_entry and market_oid is None and not profit_target_hit:
            # ── ENTRY / ADD / FLIP: market order ─────────────────────────────
            # Only submit if no market order is currently in flight.
            order_side = "BUY" if delta_lots > 0 else "SELL"
            order_lots = abs(delta_lots)

            # Cancel any resting exit order before entering
            if exit_oid:
                cancel_all(trader, symbol)
                exit_oid = None; exit_side = None; exit_lots = 0

            # BP check — degrade flip to close-only if insufficient
            if not has_enough_bp(trader, order_side, order_lots, mid, pos_shares):
                if is_flip and pos_lots != 0:
                    order_lots = abs(pos_lots)
                    print(f"[BP] Degrading flip to exit-only: {order_lots}L", flush=True)
                else:
                    order_lots = 0

            if order_lots > 0:
                # ── Chunk: split orders > CHUNK_THRESHOLD into 2 halves ───────
                # Submit only the first chunk now. The market_oid guard ensures
                # the second chunk fires automatically on the next tick after
                # the first fill is confirmed via pos_lots changing.
                if order_lots > CHUNK_THRESHOLD:
                    chunk_lots = math.ceil(order_lots / 2)
                    print(f"[CHUNK] {order_lots}L → submitting chunk 1 of 2 "
                          f"({chunk_lots}L), remainder on next fill", flush=True)
                    order_lots = chunk_lots

                action = ("ENTRY" if is_entry else
                          "FLIP"  if is_flip  else "ADD")
                if buy_signal_streak >= BUY_STREAK_TRIGGER:
                    streak_tag = f" [BSTREAK={buy_signal_streak}]"
                elif sell_signal_streak >= SELL_STREAK_TRIGGER:
                    streak_tag = f" [SSTREAK={sell_signal_streak}]"
                else:
                    streak_tag = ""
                reason = (f"MKT-{action}{streak_tag} target={target_lots} "
                          f"cur={pos_lots} sig={signal} z={ema_z:.2f}")
                oid = submit_market_order(
                    trader, symbol, order_side, order_lots,
                    reason, step, tracked_orders, pos_shares
                )
                if oid:
                    market_oid = oid   # freeze until pos changes

        elif wants_entry and market_oid is not None:
            print(f"[SKIP] Market order {market_oid[:8]}… in flight — not stacking",
                  flush=True)

        # ── Status line ───────────────────────────────────────────────────────
        if buy_signal_streak >= BUY_STREAK_TRIGGER:
            streak_indicator = f"*BSTREAK{buy_signal_streak}*"
        elif sell_signal_streak >= SELL_STREAK_TRIGGER:
            streak_indicator = f"*SSTREAK{sell_signal_streak}*"
        else:
            streak_indicator = f"Bstr={buy_signal_streak}/Sstr={sell_signal_streak}"

        cap_indicator  = ""
        if buy_acc  >= MAX_LONG_LOTS:  cap_indicator = " [LONG_CAP]"
        if sell_acc >= MAX_SHORT_LOTS: cap_indicator = " [SHORT_CAP]"

        warmup_tag = "" if zscore_valid else " [WARMUP]"
        print(
            f"[{sim_time}][{symbol}] "
            f"Sig: {signal:14s} | Z: {ema_z:+.2f}{warmup_tag} | "
            f"Pos: {pos_lots:+3d}L | Target: {target_lots:+3d}L{cap_indicator} | "
            f"Delta: {delta_lots:+3d}L | Acc: B{buy_acc}/S{sell_acc} | "
            f"Mid: {mid:.4f} | {streak_indicator} | "
            f"MktOrd: {'YES' if market_oid else 'no ':3s} | "
            f"Exit: {'YES' if exit_oid else 'no ':3s} | "
            f"Trades: {unique_trades}/{MIN_TRADES_REQUIRED} | "
            f"PnL: ${realized_pl:.0f} | "
            f"BP: {get_bp(trader):.0f}",
            flush=True
        )

        prev_bids = bids
        prev_asks = asks
        step += 1
        time.sleep(POLL_INTERVAL)

    poll_executions(trader, tracked_orders, seen_keys)
    cancel_all(trader, symbol)
    print(f"[{symbol}] Strategy finished. Final pos: {get_pos(trader, symbol)} shares",
          flush=True)


if __name__ == "__main__":
    with shift.Trader("columbia-traders") as trader:
        trader.connect("initiator.cfg", "aRkkZSrj")
        time.sleep(1.0)
        trader.sub_all_order_book()
        time.sleep(1.0)
        end_time = trader.get_last_trade_time() + timedelta(minutes=380.0)
        try:
            run_strategy(trader, symbol=SYMBOL, end_time=end_time)
        except KeyboardInterrupt:
            trader.disconnect()