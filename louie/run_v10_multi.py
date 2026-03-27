import shift
import time
import csv
import os
from collections import deque
from datetime import timedelta
from threading import Thread
from dataclasses import dataclass, field
from typing import Optional

# ---------------------------------------------------------------------------
# Per-ticker configuration
# ---------------------------------------------------------------------------

@dataclass
class TickerConfig:
    symbol: str

    # Book / OFI
    levels: int                  = 10
    poll_interval: float         = 1.0
    ofi_window_seconds: float    = 5.0
    ema_alpha: float             = 0.3
    level_weights: list          = field(default_factory=lambda: [1.0, 0.9, 0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2, 0.1])

    # Signal classification
    persistence_lookback:  int   = 5
    persistence_required:  int   = 4
    final_score_threshold: float = 0.5
    ofi_floor:             float = 100.0

    # Sizing thresholds — normal regime
    buy_tier1_ema:  float = 150.0
    buy_tier2_ema:  float = 200.0
    sell_tier1_ema: float = 130.0
    sell_tier2_ema: float = 200.0
    overlap_min_ema: float = 150.0

    # Streak rule: if buy_signal_streak >= BUY_STREAK_TRIGGER,
    # replace buy_tier1_ema with streak_buy_threshold on the next entry.
    buy_streak_trigger:     int   = 3
    streak_buy_threshold:   float = 175.0

    # Execution
    tick_size:             float = 0.01
    lot_size:              int   = 100
    exit_reprice_seconds:  float = 2.0

    # Per-ticker CSV paths (auto-derived from symbol if left empty)
    submission_log_path: str = ""
    execution_log_path:  str = ""

    def __post_init__(self):
        if not self.submission_log_path:
            self.submission_log_path = f"ofi_{self.symbol}_submissions.csv"
        if not self.execution_log_path:
            self.execution_log_path = f"ofi_{self.symbol}_executions.csv"


# ---------------------------------------------------------------------------
# Edit per-ticker parameters here
# ---------------------------------------------------------------------------

TICKER_CONFIGS: dict[str, TickerConfig] = {
    "AAPL": TickerConfig(
        symbol          = "AAPL",
        buy_tier1_ema   = 200.0,
        buy_tier2_ema   = 250.0,
        sell_tier1_ema  = 190.0,
        sell_tier2_ema  = 230.0,
        overlap_min_ema = 150.0,
        streak_buy_threshold = 175.0,
    ),
    "MSFT": TickerConfig(
        symbol          = "MSFT",
        buy_tier1_ema   = 200.0,
        buy_tier2_ema   = 230.0,
        sell_tier1_ema  = 180.0,
        sell_tier2_ema  = 230.0,
        overlap_min_ema = 160.0,
        streak_buy_threshold = 180.0,
    ),
    "NVDA": TickerConfig(
        symbol          = "NVDA",
        buy_tier1_ema   = 300.0,
        buy_tier2_ema   = 340.0,
        sell_tier1_ema  = 200.0,
        sell_tier2_ema  = 250.0,
        overlap_min_ema = 200.0,
        streak_buy_threshold = 250.0,
    ),
}

TICKERS   = ["AAPL", "MSFT", "NVDA"]
RUN_MINUTES = 500.0

# ---------------------------------------------------------------------------
# Tick helpers
# ---------------------------------------------------------------------------

def round_to_tick(x, tick_size=0.01):
    return round(round(x / tick_size, 6) * tick_size, 2)

def sanitise_price(p):
    return round(float(p), 2)

# ---------------------------------------------------------------------------
# CSV logging  (per-ticker paths come from cfg)
# ---------------------------------------------------------------------------

def ensure_csv_headers(cfg: TickerConfig):
    if not os.path.exists(cfg.submission_log_path):
        with open(cfg.submission_log_path, "w", newline="") as f:
            csv.writer(f).writerow([
                "sim_time", "order_id", "symbol", "side", "price",
                "shares", "lots", "reason", "step", "pos_before",
            ])
    if not os.path.exists(cfg.execution_log_path):
        with open(cfg.execution_log_path, "w", newline="") as f:
            csv.writer(f).writerow([
                "sim_time", "order_id", "symbol", "side",
                "executed_price", "executed_size", "order_size", "status", "exec_timestamp",
            ])

def log_submission(cfg, sim_time, order_id, side, price, shares, reason, step, pos):
    with open(cfg.submission_log_path, "a", newline="") as f:
        csv.writer(f).writerow([
            sim_time, order_id, cfg.symbol, side, price,
            int(shares), int(shares // cfg.lot_size), reason, step, int(pos),
        ])

def log_execution(cfg, sim_time, order_id, side, exec_price, exec_size,
                  order_size, status, exec_ts):
    with open(cfg.execution_log_path, "a", newline="") as f:
        csv.writer(f).writerow([
            sim_time, order_id, cfg.symbol, side, f"{float(exec_price):.4f}",
            int(exec_size), int(order_size), str(status), exec_ts,
        ])

# ---------------------------------------------------------------------------
# Execution audit
# ---------------------------------------------------------------------------

def poll_executions(trader, cfg: TickerConfig, tracked_orders, seen_keys):
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
                        print(f"[{cfg.symbol}][FILL] {meta['side']} {sz} @ {px:.4f} "
                              f"| status={getattr(ex,'status','')} "
                              f"| sim={exec_ts}", flush=True)
                        log_execution(cfg, sim_time, oid, meta["side"], px, sz,
                                      meta["lots"] * cfg.lot_size,
                                      str(getattr(ex, "status", "")), exec_ts)
            cur = trader.get_order(oid)
            if cur is not None:
                s = str(getattr(cur, "status", ""))
                if ("FILLED" in s or "CANCELED" in s or "REJECTED" in s
                        or int(getattr(cur, "executed_size", 0))
                        >= meta["lots"] * cfg.lot_size):
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
    while len(lst) < n:
        lst.append((0., 0.))
    return lst

def level_ofi(pb, pb_q, pa, pa_q, nb, nb_q, na, na_q):
    b = (nb_q if nb >= pb else 0.) - (pb_q if nb <= pb else 0.)
    a = -(na_q if na <= pa else 0.) + (pa_q if na >= pa else 0.)
    return float(b + a)

def multilevel_ofi(prev_bids, prev_asks, new_bids, new_asks, levels):
    pb = pad(prev_bids, levels);  pa = pad(prev_asks, levels)
    nb = pad(new_bids,  levels);  na = pad(new_asks,  levels)
    return [level_ofi(pb[m][0], pb[m][1], pa[m][0], pa[m][1],
                      nb[m][0], nb[m][1], na[m][0], na[m][1])
            for m in range(levels)]

def prune(dq, now_ts, window):
    cut = now_ts - window
    while dq and dq[0][0] < cut:
        dq.popleft()

def rolling_ofi(events, levels):
    t = [0.] * levels
    for _, v in events:
        for m in range(levels):
            t[m] += v[m]
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

def cancel_all(trader, symbol):
    for order in trader.get_waiting_list():
        if order.symbol == symbol:
            trader.submit_cancellation(order)
            time.sleep(1.0)

def submit_market_order(trader, cfg: TickerConfig, side, lots, reason, step,
                        tracked_orders, pos_before):
    if lots <= 0:
        return None
    sim_time = trader.get_last_trade_time()
    if side == "BUY":
        order = shift.Order(shift.Order.Type.MARKET_BUY,  cfg.symbol, int(lots))
    else:
        order = shift.Order(shift.Order.Type.MARKET_SELL, cfg.symbol, int(lots))
    trader.submit_order(order)
    tracked_orders[order.id] = {
        "symbol": cfg.symbol, "side": side,
        "lots": int(lots), "done": False,
    }
    log_submission(cfg, sim_time, order.id, side, "MARKET",
                   lots * cfg.lot_size, reason, step, pos_before)
    print(f"[{cfg.symbol}][ORDER] {side} {lots}L MARKET | {reason}", flush=True)
    return order.id

def submit_limit_order(trader, cfg: TickerConfig, side, lots, price, reason, step,
                       tracked_orders, pos_before):
    if lots <= 0:
        return None
    sim_time = trader.get_last_trade_time()
    if side == "BUY":
        order = shift.Order(shift.Order.Type.LIMIT_BUY,  cfg.symbol, int(lots), float(price))
    else:
        order = shift.Order(shift.Order.Type.LIMIT_SELL, cfg.symbol, int(lots), float(price))
    trader.submit_order(order)
    tracked_orders[order.id] = {
        "symbol": cfg.symbol, "side": side,
        "lots": int(lots), "done": False,
    }
    log_submission(cfg, sim_time, order.id, side, f"{float(price):.4f}",
                   lots * cfg.lot_size, reason, step, pos_before)
    return order.id

def has_enough_bp(trader, cfg: TickerConfig, side, lots, price_est, current_pos_shares):
    bp     = get_bp(trader)
    shares = lots * cfg.lot_size
    if side == "BUY":
        required = price_est * shares
    else:
        resulting = current_pos_shares - shares
        required  = 2.0 * price_est * abs(resulting) if resulting < 0 else 0.0
    if bp >= required:
        return True
    print(f"[{cfg.symbol}][BP] Insufficient: need ${required:.0f}, have ${bp:.0f}", flush=True)
    return False

# ---------------------------------------------------------------------------
# Target accumulator  (cfg-aware + streak rule)
# ---------------------------------------------------------------------------

def update_target_accumulator(signal, ema_t, current_lots,
                               buy_acc, sell_acc, neutral_streak,
                               cfg: TickerConfig, buy_signal_streak: int):
    """
    Streak rule: if buy_signal_streak >= cfg.buy_streak_trigger,
    the effective tier-1 buy threshold is replaced by cfg.streak_buy_threshold
    (default 170) instead of cfg.buy_tier1_ema.
    This makes the strategy more aggressive only after a confirmed streak.
    """
    effective_buy_tier1 = (cfg.streak_buy_threshold
                           if buy_signal_streak >= cfg.buy_streak_trigger
                           else cfg.buy_tier1_ema)

    if signal == "BUY_PRESSURE":
        neutral_streak = 0
        sell_acc = 0
        if current_lots < 0 and abs(ema_t) < cfg.overlap_min_ema:
            return 0, 0, 0, 0
        if ema_t >= cfg.buy_tier2_ema:
            buy_acc = max(buy_acc + 1, 3)
        elif ema_t >= effective_buy_tier1:
            buy_acc = buy_acc + 1
        else:
            buy_acc = 0
        return buy_acc, buy_acc, 0, 0

    elif signal == "SELL_PRESSURE":
        neutral_streak = 0
        buy_acc = 0
        if current_lots > 0 and abs(ema_t) < cfg.overlap_min_ema:
            return 0, 0, 0, 0
        abs_ema = abs(ema_t)
        if abs_ema >= cfg.sell_tier2_ema:
            sell_acc = max(sell_acc + 1, 2)
        elif abs_ema >= cfg.sell_tier1_ema:
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
# Per-ticker strategy loop
# ---------------------------------------------------------------------------

def run_strategy(trader, cfg: TickerConfig, end_time):
    symbol = cfg.symbol
    ensure_csv_headers(cfg)
    cancel_all(trader, symbol)

    # OFI state
    prev_bids       = None
    prev_asks       = None
    ofi_events      = deque()
    raw_ofi_history = deque(maxlen=cfg.persistence_lookback)
    ema_t           = None

    # Target accumulators
    buy_acc          = 0
    sell_acc         = 0
    neutral_streak   = 0
    buy_signal_streak = 0   # consecutive BUY_PRESSURE ticks

    # Exit order state
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

        poll_executions(trader, cfg, tracked_orders, seen_keys)

        # Book parse
        bids, asks = parse_book(trader, symbol, cfg.levels)
        if not bids or not asks:
            time.sleep(cfg.poll_interval)
            continue

        best_bid, _ = bids[0]
        best_ask, _ = asks[0]
        if best_ask <= best_bid:
            time.sleep(cfg.poll_interval)
            continue

        mid    = 0.5 * (best_bid + best_ask)
        spread = best_ask - best_bid

        if prev_bids is None or prev_asks is None:
            prev_bids, prev_asks = bids, asks
            time.sleep(cfg.poll_interval)
            continue

        # OFI / signal
        inc = multilevel_ofi(prev_bids, prev_asks, bids, asks, cfg.levels)
        ofi_events.append((time.time(), inc))
        prune(ofi_events, time.time(), cfg.ofi_window_seconds)

        raw_ofi  = weighted_ofi(rolling_ofi(ofi_events, cfg.levels), cfg.level_weights)
        ema_t    = ema_update(ema_t, raw_ofi, cfg.ema_alpha)
        raw_ofi_history.append(raw_ofi)

        pos_count, neg_count, pscore = persistence_stats(raw_ofi_history)
        ema_dir = ema_direction(ema_t, cfg.ofi_floor)
        signal  = classify_signal(ema_dir, pos_count, neg_count, pscore,
                                  cfg.persistence_required, cfg.final_score_threshold)

        # Update buy streak counter BEFORE passing to accumulator
        if signal == "BUY_PRESSURE":
            buy_signal_streak += 1
        else:
            buy_signal_streak = 0

        # Position
        pos_shares = get_pos(trader, symbol)
        pos_lots   = pos_shares // cfg.lot_size

        if pos_lots != last_known_pos:
            print(f"[{symbol}][FILL CONFIRMED] Pos {last_known_pos:+d} → {pos_lots:+d}",
                  flush=True)
            last_known_pos = pos_lots
            market_oid     = None

        # Target accumulator
        if market_oid is None:
            target_lots, buy_acc, sell_acc, neutral_streak = update_target_accumulator(
                signal, ema_t, pos_lots, buy_acc, sell_acc, neutral_streak,
                cfg, buy_signal_streak
            )
        else:
            # Order in flight — freeze accumulators
            if signal == "BUY_PRESSURE":
                target_lots = buy_acc
            elif signal == "SELL_PRESSURE":
                target_lots = -sell_acc
            else:
                neutral_streak += 1
                if neutral_streak >= 2:
                    target_lots = 0
                    buy_acc = 0; sell_acc = 0
                else:
                    target_lots = buy_acc if buy_acc > 0 else -sell_acc
            print(f"[{symbol}][WAITING] in-flight | pos={pos_lots:+d} "
                  f"target={target_lots:+d} acc=B{buy_acc}/S{sell_acc}", flush=True)

        delta_lots = target_lots - pos_lots

        # Classify action
        is_exit  = (target_lots == 0 and pos_lots != 0)
        is_entry = (target_lots != 0 and pos_lots == 0)
        is_flip  = (target_lots != 0 and pos_lots != 0
                    and (target_lots > 0) != (pos_lots > 0))
        is_add   = (target_lots != 0 and pos_lots != 0
                    and (target_lots > 0) == (pos_lots > 0)
                    and abs(target_lots) > abs(pos_lots))

        # Check exit order status
        if exit_oid:
            cur = trader.get_order(exit_oid)
            if cur is None:
                exit_oid = None; exit_side = None; exit_lots = 0
            else:
                s = str(getattr(cur, "status", ""))
                exec_sz = int(getattr(cur, "executed_size", 0))
                if ("FILLED" in s or "CANCELED" in s or "REJECTED" in s
                        or exec_sz >= exit_lots * cfg.lot_size):
                    exit_oid = None; exit_side = None; exit_lots = 0

        # Order management
        if delta_lots == 0:
            if exit_oid:
                cancel_all(trader, symbol)
                exit_oid = None; exit_side = None; exit_lots = 0

        elif is_exit:
            order_side  = "BUY" if delta_lots > 0 else "SELL"
            order_lots  = abs(delta_lots)
            order_price = sanitise_price(round_to_tick(mid, cfg.tick_size))

            same_exit      = (exit_oid is not None
                              and exit_side == order_side
                              and exit_lots == order_lots)
            age            = time.time() - exit_submit_ts
            should_reprice = (not same_exit) or (age >= cfg.exit_reprice_seconds)

            if should_reprice:
                cancel_all(trader, symbol)
                exit_oid   = None
                pos_shares = get_pos(trader, symbol)
                pos_lots   = pos_shares // cfg.lot_size
                order_lots = abs(target_lots - pos_lots)
                if order_lots > 0:
                    reason   = (f"EXIT target=0 cur={pos_lots} "
                                f"sig={signal} ema={ema_t:.1f}")
                    exit_oid = submit_limit_order(
                        trader, cfg, order_side, order_lots, order_price,
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
                print(f"[{symbol}][SKIP] market order still in flight", flush=True)
            elif has_enough_bp(trader, cfg, order_side, order_lots, mid, pos_shares):
                if exit_oid:
                    cancel_all(trader, symbol)
                    exit_oid = None; exit_side = None; exit_lots = 0

                action = ("ENTRY" if is_entry else "FLIP" if is_flip else "ADD")
                streak_tag = (f" [STREAK={buy_signal_streak}→tier1={cfg.streak_buy_threshold}]"
                              if buy_signal_streak >= cfg.buy_streak_trigger else "")
                reason = (f"MKT-{action}{streak_tag} target={target_lots} cur={pos_lots} "
                          f"sig={signal} ema={ema_t:.1f}")
                oid = submit_market_order(
                    trader, cfg, order_side, order_lots,
                    reason, step, tracked_orders, pos_shares
                )
                if oid:
                    market_oid = oid

        streak_indicator = (f"*STREAK{buy_signal_streak}*"
                            if buy_signal_streak >= cfg.buy_streak_trigger else
                            f"streak={buy_signal_streak}")
        print(
            f"[{sim_time}][{symbol}] "
            f"Sig:{signal:14s} | EMA:{ema_t:8.2f} | "
            f"Pos:{pos_lots:+3d}L | Tgt:{target_lots:+3d}L | "
            f"Δ:{delta_lots:+3d}L | Acc:B{buy_acc}/S{sell_acc} | "
            f"Mid:{mid:.4f} | {streak_indicator} | "
            f"Exit:{'YES' if exit_oid else 'no':3s} | BP:{get_bp(trader):.0f}",
            flush=True
        )

        prev_bids = bids
        prev_asks = asks
        step += 1

        time.sleep(cfg.poll_interval)

    # Shutdown
    poll_executions(trader, cfg, tracked_orders, seen_keys)
    cancel_all(trader, symbol)
    print(f"[{symbol}] Strategy finished. Final pos: {get_pos(trader, symbol)} shares",
          flush=True)


# ---------------------------------------------------------------------------
# Main — spins one thread per ticker
# ---------------------------------------------------------------------------

def main(trader):
    current   = trader.get_last_trade_time()
    end_time  = current + timedelta(minutes=RUN_MINUTES)

    threads = []
    for symbol in TICKERS:
        cfg = TICKER_CONFIGS[symbol]
        t   = Thread(
            target=run_strategy,
            args=(trader, cfg, end_time),
            name=f"strategy-{symbol}",
            daemon=True,
        )
        threads.append(t)

    print(f"Starting {len(threads)} strategy threads: {TICKERS}", flush=True)
    for t in threads:
        t.start()
        time.sleep(1.0)   # stagger to avoid burst on connect

    for t in threads:
        t.join()

    print("All strategy threads finished.", flush=True)
    print(f"Final BP: {trader.get_portfolio_summary().get_total_bp():.2f}", flush=True)
    print(f"Final P&L: {trader.get_portfolio_summary().get_total_realized_pl():.2f}",
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
        try:
            main(trader)
        except KeyboardInterrupt:
            trader.disconnect()