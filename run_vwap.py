import shift
import time
import math
import csv
import os
import statistics
from collections import deque
from datetime import datetime, timedelta

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
SYMBOL          = "JPM"
POLL_INTERVAL   = 1.0          # seconds per tick
WARMUP_MINUTES  = 20           # do nothing for first N sim-minutes
MAX_HOLD_MINUTES = 60          # time-based exit if position not closed
ENTRY_AGGR_FRAC = 0.2          # limit entry: mid ± frac*spread
EXIT_REPRICE_TICKS = 3         # reprice exit order every N ticks
ENTRY_REPRICE_TICKS = 2        # reprice entry order every N ticks (give 1 tick grace)
CHUNK_LOTS      = 2            # lots per chunk when BP is insufficient for full exit
LOT_SIZE        = 100
TICK_SIZE       = 0.01

SUBMISSION_LOG = "vwap_band_submissions.csv"
EXECUTION_LOG  = "vwap_band_executions.csv"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def round_to_tick(x):
    return round(round(x / TICK_SIZE, 6) * TICK_SIZE, 2)

def sanitise(p):
    return round(float(p), 2)

def get_pos(trader, symbol):
    item = trader.get_portfolio_item(symbol)
    return int(item.get_long_shares()) - int(item.get_short_shares())

def get_bp(trader):
    return float(trader.get_portfolio_summary().get_total_bp())

def cancel_order(trader, oid):
    """Cancel a specific order by id."""
    try:
        o = trader.get_order(oid)
        if o is not None:
            trader.submit_cancellation(o)
            time.sleep(0.2)
    except Exception as e:
        print(f"[CANCEL ERROR] {oid[:8]}… {e}", flush=True)

def cancel_all(trader, symbol):
    for order in trader.get_waiting_list():
        if order.symbol == symbol:
            trader.submit_cancellation(order)
    time.sleep(1.0)

# ---------------------------------------------------------------------------
# CSV logging
# ---------------------------------------------------------------------------

def ensure_csv_headers():
    if not os.path.exists(SUBMISSION_LOG):
        with open(SUBMISSION_LOG, "w", newline="") as f:
            csv.writer(f).writerow([
                "sim_time", "order_id", "symbol", "side", "type",
                "price", "lots", "reason", "pos_before",
            ])
    if not os.path.exists(EXECUTION_LOG):
        with open(EXECUTION_LOG, "w", newline="") as f:
            csv.writer(f).writerow([
                "sim_time", "order_id", "symbol", "side",
                "exec_price", "exec_size", "order_size", "status", "exec_ts",
            ])

def log_submission(sim_time, oid, symbol, side, order_type,
                   price, lots, reason, pos_before):
    with open(SUBMISSION_LOG, "a", newline="") as f:
        csv.writer(f).writerow([
            sim_time, oid, symbol, side, order_type,
            f"{float(price):.4f}" if price != "MARKET" else "MARKET",
            lots, reason, pos_before,
        ])
    print(f"[SUBMIT] {side} {lots}L {order_type} "
          f"@ {price if price == 'MARKET' else f'{float(price):.4f}'} "
          f"{symbol} | {reason} | pos_before={pos_before:+d}",
          flush=True)

def log_execution(sim_time, oid, symbol, side,
                  exec_price, exec_size, order_size, status, exec_ts):
    with open(EXECUTION_LOG, "a", newline="") as f:
        csv.writer(f).writerow([
            sim_time, oid, symbol, side,
            f"{float(exec_price):.4f}", exec_size, order_size, status, exec_ts,
        ])
    print(f"[FILL] {side} {exec_size}/{order_size} {symbol} "
          f"@ {float(exec_price):.4f} | status={status} | sim={exec_ts}",
          flush=True)

# ---------------------------------------------------------------------------
# Execution audit  (same poll_executions pattern as OFI strategy)
# ---------------------------------------------------------------------------

def poll_executions(trader, tracked_orders, seen_keys):
    """
    Check fills via get_executed_orders(). Returns number of new fills logged.
    Marks fully settled orders as done so they get cleaned from tracked_orders.
    """
    new_fills = 0
    sim_time  = trader.get_last_trade_time()
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
                        log_execution(sim_time, oid,
                                      getattr(ex, "symbol", meta["symbol"]),
                                      meta["side"], px, sz,
                                      meta["lots"] * LOT_SIZE,
                                      str(getattr(ex, "status", "")), exec_ts)

            cur = trader.get_order(oid)
            if cur is not None:
                s = str(getattr(cur, "status", ""))
                exec_sz = int(getattr(cur, "executed_size", 0))
                if ("FILLED" in s or "CANCELED" in s or "REJECTED" in s
                        or exec_sz >= meta["lots"] * LOT_SIZE):
                    tracked_orders[oid]["done"] = True
        except Exception:
            pass

    for oid in [k for k, v in tracked_orders.items() if v.get("done")]:
        print(f"[ORDER SETTLED] {oid[:8]}… side={tracked_orders[oid]['side']} "
              f"lots={tracked_orders[oid]['lots']} "
              f"status=settled", flush=True)
        del tracked_orders[oid]
    return new_fills

# ---------------------------------------------------------------------------
# Order submission
# ---------------------------------------------------------------------------

def submit_limit(trader, symbol, side, lots, price,
                 reason, tracked_orders, pos_before):
    """Submit a limit order, log it, add to tracked_orders."""
    if lots <= 0:
        return None
    sim_time = trader.get_last_trade_time()
    if side == "BUY":
        order = shift.Order(shift.Order.Type.LIMIT_BUY,
                            symbol, int(lots), float(price))
    else:
        order = shift.Order(shift.Order.Type.LIMIT_SELL,
                            symbol, int(lots), float(price))
    trader.submit_order(order)
    tracked_orders[order.id] = {
        "symbol": symbol, "side": side,
        "lots": int(lots), "done": False,
    }
    log_submission(sim_time, order.id, symbol, side, "LIMIT",
                   price, lots, reason, pos_before)
    return order.id

def submit_market(trader, symbol, side, lots,
                  reason, tracked_orders, pos_before):
    """Submit a market order (used for chunked exit when BP is tight)."""
    if lots <= 0:
        return None
    sim_time = trader.get_last_trade_time()
    if side == "BUY":
        order = shift.Order(shift.Order.Type.MARKET_BUY,  symbol, int(lots))
    else:
        order = shift.Order(shift.Order.Type.MARKET_SELL, symbol, int(lots))
    trader.submit_order(order)
    tracked_orders[order.id] = {
        "symbol": symbol, "side": side,
        "lots": int(lots), "done": False,
    }
    log_submission(sim_time, order.id, symbol, side, "MARKET",
                   "MARKET", lots, reason, pos_before)
    return order.id

# ---------------------------------------------------------------------------
# VWAP + SD tracker
# ---------------------------------------------------------------------------

class VWAPBandTracker:
    """
    Tracks running VWAP and VWAP standard deviation from session start.
    Updates only when price or size changes (new trade detected).
    """
    def __init__(self):
        self.cum_pv   = 0.0
        self.cum_pv2  = 0.0
        self.cum_vol  = 0.0
        self.vwap     = None
        self.sd       = 0.0
        self.prev_price = None
        self.prev_size  = None

    def update(self, price, size):
        if price <= 0 or size <= 0:
            return False
        if self.prev_price is None:
            self.prev_price = price
            self.prev_size  = size
            return False

        price_changed = (price != self.prev_price)
        size_changed  = (size  != self.prev_size)

        if not price_changed and not size_changed:
            return False   # no new trade

        if price_changed:
            trade_vol = float(size)
        else:
            delta = size - self.prev_size
            if delta <= 0:
                self.prev_price = price
                self.prev_size  = size
                return False
            trade_vol = float(delta)

        self.cum_pv   += price * trade_vol
        self.cum_pv2  += price * price * trade_vol
        self.cum_vol  += trade_vol

        self.vwap = self.cum_pv / self.cum_vol
        var = (self.cum_pv2 / self.cum_vol) - (self.vwap ** 2)
        self.sd = var ** 0.5 if var > 0 else 0.0

        self.prev_price = price
        self.prev_size  = size
        return True   # new trade detected

    @property
    def upper_band(self):
        return (self.vwap + self.sd) if self.vwap is not None else None

    @property
    def lower_band(self):
        return (self.vwap - self.sd) if self.vwap is not None else None

    def band_signal(self, mid):
        """
        Returns:
          +1 if mid > upper band  (sell pressure — mean revert down)
          -1 if mid < lower band  (buy pressure  — mean revert up)
           0 if inside bands or bands not ready
        """
        if self.vwap is None or self.sd == 0:
            return 0
        if mid > self.upper_band:
            return +1
        if mid < self.lower_band:
            return -1
        return 0

# ---------------------------------------------------------------------------
# BP check for opening / closing positions
# ---------------------------------------------------------------------------

def can_open(trader, side, lots, price_est, current_pos_shares):
    """Check we have enough BP to open `lots` lots on `side`."""
    bp     = get_bp(trader)
    shares = lots * LOT_SIZE
    if side == "BUY":
        existing_short  = max(-current_pos_shares, 0)
        new_long_shares = max(shares - existing_short, 0)
        required        = price_est * new_long_shares
    else:
        existing_long    = max(current_pos_shares, 0)
        new_short_shares = max(shares - existing_long, 0)
        required         = 2.0 * price_est * new_short_shares
    if bp >= required:
        return True
    print(f"[BP] Cannot open {side} {lots}L — need ${required:.0f}, "
          f"have ${bp:.0f}", flush=True)
    return False

def affordable_exit_lots(trader, side, total_lots, price_est):
    """
    How many lots can we afford to close right now?
    Closing a short = buying back → costs price * shares.
    Closing a long  = selling     → no BP cost.
    Returns the max lots we can do, capped at CHUNK_LOTS if bp is tight.
    """
    if side == "BUY":   # closing a short
        bp       = get_bp(trader)
        shares_per_lot = LOT_SIZE * price_est
        affordable = int(bp // shares_per_lot)
        affordable = max(1, min(affordable, total_lots))
        if affordable < total_lots:
            affordable = min(affordable, CHUNK_LOTS)
    else:               # closing a long — no BP cost
        affordable = total_lots
    return affordable

# ---------------------------------------------------------------------------
# Main strategy loop
# ---------------------------------------------------------------------------

def run_strategy(trader, symbol=SYMBOL, end_time=None):
    ensure_csv_headers()
    cancel_all(trader, symbol)

    vwap_tracker = VWAPBandTracker()

    # ── Order state ───────────────────────────────────────────────────────────
    # Entry: one resting limit order at a time (repriced every other tick)
    entry_oid        = None
    entry_side       = None
    entry_lots       = 1
    entry_price      = None
    entry_ticks_held = 0   # ticks since last entry submission

    # Exit: one consolidated limit order for full position (repriced every 3 ticks)
    exit_oid         = None
    exit_side        = None
    exit_lots        = 0
    exit_ticks_held  = 0   # ticks since last exit submission

    # Chunked exit: when BP is tight, track remaining lots to close
    chunk_pending_lots  = 0    # remaining lots still needing exit orders
    chunk_oid           = None # current chunk order id
    chunk_ticks_held    = 0

    tracked_orders  = {}
    seen_keys       = set()

    # Position & time tracking
    last_known_pos  = 0
    position_opened_at = None   # sim time when we first entered a position
    tick            = 0
    warmup_done     = False
    warmup_end      = None

    print(f"[{symbol}] VWAP band strategy started | end={end_time}", flush=True)
    print(f"[{symbol}] Warming up for {WARMUP_MINUTES} sim-minutes...", flush=True)

    while trader.get_last_trade_time() < end_time:
        sim_time = trader.get_last_trade_time()

        # ── Execution audit ───────────────────────────────────────────────────
        poll_executions(trader, tracked_orders, seen_keys)

        # ── Warmup ───────────────────────────────────────────────────────────
        if not warmup_done:
            if warmup_end is None:
                warmup_end = sim_time + timedelta(minutes=WARMUP_MINUTES)
            if sim_time < warmup_end:
                print(f"[WARMUP] {sim_time} — waiting until {warmup_end}",
                      flush=True)
                time.sleep(POLL_INTERVAL)
                continue
            warmup_done = True
            print(f"[WARMUP DONE] Starting strategy at {sim_time}", flush=True)

        # ── Book / price ──────────────────────────────────────────────────────
        try:
            bo = trader.get_order_book(symbol, shift.OrderBookType.GLOBAL_BID)
            ao = trader.get_order_book(symbol, shift.OrderBookType.GLOBAL_ASK)
            if not bo or not ao or bo[0].price <= 0 or ao[0].price <= 0:
                time.sleep(POLL_INTERVAL)
                continue
            best_bid = float(bo[0].price)
            best_ask = float(ao[0].price)
        except Exception:
            time.sleep(POLL_INTERVAL)
            continue

        mid    = 0.5 * (best_bid + best_ask)
        spread = best_ask - best_bid

        # ── VWAP update ───────────────────────────────────────────────────────
        try:
            last_price = float(trader.get_last_price(symbol))
            last_size  = int(trader.get_last_size(symbol))
        except Exception:
            last_price, last_size = 0.0, 0

        trade_detected = vwap_tracker.update(last_price, last_size)
        vwap      = vwap_tracker.vwap
        upper     = vwap_tracker.upper_band
        lower     = vwap_tracker.lower_band
        band_sig  = vwap_tracker.band_signal(mid)   # +1 sell, -1 buy, 0 inside

        # ── Position ──────────────────────────────────────────────────────────
        pos_shares = get_pos(trader, symbol)
        pos_lots   = pos_shares // LOT_SIZE

        if pos_lots != last_known_pos:
            print(f"[POSITION CHANGE] {last_known_pos:+d}L → {pos_lots:+d}L "
                  f"| sim={sim_time}", flush=True)
            last_known_pos = pos_lots
            if pos_lots != 0 and position_opened_at is None:
                position_opened_at = sim_time
            if pos_lots == 0:
                position_opened_at = None
                print(f"[FLAT] Position closed at {sim_time}", flush=True)

        # ── Max hold time check ───────────────────────────────────────────────
        max_hold_hit = False
        if position_opened_at is not None:
            held_minutes = (sim_time - position_opened_at).total_seconds() / 60
            if held_minutes >= MAX_HOLD_MINUTES:
                max_hold_hit = True
                print(f"[MAX HOLD] {held_minutes:.1f} min >= {MAX_HOLD_MINUTES} min "
                      f"— forcing exit | pos={pos_lots:+d}L", flush=True)

        # ── Current BP ────────────────────────────────────────────────────────
        current_bp = get_bp(trader)

        # =====================================================================
        # ENTRY LOGIC
        # Only add 1 lot per tick when price is outside the band.
        # Skip if max hold hit, or if no bands yet (SD still zero).
        # Reprice entry order every ENTRY_REPRICE_TICKS ticks.
        # =====================================================================
        if (not max_hold_hit
                and vwap is not None
                and vwap_tracker.sd > 0
                and band_sig != 0):

            entry_side = "BUY" if band_sig == -1 else "SELL"
            if entry_side == "BUY":
                entry_price = sanitise(round_to_tick(mid + ENTRY_AGGR_FRAC * spread))
            else:
                entry_price = sanitise(round_to_tick(mid - ENTRY_AGGR_FRAC * spread))

            # Check BP before entering
            if can_open(trader, entry_side, 1, mid, pos_shares):
                entry_ticks_held += 1
                should_reprice = (entry_oid is None
                                  or entry_ticks_held >= ENTRY_REPRICE_TICKS)

                if should_reprice:
                    # Cancel previous entry if resting
                    if entry_oid:
                        cancel_order(trader, entry_oid)
                        entry_oid = None
                        print(f"[ENTRY REPRICE] Cancelled {entry_oid[:8] if entry_oid else 'n/a'}… "
                              f"resubmitting {entry_side} 1L @ {entry_price:.4f}",
                              flush=True)

                    reason = (f"ENTRY band_sig={band_sig:+d} "
                              f"mid={mid:.4f} vwap={vwap:.4f} "
                              f"upper={upper:.4f} lower={lower:.4f}")
                    entry_oid    = submit_limit(
                        trader, symbol, entry_side, 1, entry_price,
                        reason, tracked_orders, pos_shares
                    )
                    entry_ticks_held = 0
        else:
            # Price back inside band or max hold — cancel any pending entry
            if entry_oid:
                cancel_order(trader, entry_oid)
                entry_oid = None
                entry_ticks_held = 0
                print(f"[ENTRY CANCEL] Price back inside band or max hold "
                      f"| mid={mid:.4f} vwap={vwap_str}",
                      flush=True)

        # =====================================================================
        # EXIT LOGIC
        # If we have a position, maintain a limit order at current VWAP.
        # Reprice every EXIT_REPRICE_TICKS ticks.
        # If max hold hit, close at market in chunks if needed.
        # =====================================================================
        if pos_lots != 0:
            close_side = "SELL" if pos_lots > 0 else "BUY"
            abs_pos    = abs(pos_lots)

            if max_hold_hit:
                # ── Force exit: cancel resting exit, close in affordable chunks ─
                if exit_oid:
                    cancel_order(trader, exit_oid)
                    exit_oid = None
                    exit_ticks_held = 0

                # Determine how many lots we can close right now
                lots_to_close = affordable_exit_lots(
                    trader, close_side, abs_pos, mid)
                reason = (f"MAX_HOLD_EXIT pos={pos_lots:+d}L "
                          f"closing {lots_to_close}L")
                if lots_to_close > 0:
                    # Chunk exit: check chunk order status first
                    if chunk_oid is not None:
                        chunk_ticks_held += 1
                        if chunk_ticks_held >= 1:   # wait 1 tick then reprice
                            cur_pos = get_pos(trader, symbol)
                            if cur_pos == pos_lots:
                                # Didn't fill — cancel and reprice
                                cancel_order(trader, chunk_oid)
                                chunk_oid = None
                                chunk_ticks_held = 0
                                print(f"[CHUNK REPRICE] No fill — repricing",
                                      flush=True)
                    if chunk_oid is None:
                        chunk_oid = submit_market(
                            trader, symbol, close_side, lots_to_close,
                            reason, tracked_orders, pos_shares
                        )
                        chunk_ticks_held = 0

            else:
                # ── Normal exit: limit order at current VWAP ─────────────────
                if vwap is not None:
                    exit_price     = sanitise(round_to_tick(vwap))
                    exit_ticks_held += 1
                    should_reprice  = (exit_oid is None
                                       or exit_ticks_held >= EXIT_REPRICE_TICKS)

                    # Also reprice if position size changed (new entry filled)
                    if exit_lots != abs_pos:
                        should_reprice = True

                    if should_reprice:
                        if exit_oid:
                            cancel_order(trader, exit_oid)
                            exit_oid = None
                            print(f"[EXIT REPRICE] Repricing exit "
                                  f"{close_side} {abs_pos}L @ {exit_price:.4f} "
                                  f"(vwap={vwap:.4f})", flush=True)

                        reason = (f"EXIT_VWAP pos={pos_lots:+d}L "
                                  f"vwap={vwap:.4f} "
                                  f"entry_band={'lower' if pos_lots > 0 else 'upper'}")
                        exit_oid   = submit_limit(
                            trader, symbol, close_side, abs_pos, exit_price,
                            reason, tracked_orders, pos_shares
                        )
                        exit_side      = close_side
                        exit_lots      = abs_pos
                        exit_ticks_held = 0

        else:
            # Flat — clear exit state
            if exit_oid:
                cancel_order(trader, exit_oid)
                exit_oid = None
            exit_ticks_held = 0
            exit_lots       = 0
            chunk_oid       = None
            chunk_ticks_held = 0

        # ── Status line ───────────────────────────────────────────────────────
        bands_str = (f"Upper={upper:.4f} Lower={lower:.4f}"
                     if upper is not None else "bands not ready")
        vwap_str  = f"{vwap:.4f}" if vwap is not None else "n/a"
        print(
            f"[{sim_time}][{symbol}] "
            f"Mid={mid:.4f} | VWAP={vwap_str} | "
            f"{bands_str} | "
            f"BandSig={band_sig:+d} | "
            f"Pos={pos_lots:+d}L | "
            f"Entry={'YES '+entry_oid[:8]+'…' if entry_oid else 'no':12s} | "
            f"Exit={'YES '+exit_oid[:8]+'…' if exit_oid else 'no':12s} | "
            f"BP=${current_bp:.0f} | "
            f"Trade={'YES' if trade_detected else 'no'}",
            flush=True
        )

        tick += 1
        time.sleep(POLL_INTERVAL)

    # ── Shutdown ──────────────────────────────────────────────────────────────
    poll_executions(trader, tracked_orders, seen_keys)
    cancel_all(trader, symbol)
    pos_shares = get_pos(trader, symbol)
    if pos_shares != 0:
        print(f"[SHUTDOWN] Non-zero position {pos_shares} shares — closing at market",
              flush=True)
        side = "SELL" if pos_shares > 0 else "BUY"
        lots = abs(pos_shares) // LOT_SIZE
        submit_market(trader, symbol, side, lots,
                      "SHUTDOWN_CLOSE", tracked_orders, pos_shares)
        time.sleep(2.0)
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
        end_time = trader.get_last_trade_time() + timedelta(minutes=380.0)
        try:
            run_strategy(trader, symbol=SYMBOL, end_time=end_time)
        except KeyboardInterrupt:
            cancel_all(trader, SYMBOL)
            trader.disconnect()