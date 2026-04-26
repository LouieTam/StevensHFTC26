import shift
import time
import csv
import os
from datetime import datetime, timedelta
from collections import deque
import statistics

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
SYMBOLS            = ["CS1", "CS2"]
TICK               = 0.01
LOT_SIZE           = 100
QUOTE_LOTS         = 3
MIN_SPREAD         = 0.04
MAX_SPREAD         = 20.0
CYCLE_SECONDS      = 2
POLL_INTERVAL      = 0.2
PNL_INTERVAL       = 1
MAX_POSITION_LOTS  = 40
SKEW_MULT          = 0.4
ARB_INTERVAL       = 0.5

MAX_BUYING_POWER   = 1_000_000

ZSCORE_WINDOW      = 40
ZSCORE_THRESHOLD   = 5.0
MM_PAUSE_SECONDS   = 3.0

LOG_PATH     = "zi_mm_log_r10.csv"
PNL_LOG_PATH = "zi_mm_pnl_r10.csv"

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def ensure_log():
    if not os.path.exists(LOG_PATH):
        with open(LOG_PATH, "w", newline="") as f:
            csv.writer(f).writerow([
                "wall_time", "sim_time", "symbol", "event",
                "side", "price", "lots", "detail",
            ])

def ensure_pnl_log():
    if not os.path.exists(PNL_LOG_PATH):
        with open(PNL_LOG_PATH, "w", newline="") as f:
            csv.writer(f).writerow([
                "wall_time", "sim_time",
                "cs1_pos_lots", "cs1_unrealized_pl",
                "cs2_pos_lots", "cs2_unrealized_pl",
                "total_realized_pl", "total_unrealized_pl", "total_pl",
            ])

def snapshot_pnl(trader, sim_time, pnl_writer):
    wall    = datetime.now().strftime("%H:%M:%S")
    total_u = 0.0
    row     = {"wall_time": wall, "sim_time": str(sim_time)}
    for sym in SYMBOLS:
        pos_lots   = get_pos(trader, sym)
        unrealized = float(trader.get_unrealized_pl(sym))
        row[f"{sym.lower()}_pos_lots"]      = pos_lots
        row[f"{sym.lower()}_unrealized_pl"] = round(unrealized, 4)
        total_u += unrealized
    total_realized = float(
        trader.get_portfolio_summary().get_total_realized_pl()
    )
    row["total_realized_pl"]   = round(total_realized, 4)
    row["total_unrealized_pl"] = round(total_u, 4)
    row["total_pl"]            = round(total_realized + total_u, 4)
    pnl_writer.writerow(row)

def log(sim_time, symbol, event, side="", price="", lots="", detail=""):
    wall = datetime.now().strftime("%H:%M:%S")
    with open(LOG_PATH, "a", newline="") as f:
        csv.writer(f).writerow([
            wall, sim_time, symbol, event, side, price, lots, detail,
        ])
    print(
        f"[{wall}][{sim_time}][{symbol}] {event:18s} | "
        f"side={side:4s} px={str(price):8s} lots={str(lots):3s} | {detail}",
        flush=True
    )

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def round_tick(x):
    return round(round(x / TICK) * TICK, 2)

def get_pos(trader, symbol):
    item = trader.get_portfolio_item(symbol)
    return (int(item.get_long_shares())
            - int(item.get_short_shares())) // LOT_SIZE

def get_best(trader, symbol):
    try:
        bo = trader.get_order_book(symbol, shift.OrderBookType.LOCAL_BID)
        ao = trader.get_order_book(symbol, shift.OrderBookType.LOCAL_ASK)
        if bo and ao and bo[0].price > 0 and ao[0].price > 0:
            return float(bo[0].price), float(ao[0].price)
    except Exception:
        pass
    return None, None

def get_book_levels(trader, symbol, levels=5):
    try:
        bids = trader.get_order_book(symbol, shift.OrderBookType.LOCAL_BID, levels)
        asks = trader.get_order_book(symbol, shift.OrderBookType.LOCAL_ASK, levels)
        return bids or [], asks or []
    except Exception:
        return [], []

def get_waiting_for(trader, symbol):
    return [o for o in trader.get_waiting_list() if o.symbol == symbol]

def cancel_order(trader, order):
    try:
        trader.submit_cancellation(order)
    except Exception as e:
        print(f"[CANCEL ERROR] {e}", flush=True)

def compute_skewed_quotes(best_bid, best_ask, pos_lots):
    spread    = best_ask - best_bid
    inv_ratio = max(-1.0, min(1.0, pos_lots / MAX_POSITION_LOTS))
    skew      = inv_ratio * spread * SKEW_MULT
    my_bid    = round_tick((best_bid + TICK) - skew)
    my_ask    = round_tick((best_ask - TICK) - skew)
    if my_bid >= best_ask:
        my_bid = round_tick(best_ask - TICK)
    if my_ask <= best_bid:
        my_ask = round_tick(best_bid + TICK)
    if my_bid >= my_ask:
        return None, None, skew, inv_ratio
    return my_bid, my_ask, skew, inv_ratio

# ---------------------------------------------------------------------------
# Z-score price filter
# ---------------------------------------------------------------------------

class PriceFilter:
    """
    Maintains a rolling window of up to ZSCORE_WINDOW valid mid prices.
    Samples only when both sides non-empty and spread <= MAX_SPREAD.
    Anomalous prices (> ZSCORE_THRESHOLD SDs) are discarded and reset
    the MM pause timer. Each new anomaly resets the timer.
    mm_allowed() returns True only when window is full and not paused.
    """
    def __init__(self, symbol):
        self.symbol         = symbol
        self.window         = deque(maxlen=ZSCORE_WINDOW)
        self.mm_pause_until = 0.0

    def update(self, sim_time, bid, ask):
        if bid is None or ask is None:
            return 'skipped'
        spread = ask - bid
        if spread > MAX_SPREAD or spread < 0:
            return 'skipped'

        mid = (bid + ask) / 2.0

        if len(self.window) < ZSCORE_WINDOW:
            self.window.append(mid)
            return 'ok'

        mean = statistics.mean(self.window)
        std  = statistics.stdev(self.window)

        if std < 1e-9:
            self.window.append(mid)
            return 'ok'

        z = abs(mid - mean) / std
        if z > ZSCORE_THRESHOLD:
            self.mm_pause_until = time.time() + MM_PAUSE_SECONDS
            log(sim_time, self.symbol, "PRICE_ANOMALY", "",
                round(mid, 4), "",
                f"z={z:.2f} mean={mean:.4f} std={std:.4f} — MM paused {MM_PAUSE_SECONDS}s")
            return 'anomaly'

        self.window.append(mid)
        return 'ok'

    def mm_allowed(self):
        if len(self.window) < ZSCORE_WINDOW:
            return False
        if time.time() < self.mm_pause_until:
            return False
        return True

# ---------------------------------------------------------------------------
# Crossed-book arbitrage
# ---------------------------------------------------------------------------

class CrossedBookArb:
    def __init__(self, symbol):
        self.symbol      = symbol
        self.last_arb_ts = 0.0
        self.pending     = {}

    def _check_pending(self, trader, sim_time):
        done = []
        for oid, info in self.pending.items():
            try:
                order = trader.get_order(oid)
                if order.status == shift.Order.Status.FILLED:
                    log(sim_time, self.symbol, "ARB_FILLED", info['side'],
                        round(order.executed_price, 4), order.executed_size,
                        "fully filled")
                    done.append(oid)
                elif order.status == shift.Order.Status.PARTIALLY_FILLED:
                    log(sim_time, self.symbol, "ARB_PARTIAL", info['side'],
                        round(order.executed_price, 4), order.executed_size,
                        "partially filled — leaving open")
            except Exception as e:
                print(f"[ARB PENDING ERROR] {e}", flush=True)
                done.append(oid)
        for oid in done:
            del self.pending[oid]

    def tick(self, trader, sim_time):
        now = time.time()
        if now - self.last_arb_ts < ARB_INTERVAL:
            return
        self.last_arb_ts = now

        self._check_pending(trader, sim_time)

        bids, asks = get_book_levels(trader, self.symbol)
        if not bids or not asks:
            return

        best_bid = float(bids[0].price)
        best_ask = float(asks[0].price)

        if best_bid <= best_ask:
            return

        agg_bid = sum(int(b.size) for b in bids if float(b.price) > best_ask)
        agg_ask = sum(int(a.size) for a in asks if float(a.price) < best_bid)

        qty = min(agg_bid, agg_ask)
        if qty <= 0:
            return

        max_lots = int(MAX_BUYING_POWER / (best_ask * LOT_SIZE))
        qty = min(qty, max(1, max_lots))

        log(sim_time, self.symbol, "ARB_DETECT", "",
            f"{best_bid}/{best_ask}", qty,
            f"agg_bid={agg_bid}L agg_ask={agg_ask}L submitting={qty}L")

        buy_ord = shift.Order(shift.Order.Type.MARKET_BUY, self.symbol, int(qty))
        trader.submit_order(buy_ord)
        self.pending[buy_ord.id] = {'side': 'BUY', 'qty': qty}

        sell_ord = shift.Order(shift.Order.Type.MARKET_SELL, self.symbol, int(qty))
        trader.submit_order(sell_ord)
        self.pending[sell_ord.id] = {'side': 'SELL', 'qty': qty}

        log(sim_time, self.symbol, "ARB_SUBMIT", "BOTH", "", qty,
            f"buy_id={buy_ord.id[:8]} sell_id={sell_ord.id[:8]}")

# ---------------------------------------------------------------------------
# Per-ticker market making
# ---------------------------------------------------------------------------

class TickerMM:
    def __init__(self, symbol):
        self.symbol        = symbol
        self.bid_oid       = None
        self.bid_price     = None
        self.ask_oid       = None
        self.ask_price     = None
        self.last_cycle    = 0.0
        self.ext_ask_stage = 0  # 0=unsent, 1=batch1 sent, 2=all sent
        self.ext_bid_stage = 0

    def _get_my_waiting(self, trader):
        return {o.id: o for o in get_waiting_for(trader, self.symbol)}

    def _cancel_if_outside_spread(self, trader, sim_time, bid, ask):
        waiting = self._get_my_waiting(trader)
        for oid, price, side_label in [
            (self.bid_oid, self.bid_price, "BID"),
            (self.ask_oid, self.ask_price, "ASK"),
        ]:
            if oid is None:
                continue
            if oid not in waiting:
                log(sim_time, self.symbol, "FILLED", side_label, price, "",
                    "order no longer in waiting list")
                if side_label == "BID":
                    self.bid_oid = None; self.bid_price = None
                else:
                    self.ask_oid = None; self.ask_price = None
                continue
            inside = bid < price < ask
            if not inside:
                cancel_order(trader, waiting[oid])
                log(sim_time, self.symbol, "CANCEL", side_label, price, "",
                    f"outside spread bid={bid} ask={ask}")
                if side_label == "BID":
                    self.bid_oid = None; self.bid_price = None
                else:
                    self.ask_oid = None; self.ask_price = None

    def _cancel_all_mm_orders(self, trader, sim_time):
        waiting = self._get_my_waiting(trader)
        for oid, price, side_label in [
            (self.bid_oid, self.bid_price, "BID"),
            (self.ask_oid, self.ask_price, "ASK"),
        ]:
            if oid is None:
                continue
            if oid in waiting:
                cancel_order(trader, waiting[oid])
                log(sim_time, self.symbol, "CANCEL", side_label, price, "",
                    "MM suspended — cancelling resting quote")
            if side_label == "BID":
                self.bid_oid = None; self.bid_price = None
            else:
                self.ask_oid = None; self.ask_price = None

    def _handle_empty_sides(self, trader, sim_time, bid_empty, ask_empty):
        """
        Submit tiered extreme orders in 2 batches per cycle to avoid
        hitting the order submission rate limit.

        Batch 1 (stage 0→1): tier[0] + 3 fixed prices = 4 orders
        Batch 2 (stage 1→2): tier[1] + tier[2]        = 2 orders
        Stage 2: nothing more to send until side repopulates (resets to 0)

        Ask side empty → layered sells:
          - pos > 0: position-weighted sizes at bid+50, bid+100, bid+200
          - pos <= 0: QUOTE_LOTS at bid+50, bid+100, bid+200
          - fixed: 3 lots at 400, 500, 600

        Bid side empty → layered buys:
          - pos < 0: position-weighted sizes at ask-50, ask-100, ask-200
          - pos >= 0: QUOTE_LOTS at ask-50, ask-100, ask-200
          - fixed: 3 lots at 1, 2, 3
          - all bid prices floored at 0.1
        """
        pos = get_pos(trader, self.symbol)

        # ── Ask side empty ─────────────────────────────────────────────────
        if ask_empty:
            bids, _ = get_book_levels(trader, self.symbol, levels=1)
            ref_bid = (float(bids[0].price)
                       if bids and float(bids[0].price) > 0 else 100.0)

            if pos > 0:
                tier_sizes = [
                    max(1, round(0.5 * pos)),
                    max(1, round(0.3 * pos)),
                    max(1, round(0.2 * pos)),
                ]
            else:
                tier_sizes = [QUOTE_LOTS, QUOTE_LOTS, QUOTE_LOTS]

            tier_prices = [
                round_tick(ref_bid + 50),
                round_tick(ref_bid + 100),
                round_tick(ref_bid + 200),
            ]

            if self.ext_ask_stage == 0:
                # Batch 1: tier[0] + fixed 400/500/600 = 4 orders
                order = shift.Order(shift.Order.Type.LIMIT_SELL,
                                    self.symbol, tier_sizes[0], tier_prices[0])
                trader.submit_order(order)
                log(sim_time, self.symbol, "EXTREME_ASK", "SELL",
                    tier_prices[0], tier_sizes[0],
                    f"ask empty batch1 tier — ref_bid={ref_bid} pos={pos:+d}L")

                for px in [400.0, 500.0, 600.0]:
                    order = shift.Order(shift.Order.Type.LIMIT_SELL,
                                        self.symbol, QUOTE_LOTS, px)
                    trader.submit_order(order)
                    log(sim_time, self.symbol, "EXTREME_ASK", "SELL",
                        px, QUOTE_LOTS,
                        f"ask empty batch1 fixed — pos={pos:+d}L")

                self.ext_ask_stage = 1

            elif self.ext_ask_stage == 1:
                # Batch 2: tier[1] + tier[2] = 2 orders
                for px, qty in zip(tier_prices[1:], tier_sizes[1:]):
                    order = shift.Order(shift.Order.Type.LIMIT_SELL,
                                        self.symbol, qty, px)
                    trader.submit_order(order)
                    log(sim_time, self.symbol, "EXTREME_ASK", "SELL", px, qty,
                        f"ask empty batch2 tier — ref_bid={ref_bid} pos={pos:+d}L")

                self.ext_ask_stage = 2

            # stage == 2: nothing more to send

        else:
            self.ext_ask_stage = 0  # reset when ask side repopulates

        # ── Bid side empty ─────────────────────────────────────────────────
        if bid_empty:
            _, asks = get_book_levels(trader, self.symbol, levels=1)
            ref_ask = (float(asks[0].price)
                       if asks and float(asks[0].price) > 0 else 100.0)

            if pos < 0:
                tier_sizes = [
                    max(1, round(0.5 * abs(pos))),
                    max(1, round(0.3 * abs(pos))),
                    max(1, round(0.2 * abs(pos))),
                ]
            else:
                tier_sizes = [QUOTE_LOTS, QUOTE_LOTS, QUOTE_LOTS]

            tier_prices = [
                max(0.1, round_tick(ref_ask - 50)),
                max(0.1, round_tick(ref_ask - 100)),
                max(0.1, round_tick(ref_ask - 200)),
            ]

            if self.ext_bid_stage == 0:
                # Batch 1: tier[0] + fixed 1/2/3 = 4 orders
                order = shift.Order(shift.Order.Type.LIMIT_BUY,
                                    self.symbol, tier_sizes[0], tier_prices[0])
                trader.submit_order(order)
                log(sim_time, self.symbol, "EXTREME_BID", "BUY",
                    tier_prices[0], tier_sizes[0],
                    f"bid empty batch1 tier — ref_ask={ref_ask} pos={pos:+d}L")

                for px in [1.0, 2.0, 3.0]:
                    order = shift.Order(shift.Order.Type.LIMIT_BUY,
                                        self.symbol, QUOTE_LOTS, px)
                    trader.submit_order(order)
                    log(sim_time, self.symbol, "EXTREME_BID", "BUY",
                        px, QUOTE_LOTS,
                        f"bid empty batch1 fixed — pos={pos:+d}L")

                self.ext_bid_stage = 1

            elif self.ext_bid_stage == 1:
                # Batch 2: tier[1] + tier[2] = 2 orders
                for px, qty in zip(tier_prices[1:], tier_sizes[1:]):
                    order = shift.Order(shift.Order.Type.LIMIT_BUY,
                                        self.symbol, qty, px)
                    trader.submit_order(order)
                    log(sim_time, self.symbol, "EXTREME_BID", "BUY", px, qty,
                        f"bid empty batch2 tier — ref_ask={ref_ask} pos={pos:+d}L")

                self.ext_bid_stage = 2

            # stage == 2: nothing more to send

        else:
            self.ext_bid_stage = 0  # reset when bid side repopulates

    def tick(self, trader, sim_time, price_filter):
        now = time.time()
        if now - self.last_cycle < CYCLE_SECONDS:
            return
        self.last_cycle = now

        # ── Step 1: check book state ───────────────────────────────────────
        bids, asks = get_book_levels(trader, self.symbol, levels=1)
        bid_empty  = not bids or float(bids[0].price) <= 0
        ask_empty  = not asks or float(asks[0].price) <= 0

        # ── Step 2: handle empty sides (always runs) ──────────────────────
        self._handle_empty_sides(trader, sim_time, bid_empty, ask_empty)

        if bid_empty and ask_empty:
            return

        # ── Step 3: update price filter ───────────────────────────────────
        bid, ask = get_best(trader, self.symbol)
        price_filter.update(sim_time, bid, ask)

        # ── Step 4: check if MM is allowed ────────────────────────────────
        if not price_filter.mm_allowed():
            reason = ("warming up" if len(price_filter.window) < ZSCORE_WINDOW
                      else "price anomaly pause")
            log(sim_time, self.symbol, "MM_SUSPENDED", detail=reason)
            self._cancel_all_mm_orders(trader, sim_time)
            return

        # ── Step 5: cancel normal legs outside spread ─────────────────────
        if bid is None or ask is None:
            log(sim_time, self.symbol, "NO_BOOK", detail="skipping MM cycle")
            return

        self._cancel_if_outside_spread(trader, sim_time, bid, ask)

        # ── Step 6: refresh book after cancels ────────────────────────────
        bid, ask = get_best(trader, self.symbol)
        if bid is None or ask is None:
            return
        spread = ask - bid

        # ── Step 7: check entry condition ─────────────────────────────────
        if spread < MIN_SPREAD:
            log(sim_time, self.symbol, "SPREAD_TIGHT",
                detail=f"spread={spread:.4f} < {MIN_SPREAD} — waiting")
            return

        # ── Step 8: compute skewed quotes ─────────────────────────────────
        pos = get_pos(trader, self.symbol)
        my_bid, my_ask, skew, inv_ratio = compute_skewed_quotes(bid, ask, pos)

        if my_bid is None:
            log(sim_time, self.symbol, "QUOTE_CROSSED",
                detail=f"skewed quotes crossed — skipping "
                       f"pos={pos:+d}L inv_ratio={inv_ratio:.2f} skew={skew:.4f}")
            return

        # ── Step 9: submit missing bid leg ────────────────────────────────
        if self.bid_oid is None and not bid_empty:
            order = shift.Order(shift.Order.Type.LIMIT_BUY,
                                self.symbol, QUOTE_LOTS, my_bid)
            trader.submit_order(order)
            time.sleep(1.0)
            self.bid_oid   = order.id
            self.bid_price = my_bid
            log(sim_time, self.symbol, "SUBMIT", "BUY", my_bid, QUOTE_LOTS,
                f"bid={my_bid} spread={spread:.4f} pos={pos:+d}L "
                f"inv_ratio={inv_ratio:.2f} skew={skew:.4f}")

        # ── Step 10: submit missing ask leg ───────────────────────────────
        if self.ask_oid is None and not ask_empty:
            order = shift.Order(shift.Order.Type.LIMIT_SELL,
                                self.symbol, QUOTE_LOTS, my_ask)
            trader.submit_order(order)
            time.sleep(1.0)
            self.ask_oid   = order.id
            self.ask_price = my_ask
            log(sim_time, self.symbol, "SUBMIT", "SELL", my_ask, QUOTE_LOTS,
                f"ask={my_ask} spread={spread:.4f} pos={pos:+d}L "
                f"inv_ratio={inv_ratio:.2f} skew={skew:.4f}")

        log(sim_time, self.symbol, "STATUS",
            detail=f"bid={my_bid}({'REST' if self.bid_oid else 'NONE'}) "
                   f"ask={my_ask}({'REST' if self.ask_oid else 'NONE'}) "
                   f"spread={spread:.4f} pos={pos:+d}L "
                   f"inv_ratio={inv_ratio:.2f} skew={skew:.4f} "
                   f"filter_n={len(price_filter.window)}")

# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

PNL_FIELDS = [
    "wall_time", "sim_time",
    "cs1_pos_lots", "cs1_unrealized_pl",
    "cs2_pos_lots", "cs2_unrealized_pl",
    "total_realized_pl", "total_unrealized_pl", "total_pl",
]

def run(trader, end_time):
    ensure_log()
    ensure_pnl_log()
    machines = {sym: TickerMM(sym)       for sym in SYMBOLS}
    arbs     = {sym: CrossedBookArb(sym) for sym in SYMBOLS}
    filters  = {sym: PriceFilter(sym)    for sym in SYMBOLS}
    last_pnl_ts = 0.0

    print(f"[ZI MM] Starting | tickers={SYMBOLS} | end={end_time}", flush=True)

    pnl_file   = open(PNL_LOG_PATH, "a", newline="")
    pnl_writer = csv.DictWriter(pnl_file, fieldnames=PNL_FIELDS)

    try:
        while trader.get_last_trade_time() < end_time:
            sim_time = trader.get_last_trade_time()

            for sym in SYMBOLS:
                # Arb — every 0.5s, gated internally
                try:
                    arbs[sym].tick(trader, sim_time)
                except Exception as e:
                    print(f"[ARB ERROR][{sym}] {e}", flush=True)

                # MM — every 2s, gated internally
                try:
                    machines[sym].tick(trader, sim_time, filters[sym])
                except Exception as e:
                    print(f"[MM ERROR][{sym}] {e}", flush=True)

            now = time.time()
            if now - last_pnl_ts >= PNL_INTERVAL:
                last_pnl_ts = now
                try:
                    snapshot_pnl(trader, sim_time, pnl_writer)
                    pnl_file.flush()
                except Exception as e:
                    print(f"[PNL ERROR] {e}", flush=True)

            time.sleep(POLL_INTERVAL)

    except KeyboardInterrupt:
        print("\n[ZI MM] KeyboardInterrupt — shutting down cleanly", flush=True)

    finally:
        print("[ZI MM] Cancelling all resting orders...", flush=True)
        for sym in SYMBOLS:
            for o in get_waiting_for(trader, sym):
                cancel_order(trader, o)
            time.sleep(0.5)
        try:
            snapshot_pnl(trader, trader.get_last_trade_time(), pnl_writer)
            pnl_file.flush()
        except Exception:
            pass
        pnl_file.close()
        print(f"[ZI MM] PnL log closed → {PNL_LOG_PATH}", flush=True)
        for sym in SYMBOLS:
            pos = get_pos(trader, sym)
            print(f"[SHUTDOWN] {sym} final pos: {pos:+d}L", flush=True)
        print("[ZI MM] Done.", flush=True)

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    with shift.Trader("columbia-traders") as trader:
        trader.connect("initiator.cfg", "aRkkZSrj")
        time.sleep(1.0)
        trader.sub_all_order_book()
        print("wait 20 sec")
        time.sleep(20.0)
        end_time = (trader.get_last_trade_time()
                    + timedelta(minutes=390.0))
        run(trader, end_time)