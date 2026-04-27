import shift
import time
import csv
import os
from datetime import datetime, timedelta
from collections import deque

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

# Extreme-order trigger: fire when a book side is empty OR its total visible
# volume (summed across all fetched levels) is below this threshold.
THIN_BOOK_LOTS     = 20
# Maximum levels to fetch when computing book volume for thin-book detection.
BOOK_VOL_LEVELS    = 10

# Stability filter: 15-cycle window, all mids must stay within ±50 of the
# reference mid (window[0]).  Empty book or spike resets the window.
STABILITY_WINDOW   = 15
STABILITY_BAND     = 50.0

LOG_PATH     = "rl_mm_log_r11.csv"
PNL_LOG_PATH = "rl_mm_pnl_r11.csv"

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
# Stability filter (replaces z-score PriceFilter)
# ---------------------------------------------------------------------------

class StabilityFilter:
    """
    Maintains a rolling window of STABILITY_WINDOW mid-price samples, one
    per MM cycle (every CYCLE_SECONDS seconds).

    MM is allowed only when:
      - The window is full (STABILITY_WINDOW consecutive valid samples), AND
      - Every sample in the window is within ±STABILITY_BAND of window[0]
        (the reference mid from STABILITY_WINDOW cycles ago).

    Resets (clears the window) when:
      - The book has an empty side  → no valid mid this cycle
      - The new mid falls outside ±STABILITY_BAND of window[0]

    After a reset the next valid non-empty mid starts a fresh 15-cycle count.
    """

    def __init__(self, symbol):
        self.symbol = symbol
        self.window = deque(maxlen=STABILITY_WINDOW)

    def update(self, sim_time, bid, ask):
        """
        Call once per MM cycle with the current best bid/ask.
        Returns one of: 'empty' | 'spike' | 'warming' | 'ok'
        """
        # Empty book: reset and bail
        if bid is None or ask is None:
            if self.window:
                log(sim_time, self.symbol, "STAB_RESET", detail="empty book — window cleared")
                self.window.clear()
            return 'empty'

        spread = ask - bid
        if spread > MAX_SPREAD or spread < 0:
            if self.window:
                log(sim_time, self.symbol, "STAB_RESET", detail=f"bad spread={spread:.4f} — window cleared")
                self.window.clear()
            return 'empty'

        mid = (bid + ask) / 2.0

        # Spike check against reference (oldest sample)
        if self.window:
            ref = self.window[0]
            if abs(mid - ref) > STABILITY_BAND:
                log(sim_time, self.symbol, "STAB_SPIKE",
                    price=round(mid, 4), detail=
                    f"mid={mid:.4f} ref={ref:.4f} delta={abs(mid-ref):.4f} > {STABILITY_BAND} — window cleared")
                self.window.clear()
                # Don't add the spike price; restart from next cycle
                return 'spike'

        self.window.append(mid)

        n = len(self.window)
        if n < STABILITY_WINDOW:
            return 'warming'

        return 'ok'

    def mm_allowed(self):
        """True only when the window is full and all samples passed the band check."""
        return len(self.window) == STABILITY_WINDOW

# ---------------------------------------------------------------------------
# Crossed-book arbitrage (unchanged)
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
# Per-ticker market making (unchanged except filter interface)
# ---------------------------------------------------------------------------

class TickerMM:
    def __init__(self, symbol):
        self.symbol        = symbol
        self.bid_oid       = None
        self.bid_price     = None
        self.ask_oid       = None
        self.ask_price     = None
        self.last_cycle    = 0.0
        self.ext_ask_stage = 0
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

    def _handle_extreme_orders(self, trader, sim_time,
                               bid_needs_extreme, ask_needs_extreme,
                               bid_vol, ask_vol):
        """
        Submit tiered extreme orders when a book side is empty OR thin
        (total volume < THIN_BOOK_LOTS).

        Orders are grouped into batches of exactly 3 to stay within the
        exchange rate limit of 5 orders/second (arb may fire simultaneously).
        Each batch is sent on a separate MM cycle (~2 s apart via ext_*_stage).

        Ask side (stage 0→1→2):
          Batch 1:  tier[0],  fixed@400,  fixed@500       ← 3 orders
          Batch 2:  fixed@600, tier[1],   tier[2]         ← 3 orders

        Bid side (stage 0→1→2):
          Batch 1:  tier[0],  fixed@1,    fixed@2         ← 3 orders
          Batch 2:  fixed@3,  tier[1],    tier[2]         ← 3 orders

        Stage resets to 0 when the book side recovers above THIN_BOOK_LOTS.
        """
        pos = get_pos(trader, self.symbol)
        reason_suffix = lambda vol: (
            "empty" if vol == 0 else f"thin vol={vol}L"
        )

        # ── Ask side ──────────────────────────────────────────────────────────
        if ask_needs_extreme:
            bids, _ = get_book_levels(trader, self.symbol, levels=1)
            ref_bid = (float(bids[0].price)
                       if bids and float(bids[0].price) > 0 else 100.0)

            tier_sizes = (
                [max(1, round(0.5 * pos)),
                 max(1, round(0.3 * pos)),
                 max(1, round(0.2 * pos))]
                if pos > 0 else
                [QUOTE_LOTS, QUOTE_LOTS, QUOTE_LOTS]
            )
            tier_prices = [
                round_tick(ref_bid + 50),
                round_tick(ref_bid + 100),
                round_tick(ref_bid + 200),
            ]
            tag = reason_suffix(ask_vol)

            if self.ext_ask_stage == 0:
                # Batch 1: fixed anchors @400/500/600/700  (4 orders)
                for px in [400.0, 500.0, 600.0, 700.0]:
                    order = shift.Order(shift.Order.Type.LIMIT_SELL,
                                        self.symbol, QUOTE_LOTS, px)
                    trader.submit_order(order)
                    log(sim_time, self.symbol, "EXTREME_ASK", "SELL", px, QUOTE_LOTS,
                        f"ask fixed {tag} batch1 — pos={pos:+d}L")
                self.ext_ask_stage = 1

            elif self.ext_ask_stage == 1:
                # Batch 2: tiered prices near market  (3 orders)
                for px, qty, label in [
                    (tier_prices[0], tier_sizes[0], f"tier0 {tag}"),
                    (tier_prices[1], tier_sizes[1], f"tier1 {tag}"),
                    (tier_prices[2], tier_sizes[2], f"tier2 {tag}"),
                ]:
                    order = shift.Order(shift.Order.Type.LIMIT_SELL,
                                        self.symbol, qty, px)
                    trader.submit_order(order)
                    log(sim_time, self.symbol, "EXTREME_ASK", "SELL", px, qty,
                        f"ask {label} batch2 — ref_bid={ref_bid} pos={pos:+d}L")
                self.ext_ask_stage = 2

            # stage == 2: nothing more to send this cycle

        else:
            self.ext_ask_stage = 0      # book has recovered — reset for next time

        # ── Bid side ──────────────────────────────────────────────────────────
        if bid_needs_extreme:
            _, asks = get_book_levels(trader, self.symbol, levels=1)
            ref_ask = (float(asks[0].price)
                       if asks and float(asks[0].price) > 0 else 100.0)

            tier_sizes = (
                [max(1, round(0.5 * abs(pos))),
                 max(1, round(0.3 * abs(pos))),
                 max(1, round(0.2 * abs(pos)))]
                if pos < 0 else
                [QUOTE_LOTS, QUOTE_LOTS, QUOTE_LOTS]
            )
            tier_prices = [
                max(0.1, round_tick(ref_ask - 50)),
                max(0.1, round_tick(ref_ask - 100)),
                max(0.1, round_tick(ref_ask - 200)),
            ]
            tag = reason_suffix(bid_vol)

            if self.ext_bid_stage == 0:
                # Batch 1: fixed anchors @1/2/3  (3 orders)
                for px in [1.0, 2.0, 3.0]:
                    order = shift.Order(shift.Order.Type.LIMIT_BUY,
                                        self.symbol, QUOTE_LOTS, px)
                    trader.submit_order(order)
                    log(sim_time, self.symbol, "EXTREME_BID", "BUY", px, QUOTE_LOTS,
                        f"bid fixed {tag} batch1 — pos={pos:+d}L")
                self.ext_bid_stage = 1

            elif self.ext_bid_stage == 1:
                # Batch 2: tiered prices near market  (3 orders)
                for px, qty, label in [
                    (tier_prices[0], tier_sizes[0], f"tier0 {tag}"),
                    (tier_prices[1], tier_sizes[1], f"tier1 {tag}"),
                    (tier_prices[2], tier_sizes[2], f"tier2 {tag}"),
                ]:
                    order = shift.Order(shift.Order.Type.LIMIT_BUY,
                                        self.symbol, qty, px)
                    trader.submit_order(order)
                    log(sim_time, self.symbol, "EXTREME_BID", "BUY", px, qty,
                        f"bid {label} batch2 — ref_ask={ref_ask} pos={pos:+d}L")
                self.ext_bid_stage = 2

            # stage == 2: nothing more to send this cycle

        else:
            self.ext_bid_stage = 0      # book has recovered — reset for next time

    def tick(self, trader, sim_time, stab_filter):
        now = time.time()
        if now - self.last_cycle < CYCLE_SECONDS:
            return
        self.last_cycle = now

        # ── Step 1: fetch book with enough levels to measure total volume ──
        bids, asks = get_book_levels(trader, self.symbol, levels=BOOK_VOL_LEVELS)
        bid_empty  = not bids or float(bids[0].price) <= 0
        ask_empty  = not asks or float(asks[0].price) <= 0

        bid_vol = sum(int(b.size) for b in bids) if not bid_empty else 0
        ask_vol = sum(int(a.size) for a in asks) if not ask_empty else 0

        # Trigger extreme orders when a side is absent OR volume is thin
        bid_needs_extreme = bid_empty or bid_vol < THIN_BOOK_LOTS
        ask_needs_extreme = ask_empty or ask_vol < THIN_BOOK_LOTS

        # ── Step 2: handle extreme orders (always runs) ────────────────────
        self._handle_extreme_orders(trader, sim_time,
                                    bid_needs_extreme, ask_needs_extreme,
                                    bid_vol, ask_vol)

        if bid_empty and ask_empty:
            # no prices at all — update filter so it registers the gap
            stab_filter.update(sim_time, None, None)
            return

        # ── Step 3: update stability filter ───────────────────────────────
        bid, ask = get_best(trader, self.symbol)
        status = stab_filter.update(sim_time, bid, ask)

        # ── Step 4: check if MM is allowed ────────────────────────────────
        if not stab_filter.mm_allowed():
            n = len(stab_filter.window)
            if status == 'spike':
                reason = "price spike — window reset"
            elif status == 'empty':
                reason = "empty book — window reset"
            else:
                reason = f"warming up ({n}/{STABILITY_WINDOW} cycles)"
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
        # Only quote when the bid side has at least a best price to anchor to.
        # (Thin is fine — extreme orders are already covering the slack.)
        ref = stab_filter.window[0]
        if self.bid_oid is None and not bid_empty:
            order = shift.Order(shift.Order.Type.LIMIT_BUY,
                                self.symbol, QUOTE_LOTS, my_bid)
            trader.submit_order(order)
            time.sleep(1.0)
            self.bid_oid   = order.id
            self.bid_price = my_bid
            log(sim_time, self.symbol, "SUBMIT", "BUY", my_bid, QUOTE_LOTS,
                f"bid={my_bid} spread={spread:.4f} pos={pos:+d}L "
                f"inv_ratio={inv_ratio:.2f} skew={skew:.4f} ref={ref:.2f} "
                f"bid_vol={bid_vol}L")

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
                f"inv_ratio={inv_ratio:.2f} skew={skew:.4f} ref={ref:.2f} "
                f"ask_vol={ask_vol}L")

        log(sim_time, self.symbol, "STATUS",
            detail=f"bid={my_bid}({'REST' if self.bid_oid else 'NONE'}) "
                   f"ask={my_ask}({'REST' if self.ask_oid else 'NONE'}) "
                   f"spread={spread:.4f} pos={pos:+d}L "
                   f"inv_ratio={inv_ratio:.2f} skew={skew:.4f} "
                   f"ref={ref:.2f} window={len(stab_filter.window)} "
                   f"bid_vol={bid_vol}L ask_vol={ask_vol}L")

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
    machines = {sym: TickerMM(sym)          for sym in SYMBOLS}
    arbs     = {sym: CrossedBookArb(sym)    for sym in SYMBOLS}
    filters  = {sym: StabilityFilter(sym)   for sym in SYMBOLS}
    last_pnl_ts = 0.0

    print(f"[ZI MM] Starting | tickers={SYMBOLS} | end={end_time}", flush=True)

    pnl_file   = open(PNL_LOG_PATH, "a", newline="")
    pnl_writer = csv.DictWriter(pnl_file, fieldnames=PNL_FIELDS)

    try:
        while trader.get_last_trade_time() < end_time:
            sim_time = trader.get_last_trade_time()

            for sym in SYMBOLS:
                try:
                    arbs[sym].tick(trader, sim_time)
                except Exception as e:
                    print(f"[ARB ERROR][{sym}] {e}", flush=True)

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