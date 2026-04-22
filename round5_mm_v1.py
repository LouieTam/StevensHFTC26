import shift
import time
import csv
import os
from datetime import datetime, timedelta

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
SYMBOLS            = ["CS1", "CS2"]
TICK               = 0.01
LOT_SIZE           = 100
QUOTE_LOTS         = 3
MIN_SPREAD         = 0.04
CYCLE_SECONDS      = 2
POLL_INTERVAL      = 0.2
PNL_INTERVAL       = 1
MAX_POSITION_LOTS  = 30
SKEW_MULT          = 0.4
ARB_INTERVAL       = 0.5        # crossed-book check frequency (seconds)

EMPTY_BID_PRICE    = 1.0       # bid to submit when bid side is empty
EMPTY_ASK_PRICE    = 100000.0      # ask to submit when ask side is empty
BOTH_EMPTY_BID     = 1.0        # bid when both sides empty
BOTH_EMPTY_ASK     = 100000.0     # ask when both sides empty
MAX_BUYING_POWER   = 1_000_000

LOG_PATH     = "rl_mm_log_r5_run2.csv"
PNL_LOG_PATH = "rl_mm_pnl_r5_run2.csv"

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
    """Local book best bid/ask for MM quoting."""
    try:
        bo = trader.get_order_book(symbol, shift.OrderBookType.LOCAL_BID)
        ao = trader.get_order_book(symbol, shift.OrderBookType.LOCAL_ASK)
        if bo and ao and bo[0].price > 0 and ao[0].price > 0:
            return float(bo[0].price), float(ao[0].price)
    except Exception:
        pass
    return None, None

def get_global_book(trader, symbol, levels=5):
    """Global book up to N levels — used for arb and empty-side detection."""
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
# Crossed-book arbitrage
# ---------------------------------------------------------------------------

class CrossedBookArb:
    """
    Every ARB_INTERVAL seconds, checks if the global book is crossed
    (best bid > best ask). If so, submits simultaneous market buy and sell
    for the minimum of aggregated crossing quantities on each side.
    Tracks pending market orders and logs their execution status.
    """
    def __init__(self, symbol):
        self.symbol      = symbol
        self.last_arb_ts = 0.0
        self.pending     = {}  # oid -> {'side': str, 'qty': int}

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

        bids, asks = get_global_book(trader, self.symbol)
        if not bids or not asks:
            return

        best_bid = float(bids[0].price)
        best_ask = float(asks[0].price)

        if best_bid <= best_ask:
            return  # no crossing

        # Aggregate all bid levels strictly above best_ask
        agg_bid = sum(int(b.size) for b in bids if float(b.price) > best_ask)
        # Aggregate all ask levels strictly below best_bid
        agg_ask = sum(int(a.size) for a in asks if float(a.price) < best_bid)

        qty = min(agg_bid, agg_ask)
        if qty <= 0:
            return

        # Cap by available buying power
        max_lots = int(MAX_BUYING_POWER / (best_ask * LOT_SIZE))
        qty = min(qty, max(1, max_lots))

        log(sim_time, self.symbol, "ARB_DETECT", "",
            f"{best_bid}/{best_ask}", qty,
            f"agg_bid={agg_bid}L agg_ask={agg_ask}L submitting={qty}L")

        # Market buy — fills at best ask (cheap, since book is crossed)
        buy_ord = shift.Order(shift.Order.Type.MARKET_BUY, self.symbol, int(qty))
        trader.submit_order(buy_ord)
        self.pending[buy_ord.id] = {'side': 'BUY', 'qty': qty}

        # Market sell — fills at best bid (high, since book is crossed)
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
        self.symbol     = symbol
        self.bid_oid    = None   # normal MM bid order id
        self.bid_price  = None
        self.ask_oid    = None   # normal MM ask order id
        self.ask_price  = None
        self.last_cycle = 0.0
        # Extreme orders (empty-book case) — tracked separately,
        # not touched by _cancel_if_outside_spread
        self.ext_bid_submitted = False
        self.ext_ask_submitted = False

    def _get_my_waiting(self, trader):
        return {o.id: o for o in get_waiting_for(trader, self.symbol)}

    def _cancel_if_outside_spread(self, trader, sim_time, bid, ask):
        """Cancel normal MM legs that have drifted outside the current spread."""
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

    def _handle_empty_sides(self, trader, sim_time, bid_empty, ask_empty):
        """
        Submit extreme-price limit orders when one or both sides are empty.
        Flags prevent resubmission until the side repopulates.
        These orders are left resting — the normal cancel logic ignores them
        since they're not tracked in self.bid_oid / self.ask_oid.
        """
        both_empty = bid_empty and ask_empty

        if bid_empty and not self.ext_bid_submitted:
            price = BOTH_EMPTY_BID if both_empty else EMPTY_BID_PRICE
            order = shift.Order(shift.Order.Type.LIMIT_BUY,
                                self.symbol, QUOTE_LOTS, price)
            trader.submit_order(order)
            self.ext_bid_submitted = True
            log(sim_time, self.symbol, "EXTREME_BID", "BUY", price, QUOTE_LOTS,
                "bid side empty — submitting low limit bid")

        if ask_empty and not self.ext_ask_submitted:
            price = BOTH_EMPTY_ASK if both_empty else EMPTY_ASK_PRICE
            order = shift.Order(shift.Order.Type.LIMIT_SELL,
                                self.symbol, QUOTE_LOTS, price)
            trader.submit_order(order)
            self.ext_ask_submitted = True
            log(sim_time, self.symbol, "EXTREME_ASK", "SELL", price, QUOTE_LOTS,
                "ask side empty — submitting high limit ask")

        # Reset flags when sides repopulate so we can resubmit if needed later
        if not bid_empty:
            self.ext_bid_submitted = False
        if not ask_empty:
            self.ext_ask_submitted = False

    def tick(self, trader, sim_time):
        now = time.time()
        if now - self.last_cycle < CYCLE_SECONDS:
            return
        self.last_cycle = now

        # ── Step 1: check global book for empty sides ──────────────────────
        bids, asks = get_global_book(trader, self.symbol, levels=1)
        bid_empty  = not bids or float(bids[0].price) <= 0
        ask_empty  = not asks or float(asks[0].price) <= 0

        # ── Step 2: handle empty sides ────────────────────────────────────
        self._handle_empty_sides(trader, sim_time, bid_empty, ask_empty)

        # ── Step 3: if both sides empty, nothing more to do this cycle ────
        if bid_empty and ask_empty:
            return

        # ── Step 4: get local book for MM pricing ─────────────────────────
        bid, ask = get_best(trader, self.symbol)
        if bid is None or ask is None:
            log(sim_time, self.symbol, "NO_BOOK", detail="skipping MM cycle")
            return

        # ── Step 5: cancel normal legs outside spread ─────────────────────
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
                detail=f"skewed quotes crossed after clamping — skipping "
                       f"pos={pos:+d}L inv_ratio={inv_ratio:.2f} skew={skew:.4f}")
            return

        # ── Step 9: submit missing bid leg (skip if bid side is empty) ────
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

        # ── Step 10: submit missing ask leg (skip if ask side is empty) ───
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
                   f"inv_ratio={inv_ratio:.2f} skew={skew:.4f}")

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
    machines    = {sym: TickerMM(sym)       for sym in SYMBOLS}
    arbs        = {sym: CrossedBookArb(sym) for sym in SYMBOLS}
    last_pnl_ts = 0.0

    print(f"[ZI MM] Starting | tickers={SYMBOLS} | end={end_time}", flush=True)

    pnl_file   = open(PNL_LOG_PATH, "a", newline="")
    pnl_writer = csv.DictWriter(pnl_file, fieldnames=PNL_FIELDS)

    try:
        while trader.get_last_trade_time() < end_time:
            sim_time = trader.get_last_trade_time()

            # Arb check — every 0.5s, gated internally per symbol
            for sym, arb in arbs.items():
                try:
                    arb.tick(trader, sim_time)
                except Exception as e:
                    print(f"[ARB ERROR][{sym}] {e}", flush=True)

            # MM cycle — every 2s, gated internally per symbol
            for sym, mm in machines.items():
                try:
                    mm.tick(trader, sim_time)
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