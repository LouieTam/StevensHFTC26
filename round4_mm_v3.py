import shift
import time
import csv
import os
from datetime import datetime, timedelta

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
SYMBOLS            = ["CS1", "CS2", "CS3"]
TICK               = 0.01
LOT_SIZE           = 100
QUOTE_LOTS         = 3            # lots per side
MIN_SPREAD         = 0.04         # only quote when spread >= this
CYCLE_SECONDS      = 2            # reprice cycle
POLL_INTERVAL      = 0.2          # inner loop resolution
PNL_INTERVAL       = 1            # log PnL every N seconds
MAX_POSITION_LOTS  = 40           # inventory skew reference — tune this
SKEW_MULT          = 0.2          # fraction of spread to skew per unit inv_ratio

LOG_PATH     = "zi_mm_log_run6.csv"
PNL_LOG_PATH = "zi_mm_pnl_run6.csv"

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
                "cs3_pos_lots", "cs3_unrealized_pl",
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

def get_waiting_for(trader, symbol):
    return [o for o in trader.get_waiting_list() if o.symbol == symbol]

def cancel_order(trader, order):
    try:
        trader.submit_cancellation(order)
    except Exception as e:
        print(f"[CANCEL ERROR] {e}", flush=True)

def compute_skewed_quotes(best_bid, best_ask, pos_lots):
    """
    Inventory-aware quote generation.
    Baseline:    bid+1tick, ask-1tick (1 tick inside the book)
    Skew:        shift BOTH quotes by (inv_ratio * spread * SKEW_MULT)
                 where inv_ratio = pos_lots / MAX_POSITION_LOTS, clipped to [-1, 1]
    Long  (pos>0) → skew > 0 → both quotes shift DOWN:
        - our bid gets less aggressive (buyers less likely to hit us)
        - our ask gets more aggressive (more likely to offload inventory)
    Short (pos<0) → skew < 0 → both quotes shift UP (symmetric).
    Quotes can intentionally sit outside the best bid/ask when skew is large —
    this is expected behavior for deep inventory.
    Returns (my_bid, my_ask, skew, inv_ratio), or (None, None, skew, inv_ratio)
    if skewed quotes cross each other.
    """
    spread    = best_ask - best_bid
    inv_ratio = max(-1.0, min(1.0, pos_lots / MAX_POSITION_LOTS))
    skew      = inv_ratio * spread * SKEW_MULT

    my_bid = round_tick((best_bid + TICK) - skew)
    my_ask = round_tick((best_ask - TICK) - skew)

    # Safety: prevent crossing the market
    if my_bid >= best_ask:
        my_bid = round_tick(best_ask - TICK)
    if my_ask <= best_bid:
        my_ask = round_tick(best_bid + TICK)

    # If quotes still cross each other after clamping, skip quoting
    if my_bid >= my_ask:
        return None, None, skew, inv_ratio

    return my_bid, my_ask, skew, inv_ratio

# ---------------------------------------------------------------------------
# Per-ticker state
# ---------------------------------------------------------------------------

class TickerMM:
    def __init__(self, symbol):
        self.symbol     = symbol
        self.bid_oid    = None
        self.bid_price  = None
        self.ask_oid    = None
        self.ask_price  = None
        self.last_cycle = 0.0

    def _get_my_waiting(self, trader):
        """Return waiting orders for this symbol keyed by order id."""
        return {o.id: o for o in get_waiting_for(trader, self.symbol)}

    def _cancel_if_outside_spread(self, trader, sim_time, bid, ask):
        """
        For each tracked resting leg:
          - If no longer in waiting list → filled externally, clear state
          - If price not strictly inside bid < price < ask → cancel, clear state
        """
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

    def tick(self, trader, sim_time):
        now = time.time()
        if now - self.last_cycle < CYCLE_SECONDS:
            return
        self.last_cycle = now

        # ── Step 1: get current book ───────────────────────────────────────
        bid, ask = get_best(trader, self.symbol)
        if bid is None or ask is None:
            log(sim_time, self.symbol, "NO_BOOK", detail="skipping cycle")
            return

        # ── Step 2: cancel resting legs that have drifted outside spread ──
        self._cancel_if_outside_spread(trader, sim_time, bid, ask)

        # ── Step 3: refresh book after cancels ────────────────────────────
        bid, ask = get_best(trader, self.symbol)
        if bid is None or ask is None:
            return
        spread = ask - bid

        # ── Step 4: check entry condition ─────────────────────────────────
        if spread < MIN_SPREAD:
            log(sim_time, self.symbol, "SPREAD_TIGHT",
                detail=f"spread={spread:.4f} < {MIN_SPREAD} — waiting")
            return

        # ── Step 5: compute skewed quotes ─────────────────────────────────
        pos = get_pos(trader, self.symbol)
        my_bid, my_ask, skew, inv_ratio = compute_skewed_quotes(bid, ask, pos)

        if my_bid is None:
            log(sim_time, self.symbol, "QUOTE_CROSSED",
                detail=f"skewed quotes crossed after clamping — skipping "
                       f"pos={pos:+d}L inv_ratio={inv_ratio:.2f} skew={skew:.4f}")
            return

        # ── Step 6: submit missing bid leg ────────────────────────────────
        if self.bid_oid is None:
            order = shift.Order(shift.Order.Type.LIMIT_BUY,
                                self.symbol, QUOTE_LOTS, my_bid)
            trader.submit_order(order)
            time.sleep(1.0)  # let order land in waiting list
            self.bid_oid   = order.id
            self.bid_price = my_bid
            log(sim_time, self.symbol, "SUBMIT", "BUY", my_bid, QUOTE_LOTS,
                f"bid={my_bid} spread={spread:.4f} pos={pos:+d}L "
                f"inv_ratio={inv_ratio:.2f} skew={skew:.4f}")

        # ── Step 7: submit missing ask leg ────────────────────────────────
        if self.ask_oid is None:
            order = shift.Order(shift.Order.Type.LIMIT_SELL,
                                self.symbol, QUOTE_LOTS, my_ask)
            trader.submit_order(order)
            time.sleep(1.0)  # let order land in waiting list
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
    "cs3_pos_lots", "cs3_unrealized_pl",
    "total_realized_pl", "total_unrealized_pl", "total_pl",
]


def run(trader, end_time):
    ensure_log()
    ensure_pnl_log()
    machines    = {sym: TickerMM(sym) for sym in SYMBOLS}
    last_pnl_ts = 0.0

    print(f"[ZI MM] Starting | tickers={SYMBOLS} | end={end_time}",
          flush=True)

    pnl_file   = open(PNL_LOG_PATH, "a", newline="")
    pnl_writer = csv.DictWriter(pnl_file, fieldnames=PNL_FIELDS)

    try:
        while trader.get_last_trade_time() < end_time:
            sim_time = trader.get_last_trade_time()

            for sym, mm in machines.items():
                try:
                    mm.tick(trader, sim_time)
                except Exception as e:
                    print(f"[ERROR][{sym}] {e}", flush=True)

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
        print("\n[ZI MM] KeyboardInterrupt — shutting down cleanly",
              flush=True)

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