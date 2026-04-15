import shift
import time
import csv
import os
from datetime import datetime, timedelta

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
SYMBOLS         = ["CS1", "CS2", "CS3"]
TICK            = 0.01
LOT_SIZE        = 100
QUOTE_LOTS      = 3            # lots per side
MIN_SPREAD      = 0.05         # only quote when spread >= this
CYCLE_SECONDS   = 2            # reprice cycle
POLL_INTERVAL   = 0.2          # inner loop resolution
PNL_INTERVAL    = 1            # log PnL every N seconds

LOG_PATH    = "zi_mm_log_run4.csv"
PNL_LOG_PATH = "zi_mm_pnl_run4.csv"

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
    """
    Capture PnL and write directly to the already-open CSV writer.
    Called every PNL_INTERVAL seconds.
    """
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

def submit_limit(trader, symbol, side, lots, price, sim_time, detail=""):
    if side == "BUY":
        order = shift.Order(shift.Order.Type.LIMIT_BUY,
                            symbol, int(lots), float(price))
    else:
        order = shift.Order(shift.Order.Type.LIMIT_SELL,
                            symbol, int(lots), float(price))
    trader.submit_order(order)
    log(sim_time, symbol, "SUBMIT", side, price, lots, detail)
    return order.id, float(price)

# ---------------------------------------------------------------------------
# Per-ticker state
# ---------------------------------------------------------------------------

class TickerMM:
    def __init__(self, symbol):
        self.symbol      = symbol
        self.bid_oid     = None
        self.bid_price   = None   # price we submitted the bid at
        self.ask_oid     = None
        self.ask_price   = None   # price we submitted the ask at
        self.last_cycle  = 0.0

    def _order_resting(self, trader, oid):
        """True if order is still in the waiting list."""
        if oid is None:
            return False
        return any(o.id == oid for o in trader.get_waiting_list())

    def tick(self, trader, sim_time):
        now = time.time()
        if now - self.last_cycle < CYCLE_SECONDS:
            return
        self.last_cycle = now

        bid, ask = get_best(trader, self.symbol)
        if bid is None or ask is None:
            log(sim_time, self.symbol, "NO_BOOK",
                detail="skipping cycle")
            return

        spread = ask - bid
        pos    = get_pos(trader, self.symbol)

        # ── Spread too tight — cancel everything and wait ──────────────────
        if spread < MIN_SPREAD:
            log(sim_time, self.symbol, "SPREAD_TIGHT",
                detail=f"spread={spread:.4f} < {MIN_SPREAD} — cancelling")
            for o in get_waiting_for(trader, self.symbol):
                cancel_order(trader, o)
            self.bid_oid = None; self.bid_price = None
            self.ask_oid = None; self.ask_price = None
            return

        # Our desired quotes: 1 tick inside best bid/ask
        my_bid = round_tick(bid + TICK)
        my_ask = round_tick(ask - TICK)

        # Sanity check — should never cross given MIN_SPREAD >= 0.05
        if my_bid >= my_ask:
            log(sim_time, self.symbol, "QUOTE_CROSSED",
                detail=f"my_bid={my_bid} >= my_ask={my_ask} — skipping")
            return

        # ── Check each resting leg: still valid or needs repricing? ───────
        bid_needs_reprice = self._needs_reprice(trader, self.bid_oid,
                                                self.bid_price, my_bid,
                                                bid, ask, "BUY")
        ask_needs_reprice = self._needs_reprice(trader, self.ask_oid,
                                                self.ask_price, my_ask,
                                                bid, ask, "SELL")

        # ── Cancel legs that need repricing ───────────────────────────────
        if bid_needs_reprice and self.bid_oid is not None:
            for o in get_waiting_for(trader, self.symbol):
                if o.id == self.bid_oid:
                    cancel_order(trader, o)
                    log(sim_time, self.symbol, "CANCEL", "BUY",
                        self.bid_price, "",
                        f"outside spread bid={bid} ask={ask}")
            self.bid_oid   = None
            self.bid_price = None
            time.sleep(0.3)

        if ask_needs_reprice and self.ask_oid is not None:
            for o in get_waiting_for(trader, self.symbol):
                if o.id == self.ask_oid:
                    cancel_order(trader, o)
                    log(sim_time, self.symbol, "CANCEL", "SELL",
                        self.ask_price, "",
                        f"outside spread bid={bid} ask={ask}")
            self.ask_oid   = None
            self.ask_price = None
            time.sleep(0.3)

        # Refresh best after cancels
        bid, ask = get_best(trader, self.symbol)
        if bid is None or ask is None:
            return
        spread = ask - bid
        if spread < MIN_SPREAD:
            return
        my_bid = round_tick(bid + TICK)
        my_ask = round_tick(ask - TICK)
        if my_bid >= my_ask:
            return

        # ── Submit missing legs ────────────────────────────────────────────
        if self.bid_oid is None:
            oid, px = submit_limit(
                trader, self.symbol, "BUY", QUOTE_LOTS, my_bid,
                sim_time,
                f"bid+1tick={my_bid} spread={spread:.4f} pos={pos:+d}L"
            )
            self.bid_oid   = oid
            self.bid_price = px

        if self.ask_oid is None:
            oid, px = submit_limit(
                trader, self.symbol, "SELL", QUOTE_LOTS, my_ask,
                sim_time,
                f"ask-1tick={my_ask} spread={spread:.4f} pos={pos:+d}L"
            )
            self.ask_oid   = oid
            self.ask_price = px

        log(sim_time, self.symbol, "STATUS",
            detail=f"bid={my_bid}({'REST' if self._order_resting(trader, self.bid_oid) else 'NEW'}) "
                   f"ask={my_ask}({'REST' if self._order_resting(trader, self.ask_oid) else 'NEW'}) "
                   f"spread={spread:.4f} pos={pos:+d}L")

    def _needs_reprice(self, trader, oid, submitted_price,
                       desired_price, best_bid, best_ask, side):
        """
        Returns True if the resting order needs to be cancelled and repriced.

        A leg is still valid if:
          1. It is still resting in the waiting list
          2. Its submitted price is strictly inside the current spread
             (bid < submitted_price < ask)
          3. Its submitted price equals the desired new price
             (no need to reprice if quote is still optimal)

        Any failure → needs reprice.
        """
        if oid is None:
            return True   # no order resting, need to submit

        if not self._order_resting(trader, oid):
            # Order filled or cancelled externally — need fresh submit
            if side == "BUY":
                self.bid_oid   = None
                self.bid_price = None
            else:
                self.ask_oid   = None
                self.ask_price = None
            return True

        if submitted_price is None:
            return True

        # Check if submitted price is still inside the spread
        inside = best_bid < submitted_price < best_ask
        if not inside:
            return True

        # Check if the desired price has changed
        if abs(submitted_price - desired_price) >= TICK - 1e-9:
            return True

        return False   # still valid, leave it resting


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

            # Write PnL every PNL_INTERVAL seconds
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
        # Always runs — normal end, KeyboardInterrupt, or any exception
        print("[ZI MM] Cancelling all resting orders...", flush=True)
        for sym in SYMBOLS:
            for o in get_waiting_for(trader, sym):
                cancel_order(trader, o)
            time.sleep(0.5)

        # Final PnL snapshot before closing
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
        time.sleep(1.0)
        end_time = (trader.get_last_trade_time()
                    + timedelta(minutes=380.0))
        run(trader, end_time)