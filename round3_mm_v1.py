import shift
import time
import csv
import os
from datetime import datetime, timedelta

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
SYMBOLS        = ["TXRH", "CROX", "PZZA", "CAR" ,"HELE", "JACK", "SHOO"]
TICK           = 0.01
LOT_SIZE       = 100
CYCLE_SECONDS  = 3       # check / reprice every N seconds
POLL_INTERVAL  = 0.5     # inner poll loop resolution

LOG_PATH = "simple_mm_log.csv"

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def ensure_log():
    if not os.path.exists(LOG_PATH):
        with open(LOG_PATH, "w", newline="") as f:
            csv.writer(f).writerow([
                "wall_time", "sim_time", "symbol", "event",
                "side", "price", "pos_lots", "detail",
            ])

def log(sim_time, symbol, event, side="", price="", pos_lots="", detail=""):
    wall = datetime.now().isoformat()
    with open(LOG_PATH, "a", newline="") as f:
        csv.writer(f).writerow([
            wall, sim_time, symbol, event, side, price, pos_lots, detail,
        ])
    print(f"[{sim_time}][{symbol}] {event:20s} | side={side:4s} "
          f"price={str(price):8s} pos={str(pos_lots):4s} | {detail}",
          flush=True)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def round_tick(x):
    return round(round(x / TICK) * TICK, 2)

def get_pos(trader, symbol):
    item = trader.get_portfolio_item(symbol)
    return (int(item.get_long_shares()) - int(item.get_short_shares())) // LOT_SIZE

def get_best(trader, symbol):
    """Returns (best_bid, best_ask) or (None, None)."""
    try:
        bo = trader.get_order_book(symbol, shift.OrderBookType.GLOBAL_BID)
        ao = trader.get_order_book(symbol, shift.OrderBookType.GLOBAL_ASK)
        if bo and ao and bo[0].price > 0 and ao[0].price > 0:
            return float(bo[0].price), float(ao[0].price)
    except Exception:
        pass
    return None, None

def get_waiting(trader, symbol):
    """Return resting orders for symbol."""
    return [o for o in trader.get_waiting_list() if o.symbol == symbol]

def cancel_symbol(trader, symbol):
    """Cancel all resting orders for symbol and wait for confirmation."""
    for o in trader.get_waiting_list():
        if o.symbol == symbol:
            trader.submit_cancellation(o)
    time.sleep(1.0)

def submit_limit(trader, symbol, side, lots, price, sim_time, detail=""):
    if side == "BUY":
        order = shift.Order(shift.Order.Type.LIMIT_BUY,  symbol, int(lots), float(price))
    else:
        order = shift.Order(shift.Order.Type.LIMIT_SELL, symbol, int(lots), float(price))
    trader.submit_order(order)
    log(sim_time, symbol, "SUBMIT", side, price, get_pos(trader, symbol), detail)
    return order.id

def order_filled(trader, oid):
    """True if order is fully filled or no longer in waiting list."""
    waiting_ids = {o.id for o in trader.get_waiting_list()}
    if oid not in waiting_ids:
        return True
    try:
        o = trader.get_order(oid)
        if o is None:
            return True
        s = str(getattr(o, "status", ""))
        return "FILLED" in s or "CANCELED" in s or "REJECTED" in s
    except Exception:
        return False

# ---------------------------------------------------------------------------
# Per-ticker state machine
# ---------------------------------------------------------------------------

class TickerMM:
    """
    States:
      IDLE        — flat, no resting orders, ready for a new pair
      QUOTING     — both legs resting, waiting for fills
      LIQUIDATING — one leg filled, working to close the remaining position
    """
    IDLE        = "IDLE"
    QUOTING     = "QUOTING"
    LIQUIDATING = "LIQUIDATING"

    def __init__(self, symbol):
        self.symbol      = symbol
        self.state       = self.IDLE
        self.bid_oid     = None
        self.ask_oid     = None
        self.liq_oid     = None
        self.liq_cycles  = 0      # cycles spent in liquidation on this leg
        self.last_cycle  = 0.0    # wall time of last action
        self.cycle_count = 0      # total cycles run

    def reset(self):
        self.state      = self.IDLE
        self.bid_oid    = None
        self.ask_oid    = None
        self.liq_oid    = None
        self.liq_cycles = 0

    def tick(self, trader, sim_time):
        """Called every POLL_INTERVAL. Acts only when CYCLE_SECONDS have elapsed."""
        now = time.time()
        if now - self.last_cycle < CYCLE_SECONDS:
            return
        self.last_cycle  = now
        self.cycle_count += 1

        pos = get_pos(trader, self.symbol)
        bid, ask = get_best(trader, self.symbol)

        if bid is None or ask is None or bid >= ask:
            log(sim_time, self.symbol, "NO_BOOK", detail="skipping cycle")
            return

        mid = round_tick((bid + ask) / 2)

        # ── Dispatch to state handler ─────────────────────────────────────
        if self.state == self.IDLE:
            self._handle_idle(trader, sim_time, pos, bid, ask)

        elif self.state == self.QUOTING:
            self._handle_quoting(trader, sim_time, pos, bid, ask, mid)

        elif self.state == self.LIQUIDATING:
            self._handle_liquidating(trader, sim_time, pos, bid, ask, mid)

    # ── IDLE: flat and no orders — submit fresh pair ──────────────────────
    def _handle_idle(self, trader, sim_time, pos, bid, ask):
        if pos != 0:
            # Safety — shouldn't happen, but transition to liquidation
            log(sim_time, self.symbol, "UNEXPECTED_POS",
                pos_lots=pos, detail="entering liquidation from IDLE")
            self.state = self.LIQUIDATING
            return

        my_bid = round_tick(bid + 2 * TICK)
        my_ask = round_tick(ask - 2 * TICK)

        if my_bid >= my_ask:
            log(sim_time, self.symbol, "SPREAD_TOO_TIGHT",
                detail=f"bid={bid} ask={ask} spread={ask-bid:.2f} < 4 ticks")
            return

        cancel_symbol(trader, self.symbol)   # clean slate
        self.bid_oid = submit_limit(trader, self.symbol, "BUY",  1, my_bid,
                                    sim_time, f"PAIR bid+2tick={my_bid}")
        self.ask_oid = submit_limit(trader, self.symbol, "SELL", 1, my_ask,
                                    sim_time, f"PAIR ask-2tick={my_ask}")
        self.state = self.QUOTING
        log(sim_time, self.symbol, "QUOTING",
            detail=f"BUY@{my_bid} SELL@{my_ask}")

    # ── QUOTING: both legs resting — check for fills ──────────────────────
    def _handle_quoting(self, trader, sim_time, pos, bid, ask, mid):
        bid_filled = order_filled(trader, self.bid_oid)
        ask_filled = order_filled(trader, self.ask_oid)

        log(sim_time, self.symbol, "CYCLE_CHECK",
            pos_lots=pos,
            detail=f"bid_filled={bid_filled} ask_filled={ask_filled} "
                   f"bid={bid} ask={ask}")

        # ── Both filled: clean round trip ─────────────────────────────────
        if bid_filled and ask_filled:
            log(sim_time, self.symbol, "BOTH_FILLED",
                pos_lots=pos, detail="round trip complete → IDLE")
            self.reset()
            return

        # ── One leg filled: cancel other, immediately submit mid ─────────────
        if bid_filled and not ask_filled:
            log(sim_time, self.symbol, "BID_FILLED",
                pos_lots=pos, detail="long 1L — cancelling ask, liquidating at mid")
            cancel_symbol(trader, self.symbol)
            self.state      = self.LIQUIDATING
            self.liq_cycles = 0
            self.liq_oid    = None
            # Immediately submit ask at mid
            bid, ask = get_best(trader, self.symbol)
            if bid and ask:
                mid = round_tick((bid + ask) / 2)
                self.liq_oid = submit_limit(
                    trader, self.symbol, "SELL", 1, mid,
                    sim_time, f"LIQ_AT_MID mid={mid}"
                )
            return

        if ask_filled and not bid_filled:
            log(sim_time, self.symbol, "ASK_FILLED",
                pos_lots=pos, detail="short 1L — cancelling bid, liquidating at mid")
            cancel_symbol(trader, self.symbol)
            self.state      = self.LIQUIDATING
            self.liq_cycles = 0
            self.liq_oid    = None
            # Immediately submit bid at mid
            bid, ask = get_best(trader, self.symbol)
            if bid and ask:
                mid = round_tick((bid + ask) / 2)
                self.liq_oid = submit_limit(
                    trader, self.symbol, "BUY", 1, mid,
                    sim_time, f"LIQ_AT_MID mid={mid}"
                )
            return

        # ── Neither filled: reprice both legs ─────────────────────────────
        my_bid = round_tick(bid + 2 * TICK)
        my_ask = round_tick(ask - 2 * TICK)

        if my_bid >= my_ask:
            log(sim_time, self.symbol, "SPREAD_TOO_TIGHT",
                detail="cancelling pair, back to IDLE")
            cancel_symbol(trader, self.symbol)
            self.reset()
            return

        cancel_symbol(trader, self.symbol)
        self.bid_oid = submit_limit(trader, self.symbol, "BUY",  1, my_bid,
                                    sim_time, f"REPRICE bid+2tick={my_bid}")
        self.ask_oid = submit_limit(trader, self.symbol, "SELL", 1, my_ask,
                                    sim_time, f"REPRICE ask-2tick={my_ask}")
        log(sim_time, self.symbol, "REPRICE",
            detail=f"BUY@{my_bid} SELL@{my_ask}")

    # ── LIQUIDATING: one position open, reprice at mid every cycle ───────
    def _handle_liquidating(self, trader, sim_time, pos, bid, ask, mid):
        if pos == 0:
            log(sim_time, self.symbol, "LIQ_COMPLETE",
                pos_lots=0, detail="position closed → IDLE")
            cancel_symbol(trader, self.symbol)
            self.reset()
            return

        self.liq_cycles += 1
        close_side = "SELL" if pos > 0 else "BUY"

        # Reprice at current mid every cycle
        mid = round_tick((bid + ask) / 2)
        cancel_symbol(trader, self.symbol)
        self.liq_oid = submit_limit(
            trader, self.symbol, close_side, abs(pos), mid,
            sim_time, f"LIQ_MID cycle={self.liq_cycles} mid={mid}"
        )
        log(sim_time, self.symbol, "LIQ_REPRICE",
            side=close_side, price=mid, pos_lots=pos,
            detail=f"cycle={self.liq_cycles}")


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def run(trader, end_time):
    ensure_log()

    # Initialise one state machine per ticker
    machines = {sym: TickerMM(sym) for sym in SYMBOLS}

    print(f"[MM] Starting | tickers={SYMBOLS} | end={end_time}", flush=True)

    while trader.get_last_trade_time() < end_time:
        sim_time = trader.get_last_trade_time()

        for sym, mm in machines.items():
            try:
                mm.tick(trader, sim_time)
            except Exception as e:
                print(f"[ERROR][{sym}] {e}", flush=True)

        time.sleep(POLL_INTERVAL)

    # Shutdown — cancel all and flatten
    print("[MM] Session ending — cancelling all orders", flush=True)
    for sym in SYMBOLS:
        cancel_symbol(trader, sym)

    # Market sell/buy to flatten any remaining positions
    for sym in SYMBOLS:
        pos = get_pos(trader, sym)
        if pos != 0:
            side = "SELL" if pos > 0 else "BUY"
            order = (shift.Order(shift.Order.Type.MARKET_SELL, sym, abs(pos))
                     if side == "SELL"
                     else shift.Order(shift.Order.Type.MARKET_BUY, sym, abs(pos)))
            trader.submit_order(order)
            print(f"[SHUTDOWN] {side} {abs(pos)}L {sym} at market", flush=True)
            time.sleep(1.0)

    print("[MM] Done.", flush=True)


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
            run(trader, end_time)
        except KeyboardInterrupt:
            for sym in SYMBOLS:
                cancel_symbol(trader, sym)
            trader.disconnect()