import shift
import time
import csv
import os
from collections import deque
from datetime import datetime, timedelta

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
SYMBOLS         = ["TXRH", "CROX", "PZZA", "CAR" ,"HELE", "JACK", "SHOO"]
TICK            = 0.01
LOT_SIZE        = 100
MAX_LOTS        = 10          # max position per ticker
CYCLE_SECONDS   = 5          # reprice / check every N seconds
POLL_INTERVAL   = 0.5        # inner loop resolution

# Directional signal: N of last WINDOW trades
SIGNAL_WINDOW   = 15         # lookback window in trades
SIGNAL_THRESH   = 10         # need >= THRESH up/down trades to signal

LOG_PATH        = "directional_mm_log.csv"

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def ensure_log():
    if not os.path.exists(LOG_PATH):
        with open(LOG_PATH, "w", newline="") as f:
            csv.writer(f).writerow([
                "wall_time", "sim_time", "symbol", "event",
                "signal", "side", "price", "pos_lots", "detail",
            ])

def log(sim_time, symbol, event, signal="", side="",
        price="", pos_lots="", detail=""):
    wall = datetime.now().strftime("%H:%M:%S")
    with open(LOG_PATH, "a", newline="") as f:
        csv.writer(f).writerow([
            wall, sim_time, symbol, event,
            signal, side, price, pos_lots, detail,
        ])
    print(
        f"[{wall}][{sim_time}][{symbol}] {event:22s} | "
        f"sig={signal:7s} side={side:4s} "
        f"px={str(price):8s} pos={str(pos_lots):4s} | {detail}",
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
        bo = trader.get_order_book(symbol, shift.OrderBookType.GLOBAL_BID)
        ao = trader.get_order_book(symbol, shift.OrderBookType.GLOBAL_ASK)
        if bo and ao and bo[0].price > 0 and ao[0].price > 0:
            return float(bo[0].price), float(ao[0].price)
    except Exception:
        pass
    return None, None

def cancel_symbol(trader, symbol):
    for o in trader.get_waiting_list():
        if o.symbol == symbol:
            trader.submit_cancellation(o)
    time.sleep(1.0)

def submit_limit(trader, symbol, side, lots, price,
                 sim_time, signal, detail=""):
    if side == "BUY":
        order = shift.Order(shift.Order.Type.LIMIT_BUY,
                            symbol, int(lots), float(price))
    else:
        order = shift.Order(shift.Order.Type.LIMIT_SELL,
                            symbol, int(lots), float(price))
    trader.submit_order(order)
    log(sim_time, symbol, "SUBMIT", signal, side, price,
        get_pos(trader, symbol), detail)
    return order.id

# ---------------------------------------------------------------------------
# Directional signal tracker
# ---------------------------------------------------------------------------

class SignalTracker:
    """
    Tracks last WINDOW trade events (when last_price changes).
    Returns BULL / BEAR / NEUTRAL based on direction of each trade.

    BULL: >= THRESH of last WINDOW trades moved price up
    BEAR: >= THRESH of last WINDOW trades moved price down
    NEUTRAL: otherwise
    """
    def __init__(self, window=SIGNAL_WINDOW, thresh=SIGNAL_THRESH):
        self.window    = window
        self.thresh    = thresh
        self.directions = deque(maxlen=window)  # +1 up, -1 down
        self.last_price = None

    def update(self, last_price):
        """
        Call every tick with current last_price.
        Returns True if a new trade was detected.
        """
        if last_price <= 0:
            return False
        if self.last_price is None:
            self.last_price = last_price
            return False
        if last_price == self.last_price:
            return False   # no new trade

        direction = 1 if last_price > self.last_price else -1
        self.directions.append(direction)
        self.last_price = last_price
        return True

    @property
    def signal(self):
        if len(self.directions) < self.window:
            return "NEUTRAL"   # not enough trades yet
        up   = sum(1 for d in self.directions if d == 1)
        down = sum(1 for d in self.directions if d == -1)
        if up >= self.thresh:
            return "BULL"
        if down >= self.thresh:
            return "BEAR"
        return "NEUTRAL"

    @property
    def counts(self):
        up   = sum(1 for d in self.directions if d == 1)
        down = sum(1 for d in self.directions if d == -1)
        return up, down

# ---------------------------------------------------------------------------
# Quote price computation
# ---------------------------------------------------------------------------

def compute_quotes(signal, bid, ask):
    """
    BULL:    bid + 0.2*spread  /  ask - 0.01   (aggressive bid)
    BEAR:    bid + 0.01        /  ask - 0.2*spread (aggressive ask)
    NEUTRAL: bid + 0.02        /  ask - 0.02
    """
    spread = ask - bid
    if signal == "BULL":
        my_bid = round_tick(bid + 0.2 * spread)
        my_ask = round_tick(ask - TICK)
    elif signal == "BEAR":
        my_bid = round_tick(bid + TICK)
        my_ask = round_tick(ask - 0.2 * spread)
    else:
        my_bid = round_tick(bid + 2 * TICK)
        my_ask = round_tick(ask - 2 * TICK)
    return my_bid, my_ask

# ---------------------------------------------------------------------------
# Per-ticker state machine
# ---------------------------------------------------------------------------

class TickerMM:
    """
    States:
      QUOTING     — actively quoting both sides (within position limits)
      AT_MAX      — at max position in signal direction, waiting for signal change
      LIQUIDATING — signal changed while holding position, selling/buying at mid
    """
    QUOTING     = "QUOTING"
    AT_MAX      = "AT_MAX"
    LIQUIDATING = "LIQUIDATING"

    def __init__(self, symbol):
        self.symbol      = symbol
        self.state       = self.QUOTING
        self.signal      = SignalTracker()

        self.bid_oid     = None
        self.ask_oid     = None
        self.liq_oid     = None
        self.liq_price   = None   # last submitted liq price, to detect mid changes

        self.prev_signal = "NEUTRAL"
        self.last_cycle  = 0.0

    def _order_done(self, trader, oid):
        """True if order is no longer resting."""
        if oid is None:
            return True
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
            return True

    def tick(self, trader, sim_time):
        now = time.time()
        if now - self.last_cycle < CYCLE_SECONDS:
            # Still update signal tracker every poll even outside cycle
            try:
                lp = float(trader.get_last_price(self.symbol) or 0)
                self.signal.update(lp)
            except Exception:
                pass
            return
        self.last_cycle = now

        # ── Update signal ─────────────────────────────────────────────────
        try:
            lp = float(trader.get_last_price(self.symbol) or 0)
            self.signal.update(lp)
        except Exception:
            lp = 0

        current_signal = self.signal.signal
        up, down       = self.signal.counts
        pos            = get_pos(trader, self.symbol)
        bid, ask       = get_best(trader, self.symbol)

        if bid is None or ask is None or bid >= ask:
            log(sim_time, self.symbol, "NO_BOOK",
                current_signal, detail=f"up={up} dn={down}")
            return

        mid    = round_tick((bid + ask) / 2)
        spread = ask - bid

        signal_changed = (current_signal != self.prev_signal)
        if signal_changed:
            log(sim_time, self.symbol, "SIGNAL_CHANGE",
                current_signal,
                detail=f"{self.prev_signal} → {current_signal} "
                       f"up={up} dn={down} pos={pos:+d}L")

        # ── State: LIQUIDATING ────────────────────────────────────────────
        if self.state == self.LIQUIDATING:
            if pos == 0:
                log(sim_time, self.symbol, "LIQ_COMPLETE",
                    current_signal, pos_lots=0,
                    detail="flat → QUOTING")
                cancel_symbol(trader, self.symbol)
                self.liq_oid    = None
                self.liq_price  = None
                self.state      = self.QUOTING
                self.prev_signal = current_signal
                return

            close_side = "SELL" if pos > 0 else "BUY"

            # Only reprice if mid has moved
            if self.liq_price == mid:
                log(sim_time, self.symbol, "LIQ_HOLD",
                    current_signal, close_side, mid, pos,
                    detail="mid unchanged, holding")
                self.prev_signal = current_signal
                return

            cancel_symbol(trader, self.symbol)
            self.liq_oid = submit_limit(
                trader, self.symbol, close_side, abs(pos), mid,
                sim_time, current_signal,
                f"LIQ_MID mid={mid}"
            )
            self.liq_price = mid
            self.prev_signal = current_signal
            return

        # ── State: AT_MAX ─────────────────────────────────────────────────
        if self.state == self.AT_MAX:
            # Check if signal changed — if so, liquidate
            if signal_changed:
                log(sim_time, self.symbol, "AT_MAX_EXIT",
                    current_signal, pos_lots=pos,
                    detail=f"signal changed → liquidating")
                cancel_symbol(trader, self.symbol)
                self.bid_oid = None
                self.ask_oid = None
                self.state   = self.LIQUIDATING
                self.liq_price = None
                self.prev_signal = current_signal
                return

            log(sim_time, self.symbol, "AT_MAX_WAIT",
                current_signal, pos_lots=pos,
                detail=f"holding at max {MAX_LOTS}L "
                       f"up={up} dn={down}")
            self.prev_signal = current_signal
            return

        # ── State: QUOTING ────────────────────────────────────────────────

        # Check if signal changed while holding a position → liquidate
        if signal_changed and pos != 0:
            log(sim_time, self.symbol, "SIGNAL_LIQ",
                current_signal, pos_lots=pos,
                detail=f"signal changed with pos={pos:+d}L → liquidating")
            cancel_symbol(trader, self.symbol)
            self.bid_oid = None
            self.ask_oid = None
            self.state   = self.LIQUIDATING
            self.liq_price = None
            self.prev_signal = current_signal
            return

        # Check position limits
        if current_signal == "BULL" and pos >= MAX_LOTS:
            log(sim_time, self.symbol, "AT_MAX",
                current_signal, pos_lots=pos,
                detail=f"long {pos}L >= MAX {MAX_LOTS}L → waiting")
            cancel_symbol(trader, self.symbol)
            self.bid_oid = None
            self.ask_oid = None
            self.state   = self.AT_MAX
            self.prev_signal = current_signal
            return

        if current_signal == "BEAR" and pos <= -MAX_LOTS:
            log(sim_time, self.symbol, "AT_MAX",
                current_signal, pos_lots=pos,
                detail=f"short {pos}L <= -MAX {MAX_LOTS}L → waiting")
            cancel_symbol(trader, self.symbol)
            self.bid_oid = None
            self.ask_oid = None
            self.state   = self.AT_MAX
            self.prev_signal = current_signal
            return

        # Compute quotes based on signal
        my_bid, my_ask = compute_quotes(current_signal, bid, ask)

        if my_bid >= my_ask:
            log(sim_time, self.symbol, "SPREAD_TIGHT",
                current_signal, detail=f"spread={spread:.4f} < min")
            self.prev_signal = current_signal
            return

        # Reprice — cancel and resubmit both sides
        cancel_symbol(trader, self.symbol)

        self.bid_oid = submit_limit(
            trader, self.symbol, "BUY", 1, my_bid,
            sim_time, current_signal,
            f"up={up} dn={down} spread={spread:.4f}"
        )
        self.ask_oid = submit_limit(
            trader, self.symbol, "SELL", 1, my_ask,
            sim_time, current_signal,
            f"up={up} dn={down} spread={spread:.4f}"
        )

        log(sim_time, self.symbol, "QUOTED",
            current_signal, pos_lots=pos,
            detail=f"BUY@{my_bid} SELL@{my_ask} "
                   f"up={up} dn={down}")

        self.prev_signal = current_signal


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def run(trader, end_time):
    ensure_log()
    machines = {sym: TickerMM(sym) for sym in SYMBOLS}

    print(f"[MM] Starting | tickers={SYMBOLS} | end={end_time}",
          flush=True)

    while trader.get_last_trade_time() < end_time:
        sim_time = trader.get_last_trade_time()

        for sym, mm in machines.items():
            try:
                mm.tick(trader, sim_time)
            except Exception as e:
                print(f"[ERROR][{sym}] {e}", flush=True)

        time.sleep(POLL_INTERVAL)

    # Shutdown
    print("[MM] Session ending — cancelling all and flattening",
          flush=True)
    for sym in SYMBOLS:
        cancel_symbol(trader, sym)
        pos = get_pos(trader, sym)
        if pos != 0:
            side = "SELL" if pos > 0 else "BUY"
            order = (
                shift.Order(shift.Order.Type.MARKET_SELL, sym, abs(pos))
                if side == "SELL"
                else shift.Order(shift.Order.Type.MARKET_BUY, sym, abs(pos))
            )
            trader.submit_order(order)
            print(f"[SHUTDOWN] {side} {abs(pos)}L {sym} at market",
                  flush=True)
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
        end_time = (trader.get_last_trade_time()
                    + timedelta(minutes=380.0))
        try:
            run(trader, end_time)
        except KeyboardInterrupt:
            for sym in SYMBOLS:
                cancel_symbol(trader, sym)
            trader.disconnect()