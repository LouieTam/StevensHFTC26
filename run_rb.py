import shift
import time
import csv
import os
from collections import deque
from datetime import datetime, timedelta

SYMBOLS         = ["AAPL", "MSFT", "NVDA", "IBM", "JPM"]
POLL_INTERVAL   = 1.0     # seconds between ticks
BAR_SECONDS     = 5       # seconds per MACD bar — one "period"
MACD_FAST       = 12      # fast EMA period (12 bars = 60s)
MACD_SLOW       = 26      # slow EMA period (26 bars = 130s)
MACD_SIGNAL     = 9       # signal line EMA period

OUTPUT_DIR      = "vwap_macd_data"

# ---------------------------------------------------------------------------
# VWAP state
# ---------------------------------------------------------------------------

class VWAPTracker:
    """
    Rolling VWAP from session start.
    Updates only when either last_price or last_size changes from
    the previous tick — constant values mean no new trade executed.
    """
    def __init__(self):
        self.cumulative_pv  = 0.0
        self.cumulative_vol = 0.0
        self.vwap           = None
        self.prev_price     = None
        self.prev_size      = None

    def update(self, price, size):
        """
        Returns (vwap, updated) where updated=True if a new trade was detected.

        Logic:
          tick N:   price=100, size=2  → first tick, initialise
          tick N+1: price=100, size=2  → no change → no new trade → skip
          tick N+2: price=100, size=3  → size changed by +1 → 1 lot @ 100 traded
          tick N+3: price=102, size=3  → price changed → new trade @ 102, size=3

        When only size increases: the increment (size - prev_size) lots traded
        at the current price.
        When price changes: size lots traded at the new price.
        When both change: treat as new trade of size lots at new price.
        """
        if price <= 0 or size <= 0:
            return self.vwap, False

        # First tick — initialise without updating VWAP (we don't know
        # if this is a new trade or just the starting snapshot)
        if self.prev_price is None:
            self.prev_price = price
            self.prev_size  = size
            return self.vwap, False

        price_changed = (price != self.prev_price)
        size_changed  = (size  != self.prev_size)

        if not price_changed and not size_changed:
            # No new trade
            return self.vwap, False

        # Determine trade volume:
        # - If only size increased: the increment is the new volume
        # - If price changed (with or without size change): use current size
        if price_changed:
            trade_vol = float(size)
        else:
            # price unchanged, size changed
            delta = size - self.prev_size
            if delta <= 0:
                # Size decreased — can happen if a resting order was cancelled
                # and the last_size API reflects something else. Skip.
                self.prev_price = price
                self.prev_size  = size
                return self.vwap, False
            trade_vol = float(delta)

        self.cumulative_pv  += price * trade_vol
        self.cumulative_vol += trade_vol
        self.vwap            = self.cumulative_pv / self.cumulative_vol

        self.prev_price = price
        self.prev_size  = size
        return self.vwap, True

# ---------------------------------------------------------------------------
# MACD state
# ---------------------------------------------------------------------------

class EMATracker:
    """Single EMA with configurable period."""
    def __init__(self, period):
        self.period = period
        self.alpha  = 2.0 / (period + 1)
        self.value  = None
        self.count  = 0   # bars seen so far

    def update(self, price):
        self.count += 1
        if self.value is None:
            self.value = price   # seed with first price
        else:
            self.value = self.alpha * price + (1 - self.alpha) * self.value
        return self.value

    @property
    def ready(self):
        # EMA is meaningful after at least `period` bars
        return self.count >= self.period


class MACDTracker:
    """
    Standard MACD = EMA(fast) - EMA(slow)
    Signal line   = EMA(MACD, signal_period)
    Histogram     = MACD - Signal

    One "bar" = BAR_SECONDS seconds of mid-price data.
    We take the last mid price in each bar window as the bar close.
    """
    def __init__(self, fast=MACD_FAST, slow=MACD_SLOW, signal=MACD_SIGNAL):
        self.fast_ema   = EMATracker(fast)
        self.slow_ema   = EMATracker(slow)
        self.signal_ema = EMATracker(signal)
        self.macd_line  = None
        self.signal_line = None
        self.histogram  = None

    def update_bar(self, bar_close):
        """Call once per completed bar with the bar's close price."""
        fast_val = self.fast_ema.update(bar_close)
        slow_val = self.slow_ema.update(bar_close)

        # MACD line only meaningful once slow EMA has enough history
        if not self.slow_ema.ready:
            return None, None, None

        self.macd_line = fast_val - slow_val

        # Signal line is EMA of MACD line
        self.signal_line = self.signal_ema.update(self.macd_line)

        if self.signal_ema.ready:
            self.histogram = self.macd_line - self.signal_line
        else:
            self.histogram = None

        return self.macd_line, self.signal_line, self.histogram

    @property
    def ready(self):
        return self.signal_ema.ready


class BarAggregator:
    """
    Accumulates mid prices every tick and closes a bar every BAR_SECONDS.
    Returns the bar close price when a bar completes, else None.
    """
    def __init__(self, bar_seconds=BAR_SECONDS):
        self.bar_seconds   = bar_seconds
        self.bar_start_ts  = None
        self.last_mid      = None

    def update(self, mid, wall_ts):
        """Returns bar_close if bar just completed, else None."""
        self.last_mid = mid

        if self.bar_start_ts is None:
            self.bar_start_ts = wall_ts
            return None

        elapsed = wall_ts - self.bar_start_ts
        if elapsed >= self.bar_seconds:
            self.bar_start_ts = wall_ts
            return mid   # use last mid of the bar as bar close

        return None

# ---------------------------------------------------------------------------
# CSV output
# ---------------------------------------------------------------------------

def get_output_path(symbol):
    return os.path.join(OUTPUT_DIR, f"{symbol}_vwap_macd.csv")

def ensure_csv(symbol):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    path = get_output_path(symbol)
    if not os.path.exists(path):
        with open(path, "w", newline="") as f:
            csv.writer(f).writerow([
                "wall_time", "sim_time", "symbol",
                "last_price", "last_size",
                "trade_detected", "vwap",
                "mid", "bar_close",
                "macd_line", "signal_line", "histogram",
            ])
    return path

def append_row(path, row):
    with open(path, "a", newline="") as f:
        csv.writer(f).writerow(row)

# ---------------------------------------------------------------------------
# Per-ticker state container
# ---------------------------------------------------------------------------

class TickerState:
    """Holds all stateful trackers for a single symbol."""
    def __init__(self, symbol):
        self.symbol       = symbol
        self.vwap_tracker = VWAPTracker()
        self.bar_agg      = BarAggregator(BAR_SECONDS)
        self.macd_tracker = MACDTracker(MACD_FAST, MACD_SLOW, MACD_SIGNAL)
        self.csv_path     = ensure_csv(symbol)

    def process_tick(self, trader, wall_ts, sim_time):
        """Collect data, update all indicators, log one CSV row."""
        symbol = self.symbol

        # ── Last trade price and size ─────────────────────────────────────────
        try:
            last_price = float(trader.get_last_price(symbol))
        except Exception:
            last_price = 0.0
        try:
            last_size = int(trader.get_last_size(symbol))
        except Exception:
            last_size = 0

        # ── VWAP ─────────────────────────────────────────────────────────────
        vwap, trade_detected = self.vwap_tracker.update(last_price, last_size)

        # ── Mid price ─────────────────────────────────────────────────────────
        try:
            bo = trader.get_order_book(symbol, shift.OrderBookType.GLOBAL_BID)
            ao = trader.get_order_book(symbol, shift.OrderBookType.GLOBAL_ASK)
            if bo and ao and bo[0].price > 0 and ao[0].price > 0:
                mid = round(0.5 * (float(bo[0].price) + float(ao[0].price)), 4)
            else:
                mid = last_price
        except Exception:
            mid = last_price

        # ── MACD bar aggregation ──────────────────────────────────────────────
        bar_close    = self.bar_agg.update(mid, wall_ts)
        macd_line    = None
        signal_line  = None
        histogram    = None

        if bar_close is not None:
            macd_line, signal_line, histogram = self.macd_tracker.update_bar(bar_close)
            if self.macd_tracker.ready:
                print(f"[MACD][{symbol}] bar={bar_close:.4f} | "
                      f"MACD={macd_line:.4f} | "
                      f"Sig={signal_line:.4f} | "
                      f"Hist={histogram:.4f}", flush=True)

        # ── CSV row ───────────────────────────────────────────────────────────
        append_row(self.csv_path, [
            datetime.now().isoformat(),
            str(sim_time),
            symbol,
            f"{last_price:.4f}",
            last_size,
            int(trade_detected),
            f"{vwap:.4f}"        if vwap        is not None else "",
            f"{mid:.4f}",
            f"{bar_close:.4f}"   if bar_close   is not None else "",
            f"{macd_line:.6f}"   if macd_line   is not None else "",
            f"{signal_line:.6f}" if signal_line is not None else "",
            f"{histogram:.6f}"   if histogram   is not None else "",
        ])

        return last_price, last_size, trade_detected, vwap, mid

# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def run(trader, symbols=SYMBOLS, end_time=None):
    # Initialise per-ticker state
    states = {sym: TickerState(sym) for sym in symbols}

    print(f"[RECORDER] Tickers: {symbols}", flush=True)
    print(f"[RECORDER] MACD({MACD_FAST},{MACD_SLOW},{MACD_SIGNAL}) "
          f"| bar={BAR_SECONDS}s | end={end_time}", flush=True)
    print(f"[RECORDER] Output dir: ./{OUTPUT_DIR}/", flush=True)

    while trader.get_last_trade_time() < end_time:
        wall_ts  = time.time()
        sim_time = trader.get_last_trade_time()

        # Process every ticker each tick
        for sym, state in states.items():
            try:
                last_price, last_size, trade_detected, vwap, mid = \
                    state.process_tick(trader, wall_ts, sim_time)
                macd_status = (
                    f"MACD=ready"
                    if state.macd_tracker.ready
                    else f"warmup({state.macd_tracker.slow_ema.count}/{MACD_SLOW}bars)"
                )
                print(
                    f"[{sim_time}][{sym}] "
                    f"Price={last_price:.2f} Size={last_size} "
                    f"Trade={'YES' if trade_detected else 'no ':3s} | "
                    f"VWAP={f'{vwap:.4f}' if vwap is not None else 'n/a':>10s} | "
                    f"Mid={mid:.4f} | {macd_status}",
                    flush=True
                )
            except Exception as e:
                print(f"[ERROR][{sym}] {e}", flush=True)

        elapsed   = time.time() - wall_ts
        sleep_for = max(0.0, POLL_INTERVAL - elapsed)
        time.sleep(sleep_for)

    print(f"[RECORDER] Done. Files written to ./{OUTPUT_DIR}/", flush=True)
    for sym in symbols:
        print(f"  → {get_output_path(sym)}", flush=True)


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
            run(trader, symbols=SYMBOLS, end_time=end_time)
        except KeyboardInterrupt:
            trader.disconnect()