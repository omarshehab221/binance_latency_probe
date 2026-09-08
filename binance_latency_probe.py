#!/usr/bin/env python3
"""
Binance Spot Price Latency Monitor
-----------------------------------
Repeatedly fetches BTCUSDT spot price from Binance's public REST API
and measures round-trip latency. Useful for benchmarking how close
your deployment region actually is to Binance's matching engine
(AWS Tokyo / ap-northeast-1).

No external dependencies - uses only the Python standard library,
so it runs anywhere with Python 3 installed.

Configure via environment variables (all optional):
  BINANCE_SYMBOL            e.g. BTCUSDT (default: BTCUSDT)
  POLL_INTERVAL_SECONDS     seconds between requests (default: 1)
  STATS_EVERY_N_SAMPLES     print a stats summary every N samples (default: 60)
  REQUEST_TIMEOUT_SECONDS   per-request timeout (default: 5)
"""

import os
import time
import json
import statistics
import urllib.request
import urllib.error
from collections import deque
from datetime import datetime, timezone

# --- Configuration ---
SYMBOL = os.environ.get("BINANCE_SYMBOL", "BTCUSDT")
INTERVAL_SECONDS = float(os.environ.get("POLL_INTERVAL_SECONDS", "1"))
STATS_EVERY = int(os.environ.get("STATS_EVERY_N_SAMPLES", "60"))
TIMEOUT_SECONDS = float(os.environ.get("REQUEST_TIMEOUT_SECONDS", "5"))

URL = f"https://api.binance.com/api/v3/ticker/price?symbol={SYMBOL}"

recent_latencies = deque(maxlen=max(STATS_EVERY, 1))
total_count = 0
error_count = 0


def now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + "Z"


def fetch_price():
    """Send one request to Binance. Returns (latency_ms, price) or (None, None) on failure."""
    req = urllib.request.Request(URL, headers={"User-Agent": "latency-probe/1.0"})
    start = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=TIMEOUT_SECONDS) as resp:
            body = resp.read()
        end = time.perf_counter()
        data = json.loads(body)
        return (end - start) * 1000, data.get("price")
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, ValueError) as exc:
        print(f"[{now()}] ERROR fetching price: {exc}", flush=True)
        return None, None


def print_stats():
    if not recent_latencies:
        return
    data = sorted(recent_latencies)
    n = len(data)
    avg = statistics.mean(data)
    p50 = data[int(n * 0.50)]
    p95 = data[min(int(n * 0.95), n - 1)]
    p99 = data[min(int(n * 0.99), n - 1)]
    print(
        f"[{now()}] STATS  n={n}  min={data[0]:.1f}ms  avg={avg:.1f}ms  "
        f"p50={p50:.1f}ms  p95={p95:.1f}ms  p99={p99:.1f}ms  max={data[-1]:.1f}ms  "
        f"errors={error_count}/{total_count}",
        flush=True,
    )


def main():
    global total_count, error_count
    print(f"[{now()}] Starting latency probe for {SYMBOL} -> {URL}", flush=True)
    print(
        f"[{now()}] Polling every {INTERVAL_SECONDS}s, stats every {STATS_EVERY} samples",
        flush=True,
    )

    try:
        while True:
            total_count += 1
            latency_ms, price = fetch_price()

            if latency_ms is not None:
                recent_latencies.append(latency_ms)
                print(f"[{now()}] {SYMBOL}={price}  latency={latency_ms:.1f}ms", flush=True)
            else:
                error_count += 1

            if total_count % STATS_EVERY == 0:
                print_stats()

            time.sleep(INTERVAL_SECONDS)
    except KeyboardInterrupt:
        print(f"\n[{now()}] Stopped. Final stats:", flush=True)
        print_stats()


if __name__ == "__main__":
    main()
