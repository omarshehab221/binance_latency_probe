#!/usr/bin/env python3
"""
Binance WebSocket Latency Monitor
-----------------------------------
Opens ONE persistent WebSocket connection to Binance's public market
data stream, then measures round-trip latency using WebSocket
ping/pong control frames sent over that already-established connection.

This avoids the problem with REST polling: a fresh TCP + TLS handshake
on every single request. Here the handshake happens once, and every
subsequent ping/pong is just a few bytes over a warm connection - so
the numbers reflect steady-state network RTT, not connection setup cost.

Requires: websockets (pip install websockets)

Configure via environment variables (all optional):
  BINANCE_SYMBOL          e.g. btcusdt (default: btcusdt)
  STREAM                  stream type: bookTicker, trade, aggTrade (default: bookTicker)
  PING_INTERVAL_SECONDS   seconds between latency pings (default: 1)
  STATS_EVERY_N_SAMPLES   print a stats summary every N samples (default: 60)
  PING_TIMEOUT_SECONDS    max wait for a pong before counting as error (default: 5)
"""

import os
import asyncio
import time
import json
import statistics
from collections import deque
from datetime import datetime, timezone

import websockets

SYMBOL = os.environ.get("BINANCE_SYMBOL", "btcusdt").lower()
STREAM = os.environ.get("STREAM", "bookTicker")
PING_INTERVAL = float(os.environ.get("PING_INTERVAL_SECONDS", "1"))
STATS_EVERY = int(os.environ.get("STATS_EVERY_N_SAMPLES", "60"))
PING_TIMEOUT = float(os.environ.get("PING_TIMEOUT_SECONDS", "5"))

URL = f"wss://stream.binance.com:9443/ws/{SYMBOL}@{STREAM}"

recent_latencies = deque(maxlen=max(STATS_EVERY, 1))
total_pings = 0
error_count = 0
last_price_log = 0.0


def now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + "Z"


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
        f"errors={error_count}/{total_pings}",
        flush=True,
    )


async def read_messages(ws):
    """Drain incoming stream messages in the background and log a price sample occasionally."""
    global last_price_log
    async for message in ws:
        try:
            data = json.loads(message)
        except ValueError:
            continue
        bid, ask = data.get("b"), data.get("a")
        if bid and ask:
            ts = time.time()
            if ts - last_price_log >= 5:  # throttle: log a price line every ~5s, not every tick
                print(f"[{now()}] {SYMBOL.upper()} bid={bid} ask={ask}", flush=True)
                last_price_log = ts


async def ping_loop(ws):
    """Send WS ping frames on the existing connection and time the pong (steady-state RTT)."""
    global total_pings, error_count
    while True:
        await asyncio.sleep(PING_INTERVAL)
        total_pings += 1
        start = time.perf_counter()
        try:
            pong_waiter = await ws.ping()
            await asyncio.wait_for(pong_waiter, timeout=PING_TIMEOUT)
            latency_ms = (time.perf_counter() - start) * 1000
            recent_latencies.append(latency_ms)
            print(f"[{now()}] ping latency={latency_ms:.1f}ms", flush=True)
        except Exception as exc:
            error_count += 1
            print(f"[{now()}] ERROR on ping: {exc}", flush=True)

        if total_pings % STATS_EVERY == 0:
            print_stats()


async def main():
    print(f"[{now()}] Connecting to {URL}", flush=True)
    backoff = 1
    while True:
        try:
            # ping_interval=None: we drive pings ourselves for accurate timing
            # instead of letting the library send them on its own schedule.
            async with websockets.connect(URL, ping_interval=None) as ws:
                print(
                    f"[{now()}] Connected. Measuring ping RTT every {PING_INTERVAL}s, "
                    f"stats every {STATS_EVERY} samples.",
                    flush=True,
                )
                backoff = 1
                await asyncio.gather(read_messages(ws), ping_loop(ws))
        except (websockets.exceptions.ConnectionClosed, OSError) as exc:
            print(f"[{now()}] Connection lost: {exc}. Reconnecting in {backoff}s...", flush=True)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print(f"\n[{now()}] Stopped. Final stats:", flush=True)
        print_stats()
