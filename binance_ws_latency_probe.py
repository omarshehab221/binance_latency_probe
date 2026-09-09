#!/usr/bin/env python3
"""
Binance WebSocket Latency Probe - Optimized
---------------------------------------------
Minimizes Python/asyncio-side overhead so the measured number reflects
genuine network + Binance server-side RTT, not artifacts of garbage
collection pauses, logging I/O, or event-loop contention.

Realistic expectations: even fully optimized and same-region on AWS,
expect roughly high-hundreds-of-microseconds to low-single-digit
milliseconds at best - not nanoseconds. Nanosecond round trips only
exist for same-machine shared memory or specialized hardware
(FPGA-to-FPGA), never for two separate parties' servers over any
IP network, however physically close.

Requires: websockets (required), uvloop (optional but recommended)
  pip install websockets uvloop

Configure via environment variables:
  BINANCE_SYMBOL          default: btcusdt
  STREAM                  default: ticker (low-frequency stream -> less
                           event-loop contention than bookTicker)
  PING_INTERVAL_SECONDS   default: 0.2
  STATS_EVERY_N_SAMPLES   default: 200
  PING_TIMEOUT_SECONDS    default: 5
"""

import os
import gc
import socket
import asyncio
import time
import statistics
from collections import deque
from datetime import datetime, timezone

import websockets

try:
    import uvloop
    uvloop.install()
    EVENT_LOOP = "uvloop"
except ImportError:
    EVENT_LOOP = "asyncio default (install uvloop for lower overhead)"

SYMBOL = os.environ.get("BINANCE_SYMBOL", "btcusdt").lower()
STREAM = os.environ.get("STREAM", "ticker")
PING_INTERVAL = float(os.environ.get("PING_INTERVAL_SECONDS", "0.2"))
STATS_EVERY = int(os.environ.get("STATS_EVERY_N_SAMPLES", "200"))
PING_TIMEOUT = float(os.environ.get("PING_TIMEOUT_SECONDS", "5"))

URL = f"wss://stream.binance.com:9443/ws/{SYMBOL}@{STREAM}"

recent_latencies_us = deque(maxlen=max(STATS_EVERY, 1))
total_pings = 0
error_count = 0


def now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + "Z"


def print_stats():
    if not recent_latencies_us:
        return
    data = sorted(recent_latencies_us)
    n = len(data)
    avg = statistics.mean(data)
    p50 = data[int(n * 0.50)]
    p95 = data[min(int(n * 0.95), n - 1)]
    p99 = data[min(int(n * 0.99), n - 1)]
    print(
        f"[{now()}] STATS  n={n}  min={data[0]:.0f}us  avg={avg:.0f}us  "
        f"p50={p50:.0f}us  p95={p95:.0f}us  p99={p99:.0f}us  max={data[-1]:.0f}us  "
        f"errors={error_count}/{total_pings}  loop={EVENT_LOOP}",
        flush=True,
    )


async def drain(ws):
    """Consume incoming frames without parsing them. We no longer need the
    payload, just an open connection to ping against - so skip JSON decoding
    and printing entirely to keep the event loop as free as possible."""
    async for _ in ws:
        pass


async def ping_loop(ws):
    global total_pings, error_count
    while True:
        await asyncio.sleep(PING_INTERVAL)
        total_pings += 1
        start_ns = time.perf_counter_ns()
        try:
            pong_waiter = await ws.ping()
            await asyncio.wait_for(pong_waiter, timeout=PING_TIMEOUT)
            latency_us = (time.perf_counter_ns() - start_ns) / 1000
            recent_latencies_us.append(latency_us)
        except Exception:
            error_count += 1

        if total_pings % STATS_EVERY == 0:
            print_stats()


def tune_socket(ws):
    """Explicitly disable Nagle's algorithm - small control frames should be
    sent immediately, not batched by the kernel waiting for more data."""
    try:
        sock = ws.transport.get_extra_info("socket")
        if sock is not None:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    except Exception:
        pass


def pin_to_core():
    """Best-effort: pin this process to one CPU core to reduce context-switch
    and cache-migration jitter. Linux only; safe no-op elsewhere."""
    try:
        os.sched_setaffinity(0, {0})
    except (AttributeError, OSError):
        pass


async def main():
    pin_to_core()
    gc.disable()  # avoid GC pauses injecting latency spikes into ping timing

    print(f"[{now()}] Connecting to {URL}  (event loop: {EVENT_LOOP})", flush=True)
    backoff = 1
    while True:
        try:
            async with websockets.connect(URL, ping_interval=None) as ws:
                tune_socket(ws)
                print(f"[{now()}] Connected. Pinging every {PING_INTERVAL}s.", flush=True)
                backoff = 1
                await asyncio.gather(drain(ws), ping_loop(ws))
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
