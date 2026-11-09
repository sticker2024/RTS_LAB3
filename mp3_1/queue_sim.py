"""Mini Project 3.1 - Steps 5/6: bounded queue.Queue as an RTOS message queue (Section 3.4)."""
import queue
import statistics
import sys
import threading
import time

N_ITEMS = 20


def run(maxsize=5, consumer_delay=0.0, producer_delay=0.0):
    q = queue.Queue(maxsize=maxsize)
    latencies = []
    blocked_puts = 0

    def producer_task():
        nonlocal blocked_puts
        for i in range(N_ITEMS):
            if q.full():
                blocked_puts += 1            # producer is about to block (queue full)
            q.put((i, time.perf_counter()))  # blocks automatically when full
            if producer_delay:
                time.sleep(producer_delay)

    def consumer_task():
        for _ in range(N_ITEMS):
            _, sent = q.get()                # blocks when empty
            latencies.append((time.perf_counter() - sent) * 1000.0)  # ms
            if consumer_delay:
                time.sleep(consumer_delay)

    p = threading.Thread(target=producer_task)
    c = threading.Thread(target=consumer_task)
    p.start(); c.start(); p.join(); c.join()
    return min(latencies), max(latencies), statistics.mean(latencies), blocked_puts


def show(label, res):
    mn, mx, avg, blk = res
    print(f"{label:<44} min={mn:8.3f} ms  max={mx:8.3f} ms  avg={avg:8.3f} ms  producer_blocks={blk}")


if __name__ == "__main__":
    print("== Baseline vs slowed consumer (maxsize=5) ==")
    show("fast consumer (no delay)", run(5, 0.0))
    show("slow consumer (5 ms delay)", run(5, 0.005))
    show("slower consumer (20 ms delay)", run(5, 0.020))
    print()
    print("== Lab task: vary maxsize (consumer delay 5 ms) ==")
    print("maxsize | avg latency (ms) | max latency (ms) | producer blocks")
    for size in (1, 2, 5, 10, 20):
        mn, mx, avg, blk = run(size, 0.005)
        print(f"{size:7d} | {avg:16.3f} | {mx:16.3f} | {blk:15d}")
