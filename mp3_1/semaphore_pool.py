"""Mini Project 3.1 - Steps 7/8: counting semaphore as a pool of fixed-size resource blocks (Sections 3.4, 3.6)."""
import random
import statistics
import sys
import threading
import time


def run(pool_size=3, n_tasks=8, seed=1):
    rng = random.Random(seed)
    holds = [rng.uniform(0.05, 0.15) for _ in range(n_tasks)]
    sem = threading.Semaphore(pool_size)
    state_lock = threading.Lock()
    current = 0
    peak = 0
    waits = [0.0] * n_tasks

    def task(i):
        nonlocal current, peak
        t0 = time.perf_counter()
        sem.acquire()
        waits[i] = (time.perf_counter() - t0) * 1000.0
        with state_lock:
            current += 1
            peak = max(peak, current)
        time.sleep(holds[i])
        with state_lock:
            current -= 1
        sem.release()

    ts = [threading.Thread(target=task, args=(i,)) for i in range(n_tasks)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    return waits, peak


if __name__ == "__main__":
    for size in (3, 1, 6):
        waits, peak = run(size)
        immediate = sum(1 for w in waits if w < 1.0)
        waiting = len(waits) - immediate
        print(f"\n-- pool_size={size} --")
        for i, w in enumerate(waits):
            print(f"  task {i}: wait = {w:8.2f} ms")
        print(f"  peak simultaneous holders = {peak} (limit {size})")
        print(f"  tasks proceeding immediately (<1 ms wait) = {immediate}, tasks that waited = {waiting}")
        print(f"  mean wait = {statistics.mean(waits):.2f} ms, max wait = {max(waits):.2f} ms")
