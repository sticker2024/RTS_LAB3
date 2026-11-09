"""Mini Project 3.2 - core bounded-buffer producer/consumer simulation (Section 3.8).

Two explicit locks (no queue.Queue, so acquisition order is fully controlled):
    buffer_lock : guards the bounded buffer (a plain list with a maximum length)
    stats_lock  : guards the shared statistics dictionary

lock_order = "flawed"  : producers take buffer_lock -> stats_lock, consumers take stats_lock -> buffer_lock
                         (opposite orders => circular wait is possible => deadlock)
lock_order = "ordered" : EVERY task takes buffer_lock -> stats_lock (global lock ordering => no cycle)

Coffman's four conditions in this code (see report):
  1 mutual exclusion  : threading.Lock is non-shareable
  2 hold-and-wait     : a task holds its first lock while blocking on the second
  3 no preemption     : Python locks cannot be forcibly taken from the holder
  4 circular wait     : only in "flawed" mode (producer holds B waits S, consumer holds S waits B)
"""
import random
import statistics
import threading
import time


class Config:
    def __init__(self, capacity=5, producers=3, consumers=3, total_items=600,
                 lock_order="ordered", hold_gap=0.0005, produce_time=0.001,
                 consume_time=0.001, watchdog_timeout=1.5, seed=42):
        self.capacity = capacity
        self.producers = producers
        self.consumers = consumers
        self.total_items = total_items          # must be divisible by producers and consumers
        self.lock_order = lock_order
        self.hold_gap = hold_gap                # time spent holding the first lock before asking for the second
        self.produce_time = produce_time        # work done outside any lock
        self.consume_time = consume_time
        self.watchdog_timeout = watchdog_timeout
        self.seed = seed


def run_simulation(cfg, verbose=True):
    buffer = []
    buffer_lock = threading.Lock()
    stats_lock = threading.Lock()
    stats = {"produced": 0, "consumed": 0, "total_wait": 0.0}

    # Bookkeeping used only to build the "lock-wait snapshot" when the watchdog fires.
    state_lock = threading.Lock()
    status = {}                                 # thread name -> (holding, waiting_for)
    finished = threading.Event()
    deadlock = {"detected": False, "snapshot": None}
    prod_waits, cons_waits = [], []

    def set_status(name, holding, waiting):
        with state_lock:
            status[name] = (holding, waiting)

    def acquire(lock, lock_name, name, holding):
        set_status(name, holding, lock_name)    # announce what we are blocked on
        lock.acquire()
        set_status(name, holding + [lock_name], None)

    def release_all(name):
        set_status(name, [], None)

    def producer(pid):
        name = f"producer-{pid}"
        rng = random.Random(cfg.seed + pid)
        for _ in range(cfg.total_items // cfg.producers):
            time.sleep(cfg.produce_time * rng.uniform(0.5, 1.5))      # make an item (no locks held)
            t_start = time.perf_counter()
            while True:
                acquire(buffer_lock, "buffer_lock", name, [])
                time.sleep(cfg.hold_gap)                              # hold-and-wait window
                acquire(stats_lock, "stats_lock", name, ["buffer_lock"])
                if len(buffer) < cfg.capacity:
                    buffer.append(time.perf_counter())
                    stats["produced"] += 1
                    waited = time.perf_counter() - t_start
                    stats["total_wait"] += waited
                    prod_waits.append(waited)
                    ok = True
                else:
                    ok = False                                        # buffer full: back off and retry
                stats_lock.release()
                buffer_lock.release()
                release_all(name)
                if ok:
                    break
                time.sleep(0.0005)

    def consumer(cid):
        name = f"consumer-{cid}"
        rng = random.Random(cfg.seed + 100 + cid)
        for _ in range(cfg.total_items // cfg.consumers):
            t_start = time.perf_counter()
            while True:
                if cfg.lock_order == "flawed":
                    first, first_n, second, second_n = stats_lock, "stats_lock", buffer_lock, "buffer_lock"
                else:
                    first, first_n, second, second_n = buffer_lock, "buffer_lock", stats_lock, "stats_lock"
                acquire(first, first_n, name, [])
                time.sleep(cfg.hold_gap)
                acquire(second, second_n, name, [first_n])
                if buffer:
                    buffer.pop(0)
                    stats["consumed"] += 1
                    waited = time.perf_counter() - t_start
                    stats["total_wait"] += waited
                    cons_waits.append(waited)
                    ok = True
                else:
                    ok = False                                        # buffer empty: back off and retry
                second.release()
                first.release()
                release_all(name)
                if ok:
                    break
                time.sleep(0.0005)
            time.sleep(cfg.consume_time * rng.uniform(0.5, 1.5))      # use the item (no locks held)

    def watchdog():
        last = (-1, -1)
        last_change = time.perf_counter()
        while not finished.is_set():
            time.sleep(0.1)
            cur = (stats["produced"], stats["consumed"])
            now = time.perf_counter()
            if cur != last:
                last, last_change = cur, now
            elif now - last_change >= cfg.watchdog_timeout and cur[1] < cfg.total_items:
                with state_lock:
                    snap = dict(status)
                deadlock["detected"] = True
                deadlock["snapshot"] = (cur, now - last_change, snap)
                finished.set()
                return

    producers = [threading.Thread(target=producer, args=(i,), daemon=True) for i in range(cfg.producers)]
    consumers = [threading.Thread(target=consumer, args=(i,), daemon=True) for i in range(cfg.consumers)]
    wd = threading.Thread(target=watchdog, daemon=True)

    t0 = time.perf_counter()
    for t in producers + consumers + [wd]:
        t.start()

    # Wait for completion or for the watchdog to declare deadlock.
    while not finished.is_set():
        if all(not t.is_alive() for t in producers + consumers):
            break
        time.sleep(0.05)
    elapsed = time.perf_counter() - t0
    finished.set()

    if deadlock["detected"]:
        (produced, consumed), stalled, snap = deadlock["snapshot"]
        if verbose:
            print("*** WATCHDOG: DEADLOCK DETECTED ***")
            print(f"    no progress for {stalled:.2f} s (produced={produced}, consumed={consumed}, "
                  f"target={cfg.total_items})")
            print("    lock-wait snapshot (thread: holding -> waiting for):")
            for name in sorted(snap):
                holding, waiting = snap[name]
                print(f"      {name:<11}: holding {holding if holding else '[]'} -> waiting for {waiting}")
        return {"deadlock": True, "snapshot": snap, "elapsed": elapsed,
                "produced": stats["produced"], "consumed": stats["consumed"]}

    throughput = stats["consumed"] / elapsed
    result = {
        "deadlock": False, "elapsed": elapsed, "throughput": throughput,
        "produced": stats["produced"], "consumed": stats["consumed"],
        "avg_prod_block_ms": statistics.mean(prod_waits) * 1000,
        "avg_cons_block_ms": statistics.mean(cons_waits) * 1000,
        "avg_block_ms": statistics.mean(prod_waits + cons_waits) * 1000,
    }
    return result
