"""Flawed version: producers lock buffer->stats, consumers lock stats->buffer. Expected: watchdog reports deadlock."""
import argparse
from pc_core import Config, run_simulation

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--capacity", type=int, default=5)
    ap.add_argument("--producers", type=int, default=3)
    ap.add_argument("--consumers", type=int, default=3)
    ap.add_argument("--items", type=int, default=600)
    a = ap.parse_args()
    cfg = Config(capacity=a.capacity, producers=a.producers, consumers=a.consumers,
                 total_items=a.items, lock_order="flawed")
    print(f"FLAWED lock order: capacity={a.capacity}, {a.producers} producers, {a.consumers} consumers, {a.items} items")
    r = run_simulation(cfg)
    print("Result:", "DEADLOCK (simulation stalled, watchdog exited cleanly)" if r["deadlock"] else r)
