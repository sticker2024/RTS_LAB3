"""Corrected version: EVERY task acquires buffer_lock before stats_lock (global lock ordering). Expected: completes."""
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
                 total_items=a.items, lock_order="ordered")
    print(f"ORDERED locks (buffer_lock -> stats_lock everywhere): capacity={a.capacity}, "
          f"{a.producers} producers, {a.consumers} consumers, {a.items} items")
    r = run_simulation(cfg)
    if r["deadlock"]:
        print("UNEXPECTED: watchdog fired", r)
    else:
        print(f"Completed: produced={r['produced']} consumed={r['consumed']} elapsed={r['elapsed']:.2f} s "
              f"throughput={r['throughput']:.1f} items/s avg_block={r['avg_block_ms']:.2f} ms")
