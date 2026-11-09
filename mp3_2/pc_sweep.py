"""Throughput / blocking-time vs. buffer capacity for the corrected (lock-ordered) simulation. Writes sweep_results.csv and a chart."""
import csv
import statistics
from pc_core import Config, run_simulation

CAPACITIES = [1, 2, 5, 10, 25, 50]
REPEATS = 3

if __name__ == "__main__":
    rows = []
    print("capacity | throughput (items/s) | avg blocking (ms) | producer block (ms) | consumer block (ms)")
    for cap in CAPACITIES:
        res = [run_simulation(Config(capacity=cap, lock_order="ordered"), verbose=False) for _ in range(REPEATS)]
        assert not any(r["deadlock"] for r in res)
        row = {
            "capacity": cap,
            "throughput": statistics.mean(r["throughput"] for r in res),
            "avg_block_ms": statistics.mean(r["avg_block_ms"] for r in res),
            "prod_block_ms": statistics.mean(r["avg_prod_block_ms"] for r in res),
            "cons_block_ms": statistics.mean(r["avg_cons_block_ms"] for r in res),
        }
        rows.append(row)
        print(f"{cap:8d} | {row['throughput']:20.1f} | {row['avg_block_ms']:17.2f} | "
              f"{row['prod_block_ms']:19.2f} | {row['cons_block_ms']:19.2f}")
    with open("sweep_results.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader(); w.writerows(rows)
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        fig, ax1 = plt.subplots(figsize=(7, 4))
        caps = [r["capacity"] for r in rows]
        ax1.plot(caps, [r["throughput"] for r in rows], "o-", color="#1f4e79", label="Throughput")
        ax1.set_xscale("log"); ax1.set_xlabel("Buffer capacity (items, log scale)")
        ax1.set_ylabel("Throughput (items/s)", color="#1f4e79")
        ax2 = ax1.twinx()
        ax2.plot(caps, [r["avg_block_ms"] for r in rows], "s--", color="#c55a11", label="Avg blocking time")
        ax2.set_ylabel("Avg blocking time (ms)", color="#c55a11")
        ax1.set_title("Throughput and blocking time vs buffer capacity (3 producers, 3 consumers)")
        fig.tight_layout(); fig.savefig("throughput_vs_capacity.png", dpi=150)
        print("chart saved: throughput_vs_capacity.png")
    except ImportError:
        print("matplotlib not installed - use sweep_results.csv to chart in a spreadsheet")
