"""Mini Project 3.3 - Priority-based interrupt latency simulator (Section 3.5).

Model (all times in milliseconds, measured with time.perf_counter):
  * Each interrupt source is a periodic thread. At each release it records the ASSERTION timestamp.
  * TOP HALF  : runs as soon as the simulated CPU can accept an interrupt, i.e. when no other top half is
                running and the current bottom-half *slice* (the "interrupt-masked window", SLICE_MS) ends.
                Top halves ALWAYS preempt bottom-half work. Interrupt latency = top-half start - assertion.
  * BOTTOM HALF: at the end of the top half a bottom-half task (priority, assertion time, duration) is pushed
                onto a heapq priority queue. ONE CPU worker always runs the highest-priority ready bottom half
                in SLICE_MS slices; between slices it yields to any pending top half and, if preempted by a
                higher-priority bottom half, the remainder is re-queued.
                Response latency = bottom-half completion - assertion (Section 3.5).
Simplification: top halves are serialised (not nested); a top half waits for the running one to finish.
"""
import csv
import heapq
import statistics
import threading
import time
from dataclasses import dataclass

SLICE_MS = 1.0     # granularity at which the CPU can switch (models the interrupt-masked window)


@dataclass
class Source:
    name: str
    priority: int        # lower number = higher priority
    period_ms: float
    top_ms: float
    bottom_ms: float


def now_ms():
    return time.perf_counter() * 1000.0


def sleep_ms(ms):
    if ms > 0:
        time.sleep(ms / 1000.0)


def simulate(sources, duration_s=10.0, record_timeline=False):
    cv = threading.Condition()
    heap = []                                   # (priority, assertion_ms, seq, source_name, remaining_ms)
    state = {"isr_running": False, "isr_waiting": 0, "in_slice": False, "stop": False, "seq": 0}
    log = {s.name: {"irq_lat": [], "top": [], "resp": []} for s in sources}
    timeline = []
    t_origin = now_ms()

    def source_thread(src):
        next_release = now_ms() + src.period_ms * (0.3 + 0.2 * src.priority)   # staggered start
        while True:
            sleep_ms(next_release - now_ms())
            if state["stop"]:
                return
            asserted = now_ms()
            with cv:
                state["isr_waiting"] += 1
                while state["isr_running"] or state["in_slice"]:
                    cv.wait()
                state["isr_waiting"] -= 1
                state["isr_running"] = True
            top_start = now_ms()
            sleep_ms(src.top_ms)                # top-half work (acknowledge, capture data)
            top_end = now_ms()
            with cv:
                state["isr_running"] = False
                state["seq"] += 1
                heapq.heappush(heap, (src.priority, asserted, state["seq"], src.name, src.bottom_ms))
                cv.notify_all()
            log[src.name]["irq_lat"].append(top_start - asserted)
            log[src.name]["top"].append(top_end - top_start)
            if record_timeline:
                timeline.append((top_start - t_origin, top_end - t_origin, src.name, "top"))
            next_release += src.period_ms

    def cpu_worker():
        while True:
            with cv:
                while not state["stop"] and (not heap or state["isr_running"] or state["isr_waiting"] > 0):
                    cv.wait(0.05)
                if state["stop"]:
                    return
                prio, asserted, seq, name, remaining = heapq.heappop(heap)   # highest-priority ready bottom half
                state["in_slice"] = True
            run = min(SLICE_MS, remaining)
            s0 = now_ms()
            sleep_ms(run)
            s1 = now_ms()
            with cv:
                state["in_slice"] = False
                remaining -= run
                if remaining > 1e-9:
                    heapq.heappush(heap, (prio, asserted, seq, name, remaining))   # preempted / unfinished
                else:
                    log[name]["resp"].append(now_ms() - asserted)
                cv.notify_all()
            if record_timeline:
                timeline.append((s0 - t_origin, s1 - t_origin, name, "bottom"))

    threads = [threading.Thread(target=source_thread, args=(s,), daemon=True) for s in sources]
    worker = threading.Thread(target=cpu_worker, daemon=True)
    worker.start()
    for t in threads:
        t.start()
    time.sleep(duration_s)
    with cv:
        state["stop"] = True
        cv.notify_all()
    time.sleep(0.2)
    return log, timeline


def pct(data, p):
    s = sorted(data)
    return s[min(len(s) - 1, max(0, int(round(p / 100 * len(s))) - 1))]


def summarize(label, sources, log):
    rows = []
    print(f"\n=== {label} ===")
    print(f"{'source':<8}{'prio':>5}{'n':>6} | {'resp min':>9}{'mean':>9}{'p95':>9}{'max':>9} | {'irq-lat mean':>13}{'max':>8} | {'top mean':>9}")
    for s in sources:
        d = log[s.name]
        if not d["resp"]:
            continue
        r, il, tp = d["resp"], d["irq_lat"], d["top"]
        row = dict(config=label, source=s.name, priority=s.priority, n=len(r),
                   resp_min=min(r), resp_mean=statistics.mean(r), resp_p95=pct(r, 95), resp_max=max(r),
                   irq_mean=statistics.mean(il), irq_max=max(il), top_mean=statistics.mean(tp))
        rows.append(row)
        print(f"{s.name:<8}{s.priority:>5}{len(r):>6} | {row['resp_min']:9.2f}{row['resp_mean']:9.2f}{row['resp_p95']:9.2f}"
              f"{row['resp_max']:9.2f} | {row['irq_mean']:13.2f}{row['irq_max']:8.2f} | {row['top_mean']:9.2f}   (ms)")
    return rows


def base_sources(s1_period=20.0, s2_top=0.5, s1_top=0.5, s3_top=1.0):
    return [
        Source("UART",  1, s1_period, s1_top, 3.0),     # highest priority
        Source("ADC",   2, 50.0,      s2_top, 8.0),
        Source("ETH",   3, 100.0,     s3_top, 15.0),    # lowest priority
    ]


if __name__ == "__main__":
    import sys
    DUR = float(sys.argv[1]) if len(sys.argv) > 1 else 10.0
    SWEEP_DUR = float(sys.argv[2]) if len(sys.argv) > 2 else 6.0
    all_rows = []

    # Config A: well-behaved baseline (short top halves)
    src = base_sources()
    logA, timeline = simulate(src, DUR, record_timeline=True)
    all_rows += summarize("A: baseline (short top halves)", src, logA)

    # Config B: ADC (middle priority) has a deliberately long top half (violates Section 3.5 guidance)
    src = base_sources(s2_top=12.0)
    logB, _ = simulate(src, DUR)
    all_rows += summarize("B: long ADC top half (12 ms)", src, logB)

    # Config C: long top half on the highest-priority source
    src = base_sources(s1_top=8.0)
    logC, _ = simulate(src, DUR)
    all_rows += summarize("C: long UART top half (8 ms)", src, logC)

    # Arrival-rate sweep: shorten the highest-priority source's period, watch the lowest-priority source (ETH)
    print("\n=== Sweep: UART period vs ETH (lowest priority) response latency ===")
    print(f"{'UART period (ms)':>17}{'UART load %':>13}{'ETH n':>7}{'ETH mean':>10}{'ETH p95':>9}{'ETH max':>9}")
    sweep_rows = []
    for period in (40.0, 20.0, 12.0, 8.0):
        src = base_sources(s1_period=period)
        lg, _ = simulate(src, SWEEP_DUR)
        r = lg["ETH"]["resp"]
        load = (3.0 + 0.5) / period * 100
        sweep_rows.append(dict(uart_period_ms=period, uart_load_pct=load, eth_n=len(r),
                               eth_mean=statistics.mean(r), eth_p95=pct(r, 95), eth_max=max(r)))
        print(f"{period:17.1f}{load:13.1f}{len(r):7d}{statistics.mean(r):10.2f}{pct(r,95):9.2f}{max(r):9.2f}")

    with open("results_configs.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(all_rows[0].keys())); w.writeheader(); w.writerows(all_rows)
    with open("results_sweep.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(sweep_rows[0].keys())); w.writeheader(); w.writerows(sweep_rows)

    # Optional timeline plot: 200 ms window from the baseline run showing preemption
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        lo = 2000.0
        hi = lo + 200.0
        names = ["UART", "ADC", "ETH"]
        colors = {"UART": "#1f4e79", "ADC": "#c55a11", "ETH": "#548235"}
        fig, ax = plt.subplots(figsize=(10, 3.6))
        for (a, b, n, kind) in timeline:
            if b < lo or a > hi:
                continue
            y = names.index(n)
            ax.barh(y + (0.2 if kind == "top" else -0.2), b - a, left=a, height=0.35,
                    color=colors[n], alpha=1.0 if kind == "top" else 0.55,
                    hatch="//" if kind == "top" else None, edgecolor="black", linewidth=0.3)
        ax.set_xlim(lo, hi); ax.set_yticks(range(3)); ax.set_yticklabels(names)
        ax.set_xlabel("time since start (ms)")
        ax.set_title("Baseline timeline: hatched = top half (ISR), solid = bottom-half slices (1 ms each)")
        fig.tight_layout(); fig.savefig("timeline.png", dpi=150)
        print("\ntimeline saved: timeline.png")
    except ImportError:
        print("matplotlib not installed; skipping timeline plot")
