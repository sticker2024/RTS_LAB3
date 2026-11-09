"""Mini Project 3.1 - Step 9: thread dispatch latency as an analogy to context-switch / ISR latency (Sections 3.2, 3.5)."""
import statistics
import threading
import time

ITERATIONS = 200


def measure():
    samples = []
    for _ in range(ITERATIONS):
        ev = threading.Event()
        t = threading.Thread(target=ev.set)
        t0 = time.perf_counter()
        t.start()
        ev.wait()
        samples.append((time.perf_counter() - t0) * 1e6)   # microseconds
        t.join()
    return samples


if __name__ == "__main__":
    s = sorted(measure())
    p95 = s[int(0.95 * len(s)) - 1]
    print(f"samples                      : {len(s)}")
    print(f"minimum                      : {s[0]:9.1f} us")
    print(f"mean                         : {statistics.mean(s):9.1f} us")
    print(f"95th percentile              : {p95:9.1f} us")
    print(f"maximum (worst case observed): {s[-1]:9.1f} us")
    print(f"max / mean ratio             : {s[-1] / statistics.mean(s):9.1f}x")
