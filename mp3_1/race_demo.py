"""Mini Project 3.1 - Step 1/2: Unsafe shared counter (lost-update race condition, Section 3.8)."""
import threading
import time

shared_counter = 0


def increment_unsafe(iterations):
    global shared_counter
    for _ in range(iterations):
        value = shared_counter          # read
        time.sleep(0.0001)              # widen the interleaving window
        shared_counter = value + 1      # write back (non-atomic read-increment-write)


def run(num_threads=5, iterations=1000):
    global shared_counter
    shared_counter = 0
    threads = [threading.Thread(target=increment_unsafe, args=(iterations,)) for _ in range(num_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return shared_counter


if __name__ == "__main__":
    import sys
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 5
    expected = n * 1000
    result = run(n)
    print(f"threads={n} expected={expected} actual={result} lost_updates={expected - result}")
