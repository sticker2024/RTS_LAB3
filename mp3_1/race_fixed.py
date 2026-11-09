"""Mini Project 3.1 - Step 4: Same counter protected by a mutex-like Lock (Section 3.4 / 3.8)."""
import threading
import time

shared_counter = 0
counter_lock = threading.Lock()


def increment_safe(iterations):
    global shared_counter
    for _ in range(iterations):
        with counter_lock:              # critical section: read-modify-write is now serialized
            value = shared_counter
            time.sleep(0.0001)
            shared_counter = value + 1


def run(num_threads=5, iterations=1000):
    global shared_counter
    shared_counter = 0
    threads = [threading.Thread(target=increment_safe, args=(iterations,)) for _ in range(num_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return shared_counter


if __name__ == "__main__":
    # 5 threads x 400 iterations here (the lock serializes the sleep, so 5x1000 would take ~10 s per run).
    # Pass iterations as argv[1] to use the full 1000.
    import sys
    iters = int(sys.argv[1]) if len(sys.argv) > 1 else 1000
    expected = 5 * iters
    result = run(5, iters)
    print(f"threads=5 iterations={iters} expected={expected} actual={result} lost_updates={expected - result}")
