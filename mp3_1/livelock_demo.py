"""Mini Project 3.1 - Lab Task 3: livelock (Section 3.8).

Two threads each need BOTH locks but take them in opposite orders. When the second lock is busy they
politely release the first and retry -- but they retry in lockstep (synchronised by barriers), so they
collide every single round. Both stay active (retry counters climb) yet neither completes its work.
A randomised back-off (use_backoff=True) breaks the symmetry and lets them finish.
"""
import random
import threading
import time

ROUNDS = 500


def run(use_backoff=False, rounds=ROUNDS, seed=0):
    lock_a, lock_b = threading.Lock(), threading.Lock()
    retries = {"T1": 0, "T2": 0}
    done = {"T1": False, "T2": False}
    b1, b2, b3 = threading.Barrier(2), threading.Barrier(2), threading.Barrier(2)
    rng = random.Random(seed)

    def worker(name, first, second):
        for _ in range(rounds):
            if done[name]:
                return
            if not use_backoff:
                b1.wait()                              # lockstep start of every attempt
            got_first = first.acquire(blocking=False)
            time.sleep(0.001)                          # both threads now hold their first lock
            if not use_backoff:
                b2.wait()
            got_second = got_first and second.acquire(blocking=False)
            if not use_backoff:
                b3.wait()                              # both attempts finish before either releases
            if got_second:
                done[name] = True                      # real work would happen here
                second.release(); first.release()
                return
            if got_first:
                first.release()                        # polite back-off ...
            retries[name] += 1                         # ... and retry
            if use_backoff:
                time.sleep(rng.uniform(0, 0.005))      # randomised delay breaks lockstep

    t1 = threading.Thread(target=worker, args=("T1", lock_a, lock_b))
    t2 = threading.Thread(target=worker, args=("T2", lock_b, lock_a))
    t0 = time.perf_counter()
    t1.start(); t2.start(); t1.join(); t2.join()
    return retries, done, time.perf_counter() - t0


if __name__ == "__main__":
    r, d, el = run(use_backoff=False)
    print(f"[lockstep retry] after {ROUNDS} rounds ({el:.2f} s): retries T1={r['T1']} T2={r['T2']}; "
          f"work completed T1={d['T1']} T2={d['T2']}")
    print("  -> both threads kept running (retry counters climbing) with zero forward progress: LIVELOCK")
    r, d, el = run(use_backoff=True)
    print(f"[randomised back-off] finished in {el:.3f} s: retries T1={r['T1']} T2={r['T2']}; "
          f"work completed T1={d['T1']} T2={d['T2']}")
    print("  -> random back-off breaks the lockstep symmetry and both tasks complete")
