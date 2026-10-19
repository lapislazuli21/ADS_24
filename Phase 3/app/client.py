import os
import random
import statistics
import time
from concurrent.futures import ThreadPoolExecutor

import rpyc

RPC_HOST  = os.getenv("RPC_HOST", "loadbalancer")
RPC_PORT  = int(os.getenv("RPC_PORT", "18812"))
RATE      = int(os.getenv("RATE", "100"))
DURATION  = int(os.getenv("DURATION", "10"))
HOT_RATIO = float(os.getenv("HOT_RATIO", "0.2"))
OUTPUT    = os.getenv("OUTPUT", "")

TEXTS     = ["wicked", "saturday_night", "the_substance"]
KEYWORDS  = ["it", "has", "she", "away", "glimpse", "once", "noise", "hear", "light", "on", "the", "and", "love", "night", "dance", "witch", "you", "time", "never", "good"]

def request(kind):
    try:
        conn = rpyc.connect(RPC_HOST, RPC_PORT, config={"allow_public_attrs": True})
    except Exception:
        return kind, None
    try:
        start = time.perf_counter()
        root = conn.root
        if kind == "hot_keywords":
            list(root.get_hot_keywords(limit=5))
        else:
            root.count_word(random.choice(KEYWORDS), random.choice(TEXTS))
        return kind, (time.perf_counter() - start) * 1000
    except Exception:
        return kind, None
    finally:
        conn.close()


if __name__ == "__main__":
    conn = None
    last_error = None

    for attempt in range(30):
        try:
            conn = rpyc.connect(RPC_HOST, RPC_PORT, config={"allow_public_attrs": True})
            break
        except Exception as exc:  
            last_error = exc
            time.sleep(1)

    if conn is None:
        raise RuntimeError(f"Could not connect to RPC server at {RPC_HOST}:{RPC_PORT}") from last_error

    conn.close()

    pool = ThreadPoolExecutor(max_workers=1000)
    futures = []
    t0 = time.perf_counter()
    for i in range(RATE * DURATION):
        delay = t0 + i / RATE - time.perf_counter()
        if delay > 0:
            time.sleep(delay)
        kind = "hot_keywords" if random.random() < HOT_RATIO else "count_word"
        futures.append(pool.submit(request, kind))

    results = [f.result() for f in futures]
    elapsed = time.perf_counter() - t0
    pool.shutdown()

    ok = [(k, l) for k, l in results if l is not None]
    failed = len(results) - len(ok)
    print(f"Target rate: {RATE}/s, achieved: {len(ok) / elapsed:.1f}/s, failed: {failed}")

    for label, values in [("all", [l for _, l in ok])] + [
            (k, [l for kk, l in ok if kk == k]) for k in ("count_word", "hot_keywords")]:
        if len(values) >= 2:
            print(f"{label}: n={len(values)}, average {statistics.mean(values):.2f} ms, "
                  f"p99 {statistics.quantiles(values, n=100)[98]:.2f} ms")

    if OUTPUT:
        with open(OUTPUT, "w") as f:
            f.write("kind,latency_ms\n")
            f.writelines(f"{k},{l:.3f}\n" for k, l in ok)
