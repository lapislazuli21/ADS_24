import os
import random
import statistics
import time
from concurrent.futures import ThreadPoolExecutor

import rpyc

RPC_HOST = os.getenv("RPC_HOST", "loadbalancer")
RPC_PORT = int(os.getenv("RPC_PORT", "18812"))
RATE = max(1, int(os.getenv("RATE", "20")))
DURATION = max(1, int(os.getenv("DURATION", "10")))
TEXTS = ["sample"]
KEYWORDS = ["the", "service", "word", "python", "request", "and", "text"]


def request(request_index):
    try:
        request_connection = rpyc.connect(RPC_HOST, RPC_PORT, config={"allow_public_attrs": True})
        try:
            request_started = time.perf_counter()
            if request_index % 5 == 0:
                list(request_connection.root.get_hot_keywords(limit=5))
            else:
                request_connection.root.count_word(random.choice(KEYWORDS), random.choice(TEXTS))
            return (time.perf_counter() - request_started) * 1000, None
        finally:
            request_connection.close()
    except (OSError, EOFError) as error:
        return None, str(error)


if __name__ == "__main__":
    deadline = time.monotonic() + 30
    last_error = None
    while True:
        try:
            readiness_connection = rpyc.connect(
                RPC_HOST, RPC_PORT, config={"allow_public_attrs": True}
            )
            readiness_connection.close()
            break
        except (OSError, EOFError) as error:
            last_error = error
            if time.monotonic() >= deadline:
                raise RuntimeError(f"Could not connect to {RPC_HOST}:{RPC_PORT}") from last_error
            time.sleep(1)

    total = RATE * DURATION
    workload_started = time.perf_counter()
    with ThreadPoolExecutor(max_workers=min(RATE, 64)) as pool:
        results = list(pool.map(request, range(total)))
    elapsed = time.perf_counter() - workload_started

    latencies = [latency for latency, _ in results if latency is not None]
    failures = [error for _, error in results if error is not None]
    achieved_rate = len(latencies) / elapsed if elapsed else 0
    print(f"Requests: {total}; succeeded: {len(latencies)}; failed: {len(failures)}")
    print(f"Elapsed: {elapsed:.2f}s; achieved throughput: {achieved_rate:.1f} requests/s")
    if latencies:
        print(f"Mean RPC latency: {statistics.mean(latencies):.2f} ms")
