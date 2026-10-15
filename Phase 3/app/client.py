import os
import time

import rpyc

RPC_HOST = os.getenv("RPC_HOST", "loadbalancer")
RPC_PORT = int(os.getenv("RPC_PORT", "18812"))


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

    root = conn.root

    text = (
        "Python is powerful. Python is used in data science, web apps, and automation. "
        "Many developers love Python because Python is simple and readable."
    )

    root.add_text("doc-1", text)

    print("First lookup:", root.count_word("python", "doc-1"))
    print("Second lookup (from cache):", root.count_word("python", "doc-1"))
    print("Top hot keywords:", root.get_hot_keywords(limit=5))

    conn.close()
