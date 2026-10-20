import os
import re
import time

import redis
import rpyc
from rpyc.utils.server import ThreadedServer

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
RPC_PORT = int(os.getenv("RPC_PORT", "18812"))
CACHE_TTL_SECONDS = int(os.getenv("CACHE_TTL_SECONDS", "3600"))
INSTANCE_NAME = os.getenv("INSTANCE_NAME", "wordcount")
TEXT_DIR = os.getenv("TEXT_DIR", "/app/texts")

TEXT_STORE = {}
REDIS_CLIENT = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


class WordCountService(rpyc.Service):
    def exposed_count_word(self, keyword, ref):
        normalized_keyword = keyword.strip().lower()
        if not normalized_keyword:
            raise ValueError("Keyword cannot be empty.")

        if ref not in TEXT_STORE:
            raise KeyError(f"No text registered for reference '{ref}'.")

        cache_key = f"wc:{ref}:{normalized_keyword}"
        cached_value = REDIS_CLIENT.get(cache_key)

        if cached_value is not None:
            count = int(cached_value)
        else:
            tokens = re.findall(r"[A-Za-z0-9_]+", TEXT_STORE[ref].lower())
            count = sum(1 for token in tokens if token == normalized_keyword)
            REDIS_CLIENT.set(cache_key, count, ex=CACHE_TTL_SECONDS)

        REDIS_CLIENT.zincrby("hot_keywords", 1, normalized_keyword)
        print(f"{INSTANCE_NAME} served count_word keyword={normalized_keyword} ref={ref}", flush=True)
        return count

    def exposed_get_hot_keywords(self, limit=5):
        hot_keywords = REDIS_CLIENT.zrevrange(
            "hot_keywords", 0, max(0, limit - 1), withscores=True
        )
        print(f"{INSTANCE_NAME} served get_hot_keywords", flush=True)
        return [(keyword, int(score)) for keyword, score in hot_keywords]


if __name__ == "__main__":
    for _ in range(30):
        try:
            REDIS_CLIENT.ping()
            break
        except redis.exceptions.ConnectionError:
            time.sleep(1)
    else:
        raise RuntimeError("Could not connect to Redis.")

    for name in os.listdir(TEXT_DIR):
        if name.endswith(".txt"):
            with open(os.path.join(TEXT_DIR, name), encoding="utf-8") as text_file:
                TEXT_STORE[name[:-4]] = text_file.read()

    if not TEXT_STORE:
        raise RuntimeError(f"No text documents found in {TEXT_DIR}.")

    server = ThreadedServer(
        WordCountService,
        port=RPC_PORT,
        protocol_config={"allow_public_attrs": True},
    )
    print(f"{INSTANCE_NAME} listening on port {RPC_PORT}; loaded {len(TEXT_STORE)} texts", flush=True)
    server.start()
