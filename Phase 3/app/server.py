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
            REDIS_CLIENT.zincrby("hot_keywords", 1, normalized_keyword)
            return count

        text = TEXT_STORE[ref]
        tokens = re.findall(r"[A-Za-z0-9_]+", text.lower())
        count = sum(1 for token in tokens if token == normalized_keyword)

        REDIS_CLIENT.set(cache_key, count, ex=CACHE_TTL_SECONDS)
        REDIS_CLIENT.zincrby("hot_keywords", 1, normalized_keyword)
        return count

    def exposed_get_hot_keywords(self, limit=5):
        hot_keywords = REDIS_CLIENT.zrevrange("hot_keywords", 0, max(0, limit - 1), withscores=True)
        return [(keyword, int(score)) for keyword, score in hot_keywords]

    # def exposed_clear_cache(self):
    #     REDIS_CLIENT.delete("hot_keywords")
    #     for key in REDIS_CLIENT.keys("wc:*"):
    #         REDIS_CLIENT.delete(key)
    #     return {"status": "ok"}

if __name__ == "__main__":
    for _ in range(20):
        try:
            REDIS_CLIENT.ping()
            break
        except redis.exceptions.ConnectionError:
            time.sleep(1)
    else:
        raise RuntimeError("Could not connect to Redis.")

    # load every texts/*.txt, reference = file name without .txt
    for name in os.listdir("/app/texts"):
        if name.endswith(".txt"):
            TEXT_STORE[name[:-4]] = open(os.path.join("/app/texts", name), encoding="utf-8").read()

    server = ThreadedServer(
        WordCountService,
        port=RPC_PORT,
        protocol_config={"allow_public_attrs": True},
    )
    print(f"Word count RPC service listening on port {RPC_PORT}...")
    server.start()
