import os

import redis as redis_lib

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
TTL_SECONDS = 30 * 24 * 60 * 60  # 30 days

_client: redis_lib.Redis | None = None


def get_redis() -> redis_lib.Redis:
    global _client
    if _client is None:
        _client = redis_lib.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    return _client


def is_seen(kind: str, id: str) -> bool:
    return bool(get_redis().sismember(f"seen:{kind}", id))


def mark_seen(kind: str, id: str) -> None:
    r = get_redis()
    r.sadd(f"seen:{kind}", id)
    r.expire(f"seen:{kind}", TTL_SECONDS)
