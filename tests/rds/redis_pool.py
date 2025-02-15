from functools import cache

import redis


@cache
def get_redis_connection_pool() -> redis.ConnectionPool:
    return redis.ConnectionPool(
        host='localhost',
        port=6379,
        db=0,
        decode_responses=True
    )


def get_redis_client() -> redis.Redis:
    return redis.Redis(
        connection_pool=get_redis_connection_pool()
    )
