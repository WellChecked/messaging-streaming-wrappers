import redis.asyncio as redis
import asyncio


async def get_redis_connection():
    r = redis.Redis(host='localhost', port=6379, db=0)  # Replace with your Redis server details
    return r


async def main():
    pass


if __name__ == '__main__':
    asyncio.run(main())