import inspect
import json
import random
import time
from threading import Thread
from typing import Callable

import asyncer
import redis
from redis import Redis

from tests.rds.redis_pool import get_redis_client
from tests.rds.redis_publisher import RedisMessage


class RedisConsumer(Thread):

    def __init__(
            self,
            prefix: str,
            streams: dict,
            callback: Callable,
            count: int = 2,
            block: int = 1000
    ):
        super().__init__()
        self._prefix = prefix
        self._streams = streams
        self._callback = callback
        self._count = count
        self._block = block
        self._running = True

    def shutdown(self) -> None:
        self._running = False
        self.join()

    def run(self) -> None:
        redis_client = get_redis_client()

        streams = self._streams
        while self._running:
            items = redis_client.xread(
                streams=streams,
                count=self._count,
                block=self._block
            )
            for stream, messages in items:
                for mid, message in messages:
                    redis_message = RedisMessage.model_validate(message)
                    if inspect.iscoroutinefunction(self._callback):
                        asyncer.runnify(self._callback)(stream, None, None, mid, redis_message)
                    else:
                        self._callback(stream, None, None, mid, redis_message)
                    streams[stream] = mid


class RedisConsumerGroup(Thread):

    def __init__(
            self,
            prefix: str,
            streams: dict,
            group: str,
            callback: Callable,
            consumer: str = None,
            count: int = 2,
            block: int = 1000
    ):
        super().__init__()
        self._prefix = prefix
        self._streams = streams
        self._group = group
        self._consumer = consumer if consumer else f"{group}-consumer-{random.uniform(1, 999999)}"
        self._callback = callback
        self._count = count
        self._block = block
        self._running = True

    def _create_consumer_group(self, redis_client, stream_name: str, group_name: str, next_message_id: str) -> None:
        try:
            redis_client.xgroup_create(
                name=stream_name,
                groupname=group_name,
                id=next_message_id,
                mkstream=True
            )
        except redis.exceptions.ResponseError as e:
            if 'BUSYGROUP' in str(e):
                print(f"Consumer group '{group_name}' already exists for stream '{stream_name}'.")
            else:
                raise e

    def last_message_id(self, redis_client: Redis, stream: str, group: str) -> str:
        result = redis_client.xinfo_groups(name=stream)
        for group_info in result:
            if group_info['name'] == group:
                return group_info['last-delivered-id']

    def shutdown(self) -> None:
        self._running = False
        self.join()

    def run(self) -> None:
        redis_client = get_redis_client()

        for stream_name, next_message_id in self._streams.items():
            last_message_id = self.last_message_id(redis_client=redis_client, stream=stream_name, group=self._group)
            print(f"Processing from {last_message_id}")

        for stream_name, next_message_id in self._streams.items():
            self._create_consumer_group(
                redis_client=redis_client,
                stream_name=stream_name,
                group_name=self._group,
                next_message_id="$"
            )

        while self._running:
            items = redis_client.xreadgroup(
                streams=self._streams,
                groupname=self._group,
                consumername=self._consumer,
                count=self._count,
                block=self._block,
                noack=True
            )
            for stream, messages in items:
                for mid, message in messages:
                    try:
                        redis_message = RedisMessage.model_validate(message)
                        redis_message.payload = json.loads(redis_message.payload)
                        if inspect.iscoroutinefunction(self._callback):
                            asyncer.runnify(self._callback)(stream, None, None, mid, redis_message)
                        else:
                            self._callback(stream, None, None, mid, redis_message)

                        redis_client.xack(stream, self._group, mid)
                    except json.JSONDecodeError:
                        print(f"Error decoding message: {message}")
                        continue


if __name__ == '__main__':
    def print_stream_message(stream: str, group: str, consumer: str, mid: str, message: RedisMessage):
        print(
            f"Received message for [{stream}:{consumer}] with id {mid} on {message.topic}: "
            f" {message.model_dump_json(indent=2, exclude_unset=True, exclude_defaults=True)}"
        )

    async def print_stream_message_async(stream: str, group: str, consumer: str, mid: str, message: RedisMessage):
        print(
            f"ASYNC: Received message for [{stream}:{consumer}] with id {mid} on {message.topic}: "
            f" {message.model_dump_json(indent=2, exclude_unset=True, exclude_defaults=True)}"
        )

    redis_stream: RedisConsumer = RedisConsumer(
        prefix='test-stream',
        streams={
            'test-stream': '$'
        },
        callback=print_stream_message,
    )
    redis_stream1: RedisConsumerGroup = RedisConsumerGroup(
        prefix='test-stream-1',
        group='test-group',
        callback=print_stream_message_async,
        streams={
            'test-stream': '>'
        }
    )
    redis_stream2: RedisConsumerGroup = RedisConsumerGroup(
        prefix='test-stream-2',
        group='test-group',
        callback=print_stream_message_async,
        streams={
            'test-stream': '>'
        }
    )
    try:
        redis_stream.start()
        redis_stream1.start()
        redis_stream2.start()
        time.sleep(60.0)
    except KeyboardInterrupt:
        pass
    finally:
        redis_stream.shutdown()
        redis_stream1.shutdown()
        redis_stream2.shutdown()
