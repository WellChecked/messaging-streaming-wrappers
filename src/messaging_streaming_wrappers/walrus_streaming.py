import random
import uuid
import time

import asyncio
from asyncio import AbstractEventLoop
from concurrent.futures import ProcessPoolExecutor

from abc import ABC
from threading import Thread
from typing import Any, Callable, Tuple

from pydantic import BaseModel
from redis import Redis
from walrus import Database

from messaging_streaming_wrappers.core.wrapper_base import (
    MarshalerFactory, MessageManager, MessageReceiver, Publisher, Subscriber)

from messaging_streaming_wrappers.core.helpers.logging_helpers import get_logger
log = get_logger(__name__)


class RedisMessage(BaseModel):
    mid: str
    ts: int
    type: str
    topic: str
    payload: Any


class WalrusStreamConsumer(Thread, ABC):

    def __init__(self, loop: AbstractEventLoop = None):
        super().__init__()
        self._running = False
        self._active = False
        self._loop = loop
        self._executor = ProcessPoolExecutor() if self._loop else None

    @property
    def running(self):
        return self._running

    @property
    def active(self):
        return self._active

    def start(self):
        if not self.running:
            if self._loop:
                self._loop.run_until_complete(self.runner())
            else:
                super().start()

    def stop(self):
        self._running = False
        while self.active:
            time.sleep(0.3)

    @asyncio.coroutine
    def runner(self):
        yield from self._loop.run_in_executor(self._executor, self.run)


class WalrusStreamSingleConsumer(WalrusStreamConsumer):

    def __init__(
            self,
            db: Database,
            stream_name: str,
            callback: Callable = None,
            batch_size: int = 10,
            max_wait_time_ms: int = 5000,
            last_id: str = None
    ):
        super().__init__()
        self._db = db
        self._stream_name = stream_name
        self._stream = db.Stream(stream_name)
        self._callback = callback
        self._batch_size = batch_size
        self._max_wait_time_ms = max_wait_time_ms
        self._last_id = last_id if last_id else '$'

    @property
    def db(self):
        return self._db

    @property
    def stream_name(self):
        return self._stream_name

    @property
    def stream(self):
        return self._stream

    def run(self):
        self._running = True
        while self._running:
            self._active = True

            messages = self._stream.read(
                count=self._batch_size, block=self._max_wait_time_ms, last_id=self._last_id
            )
            total_messages = len(messages)
            log.debug(f"Received {total_messages} messages")
            for i, item in enumerate(messages, 1):
                log.debug(f"Consuming {i}/{total_messages} message:{item}")
                try:
                    for msgid, content in item.content:
                        msgid = msgid.decode("utf-8")
                        if content:
                            self._callback(index=i, total=total_messages, message=(msgid, content))
                        self._stream.
                except Exception as e:
                    log.error(f"Error while processing message: {e}")
                    log.exception("A problem occurred while ingesting a message")
        self._active = False


class WalrusStreamConsumerGroup(WalrusStreamSingleConsumer):

    def __init__(
            self,
            db: Database,
            stream_name: str,
            consumer_group: str,
            callback: Callable = None,
            batch_size: int = 10,
            max_wait_time_ms: int = 5000,
            last_id: str = None
    ):
        super().__init__(
            db=db,
            stream_name=stream_name,
            callback=callback,
            batch_size=batch_size,
            max_wait_time_ms=max_wait_time_ms,
            last_id=last_id
        )
        self._consumer_group_name = consumer_group if consumer_group else f"{stream}-group"
        self._consumer_group = self.db.consumer_group(
            group=self._consumer_group_name,
            keys=self.stream_name,
            consumer=f"{self._consumer_group_name}-{random.uniform(1, 9999)}",
        )
        self._consumer_group.create()

    @property
    def consumer_group_name(self):
        return self._consumer_group_name

    @property
    def consumer_group(self):
        return self._consumer_group

    def run(self):
        self._running = True
        while self._running:
            self._active = True

            stream_messages = self._consumer_group.read(count=self._batch_size, block=self._max_wait_time_ms)
            for stream_key, messages in stream_messages:
                total_messages = len(messages)
                log.debug(f"Received {total_messages} messages")
                for i, item in enumerate(messages, 1):
                    log.debug(f"Consuming {i}/{total_messages} message:{item}")
                    try:
                        for msgid, content in item.content:
                            msgid = msgid.decode("utf-8")
                            if content:
                                self._callback(index=i, total=total_messages, message=(msgid, content))
                            self._consumer_group.ack(msgid)
                    except Exception as e:
                        log.error(f"Error while processing message: {e}")
                        log.exception("A problem occurred while ingesting a message")
        self._active = False


class WalrusPublisher(Publisher):

    def __init__(self, db: Database, stream_name: str, **kwargs: Any):
        self._db = db
        self._stream_name = stream_name
        self._stream = db.Stream(stream_name)
        self._marshaler_factory = MarshalerFactory() if "marshaler_factory" not in kwargs \
            else kwargs.get("marshaler_factory")

    def publish(self, topic: str, message: Any, **kwargs: Any):
        marshaler = self._marshaler_factory.create(marshaler_type=kwargs.get("marshaler", "json"))
        payload = RedisMessage(
            mid=uuid.uuid4().hex,
            ts=int(time.time() * 1000),
            type=marshaler.type_name,
            topic=topic,
            payload=marshaler.marshal(message),
        )
        mid = self._stream.add(payload.model_dump())
        return 0, mid.decode("utf-8")


class WalrusMessageReceiver(MessageReceiver):

    def __init__(
            self,
            stream_consumer: WalrusStreamSingleConsumer | WalrusStreamConsumerGroup,
            **kwargs: Any
    ):
        super().__init__()
        self._marshaler_factory = MarshalerFactory() if "marshaler_factory" not in kwargs \
            else kwargs.get("marshaler_factory")
        self._stream_consumer = stream_consumer
        self._stream_consumer._callback = self.on_message

    @property
    def consumer(self):
        return self._stream_consumer

    def start(self):
        self.consumer.start()
        while not self.consumer.active:
            time.sleep(0.3)

    def shutdown(self):
        self._stream_consumer.stop()
        self._stream_consumer.join()

    def on_message(self, index: int, total: int, message: Tuple[str, dict]):
        def unmarshal_payload(payload, marshal_type):
            marshaler = self._marshaler_factory.create(marshaler_type=marshal_type)
            return marshaler.unmarshal(payload)

        msgid, content = message
        log.debug(f"Received message on index {index} of {total} with msgid {msgid} and content {content}")

        message_mid = content[b'mid'].decode("utf-8") if b'mid' in content else msgid
        message_ts = int(content[b'ts'].decode("utf-8")) if b'ts' in content else int(time.time() * 1000)
        message_type = content[b'type'].decode("utf-8") if b'type' in content else 'json'
        message_topic = content[b'topic'].decode("utf-8")
        message_payload = content[b'payload'].decode("utf-8")
        published_payload = unmarshal_payload(payload=message_payload, marshal_type=message_type)
        self.receive(topic=message_topic, payload={"payload": published_payload}, params={
            "i": index,
            "n": total,
            "ts": message_ts,
            "mid": message_mid,
            "msgid": msgid,
            "type": message_type,
            "content": content
        })


class RedisSubscriber(Subscriber):

    def __init__(self, redis_client: Redis, message_receiver: RedisMessageReceiver):
        super().__init__(message_receiver)
        self._redis_client = redis_client

    def subscribe(self, topic: str, callback: Callable[[str, Any, dict], None]):
        print(f"Subscribing to {topic}")
        self._message_receiver.register_handler(topic, callback)
        print(f"Subscribed to {topic}")

    def unsubscribe(self, topic: str):
        print(f"Unsubscribing from {topic}")
        self._message_receiver.unregister_handler(topic)
        print(f"Unsubscribed from {topic}")

    def establish_subscriptions(self):
        pass


class RedisStreamManager(MessageManager):

    def __init__(
            self,
            redis_client: Redis,
            redis_publisher: RedisPublisher = None,
            redis_subscriber: RedisSubscriber = None,
            stream_name: str = None,
            consumer_group: str = None,
            batch_size: int = 10,
            max_wait_time_ms: int = 5000
    ):
        stream_name = stream_name if stream_name else f"incoming-topics-stream"
        super().__init__(
            redis_publisher if redis_publisher else (
                RedisPublisher(redis_client=redis_client, stream_name=stream_name)
            ),
            redis_subscriber if redis_subscriber else (
                RedisSubscriber(
                    redis_client=redis_client,
                    message_receiver=RedisMessageReceiver(
                        redis_client=redis_client,
                        stream_name=stream_name,
                        consumer_group=consumer_group if consumer_group else None,
                        batch_size=batch_size,
                        max_wait_time_ms=max_wait_time_ms
                    )
                )
            )
        )

    @property
    def publisher(self):
        return self._publisher

    @property
    def subscriber(self):
        return self._subscriber

    @property
    def message_receiver(self):
        return self._subscriber.message_receiver

    @property
    def consumer(self):
        return self.message_receiver.consumer

    def connect(self, **kwargs):
        self.start()

    def start(self):
        self.subscriber.establish_subscriptions()
        self.message_receiver.start()

    def shutdown(self):
        self.message_receiver.shutdown()
