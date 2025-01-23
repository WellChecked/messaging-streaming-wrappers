import json
import uuid
import time
from abc import ABC, abstractmethod
from threading import Thread
from typing import Any, Callable
from xmlrpc.client import Marshaller

from pydantic import BaseModel
from redis import Redis
from redis_streams.consumer import Consumer, RedisMsg

from messaging_streaming_wrappers.core.wrapper_base import MarshalerFactory, MessageManager, MessageReceiver, Publisher, Subscriber
from messaging_streaming_wrappers.core.helpers.logging_helpers import get_logger

log = get_logger(__name__)


class RedisMessage(BaseModel):
    mid: str
    ts: int
    type: str
    topic: str
    payload: Any


class RedisStreamConsumer(Thread, ABC):

    def __init__(
            self,
            redis: Redis,
            stream: str,
            callback: Callable,
            consumer_group: str = None,
            batch_size: int = 10,
            max_wait_time_ms: int = 5000
    ):
        super().__init__()
        self.redis = redis
        self.stream = stream
        self.callback = callback
        self.consumer_group = consumer_group
        self.batch_size = batch_size
        self.max_wait_time_ms = max_wait_time_ms
        self._running = False
        self._active = False
        self._consumer = None

    @property
    def consumer(self):
        return self._consumer

    @property
    def running(self):
        return self._running

    @property
    def active(self):
        return self._active

    def start(self):
        if not self.running:
            self._running = True
            super().start()

    def stop(self):
        self._running = False
        while self.active:
            time.sleep(0.3)

    def run(self):
        self._consumer = Consumer(
            redis_conn=self.redis,
            stream=self.stream,
            consumer_group=self.consumer_group,
            batch_size=self.batch_size,
            max_wait_time_ms=self.max_wait_time_ms,
            cleanup_on_exit=False
        )
        self._active = True
        while self._running:
            messages = self._consumer.get_items()
            total_messages = len(messages)
            log.debug(f"Received {total_messages} messages")
            for i, item in enumerate(messages, 1):
                log.debug(f"Consuming {i}/{total_messages} message:{item}")
                try:
                    if item.content:
                        self.callback(index=i, total=total_messages, message=item)
                    self._consumer.remove_item_from_stream(item_id=item.msgid)
                except Exception as e:
                    log.error(f"Error while processing message: {e}")
                    log.exception("A problem occurred while ingesting a message")
        self._active = False


class RedisPublisher(Publisher):

    def __init__(self, redis_client: Redis, stream_name: str, **kwargs: Any):
        self._redis_client = redis_client
        self._stream_name = stream_name

    def publish(self, topic: str, message: Any, **kwargs: Any):
        marshaler = MarshalerFactory.create(kwargs.get("marshaler", "json"))
        payload = RedisMessage(
            mid=uuid.uuid4().hex,
            ts=int(time.time() * 1000),
            topic=topic,
            payload=marshaler.marshal(message),
        )
        mid = self._redis_client.xadd(name=self._stream_name, fields=payload.model_dump())
        return 0, mid


class RedisMessageReceiver(MessageReceiver):

    def __init__(self, redis_client: Redis, stream_name: str, consumer_group: str = None, batch_size: int = 10, max_wait_time_ms: int = 5000):
        super().__init__()
        self._redis_stream_consumer = RedisStreamConsumer(
            redis=redis_client,
            stream=stream_name,
            callback=self.on_message,
            consumer_group=consumer_group if consumer_group else f"{stream_name}-group",
            batch_size=batch_size,
            max_wait_time_ms=max_wait_time_ms
        )

    @property
    def consumer(self):
        return self._redis_stream_consumer

    def start(self):
        self.consumer.start()
        while not self.consumer.active:
            time.sleep(0.3)

    def shutdown(self):
        self._redis_stream_consumer.stop()
        self._redis_stream_consumer.join()

    def on_message(self, index: int, total: int, message: RedisMsg):
        def get_published_payload(msg):
            try:
                return json.loads(message_payload)
            except json.JSONDecodeError as e:
                log.debug(">>> JSONDecodeError:", e)
                return msg

        content = message.content
        message_mid = content['mid']
        message_ts = content['ts']
        message_topic = content['topic']
        message_payload = content['payload']
        published_payload = get_published_payload(message_payload)
        self.receive(topic=message_topic, payload={"payload": published_payload}, params={
            "i": index,
            "n": total,
            "ts": message_ts,
            "mid": message_mid,
            "message": message
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
