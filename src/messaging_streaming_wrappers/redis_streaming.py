import inspect
import json
import random
import traceback
import uuid
import time

from threading import Thread
from typing import Any, Callable, Tuple

import asyncer
from pydantic import BaseModel
from redis import Redis
from redis import exceptions as redis_exceptions
from requests import HTTPError

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


class RedisPublisher(Publisher):

    def __init__(self, redis_client: Redis, stream_name: str, **kwargs: Any):
        self._redis_client = redis_client
        self._stream_name = stream_name
        self._marshaler_factory = MarshalerFactory() if "marshaler_factory" not in kwargs \
            else kwargs.get("marshaler_factory")

    @property
    def redis_client(self):
        return self._redis_client

    def publish(self, topic: str, message: Any, **kwargs: Any):
        stream_name = kwargs.get("stream_name", self._stream_name)
        assert stream_name, "A stream name (stream_name) must be provided for publishing"

        marshaler = self._marshaler_factory.create(marshaler_type=kwargs.get("marshaler", "json"))
        payload = RedisMessage(
            mid=uuid.uuid4().hex,  # UUID
            ts=int(time.time() * 1000),  # TS
            type=marshaler.type_name,  # 'json'
            topic=topic,  # path the object in S3
            payload=marshaler.marshal(message),  # S3 Event marshaled to JSON
        )
        maxlen = kwargs.get("maxlen", 10000)
        mid = self._redis_client.xadd(name=stream_name, fields=payload.model_dump(), maxlen=maxlen)
        return 0, mid


class RedisConsumer(Thread):

    def __init__(
            self,
            redis_client: Redis,
            streams: dict,
            callback: Callable,
            consumer_id: str = None,
            count: int = 2,
            block: int = 1000
    ):
        assert redis_client.get_encoder().decode_responses, "decode_responses must be True"

        super().__init__()

        self._redis_client = redis_client
        self._streams = streams
        self._callback = callback
        self._consumer_id = consumer_id if consumer_id else f"consumer-{random.uniform(1, 999999)}"
        self._count = count
        self._block = block
        self._running = True
        self._active = False

    @property
    def redis_client(self):
        return self._redis_client

    @property
    def active(self):
        return self._active

    def shutdown(self) -> None:
        self._running = False
        self.join()

    def run(self) -> None:
        streams = self._streams

        self._active = True
        while self._running:
            items = self._redis_client.xread(
                streams=streams,
                count=self._count,
                block=self._block
            )

            for stream, messages in items:
                total_messages = len(messages)
                log.debug(f"Received {total_messages} messages")

                i = 0
                for msgid, message in messages:
                    try:
                        if inspect.iscoroutinefunction(self._callback):
                            asyncer.runnify(self._callback)(
                                stream_name=stream,
                                consumer=(self._consumer_id, None),
                                index=i,
                                total=total_messages,
                                message=(msgid, message)
                            )
                        else:
                            self._callback(
                                stream_name=stream,
                                consumer=(self._consumer_id, None),
                                index=i,
                                total=total_messages,
                                message=(msgid, message)
                            )

                        streams[stream] = msgid

                        i += 1

                    except json.JSONDecodeError as json_error:
                        log.error(f"JSONDecodeError decoding message: {json_error} - Message: {message}")

                    except Exception as e:
                        log.error(f"EXCEPTION: Processing [{msgid}]:")
                        traceback.print_exc()
                        log.error(f"ERROR found: consumer stopped")
                        self._running = False

        self._active = False


class RedisConsumerGroup(Thread):

    def __init__(
            self,
            redis_client: Redis,
            streams: dict,
            callback: Callable,
            consumer_group: str,
            consumer_id: str = None,
            count: int = 2,
            block: int = 1000
    ):
        assert redis_client.get_encoder().decode_responses, "decode_responses must be True"

        super().__init__()

        self._redis_client = redis_client
        self._streams = streams
        self._callback = callback
        self._consumer_group = consumer_group
        self._consumer_id = consumer_id if consumer_id else f"{consumer_group}-consumer-{random.uniform(1, 999999)}"
        self._count = count
        self._block = block
        self._running = True
        self._active = False

    @property
    def active(self):
        return self._active

    def create_consumer_group(self, redis_client, stream_name: str, group_name: str, next_message_id: str) -> None:
        try:
            redis_client.xgroup_create(
                name=stream_name,
                groupname=group_name,
                id=next_message_id,
                mkstream=True
            )
        except redis_exceptions.ResponseError as e:
            if 'BUSYGROUP' in str(e):
                print(f"Consumer group '{group_name}' already exists for stream '{stream_name}'.")
            else:
                raise e

    @staticmethod
    def last_message_id(redis_client: Redis, stream: str, group_name: str) -> str:
        result = redis_client.xinfo_groups(name=stream)
        for group_info in result:
            if group_info['name'] == group_name:
                return group_info['last-delivered-id']

    def shutdown(self) -> None:
        self._running = False
        self.join()

    def run(self) -> None:
        for stream_name, _ in self._streams.items():
            self.create_consumer_group(
                redis_client=self._redis_client,
                stream_name=stream_name,
                group_name=self._consumer_group,
                next_message_id="$"  # Start group at current position of stream
            )

        self._active = True
        while self._running:
            items = self._redis_client.xreadgroup(
                streams=self._streams,
                groupname=self._consumer_group,
                consumername=self._consumer_id,
                count=self._count,
                block=self._block,
                noack=True
            )

            for stream, messages in items:
                total_messages = len(messages)
                log.debug(f"Received {total_messages} messages")

                i = 0
                for msgid, message in messages:
                    log.debug(f"Consuming {i}/{total_messages} message:{msgid}")

                    try:
                        # TODO: Ensure that the loop used is the same as FastAPI
                        if inspect.iscoroutinefunction(self._callback):
                            asyncer.runnify(self._callback)(
                                stream_name=stream,
                                consumer=(self._consumer_id, self._consumer_group),
                                index=i,
                                total=total_messages,
                                message=(msgid, message)
                            )
                        else:
                            self._callback(
                                stream_name=stream,
                                consumer=(self._consumer_id, self._consumer_group),
                                index=i,
                                total=total_messages,
                                message=(msgid, message)
                            )

                        self._redis_client.xack(stream, self._consumer_group, msgid)

                        i += 1

                    except json.JSONDecodeError as json_error:
                        log.error(f"Error decoding message: {json_error} - Message: {message}")

                    except Exception as e:
                        log.error(f"ERROR: Processing [{msgid}]:")
                        traceback.print_exc()
                        log.error(f"ERROR found: consumer stopped")
                        self._running = False

        self._active = False


class RedisConsumerFactory:

    @staticmethod
    def create(
            redis_client: Redis,
            callback: Callable,
            streams: dict = None,
            stream_name: str = None,
            consumer_group: str = None,
            consumer_id: str = None,
            count: int = 2,
            block: int = 1000
    ):
        if not streams:
            streams = {}
            if stream_name:
                if consumer_group:
                    streams = {
                        stream_name: '>'
                    }
                else:
                    streams = {
                        stream_name: '$'
                    }

        if consumer_group:
            return RedisConsumerGroup(
                redis_client=redis_client,
                streams=streams,
                callback=callback,
                consumer_group=consumer_group,
                consumer_id=consumer_id,
                count=count,
                block=block
            )
        else:
            return RedisConsumer(
                redis_client=redis_client,
                streams=streams,
                callback=callback,
                consumer_id=consumer_id,
                count=count,
                block=block
            )


class RedisMessageReceiver(MessageReceiver):

    def __init__(
            self,
            redis_client: Redis,
            streams: dict = None,
            stream_name: str = None,
            consumer_id: str = None,
            consumer_group: str = None,
            count: int = 10,
            block: int = 5000,
            **kwargs: Any
    ):
        super().__init__()
        self._marshaler_factory = MarshalerFactory() if "marshaler_factory" not in kwargs \
            else kwargs.get("marshaler_factory")

        self._redis_stream_consumer = RedisConsumerFactory.create(
            redis_client=redis_client,
            streams=streams,
            stream_name=stream_name,
            callback=self.on_message,
            consumer_group=consumer_group,
            consumer_id=consumer_id,
            count=count,
            block=block
        )

    @property
    def consumer(self):
        return self._redis_stream_consumer

    def start(self):
        self.consumer.start()
        while not self.consumer.active:
            time.sleep(0.3)

    def shutdown(self):
        self.consumer.shutdown()

    def on_message(self, stream_name: str, consumer: Tuple[str, str], index: int, total: int, message: Tuple[str, RedisMessage]):
        def unmarshal_payload(payload, marshal_type):
            marshaler = self._marshaler_factory.create(marshaler_type=marshal_type)
            return marshaler.unmarshal(payload)

        msgid, content = message
        log.debug(
            f"Received message from [{stream_name}] for [{consumer}] on index {index} of {total} with "
            f" msgid {msgid} and content {content}"
        )

        redis_message = RedisMessage.model_validate(content)
        redis_message.payload = unmarshal_payload(payload=redis_message.payload, marshal_type=redis_message.type)

        self.receive(topic=redis_message.topic, payload=redis_message.payload, params={
            "i": index,
            "n": total,
            "stream": stream_name,
            "consumer": consumer,
            "msgid": msgid,
            "content": content,
            "message": redis_message
        })


class RedisSubscriber(Subscriber):

    def __init__(self, redis_client: Redis, message_receiver: RedisMessageReceiver):
        super().__init__(message_receiver)
        self._redis_client = redis_client

    def subscribe(self, topic: str, callback: Callable[[str, Any, dict], None], **kwargs: Any):
        print(f"Subscribing to {topic}")
        self._message_receiver.register_handler(topic, callback, **kwargs)
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
            streams: dict = None,
            stream_name: str = None,
            consumer_group: str = None,
            batch_size: int = 10,
            max_wait_time_ms: int = 5000
    ):
        assert redis_client.get_encoder().decode_responses == True, "decode_responses must be True"
        assert streams or stream_name, "Either streams or stream_name must be provided"
        assert batch_size > 0, "batch_size must be greater than 0"
        assert max_wait_time_ms > 0, "max_wait_time_ms must be greater than 0"

        super().__init__(
            redis_publisher if redis_publisher else (
                RedisPublisher(redis_client=redis_client, stream_name=stream_name)
            ),
            redis_subscriber if redis_subscriber else (
                RedisSubscriber(
                    redis_client=redis_client,
                    message_receiver=RedisMessageReceiver(
                        redis_client=redis_client,
                        streams=streams,
                        stream_name=stream_name,
                        consumer_group=consumer_group,
                        count=batch_size,
                        block=max_wait_time_ms
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
