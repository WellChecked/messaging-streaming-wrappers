import time
from typing import Any, Callable, cast

from messaging_streaming_wrappers.core.wrapper_base import MessageManager, MessageReceiver, Publisher, Subscriber
from messaging_streaming_wrappers.core.helpers.logging_helpers import get_logger
log = get_logger(__name__)


class LoopbackMessageReceiver(MessageReceiver):

    def send_message(self, topic: str, message: Any):
        log.debug(f"Sending message on topic {topic}: {message}")
        self.receive(topic, message)
        return 0, int(time.time() * 1000)


class LoopbackPublisher(Publisher):

    def __init__(self, message_receiver: LoopbackMessageReceiver = None):
        self._message_receiver = message_receiver if message_receiver else LoopbackMessageReceiver()

    def publish(self, topic: str, message: Any, **kwargs: Any):
        print(f"Published to {topic}: {message}")
        rc, mid = self._message_receiver.send_message(topic, message)
        return rc, mid


class LoopbackSubscriber(Subscriber):

    def subscribe(self, topic: str, callback: Callable[[str, Any, dict], None], **kwargs: Any):
        print(f"Subscribing to {topic}")
        self._message_receiver.register_handler(topic, callback)
        print(f"Subscribed to {topic}")

    def unsubscribe(self, topic: str):
        print(f"Unsubscribing from {topic}")
        self._message_receiver.unregister_handler(topic)
        print(f"Unsubscribed from {topic}")

    def establish_subscriptions(self):
        for topic in self._message_receiver.topics:
            print(f"Establish subscription for {topic}")


class LoopbackMessageManager(MessageManager):

    def __init__(
            self,
            loopback_publisher: LoopbackPublisher = None,
            loopback_subscriber: LoopbackSubscriber = None
    ):
        super().__init__(
            publisher=LoopbackPublisher() if not loopback_publisher else loopback_publisher,
            subscriber=LoopbackSubscriber(message_receiver=LoopbackMessageReceiver()) if not loopback_subscriber else loopback_subscriber
        )

    @property
    def publisher(self) -> LoopbackPublisher:
        return cast(LoopbackPublisher, self._publisher)

    @property
    def subscriber(self) -> LoopbackSubscriber:
        return cast(LoopbackSubscriber, self._subscriber)

    @property
    def message_receiver(self) -> LoopbackMessageReceiver:
        return cast(LoopbackMessageReceiver, self.subscriber.message_receiver)

    def connect(self, **kwargs):
        return self.subscriber.establish_subscriptions()

    def send_message(self, topic: str, message: dict):
        self.message_receiver.send_message(topic, message)
