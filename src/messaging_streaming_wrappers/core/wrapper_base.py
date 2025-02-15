import inspect
import json
import pickle
from abc import ABC, abstractmethod
from typing import Any, Callable, cast

import asyncer
from paho.mqtt import matcher as mqtt_matcher

from messaging_streaming_wrappers.core.helpers.logging_helpers import get_logger

log = get_logger(__name__)


class MessageReceiver:

    def __init__(self):
        self._matcher = mqtt_matcher.MQTTMatcher()
        self._topics = []
        self._params = {}

    @property
    def topics(self):
        return self._topics

    def register_handler(self, topic: str, handler: Callable[[str, Any, dict], None], **kwargs):
        """
        Register a handler for a topic pattern.  The topic can utilize MQTT wildcards.  If there
        is already a topic handler assigned the old handler will be replaced.
        :param topic: topic pattern
        :param handler: a callable handler to be called when a message is received
        :return: bool True
        """
        self._matcher[topic] = handler
        self._topics.append(topic)
        self._params = {**kwargs}
        return True

    def unregister_handler(self, topic: str):
        """
        Unregister a handler for a topic pattern.  The topic must match the same pattern used originally when
        registering a handler.
        :param topic: topic pattern
        :return: bool True if removed, otherwise False.
        """
        if topic not in self._matcher:
            return False

        del self._matcher[topic]
        self._topics.remove(topic)
        return True

    def receive(self, topic: str, payload: Any, params: dict = None):
        """
        Handle a received message divvying the processing out to a handle via the topic matcher.  A previously
        registered topic matches the handler to be called.
        :param topic: message topic
        :param payload: message payload
        :param params: any associated keyword arguments specific to the message receiver
        :return: None
        """
        log.debug(f"Received message on topic {topic}: {payload} --- {params}")
        for handler in self._matcher.iter_match(topic):
            if inspect.iscoroutinefunction(handler):
                asyncer.runnify(handler)(topic, payload, {**self._params, **params})
            else:
                handler(topic, payload, {**self._params, **params})


class Marshaler(ABC):

    def __init__(self, marshal_type: str = None):
        self._marshal_type = marshal_type if marshal_type else "json"

    @property
    def type_name(self):
        return self._marshal_type

    @abstractmethod
    def marshal(self, message: Any):
        pass

    @abstractmethod
    def unmarshal(self, message: Any):
        pass


class JsonMarshal(Marshaler):

    def __init__(self):
        super().__init__(marshal_type="json")

    def marshal(self, message: Any) -> str:
        return json.dumps(message)

    def unmarshal(self, message: Any) -> dict:
        return json.loads(message)


class PickleMarshal(Marshaler):

    def __init__(self):
        super().__init__(marshal_type="pickle")

    def marshal(self, message: Any) -> bytes:
        return pickle.dumps(message)

    def unmarshal(self, message: Any) -> dict:
        return pickle.loads(message)


class TextMarshal(Marshaler):

    def __init__(self):
        super().__init__(marshal_type="text")

    def marshal(self, message: Any) -> str:
        return message

    def unmarshal(self, message: Any) -> str:
        return message


class MarshalerFactory:

    def __init__(self, marshalers: list = None):
        self._marshalers = {}
        if not marshalers:
            self._marshalers = {
                "json": JsonMarshal(),
                "pickle": PickleMarshal(),
                "text": TextMarshal()
            }
        else:
            for marshaler in marshalers:
                self._marshalers[marshaler.type_name.lower()] = marshaler

    @property
    def marshalers(self) -> list:
        return list(self._marshalers.keys())

    def create(self, marshaler_type: str = None):
        marshaler_type = marshaler_type.lower() if marshaler_type else "json"
        if marshaler_type in self._marshalers:
            return self._marshalers[marshaler_type]
        else:
            raise ValueError(f"Unknown marshaler type: {marshaler_type}")


class Publisher:

    @abstractmethod
    def publish(self, topic: str, message: Any, **kwargs: Any):
        """
        Publish a message associated with the topic using the implemented messaging platform.
        :param topic: topic associated with the message to be published
        :param message: message to be published
        :return: Tuple[int, int] return code (0 == success) and message id
        """
        pass


class Subscriber:

    def __init__(self, message_receiver: MessageReceiver):
        self._message_receiver = message_receiver

    @property
    def message_receiver(self):
        return cast(self._message_receiver.__class__, self._message_receiver)

    @staticmethod
    def print_message(topic: str, message: Any, params: dict = None):
        print(f"Received message on topic {topic}: {message} --- {params}")

    @abstractmethod
    def subscribe(self, topic: str, callback: Callable[[str, Any, dict], None], **kwargs: Any):
        pass

    @abstractmethod
    def unsubscribe(self, topic: str):
        pass

    @abstractmethod
    def establish_subscriptions(self):
        pass


class MessageManager:

    def __init__(self, publisher: Publisher, subscriber: Subscriber):
        self._publisher = publisher
        self._subscriber = subscriber

    @property
    @abstractmethod
    def publisher(self):
        pass

    @property
    @abstractmethod
    def subscriber(self):
        pass

    @property
    @abstractmethod
    def message_receiver(self):
        pass

    def connect(self, **kwargs):
        pass

    def start(self):
        pass

    def shutdown(self):
        pass

    def publish(self, topic: str, message: Any):
        self._publisher.publish(topic, message)
