import time
import json

from enum import IntEnum
from json import JSONDecodeError
from typing import Any, Callable, cast

import paho.mqtt.client as mqtt

from messaging_streaming_wrappers.core.wrapper_base import MessageManager, MessageReceiver, Publisher, Subscriber
from messaging_streaming_wrappers.core.helpers.logging_helpers import get_logger
log = get_logger(__name__)


class MqttReturnCodes(IntEnum):
    MQTT_ERR_AGAIN = -1
    MQTT_ERR_SUCCESS = 0
    MQTT_ERR_NOMEM = 1
    MQTT_ERR_PROTOCOL = 2
    MQTT_ERR_INVAL = 3
    MQTT_ERR_NO_CONN = 4
    MQTT_ERR_CONN_REFUSED = 5
    MQTT_ERR_NOT_FOUND = 6
    MQTT_ERR_CONN_LOST = 7
    MQTT_ERR_TLS = 8
    MQTT_ERR_PAYLOAD_SIZE = 9
    MQTT_ERR_NOT_SUPPORTED = 10
    MQTT_ERR_AUTH = 11
    MQTT_ERR_ACL_DENIED = 12
    MQTT_ERR_UNKNOWN = 13
    MQTT_ERR_ERRNO = 14


class MqttPublisher(Publisher):

    def __init__(self, mqtt_client):
        self._mqtt_client = mqtt_client

    def publish(self, topic: str, message: Any, **kwargs: Any) -> (int, int):
        try:
            payload = json.dumps(message).encode("utf-8")
        except TypeError as e:
            payload = message
        (rc, mid) = self._mqtt_client.publish(topic, payload=payload, **kwargs)
        return rc, mid if rc == MqttReturnCodes.MQTT_ERR_SUCCESS else rc, None


class MqttMessageReceiver(MessageReceiver):

    def on_message(self, client, userdata, message):
        topic = message.topic
        payload = message.payload.decode("utf-8")
        log.debug(f"Receiving message on topic {topic}: {message}")
        try:
            self.receive(topic=topic, payload=json.loads(payload), params={
                "publisher": MqttPublisher(mqtt_client=client),
                "userdata": userdata,
                "message": message
            })
        except JSONDecodeError as e:
            log.debug(">>> JSONDecodeError:", e)
            self.receive(topic=topic, payload={"payload": payload}, params={
                "publisher": MqttPublisher(mqtt_client=client),
                "userdata": userdata,
                "message": message
            })


class MqttSubscriber(Subscriber):

    def __init__(self, mqtt_client: mqtt.Client, message_receiver: MessageReceiver):
        super().__init__(message_receiver=message_receiver)
        self._mqtt_client = mqtt_client
        self._mqtt_client.on_subscribe = self.on_subscribe
        self._mqtt_client.on_unsubscribe = self.on_unsubscribe
        self._acks = {}

    def wait_for_ack(self, mid):
        while True:
            now = int(time.time())
            if mid not in self._acks:
                return True
            elif (now - self._acks[mid]) > 3:
                return False
            time.sleep(0.1)

    def on_subscribe(self, client, userdata, mid, reason_codes, properties):
        log.debug(f"Subscribed to topic with mid {mid} and reason codes {reason_codes} and properties {properties}")
        if mid in self._acks:
            del self._acks[mid]

    def on_unsubscribe(self, client, userdata, mid, reason_codes, properties):
        log.debug(f"Unsubscribed to topic with mid {mid} and reason codes {reason_codes} and properties {properties}")
        if mid in self._acks:
            del self._acks[mid]

    def _mqtt_subscribe(self, topic: str):
        rc, mid = self._mqtt_client.subscribe(topic)
        if rc != MqttReturnCodes.MQTT_ERR_SUCCESS:
            raise RuntimeError(f"Cannot subscribe to {topic}: {rc}")
        self._acks[mid] = int(time.time())
        return self.wait_for_ack(mid)

    def _mqtt_unsubscribe(self, topic: str):
        rc, mid = self._mqtt_client.unsubscribe(topic)
        if rc != MqttReturnCodes.MQTT_ERR_SUCCESS:
            raise RuntimeError(f"Cannot unsubscribe to {topic}: {rc}")
        self._acks[mid] = int(time.time())
        return self.wait_for_ack(mid)

    def subscribe(self, topic: str, callback: Callable[[str, Any, dict], None], **kwargs: Any):
        print(f"Subscribing to {topic}")
        self._mqtt_client.subscribe(topic)
        self._message_receiver.register_handler(topic, callback)
        print(f"Subscribed to {topic}")

    def unsubscribe(self, topic: str):
        print(f"Unsubscribing from {topic}")
        self._mqtt_client.unsubscribe(topic)
        self._message_receiver.unregister_handler(topic)
        print(f"Unsubscribed from {topic}")

    def establish_subscriptions(self):
        for topic in self._message_receiver.topics:
            print(f"Establish subscription for {topic}")
            self._mqtt_subscribe(topic)


class MqttMessageManager(MessageManager):

    def __init__(
            self,
            mqtt_client: mqtt.Client,
            mqtt_publisher: MqttPublisher = None,
            mqtt_subscriber: MqttSubscriber = None
    ):
        if not mqtt_publisher:
            mqtt_publisher = MqttPublisher(mqtt_client=mqtt_client)
        if not mqtt_subscriber:
            mqtt_subscriber = MqttSubscriber(
                mqtt_client=mqtt_client,
                message_receiver=MqttMessageReceiver()
            )

        super().__init__(
            publisher=mqtt_publisher,
            subscriber=mqtt_subscriber
        )

        self._running = False
        self._connected = False
        self._mqtt_client = mqtt_client
        self._mqtt_client.on_message = self.message_receiver.on_message

    @property
    def mqtt_client(self):
        return self._mqtt_client

    @property
    def running(self) -> bool:
        return self._running

    @property
    def connected(self) -> bool:
        return self._connected

    @property
    def publisher(self) -> MqttPublisher:
        return cast(MqttPublisher, self._publisher)

    @property
    def subscriber(self) -> MqttSubscriber:
        return cast(MqttSubscriber, self._subscriber)

    @property
    def message_receiver(self) -> MqttMessageReceiver:
        return cast(MqttMessageReceiver, self.subscriber.message_receiver)

    def on_connect(self, client, userdata, flags, reason_code, properties=None):
        if reason_code == 0:
            log.debug("Connected to MQTT Broker!")
            self._subscriber.establish_subscriptions()
            self._connected = True
        else:
            log.error("Failed to connect, return code %d\n", reason_code)
            self._connected = False

    def on_disconnect(self, client, userdata, flags, reason_code, properties=None):
        log.debug(f"Disconnected with flags [{flags}] and reason code [{reason_code}]")
        self._connected = False

    def start(self):
        self._mqtt_client.loop_start()
        self._running = True

    def shutdown(self):
        self._mqtt_client.loop_stop()
        self._mqtt_client.disconnect()
        self._running = False

    def connect(self, **kwargs):
        """
        Connect to the MQTT broker.  This method is blocking.
        :param host: Hostname or IP address of the broker
        :param port: Port number of the broker
        :param keepalive: Keepalive time in seconds
        :param bind_address: Bind address for the socket
        :param bind_port: Bind port for the socket
        :param clean_start: Clean session flag
        :param properties: MQTT properties to be sent to the broker
        :param username: Username to authenticate with
        :param password: Password to authenticate with
        :return:
        """
        connect_args = {}
        if "host" in kwargs:
            connect_args["host"] = kwargs["host"]
        if "port" in kwargs:
            connect_args["port"] = kwargs["port"]
        if "keepalive" in kwargs:
            connect_args["keepalive"] = kwargs["keepalive"]
        if "bind_address" in kwargs:
            connect_args["bind_address"] = kwargs["bind_address"]
        if "bind_port" in kwargs:
            connect_args["bind_port"] = kwargs["bind_port"]
        if "clean_start" in kwargs:
            connect_args["clean_start"] = kwargs["clean_start"]
        if "properties" in kwargs:
            connect_args["properties"] = kwargs["properties"]

        auth_args = {}
        if "username" in kwargs:
            auth_args["username"] = kwargs["username"]
        if "password" in kwargs:
            auth_args["password"] = kwargs["password"]

        # TODO: Add code to handle TLS connections

        if auth_args:
            self._mqtt_client.username_pw_set(**auth_args)

        self._mqtt_client.on_connect = self.on_connect
        self._mqtt_client.on_disconnect = self.on_disconnect

        self.start()
        self._mqtt_client.connect(**connect_args)

    def startup(self, **kwargs):
        self.connect(**kwargs)
        for i in range(10 * 60):
            if self._connected:
                break
            time.sleep(0.1)
        return self._connected
