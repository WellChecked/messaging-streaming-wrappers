from typing import Dict, Any

import paho.mqtt.client as mqtt
from sparkplub_b_packets.builder import EdgeDevice, EdgeNode
from sparkplub_b_packets.core import sparkplug_b_pb2

from messaging_streaming_wrappers.mqtt_messaging import MqttMessageManager, MqttMessageReceiver, MqttPublisher, MqttSubscriber
from messaging_streaming_wrappers.core.helpers.logging_helpers import get_logger
log = get_logger(__name__)


class SpbMessageReceiver(MqttMessageReceiver):

    def on_message(self, client, userdata, message):
        topic = message.topic
        payload = message.payload
        self.receive(topic=topic, payload=payload, params={
            "publisher": MqttPublisher(mqtt_client=client),
            "userdata": userdata,
            "message": message
        })


class SparkplugBMessageManager(MqttMessageManager):

    def __init__(
            self,
            mqtt_client: mqtt.Client,
            edge_node: EdgeNode,
            edge_devices: Dict[str, EdgeDevice] = None
    ):
        super().__init__(
            mqtt_client=mqtt_client,
            mqtt_publisher=MqttPublisher(mqtt_client=mqtt_client),
            mqtt_subscriber=MqttSubscriber(
                mqtt_client=mqtt_client,
                message_receiver=SpbMessageReceiver()
            )
        )
        self._edge_node = edge_node
        self._edge_devices = edge_devices if edge_devices else {}

    @property
    def edge_node(self) -> EdgeNode:
        return self._edge_node

    @property
    def node_subscription_topic(self):
        return f"spBv1.0/{self.edge_node.group}/NCMD/{self.edge_node.node}/#"

    @property
    def edge_devices(self) -> Dict[str, EdgeDevice]:
        return self._edge_devices

    @property
    def device_subscription_topic(self):
        return f"spBv1.0/{self.edge_node.group}/DCMD/{self.edge_node.node}/#"

    def _publish_node_birth(self, client: mqtt.Client):
        log.debug(f"Node birth for [{self.edge_node.group}/{self.edge_node.node}]")
        client.publish(
            topic=self.edge_node.birth_certificate().topic,
            payload=self.edge_node.birth_certificate().payload()
        )

    def _publish_device_births(self, client: mqtt.Client):
        for device_id, edge_device in self.edge_devices.items():
            log.debug(f"Device birth for [{self.edge_node.group}/{self.edge_node.node}] - [{device_id}]")
            client.publish(
                topic=edge_device.birth_certificate().topic,
                payload=edge_device.birth_certificate().payload()
            )

    @staticmethod
    def parse_sparkplug_b_payload(payload):
        spb_payload = sparkplug_b_pb2.Payload()
        spb_payload.ParseFromString(payload)
        return spb_payload

    def on_ncmd_message(self, topic: str, message: Any, params: dict = None):
        print(f"Received NCMD message on topic {topic}: {message} --- {params}")
        rebirth = False
        payload = self.parse_sparkplug_b_payload(message)
        for metric in payload.metrics:
            log.debug(f"Node control [{metric.name}] for [{self.edge_node.group}/{self.edge_node.node}]")
            if not rebirth:
                self._publish_node_birth(self._mqtt_client)
                self._publish_device_births(self._mqtt_client)
            rebirth = True

    def on_dcmd_message(self, topic: str, message: Any, params: dict = None):
        print(f"Received DCMD message on topic {topic}: {message} --- {params}")
        rebirth = False
        payload = self.parse_sparkplug_b_payload(message)
        for metric in payload.metrics:
            log.debug(f"Data control [{metric.name}] for [{self.edge_node.group}/{self.edge_node.node}]")
            if not rebirth:
                self._publish_node_birth(self._mqtt_client)
                self._publish_device_births(self._mqtt_client)
            rebirth = True

    def on_connect(self, client, userdata, flags, reason_code, properties=None):
        if reason_code == 0:
            log.debug("Connected to MQTT Broker!")
            self._subscriber.establish_subscriptions()
            self._connected = True
            self._publish_node_birth(client)
            self._publish_device_births(client)
        else:
            log.error(f"Failed to connect, return code [{reason_code}]")
            self._connected = False

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
        :param will_set: Will set to be published when the client disconnects
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

        self._mqtt_client.will_set(
            self.edge_node.death_certificate().topic,
            self.edge_node.death_certificate().payload(),
            0, False
        )

        self._mqtt_client.on_connect = self.on_connect
        self._mqtt_client.on_disconnect = self.on_disconnect

        self.start()
        self._mqtt_client.connect(**connect_args)
