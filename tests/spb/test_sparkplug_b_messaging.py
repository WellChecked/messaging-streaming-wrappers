import time
import random

import paho.mqtt.client as mqtt
from sparkplub_b_packets.builder import EdgeDevice, EdgeNode

from spb.example_sparkplug_b_builder import EdgeComputer, OGIDevice
from messaging_streaming_wrappers.sparkplug_b_messaging import SparkplugBMessageManager


def perform_spb_messaging() -> None:
    def get_random_float(a=1.5, b=1.9):
        return random.uniform(a, b)

    def get_random_int(a=1, b=10):
        return random.randint(a, b)

    def get_node_data(node: EdgeNode):
        packet = node.data_packet(
            cpu_load=get_random_float(0.5, 1.0),
            memory_avail=get_random_int(1000000, 1000000000)
        )
        return packet

    def get_device_data(device: EdgeDevice):
        packet = device.data_packet(
            setpoint=get_random_int(1, 255),
            flow=get_random_float(100, 500),
            quality=get_random_float(0, 99),
            count=get_random_int(10000, 20000),
            uptime=get_random_float(0, 99)
        )
        return packet

    message_manager = SparkplugBMessageManager(
        mqtt_client=mqtt.Client(protocol=mqtt.MQTTv5),
        edge_node=EdgeComputer(group="ascent.watson", node="watson"),
        edge_devices={
            "OGI": OGIDevice(group="ascent.watson", node="watson", device_id="OGI")
        }
    )

    message_manager.subscriber.subscribe(
        topic=message_manager.node_subscription_topic,
        callback=message_manager.subscriber.print_message
    )
    message_manager.subscriber.subscribe(
        topic=message_manager.device_subscription_topic,
        callback=message_manager.subscriber.print_message
    )

    message_manager.startup(host='localhost', port=1883, keepalive=60)
    try:
        for i in range(10):
            node_packet = get_node_data(message_manager.edge_node)
            message_manager.publish(topic=node_packet.topic, message=node_packet.payload())

            device_packet = get_device_data(message_manager.edge_devices["OGI"])
            message_manager.publish(topic=device_packet.topic, message=device_packet.payload())

            time.sleep(1.0)

        time.sleep(10.0)
    finally:
        message_manager.shutdown()


if __name__ == '__main__':
    perform_spb_messaging()
