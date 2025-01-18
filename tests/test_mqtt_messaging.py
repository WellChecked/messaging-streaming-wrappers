import time

import paho.mqtt.client as mqtt

from messaging_streaming_wrappers.mqtt_messaging import MqttMessageManager


def perform_mqtt_messaging() -> None:
    message_manager = MqttMessageManager(mqtt_client=mqtt.Client(mqtt.CallbackAPIVersion.VERSION2))
    message_manager.subscriber.subscribe(topic="data/#", callback=message_manager.subscriber.print_message)
    message_manager.startup(host='localhost', port=1883, keepalive=60)
    try:
        for i in range(10):
            payload = {"i": i}
            message_manager.publish(topic=f"data/{i}/test", message=payload)
            message_manager.publish(topic=f"device/{i}/test", message=payload)

        time.sleep(10.0)
    finally:
        message_manager.shutdown()


if __name__ == '__main__':
    perform_mqtt_messaging()
