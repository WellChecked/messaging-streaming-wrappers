from messaging_streaming_wrappers.loopback_messaging import LoopbackMessageManager


def perform_loopback_messaging() -> None:
    message_manager = LoopbackMessageManager()
    message_manager.subscriber.subscribe(topic="data/#", callback=message_manager.subscriber.print_message)
    message_manager.connect()

    for i in range(10):
        payload = {"i": i}
        message_manager.message_receiver.send_message(topic=f"data/{i}/test", message=payload)
        message_manager.message_receiver.send_message(topic=f"device/{i}/test", message=payload)


if __name__ == '__main__':
    perform_loopback_messaging()
