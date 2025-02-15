import time
import redis

from typing import Any

from messaging_streaming_wrappers.redis_streaming import RedisStreamManager


def perform_redis_streaming():
    def print_data_message(topic: str, message: Any, params: dict = None):
        print(f"Received DATA message on topic {topic}: {message} --- {params}")

    def print_device_message(topic: str, message: Any, params: dict = None):
        print(f"Received DEVICE message on topic {topic}: {message} --- {params}")

    async def print_test_message(topic: str, message: Any, params: dict = None):
        print(f"Received TEST message on topic {topic}: {message} --- {params}")

    stream_manager = RedisStreamManager(
        redis_client=redis.Redis(host='localhost', port=6379, decode_responses=True),
        stream_name="test_stream"
    )
    stream_manager.subscriber.subscribe(topic="data/#", callback=print_data_message)
    stream_manager.subscriber.subscribe(topic="device/#", callback=print_device_message)
    stream_manager.subscriber.subscribe(topic="test/#", callback=print_test_message)
    stream_manager.start()
    try:
        for i in range(10):
            payload = {"i": i}
            print(f"Publishing to topic data/{i}/test: {payload}")
            stream_manager.publish(topic=f"data/{i}/test", message=payload)
            print(f"Publishing to topic device/{i}/test: {payload}")
            stream_manager.publish(topic=f"device/{i}/test", message=payload)
            print(f"Publishing to topic test/{i}/test: {payload}")
            stream_manager.publish(topic=f"test/{i}/test", message=payload)
        time.sleep(15.0)
    finally:
        stream_manager.shutdown()


if __name__ == '__main__':
    perform_redis_streaming()
