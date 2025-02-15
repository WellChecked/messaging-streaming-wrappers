import json
import time
import uuid
from typing import Any

from pydantic import BaseModel

from messaging_streaming_wrappers.core.helpers.logging_helpers import get_logger
from tests.rds.redis_pool import get_redis_client

log = get_logger(__name__)


class RedisMessage(BaseModel):
    mid: str
    ts: int
    type: str
    topic: str
    payload: Any



def publish_message(topic: str, message: Any):
    redis_client = get_redis_client()
    redis_client.xadd(name='test-stream', fields=RedisMessage(
        mid=uuid.uuid4().hex,
        ts=int(time.time() * 1000),
        type="json",
        topic=topic,
        payload=message if isinstance(message, str) else json.dumps(message)
    ).model_dump())


if __name__ == '__main__':
    publish_message('incoming/1a', {"name": "test-1-a"})
    publish_message('incoming/1b', {"name": "test-1-b"})
    publish_message('incoming/1c', {"name": "test-1-c"})
    publish_message('incoming/1d', {"name": "test-1-d"})
    time.sleep(1)
    publish_message('incoming/2a', {"name": "test-2-a"})
    publish_message('incoming/2b', {"name": "test-2-b"})
    publish_message('incoming/2c', {"name": "test-2-c"})
    publish_message('incoming/2d', {"name": "test-2-d"})
    time.sleep(1)
    publish_message('incoming/3a', {"name": "test-3-a"})
    publish_message('incoming/3b', {"name": "test-3-b"})
    publish_message('incoming/3c', {"name": "test-3-c"})
    publish_message('incoming/3d', {"name": "test-3-d"})
    time.sleep(1)
    publish_message('incoming/4a', {"name": "test-4-a"})
    publish_message('incoming/4b', {"name": "test-4-b"})
    publish_message('incoming/4c', {"name": "test-4-c"})
    publish_message('incoming/4d', {"name": "test-4-d"})
    time.sleep(1)
    publish_message('incoming/5a', {"name": "test-5-a"})
    publish_message('incoming/5b', {"name": "test-5-b"})
    publish_message('incoming/5c', {"name": "test-5-c"})
    publish_message('incoming/5d', {"name": "test-5-d"})
    time.sleep(1)
    publish_message('incoming/6a', {"name": "test-6-a"})
    publish_message('incoming/6b', {"name": "test-6-b"})
    publish_message('incoming/6c', {"name": "test-6-c"})
    publish_message('incoming/6d', {"name": "test-6-d"})
    time.sleep(1)
    publish_message('incoming/7a', {"name": "test-7-a"})
    publish_message('incoming/7b', {"name": "test-7-b"})
    publish_message('incoming/7c', {"name": "test-7-c"})
    publish_message('incoming/7d', {"name": "test-7-d"})
