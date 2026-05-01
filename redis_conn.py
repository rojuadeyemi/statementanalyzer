import redis
import os

redis_url = os.environ["REDIS_URL"]

conn = redis.from_url(redis_url)
