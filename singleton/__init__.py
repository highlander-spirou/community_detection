"""
Provide singleton access for some class, ie Redis
"""
from redis import Redis
from os import environ

redis_host = environ.get('REDIS_HOST')
if redis_host is None:
    raise Exception("Environment variable REDIS_HOST not found")


redis_0 = Redis(host=redis_host, db=0)
redis_1 = Redis(host=redis_host, db=1)
redis_2 = Redis(host=redis_host, db=2)
