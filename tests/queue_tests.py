
from nose.tools import *

import time
import json
import redis
import sparqueue.queue

class TestQueue:

    def setup(self):
        print ("TestUM:setup() before each test method")
        self.queue_name = "queueC"
        self.system_name = "TestC %s" % time.time()
        self.queue = sparqueue.queue.RedisQueue(self.redis_client, self.system_name, self.queue_name)

    @classmethod
    def setup_class(cls):
        print ("setup_class() before any methods in this class")
        TestQueue.redis_client = redis.Redis()