
from mockredis import MockRedis
from nose.tools import *

import time

import redis
import sparqueue.queue

def setup():
    global redis_client
    #redis = MockRedis()
    #redis.flushdb()

def teardown():
    print "TEAR DOWN!"



class TestQueueManager:

    def setup(self):
        print ("TestUM:setup() before each test method")
        self.queue_manager = sparqueue.queue.QueueManager({}, TestQueueManager.redis_client)
        self.queue_name = "queueC"
        self.system_name = "TestC %s" % time.time()

    def teardown(self):
        print ("TestUM:teardown() after each test method")

    @classmethod
    def setup_class(cls):
        print ("setup_class() before any methods in this class")
        TestQueueManager.redis_client = redis.Redis()

    @classmethod
    def teardown_class(cls):
        print ("teardown_class() after any methods in this class")

    @raises(sparqueue.queue.QueueDoesNotExistException)
    def test_pop_no_queues(self):
        self.queue_manager.pop(timeout=1)

    @raises(sparqueue.queue.QueueTimeoutException)
    def test_pop_empty(self):
        queue = self.queue_manager.add(self.system_name, self.queue_name)
        self.queue_manager.pop(timeout=1)

    def test_pop_job(self):
        queue = self.queue_manager.add(self.system_name, self.queue_name)
        queue.push( { "class": "dummy", "vars": {}})
        job = self.queue_manager.pop(timeout=1)
        print job

    def test_add_list(self):
        self.queue_manager.add_list([{"system": self.system_name, "queue": self.queue_name}])
        queue = self.queue_manager.get(self.system_name, self.queue_name)
        assert isinstance(queue, sparqueue.queue.RedisQueue)
        assert queue.queue_name == self.queue_name
        assert queue.system_name == self.system_name

    def test_requeue(self):
        assert len(self.queue_manager.requeue()) == 0

