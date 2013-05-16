
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

    def test_incoming_empty(self):
        assert self.queue.incoming() == None

    def test_incoming_one_job(self):
        config = {
            "class": "dummy.class",
            "vars": {},
            "metadata": {
                "jobid": "submitterid"
            }
        }
        config_json = json.dumps(config)
        assert TestQueue.redis_client.lpush(self.queue.incoming_list, config_json)
        incoming_jobid = self.queue.incoming()
        submitted_jobid = TestQueue.redis_client.hget(self.queue.submitted_hash, "submitterid")
        assert incoming_jobid == submitted_jobid, 'expected %s == %s' % (incoming_jobid, submitted_jobid)


