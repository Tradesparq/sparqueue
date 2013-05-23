
from nose.tools import *

import time
import json
import redis
import sparqueue.queue

HELLO_WORLD_JOB = """
{
    "class": "example.job.ExampleJob",
    "install": {
        "pip": "git+ssh://git@github.com/Tradesparq/sparqueue-examples.git"
    },
    "vars": {
        "string": "Hello World"
    }
}
"""

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

    def test_add_list_delete(self):
        jobid = self.queue.push(json.loads(HELLO_WORLD_JOB))
        jobs = self.queue.list()
        jobids = [job['metadata']['jobid'] for job in jobs]
        assert jobid in jobids, '%s does not contain %s' % (jobids, jobid)
        self.queue.cancel(jobid)
        l = self.queue.list()
        assert len(l) == 0, 'Expected empty: %s' % l