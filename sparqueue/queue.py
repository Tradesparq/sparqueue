import datetime
import json
import math
import os
import redis
import socket
import time
import traceback

# if we recreate the queue (due to disconnection),
# we still want to have this counter correct for the process
PROCESS_JOB_COUNTER = 0

def unix_to_iso8601(unix):
    return datetime.datetime.fromtimestamp(unix).isoformat()

class QueueException(Exception):
    pass

class QueueDoesNotExistException(QueueException):
    pass

class QueueJobCancelledException(QueueException):
    pass

class QueueTimeoutException(QueueException):
    pass

class RedisQueue():
    def __init__(self, r, system, queue_name, config=None, separator='|'):
        self.r = r
        self.system_name = system
        self.separator = separator
        self.queue_name = queue_name
        self.config = config

        self.activity_hash = self.prefix('queues', queue_name, 'workers')
        self.cancelled_hash = self.prefix('queues', queue_name, 'cancelled')
        self.current_hash = self.prefix('workers', 'current')
        self.failed_set = self.prefix('queues', queue_name, 'failed')
        self.jobs_hash = self.prefix('queues', 'jobs')
        self.ongoing_hash = self.prefix('queues', queue_name, 'all')
        self.pending_list = self.prefix('queues', queue_name, 'pending')
        self.success_set = self.prefix('queues', queue_name, 'success')
        self.step_hash = self.prefix('queues', queue_name, 'step')

        self.hostname = socket.gethostname()
        self.pid = os.getpid()
        self.client_id = '%s.%s.%s' % (self.hostname, self.pid, time.time())

    def exit(self):
        self.r.hdel(self.activity_hash, self.client_id)

    def workers(self):
        retlist = []
        workers = self.r.hgetall(self.activity_hash)
        if not workers:
            return retlist

        for w in workers:
            last = float(workers[w])
            retlist.append(self._worker_info(last, w))

        return retlist

    def worker_delete(self, workerid):
        m = self.r.pipeline()
        m.hget(self.activity_hash, workerid)
        m.hdel(self.activity_hash, workerid)
        results = m.execute()
        return self._worker_info(float(results[0]), workerid)

    def push(self, config):
        assert 'vars' in config
        assert 'class' in config
        global PROCESS_JOB_COUNTER
        PROCESS_JOB_COUNTER = PROCESS_JOB_COUNTER + 1
        self.timestamp = time.time()
        jobid = "%s.%s.%s.%s" % (
            self.hostname, self.pid, self.timestamp, PROCESS_JOB_COUNTER)
        if 'metadata' not in config:
            config['metadata'] = {}
        config['metadata']['jobid'] = jobid
        config['metadata']['queue'] = self.queue_name
        config['metadata']['system'] = self.system_name

        m = self.r.pipeline()
        m.hset(self.jobs_hash, jobid, json.dumps(config))
        m.lpush(self.pending_list, jobid)
        m.hset(self.ongoing_hash, jobid, time.time())
        m.execute()

        return jobid

    def active_worker(self):
        # we update the activity timestamp for the current worker
        self.r.hset(self.activity_hash, self.client_id, time.time())

    def active_job(self, jobid):
        self.r.hset(self.ongoing_hash, jobid, time.time())

    def pop(self, timeout=3):
        self.active_worker()

        # block pop with catchable exception
        retvalue = self.r.brpop(self.pending_list, timeout)
        if not retvalue:
            raise QueueTimeoutException('brpop returned null for %s due to timeout - please retry' % self.pending_list)
        (key, jobid) = retvalue

        self.activate_job(self, jobid)

    def activate_job(self, jobid):
        if self.r.sismember(self.cancelled_hash, jobid):
            raise QueueJobCancelledException('jobid is cancelled %s - please retry' % jobid)

        # use the jobid and set all related information
        m = self.r.pipeline()
        m.hget(self.jobs_hash, jobid)
        m.hset(self.ongoing_hash, jobid, time.time())
        m.hset(self.current_hash, self.client_id, jobid)
        retvalue = m.execute()

        config = retvalue[0]

        return json.loads(config)

    def success(self, output, stats={}):
        (jobid, current) = self._get_current_job()

        current['output'] = output
        current['stats'] = stats

        self._finalize(jobid, self.success_set, current)

    def failed(self, e):
        (jobid, current) = self._get_current_job()
        current['last_error'] = str(e)
        current['traceback'] = traceback.format_exc(e)

        self._finalize(jobid, self.failed_set, current)

    def status(self, jobid):
        m = self.r.pipeline()
        IS_ONGOING = 0
        m.hexists(self.ongoing_hash, jobid) #0
        IS_EXIST = 1
        m.hexists(self.jobs_hash, jobid) #1
        IS_FAILED = 2
        m.sismember(self.failed_set, jobid) #2
        IS_SUCCESS = 3
        m.sismember(self.success_set, jobid) #3
        IS_CANCELLED = 4
        m.sismember(self.cancelled_hash, jobid) #4
        STEP = 5
        m.hget(self.step_hash, jobid)
        states = m.execute()

        state = 'UNKNOWN'
        if states[IS_EXIST]:
            if states[IS_FAILED]:
                state = 'FAILED'
            elif states[IS_SUCCESS]:
                state = 'SUCCESS'
            elif states[IS_CANCELLED]:
                state = 'CANCELLED'
            elif states[IS_ONGOING]:
                state = 'ACTIVE'

        return {
            'state': state,
            'step': states[STEP]
        }

    def job(self, jobid):
        return json.loads(self.r.hget(self.jobs_hash, jobid))

    def requeue(self, timeout=10):
        # since ongoing_hash may change while we're checking
        # we want to use optimistic locking and retry
        # if that key change
        requeued = []
        with self.r.pipeline() as pipe:
            while 1:
                try:
                    pipe.watch(self.ongoing_hash)
                    pipe.watch(self.pending_list)
                    jobids = self.r.hkeys(self.ongoing_hash)
                    pending = self.r.lrange(self.pending_list, 0, -1)
                    pipe.multi()
                    for jobid in jobids:
                        # if the jobid has timed out
                        if time.time() - float(self.r.hget(self.ongoing_hash, jobid)) > timeout:
                            # and the jobid is not in the list of pending keys
                            try:
                                pending.index(jobid)
                            except ValueError:
                                # push back to pending
                                requeued.append(jobid)
                                pipe.lpush(self.pending_list, jobid)
                                pipe.hset(self.ongoing_hash, jobid, time.time())
                    pipe.execute()
                    break
                except redis.WatchError:
                    continue
        return requeued

    def list(self, status=set(['EVERY']), selector=set(['metadata'])):
        jobids = set()
        success = set()
        failed = set()
        pending = set()
        every = set()

        m = self.r.pipeline()
        m.smembers(self.success_set),
        m.smembers(self.failed_set),
        m.smembers(self.pending_list),
        m.hkeys(self.jobs_hash)
        results = m.execute()

        success.update(results[0])
        failed.update(results[1])
        pending.update(results[2])
        every.update(results[3])

        if 'SUCCESS' in status:
            jobids.update(success)
        if 'FAILED' in status:
            jobids.update(failed)
        if 'PENDING' in status:
            jobids.update(pending)
        if 'EVERY' in status:
            jobids.update(every)

        m = self.r.pipeline()
        for jobid in jobids:
            m.hget(self.jobs_hash, jobid)
        results = m.execute()

        jobs = []
        for result in results:
            job = json.loads(result)
            if 'metadata' in selector:
                jobid = job['metadata']['jobid']
                if jobid in success:
                    status = 'SUCCESS'
                elif jobid in failed:
                    status = 'FAILED'
                else:
                    if jobid in pending:
                        status = 'PENDING'
                    else:
                        status = 'ACTIVE'
                job['metadata']['status'] = status

            unwanted = set(job) - set(selector)
            for unwanted_key in unwanted:
                del job[unwanted_key]
            jobs.append(job)

        return jobs

    def cancel(self, jobid):
        # if it's active, there's not much to be done except if the job
        # explicitly checks for cancellation?
        m = self.r.pipeline()
        # we mark this as cancelled in case it's requeued
        m.sadd(self.cancelled_hash, jobid)
        # we fetch and delete from jobs key
        m.hget(self.jobs_hash, jobid)
        m.hdel(self.jobs_hash, jobid)
        # we delete from whatever start or end states we can
        m.hdel(self.pending_list, jobid)
        m.srem(self.success_set, jobid)
        m.hdel(self.failed_set, jobid)
        m.hdel(self.ongoing_hash, jobid)
        results = m.execute()
        return json.loads(results[1])

    def step(self, jobid, stepname):
        self.active_worker()
        self.active_job(jobid)
        return self.r.hset(self.step_hash, jobid, stepname)

    def _finalize(self, jobid, key, current):
        m = self.r.pipeline()
        m.sadd(key, jobid)
        m.hdel(self.current_hash, self.client_id)
        m.hdel(self.ongoing_hash, jobid)
        m.hset(self.jobs_hash, jobid, json.dumps(current))
        m.hdel(self.step_hash, jobid)
        m.execute()

    def _get_current_job(self):
        jobid = self.r.hget(self.current_hash, self.client_id)
        if not jobid:
            raise QueueException('Finish called with no current task')
        current_json = self.r.hget(self.jobs_hash, jobid)
        current = json.loads(current_json)
        return (jobid, current)

    def _worker_info(self, last, workerid, timeout=60):
        (hostname, pid, seconds, millis) = workerid.split('.')
        started = float('%s.%s' % (seconds, millis))
        now = time.time()
        last_update = now - last
        jobid = self.r.hget(self.current_hash, self.client_id)
        if last_update > timeout:
            status = 'STALE'
        else:
            if jobid:
                status = 'PROCESSING'
            else:
                status = 'IDLE'
        return {
            'workerid': workerid,
            'uptime': now - started,
            'last': last_update,
            'started': unix_to_iso8601(started),
            'hostname': hostname,
            'pid': pid,
            'jobid': jobid,
            'status': status
        }

    def join_key(self, *arg):
        return self.separator.join(*arg)

    def prefix(self, *arg):
        return self.join_key([self.system_name, self.separator.join(list(arg))])


class QueueManager():
    def __init__(self,  config, redisclient):
        self.queues = {}
        self.config = config
        self.r = redisclient
        self.pending_list = []
        self.pending_key_to_queue = {}
        self.last_queue = None # mostly for operations on workers

    def pop(self, timeout=3):
        # block pop with catchable exception
        if not self.pending_list:
            raise QueueDoesNotExistException('Not queues configured')
        self.last_queue.active_worker()
        retvalue = self.r.brpop(self.pending_list, timeout=timeout)
        if not retvalue:
            raise QueueTimeoutException('brpop returned null for %s due to timeout - please retry' % self.pending_list)
        (key, jobid) = retvalue
        queue = self.pending_key_to_queue[key]

        return (queue, queue.activate_job(jobid))

    def requeue(self):
        requeued = []
        for (key, queue) in self.pending_key_to_queue.iteritems():
            requeued.extend(queue.requeue())
        return requeued

    def add_list(self, queues):
        for queue in queues:
            system_name = queue['system']
            queue_name = queue['queue']
            queue = self.add(system_name, queue_name)

    def add(self, system_name, queue_name):
        queue = RedisQueue(
            self.r,
            system_name,
            queue_name,
            self.config)
        if system_name not in self.queues:
            self.queues[system_name] = {}

        self.queues[system_name][queue_name] = queue
        self.pending_list.append(queue.pending_list)
        self.pending_key_to_queue[queue.pending_list] = queue
        self.last_queue = queue
        return queue

    def get(self, system_name, queue_name):
        if system_name not in self.queues or queue_name not in self.queues[system_name]:
            raise QueueDoesNotExistException(
                'Invalid queue: %s/%s' % (system_name, queue_name))
        return self.queues[system_name][queue_name]
