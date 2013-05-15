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


class RedisQueue():
    def __init__(self, r, system, queue_name, config=None, separator='|'):
        self.r = r
        self.system = system
        self.separator = separator
        self.queue_name = queue_name
        self.config = config

        print 'Redis version %s' % self.r.info()['redis_version']

        self.current_key = self.prefix('workers', 'current')
        self.activity_key = self.prefix('queues', queue_name, 'workers')
        self.jobs_key = self.prefix('queues', 'jobs')

        self.pending_key = self.prefix('queues', queue_name, 'pending')
        self.cancelled_key = self.prefix('queues', queue_name, 'cancelled')
        self.ongoing_key = self.prefix('queues', queue_name, 'all')
        self.success_key = self.prefix('queues', queue_name, 'success')
        self.failed_key = self.prefix('queues', queue_name, 'failed')

        self.hostname = socket.gethostname()
        self.pid = os.getpid()
        self.client_id = '%s.%s.%s' % (self.hostname, self.pid, time.time())

    def exit(self):
        self.r.hdel(self.activity_key, self.client_id)

    def workers(self):
        retlist = []
        workers = self.r.hgetall(self.activity_key)
        if not workers:
            return retlist

        for w in workers:
            last = float(workers[w])
            retlist.append(self._worker_info(last, w))

        return retlist

    def worker_delete(self, workerid):
        m = self.r.pipeline()
        m.hget(self.activity_key, workerid)
        m.hdel(self.activity_key, workerid)
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

        m = self.r.pipeline()
        m.hset(self.jobs_key, jobid, json.dumps(config))
        m.lpush(self.pending_key, jobid)
        m.hset(self.ongoing_key, jobid, time.time())
        m.execute()

        return jobid

    def next(self, timeout=5):
        # we update the activity timestamp for the current worker
        self.r.hset(self.activity_key, self.client_id, time.time())

        # block pop with catchable exception
        retvalue = self.r.brpop(self.pending_key, timeout)
        if not retvalue:
            raise QueueException('brpop returned null for %s due to timeout - please retry' % self.pending_key)
        (key, jobid) = retvalue
        if self.r.sismember(self.cancelled_key, jobid):
            raise QueueException('jobid is cancelled %s - please retry' % jobid)

        # use the jobid and set all related information
        m = self.r.pipeline()
        m.hget(self.jobs_key, jobid)
        m.hset(self.ongoing_key, jobid, time.time())
        m.hset(self.current_key, self.client_id, jobid)
        retvalue = m.execute()

        config = retvalue[0]

        return json.loads(config)

    def success(self, output, stats={}):
        (jobid, current) = self._get_current_job()

        current['output'] = output
        current['stats'] = stats

        self._finalize(jobid, self.success_key, current)

    def failed(self, e):
        (jobid, current) = self._get_current_job()
        current['last_error'] = str(e)
        current['traceback'] = traceback.format_exc(e)

        self._finalize(jobid, self.failed_key, current)

    def status(self, jobid):
        m = self.r.pipeline()
        m.hexists(self.ongoing_key, jobid)
        m.hexists(self.pending_key, jobid)
        m.sismember(self.failed_key, jobid)
        m.sismember(self.success_key, jobid)
        m.sismember(self.cancelled_key, jobid)
        states = m.execute()

        if states[4]:
            return 'CANCELLED'
        elif states[0]:
            if states[1]:
                return 'PENDING'
            else:
                return 'ACTIVE'
        else:
            if states[2]:
                return 'FAILED'
            elif states[3]:
                return 'SUCCESS'
            else:
                return 'UNKNOWN'

    def job(self, jobid):
        return json.loads(self.r.hget(self.jobs_key, jobid))

    def requeue(self, timeout=60):
        # since ongoing_key may change while we're checking
        # we want to use optimistic locking and retry
        # if that key change
        requeued = []
        with self.r.pipeline() as pipe:
            while 1:
                try:
                    pipe.watch(self.ongoing_key)
                    pipe.watch(self.pending_key)
                    jobids = self.r.hkeys(self.ongoing_key)
                    pending = self.r.lrange(self.pending_key, 0, -1)
                    pipe.multi()
                    for jobid in jobids:
                        # if the jobid has timed out
                        if time.time() - float(self.r.hget(self.ongoing_key, jobid)) > timeout:
                            # and the jobid is not in the list of pending keys
                            try:
                                pending.index(jobid)
                            except ValueError:
                                # push back to pending
                                requeued.append(jobid)
                                pipe.lpush(self.pending_key, jobid)
                                pipe.hset(self.ongoing_key, jobid, time.time())
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
        m.smembers(self.success_key),
        m.smembers(self.failed_key),
        m.smembers(self.pending_key),
        m.hkeys(self.jobs_key)
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
            m.hget(self.jobs_key, jobid)
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
                jobid = job['metadata']['status'] = status

            unwanted = set(job) - set(selector)
            for unwanted_key in unwanted: del job[unwanted_key]
            jobs.append(job)

        return jobs

    def cancel(self, jobid):
        # if it's active, there's not much to be done except if the job
        # explicitly checks for cancellation?
        m = self.r.pipeline()
        # we mark this as cancelled in case it's requeued
        m.sadd(self.cancelled_key, jobid)
        # we fetch and delete from jobs key
        m.hget(self.jobs_key, jobid)
        m.hdel(self.jobs_key, jobid)
        # we delete from whatever start or end states we can
        m.hdel(self.pending_key, jobid)
        m.hdel(self.success_key, jobid)
        m.hdel(self.failed_key, jobid)
        m.hdel(self.ongoing_key, jobid)
        results = m.execute()
        return json.loads(results[1])

    def _finalize(self, jobid, key, current):
        m = self.r.pipeline()
        m.sadd(key, jobid)
        m.hdel(self.current_key, self.client_id)
        m.hdel(self.ongoing_key, jobid)
        m.hset(self.jobs_key, jobid, json.dumps(current))
        m.execute()

    def _get_current_job(self):
        jobid = self.r.hget(self.current_key, self.client_id)
        if not jobid:
            raise QueueException('Finish called with no current task')
        current_json = self.r.hget(self.jobs_key, jobid)
        current = json.loads(current_json)
        return (jobid, current)

    def _worker_info(self, last, workerid, timeout=60):
        (hostname, pid, seconds, millis) = workerid.split('.')
        started = float('%s.%s' % (seconds, millis))
        now = time.time()
        last_update = now - last
        jobid = self.r.hget(self.current_key, self.client_id)
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
        return self.join_key([self.system, self.separator.join(list(arg))])


