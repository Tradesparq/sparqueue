from __future__ import absolute_import

from bottle import route, run, request, post, get, response, delete

import json
import sys
import redis
import time

import sparqueue.queue
import sparqueue.redis
import sparqueue.config
import sparqueue.logging

logger = sparqueue.logging.getLogger(__file__)

@post('/<system>/queues/<queue>/jobs')
def queues_submit(system, queue):
    queue_instance = MANAGER.get(system, queue)
    response.headers['Content-Type'] = 'text/plain'
    try:
        data = request.forms.get('job')
        job = json.loads(data)
    except TypeError,e:
        logger.error('Error loading: %s' % data)
        raise
    except ValueError,e:
        logger.error('Error loading: %s' % data)
        raise
    return queue_instance.push(job)

@get('/<system>/queues/<queue>/jobs')
def queues_list(system, queue):
    queue_instance = MANAGER.get(system, queue)
    response.headers['Content-Type'] = 'text/json'
    l = queue_instance.list()
    return json.dumps(l)

@get('/<system>/queues/<queue>/workers')
def workers_list(system, queue):
    queue_instance = MANAGER.get(system, queue)
    response.headers['Content-Type'] = 'text/json'
    l = queue_instance.workers()
    return json.dumps(l)

@delete('/<system>/queues/<queue>/workers/<workerid>')
def workers_delete(system, queue, workerid):
    queue_instance = MANAGER.get(system, queue)
    response.headers['Content-Type'] = 'text/json'
    return queue_instance.worker_delete(workerid)

@get('/<system>/queues/<queue>/jobs/<jobid>')
def queues_job(system, queue, jobid):
    queue_instance = MANAGER.get(system, queue)
    response.headers['Content-Type'] = 'text/json'
    return queue_instance.job(jobid)

@delete('/<system>/queues/<queue>/jobs/<jobid>')
def queues_job_cancel(system, queue, jobid):
    queue_instance = MANAGER.get(system, queue)
    response.headers['Content-Type'] = 'text/json'
    return queue_instance.cancel(jobid)

@get('/<system>/queues/<queue>/jobs/<jobid>/status')
def queues_state(system, queue, jobid):
    queue_instance = MANAGER.get(system, queue)
    response.headers['Content-Type'] = 'text/json'
    return queue_instance.status(jobid)

def loop(config_filename):
    while True:
        config = sparqueue.config.get_config(config_filename)
        try:
            execute(config)
        except redis.exceptions.ConnectionError,e:
            logger.error('Error connecting: ' + str(e))
            time.sleep(5)
            continue
        break

def execute(config):
    global MANAGER

    redisclient = sparqueue.redis.client(config)

    MANAGER = sparqueue.queue.QueueManager(config, redisclient)

    for queue in config['queues']:
        system_name = queue['system']
        queue_name = queue['queue']
        MANAGER.add(system_name, queue_name)
        logger.info('System: %s Queue %s' % (system_name, queue_name))

    run(host=config['host'], port=config['port'], debug=config['debug'])