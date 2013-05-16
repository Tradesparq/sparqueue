import os
import redis
import sh

import sys
import time
import traceback

import sparqueue.config
import sparqueue.loader
import sparqueue.logging
import sparqueue.queue
import sparqueue.redis
import sparqueue.reporter

RUN = True

logger = sparqueue.logging.getLogger(__file__)

def stop():
    global RUN
    logger.info('EXIT REQUESTED, FINISHING LOOP')

    RUN = False

def execute(config_filename):
    config = sparqueue.config.get_config(config_filename)
    jobloader = sparqueue.loader.JobLoader()

    while RUN:
        try:
            loop(config, jobloader)
        except redis.exceptions.ConnectionError,e:
            logger.error('Error connecting: ' + str(e))
            time.sleep(5)


def loop(config, jobloader):
    # unique name for restart
    redisclient = sparqueue.redis.client(config)

    queue = sparqueue.queue.RedisQueue(
        redisclient,
        # TODO: support multiple queues
        config['queues'][0]['system'],
        config['queues'][0]['queue'],
        config)

    requeued = queue.requeue()
    while RUN:
        logger.info('brpop queue %s [%s]' % (queue.queue_name, queue.client_id))

        while RUN:
            try:
                job = queue.next()
                break
            except sparqueue.queue.QueueException,e:
                # TODO: want probably to spawn this in another cron process instead
                requeued = queue.requeue()
                if len(requeued) > 1:
                    print 'Requeued %s' % ','.join(requeued)

        # exit before continuing on when not running
        if not RUN:
            break

        logger.info(job)

        reporter =  sparqueue.reporter.Reporter(
            logger,
            queue,
            job['metadata']['jobid'])

        if 'install' in job:
            if 'pip' in job['install']:
                reporter.step('installing %s' % job['install']['pip'])
                try:
                    logger.info(sh.pip('install', job['install']['pip']))
                except sh.ErrorReturnCode,e:
                    logger.error('problem installing %s: %s' % (
                        job['metadata']['jobid'], traceback.format_exc(e)))
                    queue.failed(e)
                    continue

        jobInstance = jobloader.getInstance(job['class'], config)
        if not jobInstance:
            logger.error('Invalid class name: %s' % job['class'])

        try:
            job['vars']['reporter'] = reporter
            output = jobInstance.perform(**job['vars'])
            reporter.finish()
            queue.success(output, reporter.stats())
        except Exception,e:
            logger.error('problem processing %s: %s' % (
                job['metadata']['jobid'], traceback.format_exc(e)))
            queue.failed(e)

    logger.info('Exiting gracefully, no current queue jobs left unprocessed')
    queue.exit()

