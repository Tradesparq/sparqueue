from __future__ import absolute_import
import redis
import os

def client(config=None):
    #print dir(redis)
    if config and 'redis' in config:
        redis_config = config['redis']
        print 'Using alternate Redis config %s' % (redis_config)
        return redis.Redis(host=redis_config['host'], port=int(redis_config['port']))
    return redis.Redis()

#print 'Python Redis version %s' % redis.__version__

WatchError = redis.WatchError