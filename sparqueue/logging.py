from __future__ import absolute_import

import os
import logging

SUBDIR = 'logs'

def getLogger(filename):
    name = os.path.basename(filename).split('.')[0]
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    subdir = os.path.join(SUBDIR, str(os.getpid()))
    try:
        os.makedirs(subdir)
    except OSError:
        pass
    # create file handler which logs even debug messages
    logfilename = os.path.join(subdir, '%s.log' % name)
    #logfilename = ''.join([name, '.log'])
    #print 'Log in %s' % logfilename
    fh = logging.FileHandler(logfilename)
    fh.setLevel(logging.DEBUG)
    # create console handler with a higher log level
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)

    formatter = logging.Formatter('%(asctime)s %(process)d %(name)s %(levelname)s %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)

    logger.addHandler(fh)
    logger.addHandler(ch)

    return logger