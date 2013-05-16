import json
import os

import sparqueue.client
import sparqueue.config

def submit(args):
    if type(args) is dict:
        obj = args
    else:
        content = file(args.filename).read()
        obj = json.loads(content)
    if 'metadata' not in obj:
        obj['metadata'] = {}
    obj['metadata']['user'] =  os.environ['USER']
    return SPARQUEUE_CLIENT.submit(obj)

def list_jobs(args):
    return format_json(SPARQUEUE_CLIENT.list())

def workers(args):
    return format_json(SPARQUEUE_CLIENT.workers())

def job(args):
    return format_json(SPARQUEUE_CLIENT.job(args.jobid))

def status(args):
    s = json.loads(SPARQUEUE_CLIENT.status(args.jobid))
    if args.text:
        if s['step']:
            return '%s;%s' % (s['state'], s['step'])
        else:
            return s['state']
    else:
        return format_json(s)

def traceback(args):
    obj = json.loads(SPARQUEUE_CLIENT.job(args.jobid))
    if 'traceback' in obj:
        return obj['traceback']
    else:
        return 'No error'

def worker_delete(args):
    return format_json(SPARQUEUE_CLIENT.worker_delete(args.workerid))

def job_cancel(args):
    jobs = []
    for jobid in args.jobid:
        retvalue = SPARQUEUE_CLIENT.cancel(jobid)
        if retvalue:
            jobs.append(format_json(retvalue))
    if jobs:
        return '\n'.join(jobs)
    return 'None'

def format_json(s):
    try:
        obj = json.loads(s)
        return json.dumps(obj, sort_keys=True, indent=4)
    except ValueError:
        print s
        return
    except TypeError:
        print s
        return

def setup_parser(parser, subparsers, config, system, queue):
    parser.add_argument(
        '-config',
        default=config,
        help='configuration.json')
    parser.add_argument(
        '-protocol',
        default='http',
        help='protocol sparqueue API')
    parser.add_argument(
        '-hostname',
        default='localhost',
        help='hostname for sparqueue API')
    parser.add_argument(
        '-port',
        default='8080',
        help='port for sparqueue API')
    parser.add_argument(
        '-system',
        default=system,
        help='system name')
    parser.add_argument(
        '-queue',
        default=queue,
        help='queue name')


    submit_parser = subparsers.add_parser('submit', help='submit a new job')

    submit_parser.add_argument(
        'filename',
        help='filename to batch for upload')

    submit_parser.set_defaults(func=submit)

    list_parser = subparsers.add_parser('list', help='listing of all jobs')
    list_parser.set_defaults(func=list_jobs)

    workers_parser = subparsers.add_parser('workers', help='list of all workers')
    workers_parser.set_defaults(func=workers)

    job_parser = subparsers.add_parser('job', help='retrieve job information')
    job_parser.add_argument(
        'jobid',
        help='jobid to retrieve')
    job_parser.set_defaults(func=job)

    worker_delete_parser = subparsers.add_parser('worker_delete', help='delete worker entry')
    worker_delete_parser.add_argument(
        'workerid',
        help='workerid to delete')
    worker_delete_parser.set_defaults(func=worker_delete)

    job_cancel_parser = subparsers.add_parser('cancel', help='cancel jobid')
    job_cancel_parser.add_argument(
        'jobid',
        help='jobid to cancel',
        nargs='*')
    job_cancel_parser.set_defaults(func=job_cancel)

    status_parser = subparsers.add_parser('status', help='retrieve job status')
    status_parser.add_argument(
        'jobid',
        help='jobid to retrieve status from')
    status_parser.add_argument(
        '-text',
        action='store_true', default=False,
        help='text only display (semi-colon separated)')
    status_parser.set_defaults(func=status)

    traceback_parser = subparsers.add_parser('traceback', help='retrieve job traceback')
    traceback_parser.add_argument(
        'jobid',
        help='jobid to retrieve error traceback from')
    traceback_parser.set_defaults(func=traceback)

def use_args(args):
    global config, SPARQUEUE_CLIENT
    config = sparqueue.config.get_config(args.config)
    SPARQUEUE_CLIENT = sparqueue.client.Client(
        args.protocol, args.hostname, args.port, args.system, args.queue)

def sparqueue_client(args):
    return SPARQUEUE_CLIENT