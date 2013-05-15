import time

import resource

class Reporter():
    def __init__(self, logger, queue, jobid):
        self.jobid = jobid
        self.queue = queue
        self.warning = logger.warning
        self.info = logger.info
        self.start_time = time.time()
        self.start_resources = resource.getrusage(resource.RUSAGE_SELF)

        self.steps = []

    def record_step(self):
        self.steps.append({
            'step': self.stepname,
            'start_time': self.step_start_time,
            'stop_time': self.step_stop_time,
            'elapsed': self.step_stop_time - self.step_start_time,
            #'resources': list(resource.getrusage(resource.RUSAGE_SELF))
        })

    def step(self, step, count = 1):
        if hasattr(self, 'total') and self.total > 1:
            self.increment()

        if hasattr(self, 'stepname'):
            self.step_stop_time = time.time()
            self.record_step()

        self.step_start_time = time.time()
        assert count > 0
        self.stepname = step
        self.info('Step %s' % self.stepname)
        if count > 1:
            self.info('Expecting %s sub-steps' % count)
        self.total = count
        self.count = count
        self.queue.step(self.jobid, step)

    def increment(self):
        progress = 100-int((float(self.count)/float(self.total))*100)
        self.info('%s Progress: %s' % (self.stepname, progress))
        self.count = self.count - 1

    def finish(self):
        self.info('Job Completed')
        self.stop_time = time.time()

    def stats(self):
        stats = {}
        stats['start_time'] = self.start_time
        stats['stop_time'] = self.stop_time
        stats['elapsed'] = self.stop_time - self.start_time
        stats['steps'] = self.steps
        stats['resources'] = list(self.start_resources)
        return stats