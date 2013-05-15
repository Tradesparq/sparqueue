import urllib
import json
import httplib

class ClientException(Exception):
    pass

class Client():
    def __init__(self, protocol, hostname, port, system, queue):
        self.protocol = protocol
        self.hostname = hostname
        self.port = port
        self.system = system
        self.queue = queue

    def submit(self, job):
        if type(job) is dict:
            job_json = json.dumps(job)
        elif type(job) is str:
            job_json = job
        else:
            raise ClientException('Invalid type %s' % type(job))

        params = urllib.urlencode({'job': job_json})
        uri = '/%s/queues/%s/jobs' % (self.system, self.queue)
        url = self._url(uri)
        f = urllib.urlopen(url, params)
        output = f.read()

        return output

    def list(self):
        uri = '/%s/queues/%s/jobs' % (self.system, self.queue)
        return self._get(uri)

    def job(self, jobid):
        uri = '/%s/queues/%s/jobs/%s' % (self.system, self.queue, jobid)
        return self._get(uri)

    def status(self, jobid):
        uri = '/%s/queues/%s/jobs/%s/status' % (self.system, self.queue, jobid)
        return self._get(uri)

    def workers(self):
        uri = '/%s/queues/%s/workers' % (self.system, self.queue)
        return self._get(uri)

    def worker_delete(self, workerid):
        uri = '/%s/queues/%s/workers/%s' % (self.system, self.queue, workerid)
        return self._delete(uri)

    def cancel(self, jobid):
        uri = '/%s/queues/%s/jobs/%s' % (self.system, self.queue, jobid)
        return self._delete(uri)

    def _delete(self, uri):
        conn = httplib.HTTPConnection(self.hostname, self.port)
        conn.request('DELETE', uri)
        resp = conn.getresponse()
        content = resp.read()
        return content

    def _url(self, uri):
        return "%s://%s:%s%s" % (self.protocol, self.hostname, self.port, uri)

    def _get(self, uri):
        url = self._url(uri)
        f = urllib.urlopen(url)
        output = f.read()
        return output