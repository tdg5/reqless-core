'''Tests about worker information'''

from test.common import TestReqless


class TestWorker(TestReqless):
    '''Test worker information API'''
    def setUp(self):
        TestReqless.setUp(self)
        # No grace period
        self.lua('config.set', 0, 'grace-period', 0)

    def test_basic(self):
        '''Basic worker-level information'''
        self.lua('queue.put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('queue.pop', 1, 'queue', 'worker', 10)
        self.assertEqual(self.lua('worker.jobs', 2, 'worker'), {
            'jobs': ['jid'],
            'stalled': {}
        })
        self.assertEqual(self.lua('workers.counts', 2), [{
            'name': 'worker',
            'jobs': 1,
            'stalled': 0
        }])

    def test_stalled(self):
        '''We should be able to detect stalled jobs'''
        self.lua('queue.put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        job = self.lua('queue.pop', 1, 'queue', 'worker', 10)[0]
        expires = job['expires'] + 10
        self.lua('queue.peek', expires, 'queue', 0, 10)
        self.assertEqual(self.lua('worker.jobs', expires, 'worker'), {
            'jobs': {},
            'stalled': ['jid']
        })
        self.assertEqual(self.lua('workers.counts', expires), [{
            'name': 'worker',
            'jobs': 0,
            'stalled': 1
        }])

    def test_locks(self):
        '''When a lock is lost, removes the job from the worker's info'''
        self.lua('queue.put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        job = self.lua('queue.pop', 1, 'queue', 'worker', 10)[0]
        self.assertEqual(self.lua('worker.jobs', 2, 'worker'), {
            'jobs': ['jid'],
            'stalled': {}
        })
        # Once it gets handed off to another worker, we shouldn't see any info
        # about that job from that worker
        expires = job['expires'] + 10
        self.lua('queue.pop', expires, 'queue', 'another', 10)
        self.assertEqual(self.lua('worker.jobs', expires, 'worker'), {
            'jobs': {},
            'stalled': {}
        })

    def test_cancelled(self):
        '''Canceling a job removes it from the worker's stats'''
        self.lua('queue.put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('queue.pop', 1, 'queue', 'worker', 10)
        self.assertEqual(self.lua('worker.jobs', 2, 'worker'), {
            'jobs': ['jid'],
            'stalled': {}
        })
        # And now, we'll cancel it, and it should disappear
        self.lua('job.cancel', 3, 'jid')
        self.assertEqual(self.lua('worker.jobs', 4, 'worker'), {
            'jobs': {},
            'stalled': {}
        })
        self.assertEqual(self.lua('workers.counts', 4), [{
            'name': 'worker',
            'jobs': 0,
            'stalled': 0
        }])

    def test_failed(self):
        '''When a job fails, it should be removed from a worker's jobs'''
        self.lua('queue.put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('queue.pop', 1, 'queue', 'worker', 10)
        self.assertEqual(self.lua('worker.jobs', 2, 'worker'), {
            'jobs': ['jid'],
            'stalled': {}
        })
        self.lua('job.fail', 3, 'jid', 'worker', 'group', 'message', {})
        self.assertEqual(self.lua('worker.jobs', 4, 'worker'), {
            'jobs': {},
            'stalled': {}
        })
        self.assertEqual(self.lua('workers.counts', 4), [{
            'name': 'worker',
            'jobs': 0,
            'stalled': 0
        }])

    def test_complete(self):
        '''When a job completes, it should be remove from the worker's jobs'''
        self.lua('queue.put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('queue.pop', 1, 'queue', 'worker', 10)
        self.assertEqual(self.lua('worker.jobs', 2, 'worker'), {
            'jobs': ['jid'],
            'stalled': {}
        })
        self.lua('job.complete', 3, 'jid', 'worker', 'queue', {})
        self.assertEqual(self.lua('worker.jobs', 4, 'worker'), {
            'jobs': {},
            'stalled': {}
        })
        self.assertEqual(self.lua('workers.counts', 4), [{
            'name': 'worker',
            'jobs': 0,
            'stalled': 0
        }])

    def test_put(self):
        '''When a job's put in another queue, remove it from the worker'''
        self.lua('queue.put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('queue.pop', 1, 'queue', 'worker', 10)
        self.assertEqual(self.lua('worker.jobs', 2, 'worker'), {
            'jobs': ['jid'],
            'stalled': {}
        })
        self.lua('queue.put', 3, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.assertEqual(self.lua('worker.jobs', 4, 'worker'), {
            'jobs': {},
            'stalled': {}
        })
        self.assertEqual(self.lua('workers.counts', 4), [{
            'name': 'worker',
            'jobs': 0,
            'stalled': 0
        }])

    def test_reregister(self):
        '''We should be able to remove workers from the list of workers'''
        for jid in list(range(10)):
            self.lua('queue.put', 0, 'worker', 'queue', jid, 'klass', {}, 0)
        # And pop them from 10 different workers
        workers = list(map(str, range(10)))
        for worker in workers:
            self.lua('queue.pop', 1, 'queue', worker, 1)
        # And we'll forget them each one at a time and ensure they are
        # indeed removed from our list
        for worker in workers:
            found = [w['name'] for w in self.lua('workers.counts', 2)]
            self.assertTrue(worker in found)
            self.lua('worker.forget', 2, worker)
            found = [w['name'] for w in self.lua('workers.counts', 2)]
            self.assertFalse(worker in found)

    def test_expiration(self):
        '''After a certain amount of time, inactive workers expire'''
        # Set the maximum worker age
        self.lua('config.set', 0, 'max-worker-age', 3600)
        self.lua('queue.put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('queue.pop', 0, 'queue', 'worker', 10)
        self.lua('job.complete', 0, 'jid', 'worker', 'queue', {})
        # When we check on workers in a little while, it won't be listed
        self.assertEqual(self.lua('workers.counts', 3600), [])

    def test_forgotten(self):
        '''If a worker is unknown, it should still be ok'''
        self.assertEqual(self.lua('worker.jobs', 3600, 'worker'), {
            'jobs': {},
            'stalled': {}
        })

    def test_retry_worker(self):
        '''When retried, it removes a job from the worker's data'''
        self.lua('queue.put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('queue.pop', 0, 'queue', 'worker', 10)
        self.lua('job.retry', 0, 'jid', 'queue', 'worker', 0)
        self.assertEqual(self.lua('worker.jobs', 3600, 'worker'), {
            'jobs': {},
            'stalled': {}
        })
