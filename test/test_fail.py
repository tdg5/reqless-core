'''Tests about failing jobs'''

import redis
from common import TestReqless


class TestFail(TestReqless):
    '''Test the behavior of failing jobs'''
    def test_malformed(self):
        '''Enumerate all the malformed cases'''
        self.assertMalformed(self.lua, [
            ('job.fail', 0),
            ('job.fail', 0, 'jid'),
            ('job.fail', 0, 'jid', 'worker'),
            ('job.fail', 0, 'jid', 'worker', 'group'),
            ('job.fail', 0, 'jid', 'worker', 'group', 'message'),
            ('job.fail', 0, 'jid', 'worker', 'group', 'message', '[}')
        ])

    def test_basic(self):
        '''Fail a job in a very basic way'''
        self.lua('queue.put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('queue.pop', 1, 'queue', 'worker', 10)
        self.lua('job.fail', 2, 'jid', 'worker', 'group', 'message', {})
        self.assertEqual(self.lua('job.get', 3, 'jid'), {'data': '{}',
            'dependencies': {},
            'dependents': {},
            'expires': 0,
            'failure': {'group': 'group',
                        'message': 'message',
                        'when': 2,
                        'worker': 'worker'},
            'history': [{'queue': 'queue', 'what': 'put', 'when': 0},
                        {'what': 'popped', 'when': 1, 'worker': 'worker'},
                        {'group': 'group',
                         'what': 'failed',
                         'when': 2,
                         'worker': 'worker'}],
            'jid': 'jid',
            'klass': 'klass',
            'priority': 0,
            'queue': 'queue',
            'remaining': 5,
            'retries': 5,
            'state': 'failed',
            'tags': {},
            'tracked': False,
            'throttles': ['ql:q:queue'],
            'worker': u'',
            'spawned_from_jid': False})

    def test_fail_still_works(self):
        '''Deprecated fail API still works'''
        self.lua('queue.put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('queue.pop', 1, 'queue', 'worker', 10)
        self.lua('fail', 2, 'jid', 'worker', 'group', 'message', {})
        self.assertEqual(self.lua('job.get', 3, 'jid'), {'data': '{}',
            'dependencies': {},
            'dependents': {},
            'expires': 0,
            'failure': {'group': 'group',
                        'message': 'message',
                        'when': 2,
                        'worker': 'worker'},
            'history': [{'queue': 'queue', 'what': 'put', 'when': 0},
                        {'what': 'popped', 'when': 1, 'worker': 'worker'},
                        {'group': 'group',
                         'what': 'failed',
                         'when': 2,
                         'worker': 'worker'}],
            'jid': 'jid',
            'klass': 'klass',
            'priority': 0,
            'queue': 'queue',
            'remaining': 5,
            'retries': 5,
            'state': 'failed',
            'tags': {},
            'tracked': False,
            'throttles': ['ql:q:queue'],
            'worker': u'',
            'spawned_from_jid': False})

    def test_put(self):
        '''Can put a job that has been failed'''
        self.lua('queue.put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('queue.pop', 1, 'queue', 'worker', 10)
        self.lua('job.fail', 2, 'jid', 'worker', 'group', 'message', {})
        self.lua('queue.put', 3, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.assertEqual(len(self.lua('queue.peek', 4, 'queue', 0, 10)), 1)

    def test_fail_waiting(self):
        '''Only popped jobs can be failed'''
        self.lua('queue.put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.assertRaisesRegexp(redis.ResponseError, r'waiting',
            self.lua, 'job.fail', 1, 'jid', 'worker', 'group', 'message', {})
        # Pop is and it should work
        self.lua('queue.pop', 2, 'queue', 'worker', 10)
        self.lua('job.fail', 3, 'jid', 'worker', 'group', 'message', {})

    def test_fail_depends(self):
        '''Cannot fail a dependent job'''
        self.lua('queue.put', 0, 'worker', 'queue', 'a', 'klass', {}, 0)
        self.lua('queue.put', 0, 'worker', 'queue', 'b', 'klass', {}, 0, 'depends', ['a'])
        self.assertRaisesRegexp(redis.ResponseError, r'depends',
            self.lua, 'job.fail', 1, 'b', 'worker', 'group', 'message', {})

    def test_fail_scheduled(self):
        '''Cannot fail a scheduled job'''
        self.lua('queue.put', 0, 'worker', 'queue', 'jid', 'klass', {}, 1)
        self.assertRaisesRegexp(redis.ResponseError, r'scheduled',
            self.lua, 'job.fail', 1, 'jid', 'worker', 'group', 'message', {})

    def test_fail_nonexistent(self):
        '''Cannot fail a job that doesn't exist'''
        self.assertRaisesRegexp(redis.ResponseError, r'does not exist',
            self.lua, 'job.fail', 1, 'jid', 'worker', 'group', 'message', {})
        self.lua('queue.put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('queue.pop', 0, 'queue', 'worker', 10)
        self.lua('job.fail', 1, 'jid', 'worker', 'group', 'message', {})

    def test_fail_completed(self):
        '''Cannot fail a job that has been completed'''
        self.lua('queue.put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('queue.pop', 0, 'queue', 'worker', 10)
        self.lua('job.complete', 0, 'jid', 'worker', 'queue', {})
        self.assertRaisesRegexp(redis.ResponseError, r'complete',
            self.lua, 'job.fail', 1, 'jid', 'worker', 'group', 'message', {})

    def test_fail_owner(self):
        '''Cannot fail a job that's running with another worker'''
        self.lua('queue.put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('queue.pop', 1, 'queue', 'worker', 10)
        self.lua('queue.put', 2, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('queue.pop', 3, 'queue', 'another-worker', 10)
        self.assertRaisesRegexp(redis.ResponseError, r'another worker',
            self.lua, 'job.fail', 4, 'jid', 'worker', 'group', 'message', {})


class TestFailed(TestReqless):
    '''Test access to our failed jobs'''
    def test_malformed(self):
        '''Enumerate all the malformed requests'''
        self.assertMalformed(self.lua, [
            ('jobs.failedByGroup', 0, 'foo', 'foo'),
            ('jobs.failedByGroup', 0, 'foo', 0, 'foo')
        ])

    def test_basic(self):
        '''We can keep track of failed jobs'''
        self.lua('queue.put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('queue.pop', 0, 'queue', 'worker', 10)
        self.lua('job.fail', 0, 'jid', 'worker', 'group', 'message')
        self.assertEqual(self.lua('failureGroups.counts', 0), {
            'group': 1
        })
        self.assertEqual(self.lua('jobs.failedByGroup', 0, 'group'), {
            'total': 1,
            'jobs': ['jid']
        })

    def test_retries(self):
        '''Jobs that fail because of retries should show up'''
        self.lua('config.set', 0, 'grace-period', 0)
        self.lua('queue.put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0, 'retries', 0)
        job = self.lua('queue.pop', 0, 'queue', 'worker', 10)[0]
        self.lua('queue.pop', job['expires'] + 10, 'queue', 'worker', 10)
        self.assertEqual(self.lua('failureGroups.counts', 0), {
            'failed-retries-queue': 1
        })
        self.assertEqual(self.lua('jobs.failedByGroup', 0, 'failed-retries-queue'), {
            'total': 1,
            'jobs': ['jid']
        })

    def test_failed_pagination(self):
        '''Failed provides paginated access'''
        jids = list(map(str, range(100)))
        for jid in jids:
            self.lua('queue.put', jid, 'worker', 'queue', jid, 'klass', {}, 0)
            self.lua('queue.pop', jid, 'queue', 'worker', 10)
            self.lua('job.fail', jid, jid, 'worker', 'group', 'message')
        # Get two pages of 50 and make sure they're what we expect
        jids = list(reversed(jids))
        self.assertEqual(
            self.lua('jobs.failedByGroup', 0, 'group',  0, 50)['jobs'], jids[:50])
        self.assertEqual(
            self.lua('jobs.failedByGroup', 0, 'group', 50, 50)['jobs'], jids[50:])

    def test_failed_still_works(self):
        '''Deprecated failed API still works'''
        self.lua('queue.put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('queue.pop', 0, 'queue', 'worker', 10)
        self.lua('job.fail', 0, 'jid', 'worker', 'group', 'message')
        self.assertEqual(self.lua('failed', 0), {
            'group': 1
        })
        self.assertEqual(self.lua('failed', 0, 'group'), {
            'total': 1,
            'jobs': ['jid']
        })


class TestUnfailed(TestReqless):
    '''Test access to unfailed'''
    def test_basic(self):
        '''We can unfail in a basic way'''
        jids = map(str, range(10))
        for jid in jids:
            self.lua('queue.put', 0, 'worker', 'queue', jid, 'klass', {}, 0)
            self.lua('queue.pop', 0, 'queue', 'worker', 10)
            self.lua('job.fail', 0, jid, 'worker', 'group', 'message')
            self.assertEqual(self.lua('job.get', 0, jid)['state'], 'failed')
        self.lua('queue.unfail', 0, 'queue', 'group', 100)
        for jid in jids:
            self.assertEqual(self.lua('job.get', 0, jid)['state'], 'waiting')

    def test_unfail_still_works(self):
        '''Deprecated unfail API still works'''
        jids = map(str, range(10))
        for jid in jids:
            self.lua('queue.put', 0, 'worker', 'queue', jid, 'klass', {}, 0)
            self.lua('queue.pop', 0, 'queue', 'worker', 10)
            self.lua('job.fail', 0, jid, 'worker', 'group', 'message')
            self.assertEqual(self.lua('job.get', 0, jid)['state'], 'failed')
        self.lua('unfail', 0, 'queue', 'group', 100)
        for jid in jids:
            self.assertEqual(self.lua('job.get', 0, jid)['state'], 'waiting')
