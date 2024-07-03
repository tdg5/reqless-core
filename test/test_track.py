'''Test all the tracking'''

import redis
from common import TestQless


class TestTrack(TestQless):
    '''Test our tracking abilities'''
    def test_malfomed(self):
        '''Enumerate all the ways that it can be malformed'''
        self.assertMalformed(self.lua, [
            ('job.track', 0),
            ('job.untrack', 0),
        ])

    def test_track(self):
        '''Can track a job and it appears in "track"'''
        self.lua('queue.put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('job.track', 0, 'jid')
        self.assertEqual(self.lua('jobs.tracked', 0), {
            'jobs': [{
                'retries': 5,
                'jid': 'jid',
                'tracked': True,
                'tags': {},
                'worker': u'',
                'expires': 0,
                'priority': 0,
                'queue': 'queue',
                'failure': {},
                'state': 'waiting',
                'dependencies': {},
                'klass': 'klass',
                'dependents': {},
                'throttles': ['ql:q:queue'],
                'data': '{}',
                'remaining': 5,
                'spawned_from_jid': False,
                'history': [{
                    'queue': 'queue', 'what': 'put', 'when': 0
                }]
            }], 'expired': {}})

    def test_untrack(self):
        '''We can stop tracking a job'''
        self.lua('queue.put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('job.track', 0, 'jid')
        self.lua('job.untrack', 0, 'jid')
        self.assertEqual(self.lua('jobs.tracked', 0), {'jobs': {}, 'expired': {}})

    def test_track_still_works(self):
        '''Deprecated track API still works'''
        self.lua('queue.put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('track', 1, 'track', 'jid')
        self.assertEqual(self.lua('track', 2)['jobs'][0]['jid'], 'jid')
        self.lua('track', 3, 'untrack', 'jid')
        self.assertEqual(self.lua('track', 4), {'jobs': {}, 'expired': {}})

    def test_track_nonexistent(self):
        '''Tracking nonexistent jobs raises an error'''
        self.assertRaisesRegexp(redis.ResponseError, r'does not exist',
            self.lua, 'job.track', 0, 'jid')

    def test_jobs_tracked(self):
        '''Jobs know when they're tracked'''
        self.lua('queue.put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('job.track', 0, 'jid')
        self.assertEqual(self.lua('job.get', 0, 'jid')['tracked'], True)

    def test_jobs_untracked(self):
        '''Jobs know when they're not tracked'''
        self.lua('queue.put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.assertEqual(self.lua('job.get', 0, 'jid')['tracked'], False)
