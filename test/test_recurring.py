'''Tests for recurring jobs'''

from test.common import TestReqless


class TestRecurring(TestReqless):
    '''Tests for recurring jobs'''
    def test_malformed(self):
        '''Enumerate all the malformed possibilities'''
        self.assertMalformed(self.lua, [
            ('queue.recurAtInterval', 0),
            ('queue.recurAtInterval', 0, 'queue'),
            ('queue.recurAtInterval', 0, 'queue', 'jid'),
            ('queue.recurAtInterval', 0, 'queue', 'jid', 'klass'),
            ('queue.recurAtInterval', 0, 'queue', 'jid', 'klass', {}),
            ('queue.recurAtInterval', 0, 'queue', 'jid', 'klass', '[}'),
            ('queue.recurAtInterval', 0, 'queue', 'jid', 'klass', {}, 'foo'),
            ('queue.recurAtInterval', 0, 'queue', 'jid', 'klass', {}, 60, 'foo'),
            ('queue.recurAtInterval', 0, 'queue', 'jid', 'klass', {}, 60, 0, 'tags'),
            ('queue.recurAtInterval', 0, 'queue', 'jid', 'klass', {}, 60, 0, 'tags', '[}'),
            ('queue.recurAtInterval', 0, 'queue', 'jid', 'klass', {}, 60, 0, 'priority'),
            ('queue.recurAtInterval', 0, 'queue', 'jid', 'klass', {}, 60, 0, 'priority', 'foo'),
            ('queue.recurAtInterval', 0, 'queue', 'jid', 'klass', {}, 60, 0, 'retries'),
            ('queue.recurAtInterval', 0, 'queue', 'jid', 'klass', {}, 60, 0, 'retries', 'foo'),
            ('queue.recurAtInterval', 0, 'queue', 'jid', 'klass', {}, 60, 0, 'backlog'),
            ('queue.recurAtInterval', 0, 'queue', 'jid', 'klass', {}, 60, 0, 'backlog', 'foo'),
        ])

        # In order for these tests to work, there must be a job
        self.lua('queue.recurAtInterval', 0, 'queue', 'jid', 'klass', {}, 60, 0)
        self.assertMalformed(self.lua, [
            ('recurringJob.update', 0, 'jid', 'priority'),
            ('recurringJob.update', 0, 'jid', 'priority', 'foo'),
            ('recurringJob.update', 0, 'jid', 'interval'),
            ('recurringJob.update', 0, 'jid', 'interval', 'foo'),
            ('recurringJob.update', 0, 'jid', 'retries'),
            ('recurringJob.update', 0, 'jid', 'retries', 'foo'),
            ('recurringJob.update', 0, 'jid', 'data'),
            ('recurringJob.update', 0, 'jid', 'data', '[}'),
            ('recurringJob.update', 0, 'jid', 'klass'),
            ('recurringJob.update', 0, 'jid', 'queue'),
            ('recurringJob.update', 0, 'jid', 'backlog'),
            ('recurringJob.update', 0, 'jid', 'backlog', 'foo')
        ])

    def test_basic(self):
        '''Simple recurring jobs'''
        self.lua('queue.recurAtInterval', 0, 'queue', 'jid', 'klass', {}, 60, 0)
        # Pop off the first recurring job
        popped = self.lua('queue.pop', 0, 'queue', 'worker', 10)
        self.assertEqual(len(popped), 1)
        self.assertEqual(popped[0]['jid'], 'jid-1')
        self.assertEqual(popped[0]['spawned_from_jid'], 'jid')

        # If we wait 59 seconds, there won't be a job, but at 60, yes
        popped = self.lua('queue.pop', 59, 'queue', 'worker', 10)
        self.assertEqual(len(popped), 0)
        popped = self.lua('queue.pop', 61, 'queue', 'worker', 10)
        self.assertEqual(len(popped), 1)
        self.assertEqual(popped[0]['jid'], 'jid-2')
        self.assertEqual(popped[0]['spawned_from_jid'], 'jid')

    def test_offset(self):
        '''We can set an offset from now for jobs to recur on'''
        self.lua('queue.recurAtInterval', 0, 'queue', 'jid', 'klass', {}, 60, 10)
        # There shouldn't be any jobs available just yet
        popped = self.lua('queue.pop', 9, 'queue', 'worker', 10)
        self.assertEqual(len(popped), 0)
        popped = self.lua('queue.pop', 11, 'queue', 'worker', 10)
        self.assertEqual(len(popped), 1)
        self.assertEqual(popped[0]['jid'], 'jid-1')

        # And now it recurs normally
        popped = self.lua('queue.pop', 69, 'queue', 'worker', 10)
        self.assertEqual(len(popped), 0)
        popped = self.lua('queue.pop', 71, 'queue', 'worker', 10)
        self.assertEqual(len(popped), 1)
        self.assertEqual(popped[0]['jid'], 'jid-2')

    def test_tags(self):
        '''Recurring jobs can be given tags'''
        self.lua('queue.recurAtInterval', 0, 'queue', 'jid', 'klass', {}, 60, 0,
            'tags', ['foo', 'bar'])
        job = self.lua('queue.pop', 0, 'queue', 'worker', 10)[0]
        self.assertEqual(job['tags'], ['foo', 'bar'])

    def test_priority(self):
        '''Recurring jobs can be given priority'''
        # Put one job with low priority
        self.lua('queue.put', 0, 'worker', 'queue', 'low', 'klass', {}, 0, 'priority', 0)
        self.lua('queue.recurAtInterval', 0, 'queue', 'high', 'klass', {}, 60, 0, 'priority', 10)
        jobs = self.lua('queue.pop', 0, 'queue', 'worker', 10)
        # We should see high-1 and then low
        self.assertEqual(len(jobs), 2)
        self.assertEqual(jobs[0]['jid'], 'high-1')
        self.assertEqual(jobs[0]['priority'], 10)
        self.assertEqual(jobs[1]['jid'], 'low')

    def test_retries(self):
        '''Recurring job retries are passed on to child jobs'''
        self.lua('queue.recurAtInterval', 0, 'queue', 'jid', 'klass', {}, 60, 0, 'retries', 2)
        job = self.lua('queue.pop', 0, 'queue', 'worker', 10)[0]
        self.assertEqual(job['retries'], 2)
        self.assertEqual(job['remaining'], 2)

    def test_backlog(self):
        '''Recurring jobs can limit the number of jobs they spawn'''
        self.lua('queue.recurAtInterval', 0, 'queue', 'jid', 'klass', {}, 60, 0, 'backlog', 1)
        jobs = self.lua('queue.pop', 600, 'queue', 'worker', 10)
        self.assertEqual(len(jobs), 2)
        self.assertEqual(jobs[0]['jid'], 'jid-1')

    def test_get(self):
        '''We should be able to get recurring jobs'''
        self.lua('queue.recurAtInterval', 0, 'queue', 'jid', 'klass', {}, 60, 0)
        self.assertEqual(self.lua('recurringJob.get', 0, 'jid'), {
            'backlog': 0,
            'count': 0,
            'data': '{}',
            'interval': 60,
            'jid': 'jid',
            'klass': 'klass',
            'priority': 0,
            'queue': 'queue',
            'retries': 0,
            'state': 'recur',
            'tags': {},
            'throttles': ['ql:q:queue'],
        })

    def test_update_priority(self):
        '''We need to be able to update recurring job attributes'''
        self.lua('queue.recurAtInterval', 0, 'queue', 'jid', 'klass', {}, 60, 0)
        self.assertEqual(
            self.lua('queue.pop', 0, 'queue', 'worker', 10)[0]['priority'], 0)
        self.lua('recurringJob.update', 0, 'jid', 'priority', 10)
        self.assertEqual(
            self.lua('queue.pop', 60, 'queue', 'worker', 10)[0]['priority'], 10)

    def test_update_interval(self):
        '''We need to be able to update the interval'''
        self.lua('queue.recurAtInterval', 0, 'queue', 'jid', 'klass', {}, 60, 0)
        self.assertEqual(len(self.lua('queue.pop', 0, 'queue', 'worker', 10)), 1)
        self.lua('recurringJob.update', 0, 'jid', 'interval', 10)
        self.assertEqual(len(self.lua('queue.pop', 60, 'queue', 'worker', 10)), 6)

    def test_update_retries(self):
        '''We need to be able to update the retries'''
        self.lua('queue.recurAtInterval', 0, 'queue', 'jid', 'klass', {}, 60, 0, 'retries', 5)
        self.assertEqual(
            self.lua('queue.pop', 0, 'queue', 'worker', 10)[0]['retries'], 5)
        self.lua('recurringJob.update', 0, 'jid', 'retries', 2)
        self.assertEqual(
            self.lua('queue.pop', 60, 'queue', 'worker', 10)[0]['retries'], 2)

    def test_update_data(self):
        '''We need to be able to update the data'''
        self.lua('queue.recurAtInterval', 0, 'queue', 'jid', 'klass', {}, 60,  0)
        self.assertEqual(
            self.lua('queue.pop', 0, 'queue', 'worker', 10)[0]['data'], '{}')
        self.lua('recurringJob.update', 0, 'jid', 'data', {'foo': 'bar'})
        self.assertEqual(self.lua(
            'queue.pop', 60, 'queue', 'worker', 10)[0]['data'], '{"foo": "bar"}')

    def test_update_klass(self):
        '''We need to be able to update klass'''
        self.lua('queue.recurAtInterval', 0, 'queue', 'jid', 'klass', {}, 60, 0)
        self.assertEqual(
            self.lua('queue.pop', 0, 'queue', 'worker', 10)[0]['klass'], 'klass')
        self.lua('recurringJob.update', 0, 'jid', 'klass', 'class')
        self.assertEqual(
            self.lua('queue.pop', 60, 'queue', 'worker', 10)[0]['klass'], 'class')

    def test_update_queue(self):
        '''Need to be able to move the recurring job to another queue'''
        self.lua('queue.recurAtInterval', 0, 'queue', 'jid', 'klass', {}, 60, 0)
        self.assertEqual(len(self.lua('queue.pop', 0, 'queue', 'worker', 10)), 1)
        self.lua('recurringJob.update', 0, 'jid', 'queue', 'other')
        # No longer available in the old queue
        self.assertEqual(len(self.lua('queue.pop', 60, 'queue', 'worker', 10)), 0)
        popped_jobs = self.lua('queue.pop', 60, 'other', 'worker', 10)
        self.assertEqual(len(popped_jobs), 1)
        job = popped_jobs[0]
        self.assertEqual(job["throttles"], ["ql:q:other"])

    def test_update_throttles(self):
        '''We need to be able to update the throttles'''
        queue_name = 'queue'
        self.lua('queue.recurAtInterval', 0, queue_name, 'jid', 'klass', {}, 60,  0)
        self.assertEqual(
            self.lua('queue.pop', 0, queue_name, 'worker', 10)[0]['throttles'], [f'ql:q:{queue_name}'])
        self.lua('recurringJob.update', 0, 'jid', 'throttles', ['throttle'])
        self.assertEqual(self.lua(
            'queue.pop', 60, queue_name, 'worker', 10)[0]['throttles'], ['throttle'])

    def test_cancel(self):
        '''Stop a recurring job'''
        self.lua('queue.recurAtInterval', 0, 'queue', 'jid', 'klass', {}, 60, 0)
        self.assertEqual(len(self.lua('queue.pop', 0, 'queue', 'worker', 10)), 1)
        self.lua('recurringJob.cancel', 0, 'jid')
        self.assertEqual(len(self.lua('queue.pop', 60, 'queue', 'worker', 10)), 0)

    def test_empty_array_data(self):
        '''Empty array of data is preserved'''
        self.lua('queue.recurAtInterval', 0, 'queue', 'jid', 'klass', [], 60, 0)
        self.assertEqual(
            self.lua('queue.pop', 0, 'queue', 'worker', 10)[0]['data'], '[]')

    def test_multiple(self):
        '''If multiple intervals have passed, then returns multiple jobs'''
        self.lua('queue.recurAtInterval', 0, 'queue', 'jid', 'klass', {}, 60, 0)
        self.assertEqual(
            len(self.lua('queue.pop', 599, 'queue', 'worker', 10)), 10)

    def test_tag(self):
        '''We should be able to add tags to jobs'''
        self.lua('queue.recurAtInterval', 0, 'queue', 'jid', 'klass', {}, 60, 0)
        self.assertEqual(
            self.lua('queue.pop', 0, 'queue', 'worker', 10)[0]['tags'], {})
        self.lua('recurringJob.addTag', 0, 'jid', 'foo')
        self.assertEqual(
            self.lua('queue.pop', 60, 'queue', 'worker', 10)[0]['tags'], ['foo'])

    def test_untag(self):
        '''We should be able to remove tags from a job'''
        self.lua('queue.recurAtInterval', 0, 'queue', 'jid', 'klass', {}, 60, 0, 'tags', ['foo'])
        self.assertEqual(
            self.lua('queue.pop', 0, 'queue', 'worker', 10)[0]['tags'], ['foo'])
        self.lua('recurringJob.removeTag', 0, 'jid', 'foo')
        self.assertEqual(
            self.lua('queue.pop', 60, 'queue', 'worker', 10)[0]['tags'], {})

    def test_rerecur(self):
        '''Don't reset the jid counter when re-recurring a job'''
        self.lua('queue.recurAtInterval', 0, 'queue', 'jid', 'klass', {}, 60, 0)
        self.assertEqual(
            self.lua('queue.pop', 0, 'queue', 'worker', 10)[0]['jid'], 'jid-1')
        # Re-recur it
        self.lua('queue.recurAtInterval', 60, 'queue', 'jid', 'klass', {}, 60, 0)
        self.assertEqual(
            self.lua('queue.pop', 60, 'queue', 'worker', 10)[0]['jid'], 'jid-2')

    def test_rerecur_attributes(self):
        '''Re-recurring a job updates its attributes'''
        self.lua('queue.recurAtInterval', 0, 'queue', 'jid', 'klass', {}, 60, 0,
            'priority', 10, 'tags', ['foo'], 'retries', 2)
        self.assertEqual(self.lua('queue.pop', 0, 'queue', 'worker', 10)[0], {
            'data': '{}',
            'dependencies': {},
            'dependents': {},
            'expires': 60,
            'failure': {},
            'history': [{'queue': 'queue', 'what': 'put', 'when': 0},
                        {'what': 'popped', 'when': 0, 'worker': 'worker'}],
            'jid': 'jid-1',
            'klass': 'klass',
            'priority': 10,
            'queue': 'queue',
            'remaining': 2,
            'retries': 2,
            'state': 'running',
            'tags': ['foo'],
            'tracked': False,
            'throttles': ['ql:q:queue'],
            'worker': 'worker',
            'spawned_from_jid': 'jid'})
        self.lua('queue.recurAtInterval', 60, 'queue', 'jid', 'class', {'foo': 'bar'},
            10, 0, 'priority', 5, 'tags', ['bar'], 'retries', 5, 'throttles', ['lala'])
        self.assertEqual(self.lua('queue.pop', 60, 'queue', 'worker', 10)[0], {
            'data': '{"foo": "bar"}',
            'dependencies': {},
            'dependents': {},
            'expires': 120,
            'failure': {},
            'history': [{'queue': 'queue', 'what': 'put', 'when': 60},
                        {'what': 'popped', 'when': 60, 'worker': 'worker'}],
            'jid': 'jid-2',
            'klass': 'class',
            'priority': 5,
            'queue': 'queue',
            'remaining': 5,
            'retries': 5,
            'state': 'running',
            'tags': ['bar'],
            'tracked': False,
            'throttles': ['lala', 'ql:q:queue'],
            'worker': 'worker',
            'spawned_from_jid': 'jid'})

    def test_rerecur_move(self):
        '''Re-recurring a job in a new queue works like a move'''
        self.lua('queue.recurAtInterval', 0, 'queue', 'jid', 'klass', {}, 60, 0)
        self.assertEqual(
            self.lua('queue.pop', 0, 'queue', 'worker', 10)[0]['jid'], 'jid-1')
        self.lua('queue.recurAtInterval', 60, 'other', 'jid', 'klass', {}, 60, 0)
        self.assertEqual(
            self.lua('queue.pop', 60, 'other', 'worker', 10)[0]['jid'], 'jid-2')

    def test_history(self):
        '''Spawned jobs are 'put' at the time they would have been scheduled'''
        self.lua('queue.recurAtInterval', 0, 'queue', 'jid', 'klass', {}, 60, 0)
        jobs = self.lua('queue.pop', 599, 'queue', 'worker', 100)
        times = [job['history'][0]['when'] for job in jobs]
        self.assertEqual(
            times, [0, 60, 120, 180, 240, 300, 360, 420, 480, 540])
