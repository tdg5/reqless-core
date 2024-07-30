'''Test job-centric operations'''

import redis
from common import TestReqless

class TestJob(TestReqless):
    '''Some general jobby things'''
    def test_malformed(self):
        '''Enumerate all malformed input to job.setPriority'''
        self.assertMalformed(self.lua, [
            ('job.setPriority', '0'),
            ('job.setPriority', '0', 'jid'),
            ('job.setPriority', '0', 'jid', 'foo')
        ])

    def test_log(self):
        '''Can add a log to a job'''
        self.lua('queue.put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('job.log', 0, 'jid', 'foo', {'foo': 'bar'})
        self.assertEqual(self.lua('job.get', 0, 'jid')['history'], [
            {'queue': 'queue', 'what': 'put', 'when': 0},
            {'foo': 'bar', 'what': 'foo', 'when': 0}
        ])

    def test_log_still_works(self):
        '''Deprecated log API still works'''
        self.lua('queue.put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('log', 0, 'jid', 'foo', {'foo': 'bar'})
        self.assertEqual(self.lua('job.get', 0, 'jid')['history'], [
            {'queue': 'queue', 'what': 'put', 'when': 0},
            {'foo': 'bar', 'what': 'foo', 'when': 0}
        ])

    def test_log_nonexistent(self):
        '''If a job doesn't exist, logging throws an error'''
        self.assertRaisesRegexp(redis.ResponseError, r'does not exist',
            self.lua, 'job.log', 0, 'jid', 'foo', {'foo': 'bar'})

    def test_history(self):
        '''We only keep the most recent max-job-history items in history'''
        self.lua('config.set', 0, 'max-job-history', 5)
        for index in range(100):
            self.lua('queue.put', index, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.assertEqual(self.lua('job.get', 0, 'jid')['history'], [
            {'queue': 'queue', 'what': 'put', 'when': 0},
            {'queue': 'queue', 'what': 'put', 'when': 96},
            {'queue': 'queue', 'what': 'put', 'when': 97},
            {'queue': 'queue', 'what': 'put', 'when': 98},
            {'queue': 'queue', 'what': 'put', 'when': 99}])

    def test_heartbeat_still_works(self):
        '''Deprecated heartbeat API still works'''
        self.lua('queue.put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('queue.pop', 1, 'queue', 'worker', 10)
        self.lua('heartbeat', 2, 'jid', 'worker', {})
        self.lua('job.cancel', 3, 'jid')
        self.assertRaisesRegexp(redis.ResponseError, r'Job does not exist',
            self.lua, 'heartbeat', 4, 'jid', 'worker', {})

class TestRequeue(TestReqless):
    def test_requeue_existing_job(self):
        '''Requeueing an existing job is identical to `put`'''
        self.lua('queue.put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('job.requeue', 1, 'worker', 'queue-2', 'jid', 'klass', {}, 0)
        self.assertEqual(self.lua('job.get', 0, 'jid')['queue'], 'queue-2')

    def test_requeue_cancelled_job(self):
        '''Requeueing a cancelled (or non-existent) job fails'''
        self.lua('queue.put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('job.cancel', 1, 'jid')
        self.assertRaisesRegexp(redis.ResponseError, r'does not exist',
            self.lua, 'job.requeue', 2, 'worker', 'queue-2', 'jid', 'klass', {}, 0)


    def test_multiget_still_works(self):
        '''Deprecated multiget API still works'''
        self.lua('queue.put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('queue.put', 1, 'worker', 'queue', 'jid2', 'klass', {}, 0)
        self.assertEqual(2, len(self.lua('multiget', 2, 'jid', 'jid2')))

    def test_requeue_throttled_job(self):
        '''Requeueing  a throttled job should maintain correct state'''
        self.lua('queue.put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0, 'throttles', ['tid'])
        original = self.lua('job.getMulti', 1, 'jid')[0]
        self.lua('job.requeue', 2, 'worker', 'queue-2', 'jid', 'klass', {}, 0, 'throttles', ['tid'])
        updated = self.lua('job.getMulti', 3, 'jid')[0]

        # throttles and queue change during requeue
        self.assertEqual(updated['throttles'], ['tid', 'ql:q:queue-2'])
        self.assertEqual(updated['queue'], 'queue-2')
        del updated['throttles']
        del updated['queue']
        del updated['history']
        del original['throttles']
        del original['queue']
        del original['history']
        self.assertEqual(updated, original)

    def test_requeue_still_works(self):
        '''Deprecated requeue API still works'''
        self.lua('queue.put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('requeue', 1, 'worker', 'queue-2', 'jid', 'klass', {}, 0)
        self.assertEqual(self.lua('job.get', 0, 'jid')['queue'], 'queue-2')

class TestComplete(TestReqless):
    '''Test how we complete jobs'''
    def test_malformed(self):
        '''Enumerate all the way they can be malformed'''
        # Must actually create and pop a job or non-existant job will cause
        # errors that make non-malformed arguments seem malformed.
        self.lua('queue.put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('queue.pop', 1, 'queue', 'worker', 1)
        self.assertMalformed(self.lua, [
            ('job.completeAndRequeue', 2, 'jid', 'worker', 'queue', {}, 'queue-2', 'delay', 'foo'),
            ('job.completeAndRequeue', 2, 'jid', 'worker', 'queue', {}, 'queue-2', 'depends', '[}'),
        ])

    def test_complete_waiting(self):
        '''Only popped jobs can be completed'''
        self.lua('queue.put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.assertRaisesRegexp(redis.ResponseError, r'waiting',
            self.lua, 'job.complete', 1, 'jid', 'worker', 'queue', {})
        # Pop it and it should work
        self.lua('queue.pop', 2, 'queue', 'worker', 10)
        self.lua('job.complete', 1, 'jid', 'worker', 'queue', {})

    def test_complete_depends(self):
        '''Cannot complete a dependent job'''
        self.lua('queue.put', 0, 'worker', 'queue', 'a', 'klass', {}, 0)
        self.lua('queue.put', 0, 'worker', 'queue', 'b', 'klass', {}, 0, 'depends', ['a'])
        self.assertRaisesRegexp(redis.ResponseError, r'depends',
            self.lua, 'job.complete', 1, 'b', 'worker', 'queue', {})

    def test_complete_scheduled(self):
        '''Cannot complete a scheduled job'''
        self.lua('queue.put', 0, 'worker', 'queue', 'jid', 'klass', {}, 1)
        self.assertRaisesRegexp(redis.ResponseError, r'scheduled',
            self.lua, 'job.complete', 1, 'jid', 'worker', 'queue', {})

    def test_complete_nonexistent(self):
        '''Cannot complete a job that doesn't exist'''
        self.assertRaisesRegexp(redis.ResponseError, r'does not exist',
            self.lua, 'job.complete', 1, 'jid', 'worker', 'queue', {})
        self.lua('queue.put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('queue.pop', 0, 'queue', 'worker', 10)
        self.lua('job.complete', 1, 'jid', 'worker', 'queue', {})

    def test_complete_failed(self):
        '''Cannot complete a failed job'''
        self.lua('queue.put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('queue.pop', 0, 'queue', 'worker', 10)
        self.lua('job.fail', 1, 'jid', 'worker', 'group', 'message', {})
        self.assertRaisesRegexp(redis.ResponseError, r'failed',
            self.lua, 'job.complete', 0, 'jid', 'worker', 'queue', {})

    def test_complete_previously_failed(self):
        '''Erases failure data after completing'''
        self.lua('queue.put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('queue.pop', 1, 'queue', 'worker', 10)
        self.lua('job.fail', 2, 'jid', 'worker', 'group', 'message', {})
        self.lua('queue.put', 3, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('queue.pop', 4, 'queue', 'worker', 10)
        self.assertEqual(self.lua('job.get', 5, 'jid')['failure'], {
            'group': 'group',
            'message': 'message',
            'when': 2,
            'worker': 'worker'})
        self.lua('job.complete', 6, 'jid', 'worker', 'queue', {})
        self.assertEqual(self.lua('job.get', 7, 'jid')['failure'], {})

    def test_get_still_works(self):
        '''Deprecated get API still works'''
        jid = 'get_still_works_jid'
        self.lua('queue.put', 0, 'worker', 'queue', jid, 'klass', {}, 0)
        job = self.lua('get', 3, jid)
        self.assertEqual(jid, job['jid'])

    def test_basic(self):
        '''Basic completion'''
        self.lua('queue.put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('queue.pop', 1, 'queue', 'worker', 10)
        self.lua('job.complete', 2, 'jid', 'worker', 'queue', {})
        self.assertEqual(self.lua('job.get', 3, 'jid'), {
            'data': '{}',
            'dependencies': {},
            'dependents': {},
            'expires': 0,
            'failure': {},
            'history': [{'queue': 'queue', 'what': 'put', 'when': 0},
                        {'what': 'popped', 'when': 1, 'worker': 'worker'},
                        {'what': 'done', 'when': 2}],
            'jid': 'jid',
            'klass': 'klass',
            'priority': 0,
            'queue': u'',
            'remaining': 5,
            'retries': 5,
            'state': 'complete',
            'tags': {},
            'tracked': False,
            'throttles': ['ql:q:queue'],
            'worker': u'',
            'spawned_from_jid': False})

    def test_advance(self):
        '''Can complete and advance a job in one fell swooop'''
        self.lua('queue.put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('queue.pop', 1, 'queue', 'worker', 10)
        self.lua('job.completeAndRequeue', 2, 'jid', 'worker', 'queue', {}, 'foo')
        self.assertEqual(
            self.lua('queue.pop', 3, 'foo', 'worker', 10)[0]['jid'], 'jid')

    def test_advance_empty_array_mangle(self):
        '''Does not mangle empty arrays in job data when advancing'''
        self.lua('queue.put', 0, 'worker', 'queue', 'jid', 'klass', '[]', 0)
        self.lua('queue.pop', 1, 'queue', 'worker', 10)
        self.lua('job.completeAndRequeue', 2, 'jid', 'worker', 'queue', '[]', 'foo')
        self.assertEqual(
            self.lua('queue.pop', 3, 'foo', 'worker', 10)[0]['data'], '[]')

    def test_wrong_worker(self):
        '''Only the right worker can complete it'''
        self.lua('queue.put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('queue.pop', 1, 'queue', 'worker', 10)
        self.assertRaisesRegexp(redis.ResponseError, r'another worker',
            self.lua, 'job.complete', 2, 'jid', 'another', 'queue', {})

    def test_wrong_queue(self):
        '''A job can only be completed in the queue it's in'''
        self.lua('queue.put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('queue.pop', 1, 'queue', 'worker', 10)
        self.assertRaisesRegexp(redis.ResponseError, r'another queue',
            self.lua, 'job.complete', 2, 'jid', 'worker', 'another-queue', {})

    def test_expire_complete_count(self):
        '''Jobs expire after a k complete jobs'''
        self.lua('config.set', 0, 'jobs-history-count', 5)
        jids = range(10)
        for jid in range(10):
            self.lua('queue.put', 0, 'worker', 'queue', jid, 'klass', {}, 0)
        self.lua('queue.pop', 1, 'queue', 'worker', 10)
        for jid in jids:
            self.lua('job.complete', 2, jid, 'worker', 'queue', {})
        existing = [self.lua('job.get', 3, jid) for jid in range(10)]
        self.assertEqual(len([i for i in existing if i]), 5)

    def test_expire_complete_time(self):
        '''Jobs expire after a certain amount of time'''
        self.lua('config.set', 0, 'jobs-history', -1)
        jids = range(10)
        for jid in range(10):
            self.lua('queue.put', 0, 'worker', 'queue', jid, 'klass', {}, 0)
        self.lua('queue.pop', 1, 'queue', 'worker', 10)
        for jid in jids:
            self.lua('job.complete', 2, jid, 'worker', 'queue', {})
        existing = [self.lua('job.get', 3, jid) for jid in range(10)]
        self.assertEqual([i for i in existing if i], [])

    def test_expire_complete_tags_cleared(self):
        '''Tag's should be removed once they no longer have any jobs'''
        # Set all jobs to expire immediately
        self.lua('config.set', 1, 'jobs-history', -1)
        # When cancelled
        self.lua('queue.put', 2, 'worker', 'queue', 'jid', 'klass', {}, 0, 'tags', ['abc'])
        self.assertEqual(self.lua('jobs.tagged', 3, 'abc', 0, 0)['jobs'], ['jid'])
        self.lua('job.cancel', 4, 'jid', 'worker', 'queue', {})
        self.assertEqual(self.lua('jobs.tagged', 5, 'abc', 0, 0)['jobs'], {})
        self.assertEqual(self.redis.zrange('ql:tags', 0, -1), [])
        # When complete
        self.lua('queue.put', 6, 'worker', 'queue', 'jid', 'klass', {}, 0, 'tags', ['abc'])
        self.assertEqual(self.lua('jobs.tagged', 7, 'abc', 0, 0)['jobs'], ['jid'])
        self.lua('queue.pop', 8, 'queue', 'worker', 1)
        self.lua('job.complete', 9, 'jid', 'worker', 'queue', {})
        self.assertEqual(self.lua('jobs.tagged', 10, 'abc', 0, 0)['jobs'], {})
        self.assertEqual(self.redis.zrange('ql:tags', 0, -1), [])

    def test_complete_still_works(self):
        '''Deprecated complete API still works'''
        self.lua('queue.put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('queue.pop', 1, 'queue', 'worker', 1)
        self.lua('complete', 2, 'jid', 'worker', 'queue', {})
        # Ensure that it shows up everywhere it should
        self.assertEqual(self.lua('job.get', 3, 'jid')['state'], 'complete')

    def test_complete_that_reques_still_works(self):
        '''Deprecated complete API still works'''
        self.lua('queue.put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('queue.pop', 1, 'queue', 'worker', 1)
        self.lua('complete', 2, 'jid', 'worker', 'queue', {}, 'next', 'queue-2')

        # The job should be requeued immediately to queue-2
        self.assertEqual(self.lua('job.get', 3, 'jid')['queue'], 'queue-2')

class TestTimeout(TestReqless):
    '''Basic timeout works'''
    def test_timeout_running(self):
        '''You can timeout running jobs'''
        self.lua('queue.put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('queue.pop', 1, 'queue', 'worker', 1)
        self.lua('job.timeout', 0, 'jid')
        job = self.lua('job.get', 0, 'jid')
        self.assertEqual(job['state'], 'stalled')
        self.assertEqual(job['worker'], '')

    '''Deprecated timeout API still works'''
    def test_timeout_still_works(self):
        '''Deprecated timeout API still works'''
        self.lua('queue.put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('queue.pop', 1, 'queue', 'worker', 1)
        self.lua('timeout', 0, 'jid')
        job = self.lua('job.get', 0, 'jid')
        self.assertEqual(job['state'], 'stalled')
        self.assertEqual(job['worker'], '')

class TestCancel(TestReqless):
    '''Canceling jobs'''
    def test_cancel_waiting(self):
        '''You can cancel waiting jobs'''
        self.lua('queue.put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('job.cancel', 0, 'jid')
        self.assertEqual(self.lua('job.get', 0, 'jid'), None)

    def test_cancel_depends(self):
        '''You can cancel dependent job'''
        self.lua('queue.put', 0, 'worker', 'queue', 'a', 'klass', {}, 0)
        self.lua('queue.put', 0, 'worker', 'queue', 'b', 'klass', {}, 0, 'depends', ['a'])
        self.lua('job.cancel', 0, 'b')
        self.assertEqual(self.lua('job.get', 0, 'b'), None)
        self.assertEqual(self.lua('job.get', 0, 'a')['dependencies'], {})

    def test_cancel_dependents(self):
        '''Cannot cancel jobs if they still have dependencies'''
        self.lua('queue.put', 0, 'worker', 'queue', 'a', 'klass', {}, 0)
        self.lua('queue.put', 0, 'worker', 'queue', 'b', 'klass', {}, 0, 'depends', ['a'])
        self.assertRaisesRegexp(redis.ResponseError, r'dependency',
            self.lua, 'job.cancel', 0, 'a')

    def test_cancel_scheduled(self):
        '''You can cancel scheduled jobs'''
        self.lua('queue.put', 0, 'worker', 'queue', 'jid', 'klass', {}, 1)
        self.lua('job.cancel', 0, 'jid')
        self.assertEqual(self.lua('job.get', 0, 'jid'), None)

    def test_cancel_nonexistent(self):
        '''Can cancel jobs that do not exist without failing'''
        self.lua('job.cancel', 0, 'jid')

    def test_cancel_failed(self):
        '''Can cancel failed jobs'''
        self.lua('queue.put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('queue.pop', 0, 'queue', 'worker', 10)
        self.lua('job.fail', 1, 'jid', 'worker', 'group', 'message', {})
        self.lua('job.cancel', 2, 'jid')
        self.assertEqual(self.lua('job.get', 3, 'jid'), None)

    def test_cancel_running(self):
        '''Can cancel running jobs, prevents heartbeats'''
        self.lua('queue.put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('queue.pop', 1, 'queue', 'worker', 10)
        self.lua('job.heartbeat', 2, 'jid', 'worker', {})
        self.lua('job.cancel', 3, 'jid')
        self.assertRaisesRegexp(redis.ResponseError, r'Job does not exist',
            self.lua, 'job.heartbeat', 4, 'jid', 'worker', {})

    def test_cancel_retries(self):
        '''Can cancel job that has been failed from retries through retry'''
        self.lua('queue.put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0, 'retries', 0)
        self.lua('queue.pop', 1, 'queue', 'worker', 10)
        self.assertEqual(self.lua('job.get', 2, 'jid')['state'], 'running')
        self.lua('job.retry', 3, 'jid', 'queue', 'worker')
        self.lua('job.cancel', 4, 'jid')
        self.assertEqual(self.lua('job.get', 5, 'jid'), None)

    def test_cancel_pop_retries(self):
        '''Can cancel job that has been failed from retries through pop'''
        self.lua('config.set', 0, 'heartbeat', -10)
        self.lua('config.set', 0, 'grace-period', 0)
        self.lua('queue.put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0, 'retries', 0)
        self.lua('queue.pop', 1, 'queue', 'worker', 10)
        self.lua('queue.pop', 2, 'queue', 'worker', 10)
        self.lua('job.cancel', 3, 'jid')
        self.assertEqual(self.lua('job.get', 4, 'jid'), None)

    def test_cancel_still_works(self):
        '''Deprecated cancel API still works'''
        self.lua('queue.put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('cancel', 0, 'jid')
        self.assertEqual(self.lua('job.get', 0, 'jid'), None)


class TestThrottles(TestReqless):
  '''Acquiring and releasing throttles'''
  def test_acquire_throttles_acquires_all_throttles(self):
    '''Can acquire locks for all throttles'''
    # Should have throttles for queue and named throttles
    self.lua('queue.put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0, 'throttles', ['tid', 'wid'])
    self.lua('queue.pop', 0, 'queue', 'worker', 1)
    self.assertEqual(self.lua('throttle.locks', 0, 'tid'), ['jid'])
    self.assertEqual(self.lua('throttle.locks', 0, 'wid'), ['jid'])
    self.assertEqual(self.lua('throttle.locks', 0, 'ql:q:queue'), ['jid'])

  def test_release_throttles_on_acquisition_failure(self):
    '''Cancels locked throttles if locks can not be obtained for all locks'''
    # Should have throttles for queue and named throttles
    self.lua('throttle.set', 0, 'wid', 1)
    self.lua('queue.put', 1, 'worker', 'queue', 'jid1', 'klass', {}, 0, 'throttles', ['wid'])
    self.lua('queue.put', 2, 'worker', 'queue', 'jid2', 'klass', {}, 0, 'throttles', ['tid', 'wid'])
    self.lua('queue.pop', 3, 'queue', 'worker', 2)
    self.assertEqual(self.lua('throttle.locks', 4, 'wid'), ['jid1'])
    self.assertEqual(self.lua('throttle.locks', 5, 'tid'), [])
    self.assertEqual(self.lua('throttle.locks', 6, 'ql:q:queue'), ['jid1'])
    self.assertEqual(self.lua('job.get', 7, 'jid2')['state'], 'throttled')

  def test_release_throttles_after_acquisition_on_completion(self):
    '''Can acquire locks for all throttles and then release them when complete'''
    # Should have throttles for queue and named throttles
    self.lua('queue.put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0, 'throttles', ['tid', 'wid'])
    self.lua('queue.pop', 0, 'queue', 'worker', 1)
    self.assertEqual(self.lua('throttle.locks', 0, 'tid'), ['jid'])
    self.assertEqual(self.lua('throttle.locks', 0, 'wid'), ['jid'])
    self.assertEqual(self.lua('throttle.locks', 0, 'ql:q:queue'), ['jid'])
    self.lua('job.complete', 0, 'jid', 'worker', 'queue', {})
    self.assertEqual(self.lua('throttle.locks', 0, 'tid'), [])
    self.assertEqual(self.lua('throttle.locks', 0, 'wid'), [])
    self.assertEqual(self.lua('throttle.locks', 0, 'ql:q:queue'), [])

  def test_release_throttles_after_acquisition_on_retry(self):
    '''Can acquire locks for all throttles and then release them on retry'''
    # Should have throttles for queue and named throttles
    self.lua('queue.put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0, 'throttles', ['tid', 'wid'])
    self.lua('queue.pop', 0, 'queue', 'worker', 1)
    self.assertEqual(self.lua('throttle.locks', 0, 'tid'), ['jid'])
    self.assertEqual(self.lua('throttle.locks', 0, 'wid'), ['jid'])
    self.assertEqual(self.lua('throttle.locks', 0, 'ql:q:queue'), ['jid'])
    self.lua('job.retry', 0, 'jid', 'queue', 'worker', 0, 'retry', 'retrying')
    self.assertEqual(self.lua('throttle.locks', 0, 'tid'), [])
    self.assertEqual(self.lua('throttle.locks', 0, 'wid'), [])
    self.assertEqual(self.lua('throttle.locks', 0, 'ql:q:queue'), [])

  def test_release_throttles_after_acquisition_on_fail(self):
    '''Can acquire locks for all throttles and then release them on failure'''
    # Should have throttles for queue and named throttles
    self.lua('queue.put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0, 'throttles', ['tid', 'wid'])
    self.lua('queue.pop', 0, 'queue', 'worker', 1)
    self.assertEqual(self.lua('throttle.locks', 0, 'tid'), ['jid'])
    self.assertEqual(self.lua('throttle.locks', 0, 'wid'), ['jid'])
    self.assertEqual(self.lua('throttle.locks', 0, 'ql:q:queue'), ['jid'])
    self.lua('job.fail', 0, 'jid', 'worker', 'queue', {})
    self.assertEqual(self.lua('throttle.locks', 0, 'tid'), [])
    self.assertEqual(self.lua('throttle.locks', 0, 'wid'), [])
    self.assertEqual(self.lua('throttle.locks', 0, 'ql:q:queue'), [])
