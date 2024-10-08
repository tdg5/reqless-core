'''Test throttle-centric operations'''

import redis
import code

from test.common import TestReqless


class TestThrottle(TestReqless):
  '''Test setting throttle data'''
  def test_set(self):
    self.lua('throttle.set', 0, 'tid', 5, 0)
    self.assertEqual(self.redis.hmget('ql:th:tid', 'id')[0], b'tid')
    self.assertEqual(self.redis.hmget('ql:th:tid', 'maximum')[0], b'5')
    self.assertEqual(self.redis.ttl('ql:th:tid'), -1)

  '''Test setting throttle data for a queue'''
  def test_set_for_queue(self):
    self.lua('queue.throttle.set', 0, 'queue', 5)
    self.assertEqual(self.redis.hmget('ql:th:ql:q:queue', 'id')[0], b'ql:q:queue')
    self.assertEqual(self.redis.hmget('ql:th:ql:q:queue', 'maximum')[0], b'5')
    self.assertEqual(self.redis.ttl('ql:th:ql:q:queue'), -1)

  '''Test setting a expiring throttle'''
  def test_set_with_expiration(self):
    self.lua('throttle.set', 0, 'tid', 5, 1000)
    self.assertNotEqual(self.redis.ttl('ql:th:tid'), -1)

  '''Test retrieving throttle data'''
  def test_get(self):
    self.lua('throttle.set', 0, 'tid', 5, 1000)
    self.redis.hset('ql:th:tid', mapping={'id': 'tid', 'maximum' : 5})
    self.assertEqual(self.lua('throttle.get', 0, 'tid'), {'id' : 'tid', 'maximum' : 5, 'ttl': 1000})

  '''Test retrieving throttle data for queue'''
  def test_get_for_queue(self):
    self.redis.hset('ql:th:ql:q:queue', mapping={'id': 'tid', 'maximum' : 5})
    self.assertEqual(
        self.lua('queue.throttle.get', 0, 'queue'),
        {'id' : 'ql:q:queue', 'maximum' : 5, 'ttl': -1}
    )

  '''Test retrieving uninitiailized throttle data'''
  def test_get_uninitialized(self):
      self.assertEqual(self.lua('throttle.get', 0, 'tid'), {'id' : 'tid', 'maximum' : 0, 'ttl': -2})

  '''Test deleting the throttle data'''
  def test_delete(self):
    self.lua('throttle.set', 0, 'tid', 5)
    self.assertEqual(self.lua('throttle.get', 0, 'tid'), {'id' : 'tid', 'maximum' : 5, 'ttl': -1})
    self.lua('throttle.delete', 0, 'tid')
    self.assertEqual(self.lua('throttle.get', 0, 'tid'), {'id' : 'tid', 'maximum' : 0, 'ttl': -2})

  '''Test release properly removes the jid from the throttle'''
  def test_release(self):
    self.lua('throttle.set', 0, 'tid', 1)
    self.lua('queue.put', 0, 'worker', 'queue', 'jid1', 'klass', {}, 0, 'throttles', ['tid'])
    self.lua('queue.put', 1, 'worker', 'queue', 'jid2', 'klass', {}, 0, 'throttles', ['tid'])
    self.lua('queue.pop', 2, 'queue', 'worker', 2)
    self.assertEqual(self.lua('throttle.locks', 3, 'tid'), ['jid1'])
    self.assertEqual(self.lua('throttle.pending', 4, 'tid'), ['jid2'])
    self.assertEqual(self.lua('queue.jobsByState', 5, 'throttled', 'queue'), ['jid2'])
    self.lua('throttle.release', 6, 'tid', 'jid1', 'jid2')
    self.assertEqual(self.lua('throttle.locks', 7, 'tid'), [])
    self.assertEqual(self.lua('throttle.pending', 8, 'tid'), [])
    self.assertEqual(self.lua('queue.peek', 9,'queue', 0, 1)[0]['jid'], 'jid2')
    self.lua('job.cancel', 10, 'jid1', 'worker', 'queue', {})
    self.lua('job.cancel', 11, 'jid2', 'worker', 'queue', {})
    self.assertEqual(self.lua('throttle.locks', 12, 'tid'), [])
    self.assertEqual(self.lua('throttle.pending', 13, 'tid'), [])
    self.assertEqual(self.lua('queue.jobsByState', 13, 'throttled', 'queue'), [])
    self.assertEqual(self.lua('queue.jobsByState', 14, 'running', 'queue'), [])

  '''Test release of pending jobs before lock holders'''
  def test_release_pending_before_lock_holders(self):
    self.lua('throttle.set', 0, 'tid', 1)
    self.lua('queue.put', 0, 'worker', 'queue', 'jid1', 'klass', {}, 0, 'throttles', ['tid'])
    self.lua('queue.put', 1, 'worker', 'queue', 'jid2', 'klass', {}, 0, 'throttles', ['tid'])
    self.lua('queue.pop', 2, 'queue', 'worker', 2)
    self.assertEqual(self.lua('throttle.locks', 3, 'tid'), ['jid1'])
    self.assertEqual(self.lua('throttle.pending', 4, 'tid'), ['jid2'])
    self.assertEqual(self.lua('queue.jobsByState', 5, 'throttled', 'queue'), ['jid2'])
    self.lua('throttle.release', 6, 'tid', 'jid2', 'jid1')
    self.assertEqual(self.lua('throttle.locks', 7, 'tid'), [])
    self.assertEqual(self.lua('throttle.pending', 8, 'tid'), [])
    self.assertEqual(self.lua('queue.peek', 9,'queue', 0, 1), [])
    self.lua('job.cancel', 10, 'jid1', 'worker', 'queue', {})
    self.lua('job.cancel', 11, 'jid2', 'worker', 'queue', {})
    self.assertEqual(self.lua('throttle.locks', 12, 'tid'), [])
    self.assertEqual(self.lua('throttle.pending', 13, 'tid'), [])
    self.assertEqual(self.lua('queue.jobsByState', 13, 'throttled', 'queue'), [])
    self.assertEqual(self.lua('queue.jobsByState', 14, 'running', 'queue'), [])


class TestAcquire(TestReqless):
  '''Test that a job has a default queue throttle'''
  def test_default_queue_throttle(self):
    self.lua('queue.put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
    self.assertEqual(self.lua('job.get', 0, 'jid')['throttles'], ['ql:q:queue'])

  '''Test that job can specify a throttle'''
  def test_specify_throttle(self):
    self.lua('queue.put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0, 'throttles', ['tid'])
    self.assertEqual(self.lua('job.get', 0, 'jid')['throttles'], ['tid', 'ql:q:queue'])

  '''Test that a job can acquire a throttle'''
  def test_acquire_throttle(self):
    self.lua('queue.put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0, 'throttles', ['tid'])
    self.lua('queue.pop', 0, 'queue', 'worker', 1)
    self.assertEqual(self.lua('throttle.locks', 0, 'tid'), ['jid'])

  '''Test that acquiring of a throttle lock properly limits the number of jobs'''
  def test_limit_number_of_locks(self):
    self.lua('throttle.set', 0, 'tid', 1)
    self.lua('queue.put', 0, 'worker', 'queue', 'jid1', 'klass', {}, 0, 'throttles', ['tid'])
    self.lua('queue.put', 1, 'worker', 'queue', 'jid2', 'klass', {}, 0, 'throttles', ['tid'])
    self.lua('queue.put', 2, 'worker', 'queue', 'jid3', 'klass', {}, 0, 'throttles', ['tid'])
    self.lua('queue.put', 3, 'worker', 'queue', 'jid4', 'klass', {}, 0, 'throttles', ['tid'])
    self.lua('queue.pop', 0, 'queue', 'worker', 4)
    self.assertEqual(self.lua('throttle.locks', 0, 'tid'), ['jid1'])
    self.assertEqual(self.lua('throttle.pending', 0, 'tid'), ['jid2', 'jid3', 'jid4'])


class TestRelease(TestReqless):
  '''Test that when there are no pending jobs lock is properly released'''
  def test_no_pending_jobs(self):
    self.lua('queue.put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0, 'throttles', ['tid'])
    self.lua('queue.pop', 0, 'queue', 'worker', 1)
    self.assertEqual(self.lua('throttle.locks', 0, 'tid'), ['jid'])
    self.assertEqual(self.lua('queue.jobsByState', 0, 'throttled', 'queue'), [])
    self.lua('job.complete', 0, 'jid', 'worker', 'queue', {})
    self.assertEqual(self.lua('throttle.locks', 0, 'tid'), [])
    self.assertEqual(self.lua('queue.jobsByState', 0, 'throttled', 'queue'), [])

  '''Test that releasing a pending throttled job correctly cleans up'''
  def test_on_release_pending_job_is_removed_from_throttle(self):
    self.lua('throttle.set', 0, 'tid', 1)
    self.lua('queue.put', 1, 'worker', 'queue', 'jid1', 'klass', {}, 0, 'throttles', ['tid'])
    self.lua('queue.put', 2, 'worker', 'queue', 'jid2', 'klass', {}, 0, 'throttles', ['tid'])
    self.lua('queue.pop', 3, 'queue', 'worker', 2)
    self.assertEqual(self.lua('throttle.locks', 4, 'tid'), ['jid1'])
    self.assertEqual(self.lua('throttle.pending', 5, 'tid'), ['jid2'])
    self.lua('job.cancel', 6, 'jid2', 'worker', 'queue', {})
    self.assertEqual(self.lua('throttle.locks', 7, 'tid'), ['jid1'])
    self.assertEqual(self.lua('throttle.pending', 8, 'tid'), [])

  '''Test that releasing a lock properly inserts another job in the work queue'''
  def test_next_job_is_moved_into_work_qeueue(self):
    self.lua('throttle.set', 0, 'tid', 1)
    self.lua('queue.put', 0, 'worker', 'queue', 'jid1', 'klass', {}, 0, 'throttles', ['tid'])
    self.lua('queue.put', 1, 'worker', 'queue', 'jid2', 'klass', {}, 0, 'throttles', ['tid'])
    self.lua('queue.pop', 2, 'queue', 'worker', 2)
    self.assertEqual(self.lua('throttle.locks', 3, 'tid'), ['jid1'])
    self.assertEqual(self.lua('throttle.pending', 4, 'tid'), ['jid2'])
    self.assertEqual(self.lua('queue.jobsByState', 5, 'throttled', 'queue'), ['jid2'])
    self.assertEqual(self.lua('queue.jobsByState', 6, 'running', 'queue'), ['jid1'])
    self.lua('job.complete', 7, 'jid1', 'worker', 'queue', {})
    # Lock should be empty until another job is popped
    self.assertEqual(self.lua('throttle.locks', 8, 'tid'), [])
    self.assertEqual(self.lua('throttle.pending', 9, 'tid'), [])
    self.assertEqual(self.lua('queue.jobsByState', 10, 'throttled', 'queue'), [])
    self.assertEqual(self.lua('queue.jobsByState', 11, 'running', 'queue'), [])
    self.lua('queue.pop', 12, 'queue', 'worker', 1)
    self.assertEqual(self.lua('throttle.locks', 13, 'tid'), ['jid2'])
    self.assertEqual(self.lua('throttle.pending', 14, 'tid'), [])
    self.assertEqual(self.lua('queue.jobsByState', 15, 'throttled', 'queue'), [])
    self.assertEqual(self.lua('queue.jobsByState', 16, 'running', 'queue'), ['jid2'])

  '''Test that cancelling a job properly adds another job in the work queue'''
  def test_on_cancel_next_job_is_moved_into_work_queue(self):
    self.lua('throttle.set', 0, 'tid', 1)
    self.lua('queue.put', 0, 'worker', 'queue', 'jid1', 'klass', {}, 0, 'throttles', ['tid'])
    self.lua('queue.put', 1, 'worker', 'queue', 'jid2', 'klass', {}, 0, 'throttles', ['tid'])
    self.lua('queue.pop', 2, 'queue', 'worker', 2)
    self.assertEqual(self.lua('throttle.locks', 3, 'tid'), ['jid1'])
    self.assertEqual(self.lua('throttle.pending', 4, 'tid'), ['jid2'])
    self.assertEqual(self.lua('queue.jobsByState', 5, 'throttled', 'queue'), ['jid2'])
    self.lua('job.cancel', 6, 'jid1', 'worker', 'queue', {})
    # Lock should be empty until another job is popped
    self.assertEqual(self.lua('throttle.locks', 7, 'tid'), [])
    self.assertEqual(self.lua('throttle.pending', 8, 'tid'), [])
    self.assertEqual(self.lua('queue.jobsByState', 9, 'throttled', 'queue'), [])
    self.lua('queue.pop', 10, 'queue', 'worker', 1)
    self.assertEqual(self.lua('throttle.locks', 11, 'tid'), ['jid2'])
    self.assertEqual(self.lua('throttle.pending', 12, 'tid'), [])
    self.assertEqual(self.lua('queue.jobsByState', 13, 'throttled', 'queue'), [])
    self.assertEqual(self.lua('queue.jobsByState', 14, 'running', 'queue'), ['jid2'])

  '''Test that when a job completes it properly releases the lock'''
  def test_on_complete_lock_is_released(self):
    self.lua('throttle.set', 0, 'tid', 1)
    self.lua('queue.put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0, 'throttles', ['tid'])
    self.lua('queue.pop', 0, 'queue', 'worker', 1)
    self.assertEqual(self.lua('throttle.locks', 0, 'tid'), ['jid'])
    self.lua('job.complete', 0, 'jid', 'worker', 'queue', {})
    self.assertEqual(self.lua('throttle.locks', 0, 'tid'), [])
    self.assertEqual(self.lua('queue.jobsByState', 0, 'throttled', 'queue'), [])

  '''Test that when a job fails it properly releases the lock'''
  def test_on_failure_lock_is_released(self):
    self.lua('throttle.set', 0, 'tid', 1)
    self.lua('queue.put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0, 'throttles', ['tid'])
    self.lua('queue.pop', 0, 'queue', 'worker', 1)
    self.assertEqual(self.lua('throttle.locks', 0, 'tid'), ['jid'])
    self.lua('job.fail', 0, 'jid', 'worker', 'failed', 'i failed', {})
    self.assertEqual(self.lua('throttle.locks', 0, 'tid'), [])
    self.assertEqual(self.lua('queue.jobsByState', 0, 'throttled', 'queue'), [])

  '''Test that when a job retries it properly releases the lock
     and goes back into pending'''
  def test_on_retry_lock_is_released(self):
    self.lua('throttle.set', 0, 'tid', 1)
    self.lua('queue.put', 0, 'worker', 'queue', 'jid1', 'klass', {}, 0, 'throttles', ['tid'])
    self.lua('queue.pop', 0, 'queue', 'worker', 1)
    self.assertEqual(self.lua('throttle.locks', 0, 'tid'), ['jid1'])
    self.lua('job.retry', 0, 'jid1', 'queue', 'worker', 0, 'retry', 'retrying')
    self.assertEqual(self.lua('throttle.locks', 0, 'tid'), [])
    self.assertEqual(self.lua('queue.jobsByState', 0, 'throttled', 'queue'), [])

  '''Test that when a job retries it is able to reacquire the lock when next popped'''
  def test_on_retry_lock_is_reacquired(self):
    self.lua('throttle.set', 0, 'tid', 1)
    self.lua('queue.put', 0, 'worker', 'queue', 'jid1', 'klass', {}, 0, 'throttles', ['tid'])
    self.lua('queue.pop', 0, 'queue', 'worker', 1)
    self.assertEqual(self.lua('throttle.locks', 0, 'tid'), ['jid1'])
    self.lua('job.retry', 0, 'jid1', 'queue', 'worker', 0, 'retry', 'retrying')
    self.assertEqual(self.lua('throttle.locks', 0, 'tid'), [])
    self.assertEqual(self.lua('queue.jobsByState', 0, 'throttled', 'queue'), [])

  '''Test that when a job retries and has no pending jobs it acquires the lock again on next pop'''
  def test_on_retry_without_pending_lock_is_reacquired(self):
    self.lua('throttle.set', 0, 'tid', 1)
    self.lua('queue.put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0, 'throttles', ['tid'])
    self.lua('queue.pop', 0, 'queue', 'worker', 1)
    self.assertEqual(self.lua('throttle.locks', 0, 'tid'), ['jid'])
    self.lua('job.retry', 0, 'jid', 'queue', 'worker', 0, 'retry', 'retrying')
    self.lua('queue.pop', 0, 'queue', 'worker', 1)
    self.assertEqual(self.lua('throttle.locks', 0, 'tid'), ['jid'])
    self.assertEqual(self.lua('queue.jobsByState', 0, 'throttled', 'queue'), [])

  '''Test that when a job retries and another job is pending, the pending job acquires the lock'''
  def test_on_retry_with_pending_lock_is_not_reacquired(self):
    # The retrying job will only re-acquire the lock if nothing is ahead of it in
    # the work queue that requires that lock
    self.lua('throttle.set', 0, 'tid', 1)
    self.lua('queue.put', 0, 'worker', 'queue', 'jid1', 'klass', {}, 0, 'throttles', ['tid'])
    self.lua('queue.put', 1, 'worker', 'queue', 'jid2', 'klass', {}, 0, 'throttles', ['tid'])
    self.lua('queue.pop', 2, 'queue', 'worker', 2)
    self.assertEqual(self.lua('throttle.locks', 3, 'tid'), ['jid1'])
    self.assertEqual(self.lua('throttle.pending', 4, 'tid'), ['jid2'])
    self.assertEqual(self.lua('queue.jobsByState', 5, 'throttled', 'queue'), ['jid2'])
    self.lua('job.retry', 6, 'jid1', 'queue', 'worker', 0, 'retry', 'retrying')
    self.assertEqual(self.lua('throttle.locks', 7, 'tid'), [])
    self.assertEqual(self.lua('throttle.pending', 8, 'tid'), [])
    self.assertEqual(self.lua('queue.jobsByState', 9, 'throttled', 'queue'), [])
    self.lua('queue.pop', 10, 'queue', 'worker', 2)
    self.assertEqual(self.lua('throttle.locks', 11, 'tid'), ['jid2'])
    self.assertEqual(self.lua('throttle.pending', 12, 'tid'), ['jid1'])
    self.assertEqual(self.lua('queue.jobsByState', 13, 'throttled', 'queue'), ['jid1'])

  def test_on_timeout_locks_are_properly_released(self):
    self.lua('throttle.set', 0, 'tid', 1)
    self.lua('queue.put', 0, 'worker', 'queue', 'jid1', 'klass', {}, 0, 'throttles', ['tid'])
    self.lua('queue.put', 1, 'worker', 'queue', 'jid2', 'klass', {}, 0, 'throttles', ['tid'])
    self.lua('queue.pop', 2, 'queue', 'worker', 2)
    self.assertEqual(self.lua('throttle.locks', 3, 'tid'), ['jid1'])
    self.assertEqual(self.lua('throttle.pending', 4, 'tid'), ['jid2'])
    self.assertEqual(self.lua('queue.jobsByState', 5, 'throttled', 'queue'), ['jid2'])
    self.lua('job.timeout', 6, 'jid1')
    self.assertEqual(self.lua('throttle.locks', 7, 'tid'), [])
    self.assertEqual(self.lua('throttle.pending', 8, 'tid'), [])
    self.assertEqual(self.lua('queue.jobsByState', 9, 'throttled', 'queue'), [])


class TestDependents(TestReqless):
  def test_dependencies_can_acquire_lock_after_dependent_success(self):
    self.lua('throttle.set', 0, 'tid', 1)
    self.lua('queue.put', 1, 'worker', 'queue', 'jid1', 'klass', {}, 0, 'throttles', ['tid'])
    self.lua('queue.put', 2, 'worker', 'queue', 'jid2', 'klass', {}, 0, 'depends', ['jid1'], 'throttles', ['tid'])
    self.lua('queue.put', 3, 'worker', 'queue', 'jid3', 'klass', {}, 0, 'depends', ['jid2'], 'throttles', ['tid'])

    self.lua('queue.pop', 4, 'queue', 'worker', 1)
    self.assertEqual(self.lua('throttle.locks', 5, 'tid'), ['jid1'])
    self.assertEqual(self.lua('queue.jobsByState', 6, 'throttled', 'queue'), [])
    self.lua('job.complete', 7, 'jid1', 'worker', 'queue', {})

    self.lua('queue.pop', 8, 'queue', 'worker', 1)
    self.assertEqual(self.lua('throttle.locks', 9, 'tid'), ['jid2'])
    self.assertEqual(self.lua('queue.jobsByState', 10, 'throttled', 'queue'), [])
    self.lua('job.complete', 11, 'jid2', 'worker', 'queue', {})

    self.lua('queue.pop', 12, 'queue', 'worker', 1)
    self.assertEqual(self.lua('throttle.locks', 13, 'tid'), ['jid3'])
    self.assertEqual(self.lua('queue.jobsByState', 14, 'throttled', 'queue'), [])
    self.lua('job.complete', 15, 'jid3', 'worker', 'queue', {})

    self.assertEqual(self.lua('throttle.locks', 16, 'tid'), [])
    self.assertEqual(self.lua('queue.jobsByState', 17, 'throttled', 'queue'), [])

  def test_dependencies_can_acquire_lock_after_dependent_failure(self):
    self.lua('throttle.set', 0, 'tid', 1)
    self.lua('queue.put', 0, 'worker', 'queue', 'jid1', 'klass', {}, 0, 'throttles', ['tid'])
    self.lua('queue.put', 0, 'worker', 'queue', 'jid2', 'klass', {}, 0, 'depends', ['jid1'], 'throttles', ['tid'])
    self.lua('queue.put', 0, 'worker', 'queue', 'jid3', 'klass', {}, 0, 'depends', ['jid2'], 'throttles', ['tid'])

    self.lua('queue.pop', 0, 'queue', 'worker', 1)
    self.assertEqual(self.lua('throttle.locks', 0, 'tid'), ['jid1'])
    self.assertEqual(self.lua('queue.jobsByState', 0, 'throttled', 'queue'), [])
    self.lua('job.fail', 0, 'jid1', 'worker', 'failed', 'i failed', {})

    self.assertEqual(self.lua('throttle.locks', 0, 'tid'), [])
    self.assertEqual(self.lua('queue.jobsByState', 0, 'throttled', 'queue'), [])

  def test_dependencies_do_not_acquire_lock_on_dependent_retry(self):
    self.lua('throttle.set', 0, 'tid', 1)
    self.lua('queue.put', 1, 'worker', 'queue', 'jid1', 'klass', {}, 0, 'throttles', ['tid'])
    self.lua('queue.put', 2, 'worker', 'queue', 'jid2', 'klass', {}, 0, 'depends', ['jid1'], 'throttles', ['tid'])
    self.lua('queue.put', 3, 'worker', 'queue', 'jid3', 'klass', {}, 0, 'depends', ['jid2'], 'throttles', ['tid'])

    self.lua('queue.pop', 0, 'queue', 'worker', 1)
    self.assertEqual(self.lua('throttle.locks', 0, 'tid'), ['jid1'])
    self.assertEqual(self.lua('queue.jobsByState', 0, 'throttled', 'queue'), [])
    self.lua('job.retry', 0, 'jid1', 'queue', 'worker', 0, 'retry', 'retrying')

    self.lua('queue.pop', 0, 'queue', 'worker', 1)
    self.assertEqual(self.lua('throttle.locks', 0, 'tid'), ['jid1'])
    self.assertEqual(self.lua('queue.jobsByState', 0, 'throttled', 'queue'), [])
    self.lua('job.complete', 0, 'jid1', 'worker', 'queue', {})

    self.lua('queue.pop', 0, 'queue', 'worker', 1)
    self.assertEqual(self.lua('throttle.locks', 0, 'tid'), ['jid2'])
    self.assertEqual(self.lua('queue.jobsByState', 0, 'throttled', 'queue'), [])
    self.lua('job.complete', 0, 'jid2', 'worker', 'queue', {})

    self.lua('queue.pop', 0, 'queue', 'worker', 1)
    self.assertEqual(self.lua('throttle.locks', 0, 'tid'), ['jid3'])
    self.assertEqual(self.lua('queue.jobsByState', 0, 'throttled', 'queue'), [])
    self.lua('job.complete', 0, 'jid3', 'worker', 'queue', {})
    self.assertEqual(self.lua('throttle.locks', 0, 'tid'), [])
    self.assertEqual(self.lua('queue.jobsByState', 0, 'throttled', 'queue'), [])


class TestConcurrencyLevelChange(TestReqless):
  '''Test that changes to concurrency level are handled dynamically'''
  def test_increasing_concurrency_level_activates_pending_jobs(self):
    '''Activates pending jobs when concurrency level of throttle is increased'''
    self.lua('throttle.set', 0, 'tid', 1)
    self.lua('queue.put', 1, 'worker', 'queue', 'jid1', 'klass', {}, 0, 'throttles', ['tid'])
    self.lua('queue.put', 2, 'worker', 'queue', 'jid2', 'klass', {}, 0, 'throttles', ['tid'])
    self.lua('queue.put', 3, 'worker', 'queue', 'jid3', 'klass', {}, 0, 'throttles', ['tid'])
    self.lua('queue.pop', 4, 'queue', 'worker', 3)
    self.assertEqual(self.lua('throttle.locks', 5, 'tid'), ['jid1'])
    self.assertEqual(self.lua('queue.jobsByState', 6, 'throttled', 'queue'), ['jid2', 'jid3'])
    self.lua('throttle.set', 7, 'tid', 3)
    self.lua('job.complete', 8, 'jid1', 'worker', 'queue', {})
    self.assertEqual(self.lua('throttle.locks', 9, 'tid'), [])
    self.assertEqual(self.lua('throttle.pending', 10, 'tid'), [])
    self.assertEqual(self.lua('queue.jobsByState', 11, 'throttled', 'queue'), [])
    self.lua('queue.pop', 12, 'queue', 'worker', 2)
    self.assertEqual(self.lua('queue.jobsByState', 13, 'running', 'queue'), ['jid2', 'jid3'])

  def test_reducing_concurrency_level_without_pending(self):
    '''Operates at reduced concurrency level after current jobs finish'''
    self.lua('throttle.set', 0, 'tid', 3)
    self.lua('queue.put', 1, 'worker', 'queue', 'jid1', 'klass', {}, 0, 'throttles', ['tid'])
    self.lua('queue.put', 2, 'worker', 'queue', 'jid2', 'klass', {}, 0, 'throttles', ['tid'])
    self.lua('queue.put', 3, 'worker', 'queue', 'jid3', 'klass', {}, 0, 'throttles', ['tid'])
    self.lua('queue.put', 4, 'worker', 'queue', 'jid4', 'klass', {}, 0, 'throttles', ['tid'])
    self.lua('queue.put', 5, 'worker', 'queue', 'jid5', 'klass', {}, 0, 'throttles', ['tid'])
    self.lua('queue.pop', 6, 'queue', 'worker', 3)
    self.assertEqual(self.lua('throttle.locks', 7, 'tid'), ['jid1', 'jid2', 'jid3'])
    self.assertEqual(self.lua('queue.jobsByState', 8, 'throttled', 'queue'), [])
    self.lua('throttle.set', 9, 'tid', 1)
    self.lua('queue.pop', 10, 'queue', 'worker', 2)
    self.assertEqual(self.lua('throttle.locks', 11, 'tid'), ['jid1', 'jid2', 'jid3'])
    self.assertEqual(self.lua('throttle.pending', 12, 'tid'), ['jid4', 'jid5'])
    self.assertEqual(self.lua('queue.jobsByState', 13, 'throttled', 'queue'), ['jid4', 'jid5'])
    self.assertEqual(self.lua('queue.jobsByState', 14, 'running', 'queue'), ['jid1',
        'jid2', 'jid3'])
    self.lua('job.complete', 15, 'jid1', 'worker', 'queue', {})
    self.assertEqual(self.lua('throttle.locks', 16, 'tid'), ['jid2', 'jid3'])
    self.assertEqual(self.lua('throttle.pending', 17, 'tid'), ['jid4', 'jid5'])
    self.assertEqual(self.lua('queue.jobsByState', 18, 'throttled', 'queue'), ['jid4', 'jid5'])
    self.assertEqual(self.lua('queue.jobsByState', 19, 'running', 'queue'), ['jid2', 'jid3'])
    self.lua('job.complete', 20, 'jid2', 'worker', 'queue', {})
    self.assertEqual(self.lua('throttle.locks', 21, 'tid'), ['jid3'])
    self.assertEqual(self.lua('throttle.pending', 22, 'tid'), ['jid4', 'jid5'])
    self.assertEqual(self.lua('queue.jobsByState', 23, 'throttled', 'queue'), ['jid4', 'jid5'])
    self.lua('job.complete', 24, 'jid3', 'worker', 'queue', {})
    self.assertEqual(self.lua('throttle.locks', 25, 'tid'), [])
    self.assertEqual(self.lua('throttle.pending', 26, 'tid'), ['jid5'])
    self.assertEqual(self.lua('queue.jobsByState', 27, 'throttled', 'queue'), ['jid5'])
    self.assertEqual(self.lua('queue.jobsByState', 28, 'running', 'queue'), [])
    self.lua('queue.pop', 29, 'queue', 'worker', 1)
    self.assertEqual(self.lua('throttle.locks', 30, 'tid'), ['jid4'])
    self.assertEqual(self.lua('throttle.pending', 31, 'tid'), ['jid5'])
    self.assertEqual(self.lua('queue.jobsByState', 32, 'throttled', 'queue'), ['jid5'])
    self.assertEqual(self.lua('queue.jobsByState', 33, 'running', 'queue'), ['jid4'])
    self.lua('job.complete', 34, 'jid4', 'worker', 'queue', {})
    self.assertEqual(self.lua('throttle.locks', 35, 'tid'), [])
    self.assertEqual(self.lua('throttle.pending', 36, 'tid'), [])
    self.assertEqual(self.lua('queue.jobsByState', 37, 'throttled', 'queue'), [])
    self.assertEqual(self.lua('queue.jobsByState', 38, 'running', 'queue'), [])
    self.lua('queue.pop', 39, 'queue', 'worker', 2)
    self.assertEqual(self.lua('throttle.locks', 40, 'tid'), ['jid5'])
    self.assertEqual(self.lua('throttle.pending', 41, 'tid'), [])
    self.assertEqual(self.lua('queue.jobsByState', 42, 'throttled', 'queue'), [])
    self.assertEqual(self.lua('queue.jobsByState', 43, 'running', 'queue'), ['jid5'])

  def test_reducing_concurrency_level_with_pending(self):
    '''Operates at reduced concurrency level after current jobs finish'''
    self.lua('throttle.set', 0, 'tid', 3)
    self.lua('queue.put', 1, 'worker', 'queue', 'jid1', 'klass', {}, 0, 'throttles', ['tid'])
    self.lua('queue.put', 2, 'worker', 'queue', 'jid2', 'klass', {}, 0, 'throttles', ['tid'])
    self.lua('queue.put', 3, 'worker', 'queue', 'jid3', 'klass', {}, 0, 'throttles', ['tid'])
    self.lua('queue.put', 4, 'worker', 'queue', 'jid4', 'klass', {}, 0, 'throttles', ['tid'])
    self.lua('queue.put', 5, 'worker', 'queue', 'jid5', 'klass', {}, 0, 'throttles', ['tid'])

    self.lua('queue.pop', 6, 'queue', 'worker', 5)
    self.assertEqual(self.lua('throttle.locks', 7, 'tid'), ['jid1', 'jid2', 'jid3'])
    self.assertEqual(self.lua('queue.jobsByState', 8, 'throttled', 'queue'), ['jid4', 'jid5'])
    self.lua('throttle.set', 9, 'tid', 1)
    self.assertEqual(self.lua('throttle.locks', 10, 'tid'), ['jid1', 'jid2', 'jid3'])
    self.assertEqual(self.lua('queue.jobsByState', 11, 'throttled', 'queue'), ['jid4', 'jid5'])
    self.lua('job.complete', 12, 'jid1', 'worker', 'queue', {})
    self.assertEqual(self.lua('throttle.locks', 13, 'tid'), ['jid2', 'jid3'])
    self.assertEqual(self.lua('queue.jobsByState', 14, 'throttled', 'queue'), ['jid4', 'jid5'])
    self.lua('job.complete', 15, 'jid2', 'worker', 'queue', {})
    self.assertEqual(self.lua('throttle.locks', 16, 'tid'), ['jid3'])
    self.assertEqual(self.lua('queue.jobsByState', 17, 'throttled', 'queue'), ['jid4', 'jid5'])
    self.lua('job.complete', 18, 'jid3', 'worker', 'queue', {})
    self.lua('queue.pop', 19, 'queue', 'worker', 1)
    self.assertEqual(self.lua('throttle.locks', 20, 'tid'), ['jid4'])
    self.assertEqual(self.lua('queue.jobsByState', 21, 'throttled', 'queue'), ['jid5'])
    self.lua('job.complete', 23, 'jid4', 'worker', 'queue', {})
    self.lua('queue.pop', 22, 'queue', 'worker', 1)
    self.assertEqual(self.lua('throttle.locks', 24, 'tid'), ['jid5'])
    self.assertEqual(self.lua('queue.jobsByState', 25, 'throttled', 'queue'), [])
