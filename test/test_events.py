'''A large number of operations generate events. Let's test'''

from common import TestQless


class TestEvents(TestQless):
    '''Check for all the events we expect'''
    def test_track(self):
        '''We should hear chatter about tracking and untracking jobs'''
        self.lua('queue.put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        with self.lua:
            self.lua('job.track', 0, 'jid')
        self.assertEqual(self.lua.log, [{
            'channel': b'ql:track',
            'data': b'jid'
        }])

        with self.lua:
            self.lua('job.untrack', 0, 'jid')
        self.assertEqual(self.lua.log, [{
            'channel': b'ql:untrack',
            'data': b'jid'
        }])

    def test_track_canceled(self):
        '''Canceling a tracked job should spawn some data'''
        self.lua('queue.put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('job.track', 0, 'jid')
        with self.lua:
            self.lua('job.cancel', 0, 'jid')
        self.assertEqual(self.lua.log, [{
            'channel': b'ql:log',
            'data':
                b'{"jid":"jid","queue":"queue","event":"canceled","worker":""}'
        }, {
            'channel': b'ql:canceled',
            'data': b'jid'
        }])

    def test_track_completed(self):
        '''Tracked jobs get extra notifications when they complete'''
        self.lua('queue.put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('job.track', 0, 'jid')
        self.lua('queue.pop', 0, 'queue', 'worker', 10)
        with self.lua:
            self.lua('job.complete', 0, 'jid', 'worker', 'queue', {})
        self.assertEqual(self.lua.log, [{
            'channel': b'ql:completed',
            'data': b'jid'
        }, {
            'channel': b'ql:log',
            'data': b'{"jid":"jid","event":"completed","queue":"queue"}'
        }])

    def test_track_fail(self):
        '''We should hear chatter when failing a job'''
        self.lua('queue.put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('job.track', 0, 'jid')
        self.lua('queue.pop', 0, 'queue', 'worker', 10)
        with self.lua:
            self.lua('job.fail', 0, 'jid', 'worker', 'grp', 'mess', {})
        self.assertEqual(self.lua.log, [{
            'channel': b'ql:log',
            'data':
                b'{"message":"mess","jid":"jid","group":"grp","event":"failed","worker":"worker"}'
        }, {
            'channel': b'ql:failed',
            'data': b'jid'
        }])

    def test_track_popped(self):
        '''We should hear chatter when popping a tracked job'''
        self.lua('queue.put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('job.track', 0, 'jid')
        with self.lua:
            self.lua('queue.pop', 0, 'queue', 'worker', 10)
        self.assertEqual(self.lua.log, [{
            'channel': b'ql:popped',
            'data': b'jid'
        }])

    def test_track_put(self):
        '''We should hear chatter when putting a tracked job'''
        self.lua('queue.put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('job.track', 0, 'jid')
        with self.lua:
            self.lua('queue.put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.assertEqual(self.lua.log, [{
            'channel': b'ql:log',
            'data': b'{"jid":"jid","event":"put","queue":"queue"}'
        }, {
            'channel': b'ql:put',
            'data': b'jid'
        }])

    def test_track_stalled(self):
        '''We should hear chatter when a job stalls'''
        self.lua('queue.put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('job.track', 0, 'jid')
        job = self.lua('queue.pop', 0, 'queue', 'worker', 10)[0]
        with self.lua:
            self.lua('queue.pop', job['expires'] + 10, 'queue', 'worker', 10)
        self.assertEqual(self.lua.log, [{
            'channel': b'ql:stalled',
            'data': b'jid'
        }, {
            'channel': b'ql:w:worker',
            'data': b'{"jid":"jid","event":"lock_lost","worker":"worker"}'
        }, {
            'channel': b'ql:log',
            'data': b'{"jid":"jid","event":"lock_lost","worker":"worker"}'
        }])

    def test_failed_retries(self):
        '''We should hear chatter when a job fails from retries'''
        self.lua('config.set', 0, 'grace-period', 0)
        self.lua('queue.put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0, 'retries', 0)
        job = self.lua('queue.pop', 0, 'queue', 'worker', 10)[0]
        with self.lua:
            self.assertEqual(self.lua(
                'queue.pop', job['expires'] + 10, 'queue', 'worker', 10), {})
        self.assertEqual(self.lua('job.get', 0, 'jid')['state'], 'failed')
        self.assertEqual(self.lua.log, [{
            'channel': b'ql:w:worker',
            'data': b'{"jid":"jid","event":"lock_lost","worker":"worker"}'
        }, {
            'channel': b'ql:log',
            'data': b'{"jid":"jid","event":"lock_lost","worker":"worker"}'
        }, {
            'channel': b'ql:log',
            'data': b'{"message":"Job exhausted retries in queue \\"queue\\"","jid":"jid","group":"failed-retries-queue","event":"failed","worker":"worker"}'
        }])

    def test_advance(self):
        '''We should hear chatter when completing and advancing a job'''
        self.lua('queue.put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('queue.pop', 0, 'queue', 'worker', 10)
        with self.lua:
            self.lua(
                'job.complete', 0, 'jid', 'worker', 'queue', {}, 'next', 'queue')
        self.assertEqual(self.lua.log, [{
            'channel': b'ql:log',
            'data':
                b'{"jid":"jid","to":"queue","event":"advanced","queue":"queue"}'
        }])

    def test_timeout(self):
        '''We should hear chatter when a job times out'''
        self.lua('queue.put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('queue.pop', 0, 'queue', 'worker', 10)
        with self.lua:
            self.lua('job.timeout', 0, 'jid')
        self.assertEqual(self.lua.log, [{
            'channel': b'ql:w:worker',
            'data': b'{"jid":"jid","event":"lock_lost","worker":"worker"}'
        }, {
            'channel': b'ql:log',
            'data': b'{"jid":"jid","event":"lock_lost","worker":"worker"}'
        }])

    def test_put(self):
        '''We should hear chatter when a job is put into a queueu'''
        with self.lua:
            self.lua('queue.put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.assertEqual(self.lua.log, [{
            'channel': b'ql:log',
            'data': b'{"jid":"jid","event":"put","queue":"queue"}'
        }])

    def test_reput(self):
        '''When we put a popped job into a queue, it informs the worker'''
        self.lua('queue.put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('queue.pop', 0, 'queue', 'worker', 10)
        with self.lua:
            self.lua('queue.put', 0, 'another', 'another', 'jid', 'klass', {}, 10)
        self.assertEqual(self.lua.log, [{
            'channel': b'ql:log',
            'data': b'{"jid":"jid","event":"put","queue":"another"}'
        }, {
            'channel': b'ql:w:worker',
            'data': b'{"jid":"jid","event":"lock_lost","worker":"worker"}'
        }, {
            'channel': b'ql:log',
            'data': b'{"jid":"jid","event":"lock_lost","worker":"worker"}'
        }])

    def test_config_set(self):
        '''We should hear chatter about setting configurations'''
        with self.lua:
            self.lua('config.set', 0, 'foo', 'bar')
        self.assertEqual(self.lua.log, [{
            'channel': b'ql:log',
            'data': b'{"option":"foo","event":"config_set","value":"bar"}'
        }])

    def test_config_unset(self):
        '''We should hear chatter about unsetting configurations'''
        self.lua('config.set', 0, 'foo', 'bar')
        with self.lua:
            self.lua('config.unset', 0, 'foo')
        self.assertEqual(self.lua.log, [{
            'channel': b'ql:log',
            'data': b'{"event":"config_unset","option":"foo"}'
        }])

    def test_cancel_waiting(self):
        '''We should hear chatter about canceling waiting jobs'''
        self.lua('queue.put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        with self.lua:
            self.lua('job.cancel', 0, 'jid')
        self.assertEqual(self.lua.log, [{
            'channel': b'ql:log',
            'data':
                b'{"jid":"jid","queue":"queue","event":"canceled","worker":""}'
        }])

    def test_cancel_running(self):
        '''We should hear chatter about canceling running jobs'''
        self.lua('queue.put', 0, 'worker', 'q', 'jid', 'klass', {}, 0)
        self.lua('queue.pop', 0, 'q', 'wrk', 10)
        with self.lua:
            self.lua('job.cancel', 0, 'jid')
        self.assertEqual(self.lua.log, [{
            'channel': b'ql:log',
            'data':
                b'{"jid":"jid","queue":"q","event":"canceled","worker":"wrk"}'
        }, {
            'channel': b'ql:w:wrk',
            'data':
                b'{"jid":"jid","queue":"q","event":"canceled","worker":"wrk"}'
        }])

    def test_cancel_depends(self):
        '''We should hear chatter about canceling dependent jobs'''
        self.lua('queue.put', 0, 'worker', 'queue', 'a', 'klass', {}, 0)
        self.lua('queue.put', 0, 'worker', 'queue', 'b', 'klass', {}, 0, 'depends', ['a'])
        with self.lua:
            self.lua('job.cancel', 0, 'b')
        self.assertEqual(self.lua.log, [{
            'channel': b'ql:log',
            'data':
                b'{"jid":"b","queue":"queue","event":"canceled","worker":""}'
        }])

    def test_cancel_scheduled(self):
        '''We should hear chatter about canceling scheduled jobs'''
        self.lua('queue.put', 0, 'worker', 'queue', 'jid', 'klass', {}, 10)
        with self.lua:
            self.lua('job.cancel', 0, 'jid')
        self.assertEqual(self.lua.log, [{
            'channel': b'ql:log',
            'data':
                b'{"jid":"jid","queue":"queue","event":"canceled","worker":""}'
        }])

    def test_cancel_failed(self):
        '''We should hear chatter about canceling failed jobs'''
        self.lua('queue.put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('queue.pop', 0, 'queue', 'worker', 10)
        self.lua('job.fail', 0, 'jid', 'worker', 'group', 'message', {})
        with self.lua:
            self.lua('job.cancel', 0, 'jid')
        self.assertEqual(self.lua.log, [{
            'channel': b'ql:log',
            'data':
                b'{"jid":"jid","queue":"queue","event":"canceled","worker":""}'
        }])

    def test_move_lock(self):
        '''We should /not/ get lock lost events for moving a job we own'''
        self.lua('queue.put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('queue.pop', 0, 'queue', 'worker', 10)
        with self.lua:
            # Put the job under the same worker who owns it now
            self.lua('queue.put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.assertEqual(self.lua.log, [{
            'channel': b'ql:log',
            'data': b'{"jid":"jid","event":"put","queue":"queue"}'
        }])
