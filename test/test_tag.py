'''Test our tagging functionality'''

from common import TestQless


class TestTag(TestQless):
    '''Test our tagging functionality'''
    #
    # QlessAPI.tag = function(now, command, ...)
    #     return cjson.encode(Qless.tag(now, command, unpack(arg)))
    # end
    def test_malformed(self):
        '''Enumerate all the ways it could be malformed'''
        self.assertMalformed(self.lua, [
            ('tag', 0),
            ('tag', 0, 'add'),
            ('tag', 0, 'remove'),
            ('tag', 0, 'get'),
            ('tag', 0, 'get', 'foo', 'bar'),
            ('tag', 0, 'get', 'foo', 0, 'bar'),
            ('tag', 0, 'top', 'bar'),
            ('tag', 0, 'top', 0, 'bar'),
            ('tag', 0, 'foo')
        ])

    def test_add(self):
        '''Add a tag'''
        self.lua('queue.put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('tag', 0, 'add', 'jid', 'foo')
        self.assertEqual(self.lua('job.get', 0, 'jid')['tags'], ['foo'])

    def test_remove(self):
        '''Remove a tag'''
        self.lua('queue.put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0, 'tags', ['foo'])
        self.lua('tag', 0, 'remove', 'jid', 'foo')
        self.assertEqual(self.lua('job.get', 0, 'jid')['tags'], {})

    def test_add_existing(self):
        '''We shouldn't double-add tags that already exist for the job'''
        self.lua('queue.put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0, 'tags', ['foo'])
        self.lua('tag', 0, 'add', 'jid', 'foo')
        self.assertEqual(self.lua('job.get', 0, 'jid')['tags'], ['foo'])

    def test_remove_nonexistent(self):
        '''Removing a nonexistent tag from a job is ok'''
        self.lua('queue.put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0, 'tags', ['foo'])
        self.lua('tag', 0, 'remove', 'jid', 'bar')
        self.assertEqual(self.lua('job.get', 0, 'jid')['tags'], ['foo'])

    def test_add_multiple(self):
        '''Adding the same tag twice at the same time yields no duplicates'''
        self.lua('queue.put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('tag', 0, 'add', 'jid', 'foo', 'foo', 'foo')
        self.assertEqual(self.lua('job.get', 0, 'jid')['tags'], ['foo'])

    def test_get(self):
        '''Should be able to get jobs taggs with a particular tag'''
        self.lua('queue.put', 0, 'worker', 'queue', 'foo', 'klass', {}, 0,
            'tags', ['foo', 'both'])
        self.lua('queue.put', 0, 'worker', 'queue', 'bar', 'klass', {}, 0,
            'tags', ['bar', 'both'])
        self.assertEqual(
            self.lua('tag', 0, 'get', 'foo', 0, 10)['jobs'], ['foo'])
        self.assertEqual(
            self.lua('tag', 0, 'get', 'bar', 0, 10)['jobs'], ['bar'])
        self.assertEqual(
            self.lua('tag', 0, 'get', 'both', 0, 10)['jobs'], ['bar', 'foo'])

    def test_get_add(self):
        '''When adding a tag, it should be available for searching'''
        self.lua('queue.put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.assertEqual(
            self.lua('tag', 0, 'get', 'foo', 0, 10)['jobs'], {})
        self.lua('tag', 0, 'add', 'jid', 'foo')
        self.assertEqual(
            self.lua('tag', 0, 'get', 'foo', 0, 10)['jobs'], ['jid'])

    def test_order(self):
        '''It should preserve the order of the tags'''
        self.lua('queue.put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        tags = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h']
        for tag in tags:
            self.lua('tag', 0, 'add', 'jid', tag)
            found = self.lua('job.get', 0, 'jid')['tags']
            self.assertEqual(found, sorted(found))
        # And now remove them one at a time
        import random
        for tag in random.sample(tags, len(tags)):
            self.lua('tag', 0, 'remove', 'jid', tag)
            found = self.lua('job.get', 0, 'jid')['tags']
            self.assertEqual(list(found), sorted(found))

    def test_cancel(self):
        '''When a job is canceled, it's not found in tags'''
        self.lua('queue.put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0, 'tags', ['foo'])
        self.assertEqual(
            self.lua('tag', 0, 'get', 'foo', 0, 10)['jobs'], ['jid'])
        self.lua('job.cancel', 0, 'jid')
        self.assertEqual(
            self.lua('tag', 0, 'get', 'foo', 0, 10)['jobs'], {})

    def test_expired_jobs(self):
        '''When a job expires, it's removed from its tags'''
        self.lua('config.set', 0, 'jobs-history', 100)
        self.lua('queue.put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0, 'tags', ['foo'])
        self.lua('queue.pop', 0, 'queue', 'worker', 10)
        self.lua('job.complete', 0, 'jid', 'worker', 'queue', {})
        self.assertEqual(
            self.lua('tag', 99, 'get', 'foo', 0, 10)['jobs'], ['jid'])
        # We now need another job to complete to expire this job
        self.lua('queue.put', 101, 'worker', 'queue', 'foo', 'klass', {}, 0)
        self.lua('queue.pop', 101, 'queue', 'worker', 10)
        self.lua('job.complete', 101, 'foo', 'worker', 'queue', {})
        self.assertEqual(
            self.lua('tag', 101, 'get', 'foo', 0, 10)['jobs'], {})

    def test_expired_count_jobs(self):
        '''When a job expires from jobs-history-count, remove from its tags'''
        self.lua('config.set', 0, 'jobs-history-count', 1)
        self.lua('queue.put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0, 'tags', ['foo'])
        self.lua('queue.pop', 0, 'queue', 'worker', 10)
        self.lua('job.complete', 0, 'jid', 'worker', 'queue', {})
        self.assertEqual(
            self.lua('tag', 0, 'get', 'foo', 0, 10)['jobs'], ['jid'])
        # We now need another job to complete to expire this job
        self.lua('queue.put', 1, 'worker', 'queue', 'foo', 'klass', {}, 0)
        self.lua('queue.pop', 1, 'queue', 'worker', 10)
        self.lua('job.complete', 1, 'foo', 'worker', 'queue', {})
        self.assertEqual(
            self.lua('tag', 1, 'get', 'foo', 0, 10)['jobs'], {})

    def test_top(self):
        '''Ensure that we can find the most common tags'''
        for tag in range(10):
            self.lua('queue.put', 0, 'worker', 'queue', tag, 'klass', {}, 0,
                'tags', list(range(tag, 10)))
        self.assertEqual(self.lua('tag', 0, 'top', 0, 20),
            list(map(str, reversed(range(1, 10)))))

    def test_recurring(self):
        '''Ensure that jobs spawned from recurring jobs are tagged'''
        self.lua('recur', 0, 'queue', 'jid', 'klass', {},
            'interval', 60, 0, 'tags', ['foo'])
        self.lua('queue.pop', 0, 'queue', 'worker', 10)
        self.assertEqual(
            self.lua('tag', 0, 'get', 'foo', 0, 10)['jobs'], ['jid-1'])

    def test_pagination_get(self):
        '''Pagination should work for tag.get'''
        jids = list(map(str, range(100)))
        for jid in jids:
            self.lua('queue.put', jid, 'worker', 'queue', jid, 'klass', {}, 0, 'tags', ['foo'])
        # Get two pages and ensure they're what we expect
        self.assertEqual(
            list(map(int, self.lua('tag', 100, 'get', 'foo',  0, 50)['jobs'])),
            list(map(int, jids[:50])))
        self.assertEqual(
            list(map(int, self.lua('tag', 100, 'get', 'foo', 50, 50)['jobs'])),
            list(map(int, jids[50:])))

    def test_pagination_top(self):
        '''Pagination should work for tag.top'''
        jids = list(map(str, range(10)))
        for jid in jids:
            for suffix in map(str, range(int(jid) + 5)):
                self.lua('queue.put', jid, 'worker', 'queue',
                    jid + '.' + suffix, 'klass', {}, 0, 'tags', [jid])
        # Get two pages and ensure they're what we expect
        jids = list(reversed(jids))
        self.assertEqual(
            self.lua('tag', 100, 'top', 0, 5), jids[:5])
        self.assertEqual(
            self.lua('tag', 100, 'top', 5, 5), jids[5:])
