'''Tests for our configuration'''

from common import TestReqless


class TestConfig(TestReqless):
    '''Test configuration functionality'''
    def test_get_without_key_still_works(self):
        '''Deprecated config.get retrieval of all configs still works'''
        self.assertEqual(self.lua('config.get', 0), {
            'application': 'reqless',
            'grace-period': 10,
            'heartbeat': 60,
            'jobs-history': 604800,
            'jobs-history-count': 50000,
            'max-job-history': 100,
            'max-pop-retry': 1,
            'max-worker-age': 86400,
        })

    def test_get_all(self):
        '''Should be able to access all configurations'''
        self.assertEqual(self.lua('config.getAll', 0), {
            'application': 'reqless',
            'grace-period': 10,
            'heartbeat': 60,
            'jobs-history': 604800,
            'jobs-history-count': 50000,
            'max-job-history': 100,
            'max-pop-retry': 1,
            'max-worker-age': 86400,
        })

    def test_get(self):
        '''Should be able to get each key individually'''
        for key, value in self.lua('config.get', 0).items():
            retrievedValue = self.lua('config.get', 0, key)
            if isinstance(retrievedValue, bytes):
                retrievedValue = retrievedValue.decode("utf-8")
            self.assertEqual(retrievedValue, value)

    def test_set_get(self):
        '''If we update a configuration setting, we can get it back'''
        self.lua('config.set', 0, 'foo', 'bar')
        self.assertEqual(self.lua('config.get', 0, 'foo').decode("UTF-8"), 'bar')

    def test_unset_default(self):
        '''If we override a default and then unset it, it should return'''
        default = self.lua('config.get', 0, 'heartbeat')
        self.lua('config.set', 0, 'heartbeat', 100)
        self.assertEqual(self.lua('config.get', 0, 'heartbeat'), 100)
        self.lua('config.unset', 0, 'heartbeat')
        self.assertEqual(self.lua('config.get', 0, 'heartbeat'), default)

    def test_unset(self):
        '''If we set and then unset a setting, it should return to None'''
        self.assertEqual(self.lua('config.get', 0, 'foo'), None)
        self.lua('config.set', 0, 'foo', 5)
        self.assertEqual(self.lua('config.get', 0, 'foo'), 5)
        self.lua('config.unset', 0, 'foo')
        self.assertEqual(self.lua('config.get', 0, 'foo'), None)
