'''Tests for queue patterns APIs'''

import json

from test.common import TestReqless


IDENTIFIER_PATTERNS_KEYS = ['ql:qp:identifiers', 'qmore:dynamic']

PRIORITY_PATTERNS_KEYS = ['ql:qp:priorities', 'qmore:priority']

DEFAULT_IDENTIFER_PATTERNS = {'default': '["*"]'}

DEFAULT_PRIORITY_PATTERNS = []

EXAMPLE_IDENTIFIER_PATTERNS = {
    'french': ['un', 'deux', 'trois', 'quatre'],
    'spanish': ['uno', 'dos', 'tres'],
}

EXAMPLE_PRIORITY_PATTERNS = [
    {"fairly": False, "pattern": ['a','b','*','c']},
    {"fairly": True, "pattern": ['*', 'd', 'e', 'f', 'and', 'so', 'on']},
]


class TestQueuePatterns(TestReqless):
    '''Test queue patterns functionality'''

    def test_get_all_identifier_patterns_defaults(self):
        '''Should be able to get default identifier patterns'''
        self.assertEqual(self.lua(
            'queueIdentifierPatterns.getAll', 0), DEFAULT_IDENTIFER_PATTERNS
        )


    def test_get_all_identifier_patterns_from_either_expected_key(self):
        '''Should be able to get identifier patterns from primary or legacy key'''
        expected_patterns = EXAMPLE_IDENTIFIER_PATTERNS
        serialized_patterns = {
            key: json.dumps(value) for key, value in expected_patterns.items()
        }
        for identifier_patterns_key in IDENTIFIER_PATTERNS_KEYS:
            for delete_key in IDENTIFIER_PATTERNS_KEYS:
                self.redis.delete(delete_key)

            self.redis.hset(identifier_patterns_key, mapping=serialized_patterns)
            self.assertEqual(
                self.lua('queueIdentifierPatterns.getAll', 0),
                serialized_patterns | DEFAULT_IDENTIFER_PATTERNS,
            )


    def test_get_all_identifier_patterns_returns_custom_default_if_set(self):
        '''Should return custom defaults if they've been set.'''
        expected_patterns = {'default': EXAMPLE_IDENTIFIER_PATTERNS["french"]}
        serialized_patterns = {
            key: json.dumps(value) for key, value in expected_patterns.items()
        }
        for identifier_patterns_key in IDENTIFIER_PATTERNS_KEYS:
            for delete_key in IDENTIFIER_PATTERNS_KEYS:
                self.redis.delete(delete_key)

            self.redis.hset(identifier_patterns_key, mapping=serialized_patterns)
            self.assertEqual(
                self.lua('queueIdentifierPatterns.getAll', 0),
                serialized_patterns,
            )


    def test_set_all_identifier_patterns_to_clear_values(self):
        '''Should be able to reset to default identifier patterns'''
        initial_patterns = EXAMPLE_IDENTIFIER_PATTERNS
        serialized_patterns = {
            key: json.dumps(value) for key, value in initial_patterns.items()
        }
        for identifier_patterns_key in IDENTIFIER_PATTERNS_KEYS:
            for delete_key in IDENTIFIER_PATTERNS_KEYS:
                self.redis.delete(delete_key)
            self.redis.hset(identifier_patterns_key, mapping=serialized_patterns)
            self.assertEqual(
                self.lua('queueIdentifierPatterns.getAll', 0),
                serialized_patterns | DEFAULT_IDENTIFER_PATTERNS,
            )
            self.lua('queueIdentifierPatterns.setAll', 1)
            self.assertEqual(self.lua('queueIdentifierPatterns.getAll', 2), DEFAULT_IDENTIFER_PATTERNS)


    def test_set_all_identifier_patterns_can_set_default_pattern(self):
        '''Should be able to set default identifier pattern'''
        expected_patterns = {'default': EXAMPLE_IDENTIFIER_PATTERNS["french"]}
        serialized_patterns = {
            key: json.dumps(value) for key, value in expected_patterns.items()
        }
        for delete_key in IDENTIFIER_PATTERNS_KEYS:
            self.redis.delete(delete_key)
        self.lua(
            'queueIdentifierPatterns.setAll',
            1,
            "default",
            expected_patterns["default"],
        )
        self.assertEqual(self.lua('queueIdentifierPatterns.getAll', 2), serialized_patterns)


    def test_set_all_identifier_patterns_sets_a_good_default_if_bad_default_given(self):
        '''Should set a good default value when a bad default is given'''
        self.lua(
            'queueIdentifierPatterns.setAll',
            1,
            "default",
            "[]",
        )
        self.assertEqual(
            self.lua('queueIdentifierPatterns.getAll', 2),
            DEFAULT_IDENTIFER_PATTERNS,
        )


    def test_set_all_identifier_patterns_ignores_empty_patterns(self):
        '''Should ignore empty patterns when setting identifiers'''
        self.lua(
            'queueIdentifierPatterns.setAll',
            1,
            "junk",
            "[]",
        )
        self.assertEqual(
            self.lua('queueIdentifierPatterns.getAll', 2),
            DEFAULT_IDENTIFER_PATTERNS,
        )


    def test_set_all_identifier_patterns_can_set_multiple_patterns(self):
        '''Should be able to set multiple patterns'''
        expected_patterns = EXAMPLE_IDENTIFIER_PATTERNS
        serialized_patterns = {
            key: json.dumps(value) for key, value in expected_patterns.items()
        }
        for identifier_patterns_key in IDENTIFIER_PATTERNS_KEYS:
            for delete_key in IDENTIFIER_PATTERNS_KEYS:
                self.redis.delete(delete_key)
            args = [item for pair in expected_patterns.items() for item in pair]
            args.extend(["junk", "[]"])
            self.lua('queueIdentifierPatterns.setAll', 1, *args)
            self.assertEqual(self.lua(
                'queueIdentifierPatterns.getAll', 2),
                serialized_patterns | DEFAULT_IDENTIFER_PATTERNS
            )


    def test_get_all_priority_patterns_defaults(self):
        '''Should be able to get default priority patterns'''
        for priority_patterns_key in PRIORITY_PATTERNS_KEYS:
            self.redis.delete(priority_patterns_key)
        self.assertEqual(
            self.lua('queuePriorityPatterns.getAll', 0),
            DEFAULT_PRIORITY_PATTERNS,
        )


    def test_get_all_priority_patterns_from_either_expected_key(self):
        '''Should be able to get priority patterns from primary or legacy key'''
        expected_patterns = EXAMPLE_PRIORITY_PATTERNS
        serialized_patterns = [json.dumps(value) for value in expected_patterns]
        for priority_patterns_key in PRIORITY_PATTERNS_KEYS:
            for delete_key in PRIORITY_PATTERNS_KEYS:
                self.redis.delete(delete_key)

            self.redis.rpush(priority_patterns_key, *serialized_patterns)
            self.assertEqual(
                self.lua('queuePriorityPatterns.getAll', 0),
                serialized_patterns,
            )


    def test_set_all_priority_patterns_can_clear_values(self):
        '''Should be able to reset to default priority patterns'''
        initial_patterns = EXAMPLE_PRIORITY_PATTERNS
        serialized_patterns = [json.dumps(value) for value in initial_patterns]
        for priority_patterns_key in PRIORITY_PATTERNS_KEYS:
            for delete_key in PRIORITY_PATTERNS_KEYS:
                self.redis.delete(delete_key)
            self.redis.rpush(priority_patterns_key, *serialized_patterns)
            self.assertEqual(
                self.lua('queuePriorityPatterns.getAll', 0),
                serialized_patterns,
            )
            self.lua('queuePriorityPatterns.setAll', 1)
            self.assertEqual(
                self.lua('queuePriorityPatterns.getAll', 2),
                DEFAULT_PRIORITY_PATTERNS,
            )


    def test_set_all_priority_patterns_can_set_multiple_patterns(self):
        '''Should be able to set multiple patterns'''
        expected_patterns = EXAMPLE_PRIORITY_PATTERNS
        serialized_patterns = [json.dumps(value) for value in expected_patterns]
        for delete_key in PRIORITY_PATTERNS_KEYS:
            self.redis.delete(delete_key)
        self.lua('queuePriorityPatterns.setAll', 1, *serialized_patterns)
        self.assertEqual(self.lua(
            'queuePriorityPatterns.getAll', 2),
            serialized_patterns
        )
