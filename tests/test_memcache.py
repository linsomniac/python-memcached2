#!/usr/bin/env python
#
#  Test the Memcache component of the Python memcached2 module.
#
#===============
#  This is based on a skeleton test file, more information at:
#
#     https://github.com/linsomniac/python-unittest-skeleton

import unittest

import sys
sys.path.insert(0, '..')
import mctestsupp
import memcached2


class test_ServerConnection(unittest.TestCase):
    def setUp(self):
        mctestsupp.flush_local_memcache(self)

    def test_SetAndGet(self):
        memcache = memcached2.Memcache(('memcached://localhost/',))
        memcache.set('foo', 'bar')
        result = memcache.get('foo')
        self.assertEqual(result, b'bar')
        self.assertEqual(result.key, b'foo')
        self.assertEqual(result.flags, 0)
        memcache.close()

unittest.main()