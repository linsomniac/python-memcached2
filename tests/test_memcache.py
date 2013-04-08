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
from mctestsupp import RECEIVE, CommandServer
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

    def test_SetAndGetWithErrors(self):
        server = CommandServer([])
        memcache = memcached2.Memcache(('memcached://localhost:{0}/'
                .format(server.port),))
        memcache.set('foo', 'bar')

        server = CommandServer([RECEIVE])
        memcache = memcached2.Memcache(('memcached://localhost:{0}/'
                .format(server.port),))
        memcache.set('foo', 'bar')

        server = CommandServer([RECEIVE, 'STORED\r\n'])
        memcache = memcached2.Memcache(('memcached://localhost:{0}/'
                .format(server.port),))
        memcache.set('foo', 'bar')
        memcache.get('foo')

        server = CommandServer([RECEIVE, 'STORED\r\n', RECEIVE])
        memcache = memcached2.Memcache(('memcached://localhost:{0}/'
                .format(server.port),))
        memcache.set('foo', 'bar')
        memcache.get('foo')

        server = CommandServer([RECEIVE, 'STORED\r\n', RECEIVE,
                'VALUE foo 0 3\r\nbar\r\nEND\r\n'])
        memcache = memcached2.Memcache(('memcached://localhost:{0}/'
                .format(server.port),))
        memcache.set('foo', 'bar')
        memcache.get('foo')

unittest.main()
