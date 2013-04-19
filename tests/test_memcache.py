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

        with self.assertRaises(memcached2.NoValue):
            result = memcache.get('foo')

        memcache.set('foo', 'bar')
        result = memcache.get('foo')
        self.assertEqual(result, 'bar')
        self.assertEqual(result.key, 'foo')
        self.assertEqual(result.flags, 0)
        memcache.close()

    def test_SetAndGetWithErrors(self):
        server = CommandServer([])
        memcache = memcached2.Memcache(('memcached://localhost:{0}/'
                .format(server.port),))
        with self.assertRaises(memcached2.BackendDisconnect):
            memcache.set('foo', 'bar')

        server = CommandServer([RECEIVE])
        memcache = memcached2.Memcache(('memcached://localhost:{0}/'
                .format(server.port),))
        with self.assertRaises(memcached2.BackendDisconnect):
            memcache.set('foo', 'bar')

        server = CommandServer([RECEIVE, 'STORED\r\n'])
        memcache = memcached2.Memcache(('memcached://localhost:{0}/'
                .format(server.port),))
        memcache.set('foo', 'bar')
        with self.assertRaises(memcached2.BackendDisconnect):
            memcache.get('foo')

        server = CommandServer([RECEIVE, 'STORED\r\n', RECEIVE])
        memcache = memcached2.Memcache(('memcached://localhost:{0}/'
                .format(server.port),))
        memcache.set('foo', 'bar')
        with self.assertRaises(memcached2.BackendDisconnect):
            memcache.get('foo')

        server = CommandServer([RECEIVE, 'STORED\r\n', RECEIVE,
                'VALUE foo 0 3\r\nbar\r\nEND\r\n'])
        memcache = memcached2.Memcache(('memcached://localhost:{0}/'
                .format(server.port),))
        memcache.set('foo', 'bar')
        memcache.get('foo')

    def test_TestFlagsAndExptime(self):
        memcache = memcached2.Memcache(('memcached://localhost/',))

        memcache.set('foo', 'xXx', flags=12, exptime=1)
        result = memcache.get('foo')
        self.assertEqual(result, 'xXx')
        self.assertEqual(result.flags, 12)

        import time
        time.sleep(2)

        with self.assertRaises(memcached2.NoValue):
            result = memcache.get('foo')

        memcache.close()

    def test_StorageCommands(self):
        memcache = memcached2.Memcache(('memcached://localhost/',))
        memcache.set('foo', 'bar')
        result = memcache.get('foo')
        self.assertEqual(result, 'bar')

        with self.assertRaises(memcached2.NotStored):
            memcache.add('foo', '2')
        memcache.add('second_key', 'xyzzy')
        self.assertEqual(memcache.get('second_key'), 'xyzzy')

        memcache.replace('foo', 'rev2bar')
        self.assertEqual(memcache.get('foo'), 'rev2bar')
        with self.assertRaises(memcached2.NotStored):
            memcache.replace('unset_key', 'xyzzy')

        memcache.append('foo', '>>>')
        memcache.prepend('foo', '<<<')
        self.assertEqual(memcache.get('foo'), '<<<rev2bar>>>')
        with self.assertRaises(memcached2.NotStored):
            memcache.append('test_append', '>>>')
        with self.assertRaises(memcached2.NotStored):
            memcache.prepend('test_prepend', '<<<')

        memcache.close()

    def test_Cas(self):
        memcache = memcached2.Memcache(('memcached://localhost/',))
        memcache.set('foo', 'bar')
        result = memcache.get('foo', get_cas=True)

        memcache.set('foo', 'baz', cas_unique=result.cas_unique)

        result2 = memcache.get('foo', get_cas=True)
        self.assertEqual(result2, 'baz')

        with self.assertRaises(memcached2.CASFailure):
            memcache.set('foo', 'qux', cas_unique=result.cas_unique)
        self.assertEqual(memcache.get('foo', get_cas=True), 'baz')

    def test_Delete(self):
        memcache = memcached2.Memcache(('memcached://localhost/',))
        memcache.set('foo', 'bar')
        self.assertEqual(memcache.get('foo'), 'bar')
        memcache.delete('foo')
        with self.assertRaises(memcached2.NoValue):
            memcache.get('foo')
        with self.assertRaises(memcached2.NotFound):
            memcache.delete('foo')

    def test_FlushAll(self):
        memcache = memcached2.Memcache(('memcached://localhost/',))
        memcache.flush_all()

    def test_Touch(self):
        import time

        memcache = memcached2.Memcache(('memcached://localhost/',))
        memcache.set('foo', 'bar', exptime=1)
        self.assertEqual(memcache.get('foo'), 'bar')
        memcache.touch('foo', exptime=5)
        time.sleep(2)
        self.assertEqual(memcache.get('foo'), 'bar')
        memcache.touch('foo', exptime=1)
        time.sleep(2)
        with self.assertRaises(memcached2.NoValue):
            memcache.get('foo')
        with self.assertRaises(memcached2.NotFound):
            memcache.touch('foo', exptime=0)

    def test_IncrDecr(self):
        memcache = memcached2.Memcache(('memcached://localhost/',))
        with self.assertRaises(memcached2.NotFound):
            memcache.incr('foo', 1)
        memcache.set('foo', 'a')
        with self.assertRaises(memcached2.NonNumeric):
            memcache.incr('foo', 1)
        memcache.set('foo', '1')
        self.assertEqual(memcache.incr('foo', 1), 2)
        self.assertEqual(memcache.get('foo'), '2')
        self.assertEqual(memcache.decr('foo', 1), 1)
        self.assertEqual(memcache.get('foo'), '1')

        with self.assertRaises(memcached2.NotFound):
            memcache.decr('baz', 1)
        memcache.set('baz', 'a')
        with self.assertRaises(memcached2.NonNumeric):
            memcache.decr('baz', 1)

    def test_Stats(self):
        memcache = memcached2.Memcache(('memcached://localhost/',))
        memcache.set('foo', 'a')
        memcache.stats()
        memcache.stats_settings()
        memcache.stats_items()
        self.assertEqual(memcache.stats_sizes(), [(64, 1)])
        memcache.stats_slabs()

    def test_SeveralServers(self):
        memcache = memcached2.Memcache(('memcached://localhost/',
                'memcached://localhost/', 'memcached://localhost/',
                'memcached://localhost/',))

        memcache.flush_all()

        import pprint
        pprint.pprint(memcache.stats())

        self.assertEqual(len(memcache.stats()), 4)
        memcache.stats_settings()
        memcache.stats_items()
        memcache.stats_sizes()
        memcache.stats_sizes()
        for i in range(100):
            memcache.set('foo{0}'.format(i), 'bar')

unittest.main()
