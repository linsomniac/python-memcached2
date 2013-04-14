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
        self.assertEqual(result, b'bar')
        self.assertEqual(result.key, b'foo')
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
        self.assertEqual(result, b'xXx')
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
        self.assertEqual(result, b'bar')

        with self.assertRaises(memcached2.NotStored):
            memcache.add('foo', '2')
        memcache.add('second_key', 'xyzzy')
        self.assertEqual(memcache.get('second_key'), b'xyzzy')

        memcache.replace('foo', 'rev2bar')
        self.assertEqual(memcache.get('foo'), b'rev2bar')
        with self.assertRaises(memcached2.NotStored):
            memcache.replace('unset_key', 'xyzzy')

        memcache.append('foo', '>>>')
        memcache.prepend('foo', '<<<')
        self.assertEqual(memcache.get('foo'), b'<<<rev2bar>>>')
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
        self.assertEqual(result2, b'baz')

        with self.assertRaises(memcached2.CASFailure):
            memcache.set('foo', 'qux', cas_unique=result.cas_unique)
        self.assertEqual(memcache.get('foo', get_cas=True), b'baz')

    def test_Delete(self):
        memcache = memcached2.Memcache(('memcached://localhost/',))
        memcache.set('foo', 'bar')
        self.assertEqual(memcache.get('foo'), b'bar')
        memcache.delete('foo')
        with self.assertRaises(memcached2.NoValue):
            memcache.get('foo')
        with self.assertRaises(memcached2.NotFound):
            memcache.delete('foo')

unittest.main()
