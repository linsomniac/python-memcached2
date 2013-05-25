#!/usr/bin/env python
#
#  Test the Memcache component of the Python memcached2 module.
#
#===============
#  This is based on a skeleton test file, more information at:
#
#     https://github.com/linsomniac/python-unittest-skeleton
#
# Copyright 2013 Sean Reifschneider, tummy.com, ltd.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import unittest

import sys
sys.path.insert(0, '..')
import mctestsupp
from faketcpserver import RECEIVE, CommandServer
import memcached2
import time


class test_ServerConnection(unittest.TestCase):
    def setUp(self):
        mctestsupp.flush_local_memcache(self)

    def test_SetAndGet(self):
        memcache = memcached2.Memcache(
                ('memcached://localhost/',),
                value_wrapper=memcached2.ValueSuperStr)

        with self.assertRaises(memcached2.NoValue):
            result = memcache.get('foo')

        memcache.set('foo', 'bar')
        result = memcache.get('foo')
        self.assertEqual(result, 'bar')
        self.assertEqual(result.key, 'foo')
        self.assertEqual(result.flags, 0)
        memcache.close()

    def test_ValueSuperStr(self):
        memcache = memcached2.Memcache(
                ('memcached://localhost/',),
                value_wrapper=memcached2.ValueSuperStr)

        memcache.set('foo', 'bar')
        result = memcache.get('foo')
        self.assertEqual(result, 'bar')
        result.set('testing')
        self.assertEqual(memcache.get('foo'), 'testing')

        result = memcache.get('foo')
        result.append('>>>')
        result.prepend('<<<')
        self.assertEqual(memcache.get('foo'), '<<<testing>>>')
        result.delete()
        with self.assertRaises(memcached2.NoValue):
            memcache.get('foo')

        memcache.set('foo', '1')
        result = memcache.get('foo')
        self.assertEqual(result, '1')
        result.incr()
        self.assertEqual(memcache.get('foo'), '2')
        result.incr(5)
        self.assertEqual(memcache.get('foo'), '7')
        result.decr()
        self.assertEqual(memcache.get('foo'), '6')
        result.decr(3)
        self.assertEqual(memcache.get('foo'), '3')

        memcache.set('foo', '1', exptime=1)
        result = memcache.get('foo')
        result.touch(10)
        time.sleep(2)
        self.assertEqual(memcache.get('foo'), '1')
        result.touch(1)
        time.sleep(2)
        with self.assertRaises(memcached2.NoValue):
            memcache.get('foo')

        memcache.close()

    def test_ValueSuperStrCAS(self):
        memcache = memcached2.Memcache(
                ('memcached://localhost/',),
                value_wrapper=memcached2.ValueSuperStr)

        memcache.set('foo', 'testing')
        result = memcache.get('foo', get_cas=True)
        self.assertEqual(result, 'testing')
        result.set('test2')
        self.assertEqual(memcache.get('foo'), 'test2')
        with self.assertRaises(memcached2.CASFailure):
            result.set('test3')
        self.assertEqual(memcache.get('foo'), 'test2')

        memcache.set('foo', 'testing')
        result = memcache.get('foo', get_cas=True)
        self.assertEqual(result, 'testing')
        result.set('test2', update_cas=True)
        self.assertEqual(memcache.get('foo'), 'test2')
        result.set('test3')
        self.assertEqual(memcache.get('foo'), 'test3')

        server = CommandServer(
                [
                    RECEIVE, 'STORED\r\n',
                    RECEIVE, 'VALUE foo 0 7 3137\r\ntesting\r\nEND\r\n',
                    RECEIVE, 'STORED\r\n',
                    RECEIVE, 'VALUE foo 0 7 3173\r\nhacking\r\nEND\r\n',
                ])
        memcache = memcached2.Memcache(
                ('memcached://localhost:{0}/'.format(server.port),),
                value_wrapper=memcached2.ValueSuperStr)

        memcache.set('foo', 'testing')
        result = memcache.get('foo', get_cas=True)
        self.assertEqual(result, 'testing')
        with self.assertRaises(memcached2.CASRefreshFailure):
            result.set('test2', update_cas=True)

    def test_ValueDictionary(self):
        memcache = memcached2.Memcache(
                ('memcached://localhost/',),
                value_wrapper=memcached2.ValueDictionary)

        memcache.set('foo', 'bar')
        result = memcache.get('foo')
        self.assertEqual(result.get('key'), 'foo')
        self.assertEqual(result.get('value'), 'bar')
        memcache.set(result.get('key'), 'testing')
        self.assertEqual(memcache.get('foo')['value'], 'testing')

        result = memcache.get('foo', get_cas=True)
        self.assertEqual(result['key'], 'foo')
        self.assertEqual(result['value'], 'testing')
        self.assertNotIn(result['cas_unique'], [None, 0])
        memcache.set(
                result.get('key'), 'test2', cas_unique=result['cas_unique'])
        self.assertEqual(memcache.get('foo')['value'], 'test2')
        with self.assertRaises(memcached2.CASFailure):
            memcache.set(
                    result.get('key'), 'test3',
                    cas_unique=result['cas_unique'])
        self.assertEqual(memcache.get('foo')['value'], 'test2')

        memcache.close()

    def test_SetAndGetWithErrors(self):
        server = CommandServer([])
        memcache = memcached2.Memcache(
                ('memcached://localhost:{0}/'.format(server.port),))
        with self.assertRaises(memcached2.ServerDisconnect):
            memcache.set('foo', 'bar')

        server = CommandServer([RECEIVE])
        memcache = memcached2.Memcache(
                ('memcached://localhost:{0}/'.format(server.port),))
        with self.assertRaises(memcached2.ServerDisconnect):
            memcache.set('foo', 'bar')

        server = CommandServer([RECEIVE, 'STORED\r\n'])
        memcache = memcached2.Memcache(
                ('memcached://localhost:{0}/'.format(server.port),))
        memcache.set('foo', 'bar')
        with self.assertRaises(memcached2.ServerDisconnect):
            memcache.get('foo')

        server = CommandServer([RECEIVE, 'STORED\r\n', RECEIVE])
        memcache = memcached2.Memcache(
                ('memcached://localhost:{0}/'.format(server.port),))
        memcache.set('foo', 'bar')
        with self.assertRaises(memcached2.ServerDisconnect):
            memcache.get('foo')

        server = CommandServer(
                [
                    RECEIVE, 'STORED\r\n', RECEIVE,
                    'VALUE foo 0 3\r\nbar\r\nEND\r\n'
                ])
        memcache = memcached2.Memcache(
                ('memcached://localhost:{0}/'.format(server.port),))
        memcache.set('foo', 'bar')
        memcache.get('foo')

    def test_TestFlagsAndExptime(self):
        memcache = memcached2.Memcache(
                ('memcached://localhost/',),
                value_wrapper=memcached2.ValueSuperStr)

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
        memcache = memcached2.Memcache(
                ('memcached://localhost/',),
                value_wrapper=memcached2.ValueSuperStr)
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
        memcache.flush_all()
        memcache.set('foo', 'a')
        memcache.stats()
        memcache.stats_settings()
        memcache.stats_items()
        self.assertEqual(memcache.stats_sizes()[0][0], (64, 1))
        memcache.stats_slabs()

    def test_SeveralServers(self):
        memcache = memcached2.Memcache(
                (
                    'memcached://localhost/', 'memcached://localhost/',
                    'memcached://localhost/', 'memcached://localhost/',
                ))

        memcache.flush_all()

        data = memcache.stats()
        self.assertEqual(len(data), 4)
        self.assertNotIn(None, data)

        data = memcache.stats_settings()
        self.assertEqual(len(data), 4)
        self.assertNotIn(None, data)

        data = memcache.stats_items()
        self.assertEqual(len(data), 4)
        self.assertNotIn(None, data)

        data = memcache.stats_sizes()
        self.assertEqual(len(data), 4)
        self.assertNotIn(None, data)

        data = memcache.stats_sizes()
        self.assertEqual(len(data), 4)
        self.assertNotIn(None, data)

        for i in range(100):
            memcache.set('foo{0}'.format(i), 'bar')

    def test_Cached(self):
        memcache = memcached2.Memcache(('memcached://localhost/',))
        memcache.flush_all()

        class Doubler:
            def __init__(self):
                self.old_value = 1

            def __call__(self, arg, add=0):
                self.old_value = (self.old_value * 2) + add
                return str(self.old_value)

        double = Doubler()

        self.assertEqual(memcache.cache('foo', double), '2')
        self.assertEqual(memcache.cache('foo', double), '2')
        memcache.flush_all()
        self.assertEqual(memcache.cache('foo', double, 1), '5')
        self.assertEqual(memcache.cache('foo', double, 1), '5')
        memcache.set('foo', '0')
        self.assertEqual(memcache.cache('foo', double), '0')
        self.assertEqual(memcache.cache('foo', double), '0')
        memcache.flush_all()
        self.assertEqual(memcache.cache('foo', double), '10')

    def test_KeysByServer(self):
        memcache = memcached2.Memcache(('memcached://localhost/',
                'memcached://localhost/',))

        data = memcache._keys_by_server(['a', 'b', 'c', 'd', 'e', 'f'])
        self.assertEqual(data[0][1], ['a', 'c', 'd', 'f'])
        self.assertEqual(data[1][1], ['b', 'e'])

    def test_repr(self):
        server = memcached2.ServerConnection('memcached://localhost/')
        self.assertEqual(
                repr(server), '<ServerConnection to memcached://localhost/>')


unittest.main()
