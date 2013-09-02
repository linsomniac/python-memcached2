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
import memcached2
from faketcpserver import RECEIVE, CommandServer


class test_ExceptionsAreMissesMemcache(unittest.TestCase):
    def setUp(self):
        mctestsupp.flush_local_memcache(self)

    def test_Basics(self):
        mc = memcached2.ExceptionsAreMissesMemcache(
                ('memcached://localhost/',))

        self.assertEqual(mc.get('foo'), None)
        mc.set('foo', 'hello')
        self.assertEqual(mc.get('foo'), 'hello')
        mc.delete('foo')
        mc.delete('bar')
        self.assertEqual(mc.get('foo'), None)

    def test_SetMulti(self):
        memcache = memcached2.ExceptionsAreMissesMemcache(
                ('memcached://localhost/', 'memcached://localhost/'))

        data = []
        for i in range(10):
            data.append(('key{0}'.format(i), '!' * i))
        data.append(('badkey' * 512, 'value'))

        results = memcache.set_multi(data, return_successful=False)

        self.assertEqual(len(results), 1)
        self.assertTrue(list(results.keys())[0].startswith('badkey'))

    def test_SetMultiErrors(self):
        data = []
        for i in range(10):
            data.append(('key{0}'.format(i), '!' * i))

        server = CommandServer([])
        memcache = memcached2.ExceptionsAreMissesMemcache(
                ('memcached://localhost:{0}/'.format(server.port),),
                value_wrapper=memcached2.ValueSuperStr)

        results = memcache.set_multi(data)
        self.assertEqual(len(results), 0)

        server = CommandServer([RECEIVE,])
        memcache = memcached2.ExceptionsAreMissesMemcache(
                ('memcached://localhost:{0}/'.format(server.port),),
                value_wrapper=memcached2.ValueSuperStr)

        results = memcache.set_multi(data)
        self.assertEqual(len(results), 0)

        server = CommandServer([RECEIVE, 'STORED\r\n'*9,])
        memcache = memcached2.ExceptionsAreMissesMemcache(
                ('memcached://localhost:{0}/'.format(server.port),),
                value_wrapper=memcached2.ValueSuperStr)

        results = memcache.set_multi(data)
        self.assertIn('key0', results)
        self.assertIn('key8', results)
        self.assertNotIn('key9', results)

        server = CommandServer([RECEIVE, 'STORED\r\n'*10,])
        memcache = memcached2.ExceptionsAreMissesMemcache(
                ('memcached://localhost:{0}/'.format(server.port),),
                value_wrapper=memcached2.ValueSuperStr)

        results = memcache.set_multi(data)
        self.assertIn('key0', results)
        self.assertIn('key9', results)

        server = CommandServer(
                [RECEIVE, ('STORED\r\n'*5) + 'CLIENT_ERROR Failed\r\n',])
        memcache = memcached2.ExceptionsAreMissesMemcache(
                ('memcached://localhost:{0}/'.format(server.port),),
                value_wrapper=memcached2.ValueSuperStr)

        results = memcache.set_multi(data)
        self.assertTrue(results['key5'].startswith('CLIENT_ERROR'))
        self.assertEqual(results['key4'], None)
        self.assertNotIn('key6', results)


    def test_SetServerDisconnect(self):
        server = CommandServer([])
        mc = memcached2.ExceptionsAreMissesMemcache(
            ('memcached://localhost:{0}/'.format(server.port),))
        mc.set('foo', 'bar')
        mc.set('foo', 'bar')

    def test_SetServerReceive(self):
        server = CommandServer([RECEIVE])
        mc = memcached2.ExceptionsAreMissesMemcache(
            ('memcached://localhost:{0}/'.format(server.port),))
        mc.set('foo', 'bar')

    def test_SetServerNotStored(self):
        server = CommandServer([RECEIVE, 'NOT_STORED\r\n'])
        mc = memcached2.ExceptionsAreMissesMemcache(
            ('memcached://localhost:{0}/'.format(server.port),))
        mc.set('foo', 'bar')

    def test_SetServerExists(self):
        server = CommandServer([RECEIVE, 'EXISTS\r\n'])
        mc = memcached2.ExceptionsAreMissesMemcache(
            ('memcached://localhost:{0}/'.format(server.port),))
        mc.set('foo', 'bar')

    def test_SetServerNotFound(self):
        server = CommandServer([RECEIVE, 'NOT FOUND\r\n'])
        mcd = memcached2.ExceptionsAreMissesMapping((
            'memcached://localhost:{0}/'.format(server.port),))
        mcd['foo'] = 'bar'

    def test_GetServerDisconnect(self):
        server = CommandServer([])
        mc = memcached2.ExceptionsAreMissesMemcache(
            ('memcached://localhost:{0}/'.format(server.port),))
        self.assertEqual(mc.get('foo'), None)
        self.assertEqual(mc.get('foo'), None)

    def test_GetServerReceive(self):
        server = CommandServer([RECEIVE])
        mc = memcached2.ExceptionsAreMissesMemcache(
            ('memcached://localhost:{0}/'.format(server.port),))
        self.assertEqual(mc.get('foo'), None)

    def test_GetServerNoValue(self):
        server = CommandServer([RECEIVE, 'END\r\n'])
        mc = memcached2.ExceptionsAreMissesMemcache(
            ('memcached://localhost:{0}/'.format(server.port),))
        self.assertEqual(mc.get('foo'), None)

    def test_DeleteServerDisconnect(self):
        server = CommandServer([])
        mc = memcached2.ExceptionsAreMissesMemcache(
            ('memcached://localhost:{0}/'.format(server.port),))
        mc.delete('foo')
        mc.delete('foo')

    def test_DeleteServerReceive(self):
        server = CommandServer([RECEIVE])
        mc = memcached2.ExceptionsAreMissesMemcache(
            ('memcached://localhost:{0}/'.format(server.port),))
        mc.delete('foo')
        mc.delete('foo')

    def test_DeleteServerNotFound(self):
        server = CommandServer([RECEIVE, 'NOT_FOUND\r\n'])
        mc = memcached2.ExceptionsAreMissesMemcache(
            ('memcached://localhost:{0}/'.format(server.port),))
        mc.delete('foo')
        mc.delete('foo')

unittest.main()
