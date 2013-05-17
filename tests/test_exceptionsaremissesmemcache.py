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


class test_ServerConnection(unittest.TestCase):
    def setUp(self):
        mctestsupp.flush_local_memcache(self)

    def test_Basics(self):
        dic = memcached2.ExceptionsAreMissesMapping(
                ('memcached://localhost/',))

        with self.assertRaises(NotImplementedError):
            for x in dic:
                pass

        with self.assertRaises(KeyError):
            dic['foo']
        self.assertEqual(dic.get('foo'), None)
        dic['foo'] = 'hello'
        self.assertEqual(dic['foo'], 'hello')
        self.assertEqual('foo' in dic, True)
        self.assertEqual('qux' in dic, False)
        del(dic['foo'])
        del(dic['bar'])
        self.assertEqual('foo' in dic, False)
        self.assertEqual(dic.get('foo'), None)
        dic['zot'] = 'a'
        len(dic)

    def test_SetServerDisconnect(self):
        server = CommandServer([])
        mcd = memcached2.ExceptionsAreMissesMapping((
            'memcached://localhost:{0}/'.format(server.port),))
        mcd['foo'] = 'bar'
        mcd['foo'] = 'bar'

    def test_SetServerReceive(self):
        server = CommandServer([RECEIVE])
        mcd = memcached2.ExceptionsAreMissesMapping((
            'memcached://localhost:{0}/'.format(server.port),))
        mcd['foo'] = 'bar'

    def test_SetServerNotStored(self):
        server = CommandServer([RECEIVE, 'NOT_STORED\r\n'])
        mcd = memcached2.ExceptionsAreMissesMapping((
            'memcached://localhost:{0}/'.format(server.port),))
        mcd['foo'] = 'bar'

    def test_SetServerExists(self):
        server = CommandServer([RECEIVE, 'EXISTS\r\n'])
        mcd = memcached2.ExceptionsAreMissesMapping((
            'memcached://localhost:{0}/'.format(server.port),))
        mcd['foo'] = 'bar'

    def test_SetServerNotFound(self):
        server = CommandServer([RECEIVE, 'NOT FOUND\r\n'])
        mcd = memcached2.ExceptionsAreMissesMapping((
            'memcached://localhost:{0}/'.format(server.port),))
        mcd['foo'] = 'bar'

    def test_GetServerDisconnect(self):
        server = CommandServer([])
        mcd = memcached2.ExceptionsAreMissesMapping((
            'memcached://localhost:{0}/'.format(server.port),))
        with self.assertRaises(KeyError):
            mcd['foo']
        with self.assertRaises(KeyError):
            mcd['foo']

    def test_GetServerReceive(self):
        server = CommandServer([RECEIVE])
        mcd = memcached2.ExceptionsAreMissesMapping((
            'memcached://localhost:{0}/'.format(server.port),))
        with self.assertRaises(KeyError):
            mcd['foo']

    def test_GetServerNoValue(self):
        server = CommandServer([RECEIVE, 'END\r\n'])
        mcd = memcached2.ExceptionsAreMissesMapping((
            'memcached://localhost:{0}/'.format(server.port),))
        with self.assertRaises(KeyError):
            mcd['foo']

    def test_DeleteServerDisconnect(self):
        server = CommandServer([])
        mcd = memcached2.ExceptionsAreMissesMapping((
            'memcached://localhost:{0}/'.format(server.port),))
        del(mcd['foo'])
        del(mcd['foo'])

    def test_DeleteServerReceive(self):
        server = CommandServer([RECEIVE])
        mcd = memcached2.ExceptionsAreMissesMapping((
            'memcached://localhost:{0}/'.format(server.port),))
        del(mcd['foo'])
        del(mcd['foo'])

    def test_DeleteServerNotFound(self):
        server = CommandServer([RECEIVE, 'NOT_FOUND\r\n'])
        mcd = memcached2.ExceptionsAreMissesMapping((
            'memcached://localhost:{0}/'.format(server.port),))
        del(mcd['foo'])
        del(mcd['foo'])

    def test_LenServerDisconnect(self):
        server = CommandServer([])
        mcd = memcached2.ExceptionsAreMissesMapping((
            'memcached://localhost:{0}/'.format(server.port),))
        len(mcd)
        len(mcd)

    def test_LenServerReceive(self):
        server = CommandServer([RECEIVE])
        mcd = memcached2.ExceptionsAreMissesMapping((
            'memcached://localhost:{0}/'.format(server.port),))
        len(mcd)
        len(mcd)

    def test_LenServerNotFound(self):
        server = CommandServer([RECEIVE, 'END\r\n'])
        mcd = memcached2.ExceptionsAreMissesMapping((
            'memcached://localhost:{0}/'.format(server.port),))
        len(mcd)
        len(mcd)

unittest.main()
