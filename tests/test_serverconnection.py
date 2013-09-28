#!/usr/bin/env python
#
#  Test the ServerConnection component of the Python memcached2 module.
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


class test_ServerConnection(unittest.TestCase):

    def setUp(self):
        mctestsupp.flush_local_memcache(self)

    def test_URI(self):
        with self.assertRaises(memcached2.InvalidURI):
            memcached2.ServerConnection('failure://host.name/')

        sc = memcached2.ServerConnection('memcached://localhost/')
        self.assertEqual(sc.parsed_uri['port'], 11211)
        self.assertEqual(sc.parsed_uri['host'], 'localhost')
        self.assertEqual(sc.parsed_uri['protocol'], 'memcached')

        sc = memcached2.ServerConnection('memcached://127.0.0.1/')
        self.assertEqual(sc.parsed_uri['port'], 11211)
        self.assertEqual(sc.parsed_uri['host'], '127.0.0.1')
        self.assertEqual(sc.parsed_uri['protocol'], 'memcached')

        sc = memcached2.ServerConnection('memcached://localhost:11234/')
        self.assertEqual(sc.parsed_uri['port'], 11234)
        self.assertEqual(sc.parsed_uri['host'], 'localhost')
        self.assertEqual(sc.parsed_uri['protocol'], 'memcached')

        sc = memcached2.ServerConnection('memcached://127.0.0.1:11234/')
        self.assertEqual(sc.parsed_uri['port'], 11234)
        self.assertEqual(sc.parsed_uri['host'], '127.0.0.1')
        self.assertEqual(sc.parsed_uri['protocol'], 'memcached')

    def test_BasicCommand(self):
        sc = memcached2.ServerConnection('memcached://localhost/')
        sc.connect()
        sc.send_command('flush_all\r\n')
        self.assertEqual(sc.read_until('\r\n'), 'OK\r\n')
        sc.reset()

    def test_ServerFlushDisconnect(self):
        server = CommandServer([RECEIVE])
        sc = memcached2.ServerConnection(
            'memcached://127.0.0.1:{0}/'.format(server.port))
        sc.connect()
        sc.send_command('flush_all\r\n')
        with self.assertRaises(memcached2.ServerDisconnect):
            self.assertEqual(sc.read_until('\r\n'), 'OK\r\n')
        sc.reset()

        server = CommandServer([])
        sc = memcached2.ServerConnection(
            'memcached://127.0.0.1:{0}/'.format(server.port))
        sc.connect()
        sc.send_command('flush_all\r\n')
        with self.assertRaises(memcached2.ServerDisconnect):
            self.assertEqual(sc.read_until('\r\n'), 'OK\r\n')
        sc.reset()

    def test_Selectors(self):
        selector = memcached2.SelectorFirst()
        sc = memcached2.ServerConnection('memcached://127.0.0.1/')
        sc2 = memcached2.ServerConnection('memcached://127.0.0.1:11234/')
        self.assertEqual(selector.select([sc, sc2], None, None), sc)
        sc.reset()
        sc2.reset()

    def test_Hashers(self):
        self.assertEqual(memcached2.HasherZero().hash('x'), 0)
        self.assertEqual(memcached2.HasherCMemcache().hash('x'), 3292)
        self.assertEqual(memcached2.HasherCMemcache().hash('y'), 31707)

unittest.main()
