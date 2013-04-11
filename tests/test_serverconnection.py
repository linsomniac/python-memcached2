#!/usr/bin/env python
#
#  Test the ServerConnection component of the Python memcached2 module.
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
        sc.send_command(b'flush_all\r\n')
        self.assertEqual(sc.read_until(b'\r\n'), b'OK\r\n')
        sc.reset()

    def test_ServerFlushDisconnect(self):
        class DisconnetAfterCommandServer(mctestsupp.FakeMemcacheServer):
            def server(self, sock, conn, count):
                conn.recv(100)
                conn.close()
                #conn.send(b'OK\n')

        server = DisconnetAfterCommandServer()
        sc = memcached2.ServerConnection('memcached://127.0.0.1:{0}/'
                .format(server.port))
        sc.connect()
        sc.send_command(b'flush_all\r\n')
        with self.assertRaises(memcached2.BackendDisconnect):
            self.assertEqual(sc.read_until(b'\r\n'), b'OK\r\n')
        sc.reset()

        class ImmediatelyDisconnectServer(mctestsupp.FakeMemcacheServer):
            def server(self, sock, conn, count):
                conn.close()

        server = ImmediatelyDisconnectServer()
        sc = memcached2.ServerConnection('memcached://127.0.0.1:{0}/'
                .format(server.port))
        sc.connect()
        sc.send_command(b'flush_all\r\n')
        with self.assertRaises(memcached2.BackendDisconnect):
            self.assertEqual(sc.read_until(b'\r\n'), b'OK\r\n')
        sc.reset()

    def test_Selectors(self):
        selector = memcached2.SelectorFirst()
        self.assertEqual(selector.select(range(5), None), 0)

unittest.main()
