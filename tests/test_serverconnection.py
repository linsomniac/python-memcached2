#!/usr/bin/env python
#
#  Basic test of the Python memcached2 module.
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
		sc.send_command(b'flush_all')
		self.assertEqual(sc.read_until(b'\r\n'), b'OK\r\n')
		sc.reset()

unittest.main()
