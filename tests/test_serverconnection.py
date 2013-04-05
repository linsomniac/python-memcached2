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


class FakeMemcacheServer:
	'''A simple socket server so that specific error conditions can be tested.
	This must be subclassed and implment the "server()" method.'''

	def __init__(self):
		import socket
		import os
		import signal

		self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.s.listen(1)
		self.port = self.s.getsockname()[1]
		self.pid = os.fork()

		if self.pid != 0:
			self.s.close()
			del self.s
		else:
			def alarm(signum, frame):
				os._exit(0)

			count = 0
			signal.signal(signal.SIGALRM, alarm)
			signal.alarm(5)
			while True:
				conn, addr = self.s.accept()
				self.server(self.s, conn, count)
				count += 1


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

	def test_ServerFlushDisconnect(self):
		class Server(FakeMemcacheServer):
			def server(self, sock, conn, count):
				data = conn.recv(100)
				conn.close()
				#conn.send(b'OK\n')

		server = Server()
		sc = memcached2.ServerConnection('memcached://127.0.0.1:{0}/'
				.format(server.port))
		sc.connect()
		sc.send_command(b'flush_all')
		with self.assertRaises(memcached2.BackendDisconnect):
			self.assertEqual(sc.read_until(b'\r\n'), b'OK\r\n')
		sc.reset()

unittest.main()
