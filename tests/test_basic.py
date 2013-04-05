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


class test_Basic(unittest.TestCase):
    def test_ImportModule(self):
        import memcached2

    def test_ConnectToLocalhostServer(self):
        import socket
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(('127.0.0.1', 11211))
        s.send(b'flush_all\nquit\n')
        results = s.recv(1000)
        self.assertIn(b'OK', results)
        s.close()

unittest.main()
