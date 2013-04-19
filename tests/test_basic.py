#!/usr/bin/env python
#
#  Basic test of the Python memcached2 module.
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
