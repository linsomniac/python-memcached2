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


class test_ServerConnection(unittest.TestCase):
    def setUp(self):
        mctestsupp.flush_local_memcache(self)

    def test_Basics(self):
        dic = memcached2.ObliviousMapping(('memcached://localhost/',))

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

unittest.main()
