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


class test_Basic(unittest.TestCase):
	def setUp(self):
		mctestsupp.flush_local_memcache(self)

	def test_ImportModule(self):
		import memcached2

unittest.main()
