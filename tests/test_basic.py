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
sys.path.append('..')

class test_Basic:
	def test_ImportModule(self):
		import memcached2

unittest.main()
