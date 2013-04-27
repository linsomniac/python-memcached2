No-exceptions Mapping
*********************

Introduction
------------

This is a dictionary-like interface to Memcache, but it swallows
exceptions, except in the case of coding errors.  This is meant
for situations where you want to keep the code simple, and treat cache
misses, server errors, and the like as cache misses.

On the instantiation you specify the servers, and at that point it can be
accessed as a dictionary, including access, setting, and deleting keys.
See the examples for a demonstration.

.. _obliviousmapping-examples:

Examples
--------

Basic example:

>>> import memcached2
>>> mcd = memcached2.ObliviousMapping(('memcached://localhost/',))
>>> 'foo' in mcd
False
>>> mcd['foo'] = 'hello'
>>> 'foo' in mcd
True
>>> mcd['foo']
'hello'
>>> len(mcd)
1
>>> del(mcd['foo'])
>>> len(mcd)
0

Object Documentation
--------------------

.. autoclass:: memcached2.ObliviousMapping
   :members: 
