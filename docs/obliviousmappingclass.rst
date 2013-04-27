Exception-oblivious Mapping
***************************

.. _obliviousmapping-introduction:

Introduction
------------

This is a dictionary-like interface to memcache, but it swallows
server exceptions, except in the case of coding errors.  This is meant
for situations where you want to keep the code simple, and treat cache
misses, server errors, and the like as cache misses.

On the instantiation you specify the servers, and at that point it can be
accessed as a dictionary, including access, setting, and deleting keys.
See the examples for a demonstration.

For functionality beyond what you can get from the dictionary interface,
you need to use the :attr:`memcache` attribute, which is an
:py:class:`~memcached2.Memcache` instance.  See that documentation
for access to flusing servers, statistics, and other things not supported
by the mapping interface.

Note that :exc:`NotImplementedException` will be raised for situations
where there are code errors.  So it's recommended that you don't just trap
these, either catch and log them, or just let them raise up so that
application users can report the bug.

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
