High-Level Memcache() Interface
*******************************

Introduction
------------

This is a low-level interface to a group of memcache servers.  This code
tends to either return the requested data, or raises an exception if the
data is not available or there is any sort of an error.  If you want
high level control, this is probably the interface for you.  However, if
you want something easy, like the old python-memcached module, you will
want to wait for the higher level interfaces to be implemented.

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
