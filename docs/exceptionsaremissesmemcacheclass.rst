Exception as Misses Mapping
***************************

.. _exceptionsaremissesmemcache-introduction:

Introduction
------------

This is a subclass of :py:class:`~memcached2.Memcache` which swallows
exceptions and treats them like misses.  This is meant to allow code to
be a bit simpler, rather than catching all exceptions, you can just do
things like the below example.

.. _exceptionsaremissesmemcache-examples:

Examples
--------

Basic example:

>>> import memcached2
>>> mc = memcached2.ExceptionsAreMissesMemcache(('memcached://localhost/',))
>>> data = mc.get('key')
>>> if not data:
>>>    data = [compute data]
>>>    mc.set('key', data)
>>> [use data]

Object Documentation
--------------------

.. autoclass:: memcached2.ExceptionsAreMissesMemcache
   :members: 
