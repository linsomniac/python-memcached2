.. python-memcached2 documentation master file, created by
   sphinx-quickstart on Sun Apr 21 17:06:20 2013.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

python-memcached2
=================

python-memcached2 is a next-generation implementation re-implementation of
the python-memcached module.  The primary goals are to get rid of some
baggage, improve testability/maintainability/performance, and support
Python 3.  This codebase is regularly tested against Python 2.7 and Python
3.3.

As of April 2013 this is in active development and not quite suitable for
use in production yet.  However, the core functionality is implemented and
the higher level interfaces should come fairly quickly now.  The low level
:py:class:`~memcached2.Memcache` class is complete and documented, see
:py:class:`~memcached2.Memcache` for examples of use.

Documentation Index
-------------------

.. toctree::
   :maxdepth: 2

   hasherclass
   selectorclass
   memcachevalueclass
   memcacheclass
   serverconnectionclass
   exceptions


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

