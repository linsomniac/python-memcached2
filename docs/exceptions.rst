python-memcached2 Exceptions
============================

Overview
--------

The classes that throw exceptions all tend to raise exceptions that are
children of the MemcachedException.  For storage-related exceptions, they
are children of StoreException, and for retrieval they are children of
RetrieveException.

If you use the exception-exposing interfaces ("Memcache()"), *will* need to
catch these exceptions as part of your code.  They are thrown on
exceptional conditions, read the description of the exceptions for details
on when they may be thrown.

In specific error cases that likely indicate bugs in the python-memcached2
module, or where the server replies with unexpected data, the
NotImplementedError is raised.  These situations are extremely unusual and
almost certainly should be reported to the developers of either this
python-memcached2 module or the developers of the memcached server you are
using.  You probably don't want to catch these

Exceptions
----------

.. module:: memcached2

.. autoclass:: MemcachedException
.. autoclass:: UnknownProtocol
.. autoclass:: InvalidURI
.. autoclass:: ServerDisconnect
.. autoclass:: NoAvailableServers
.. autoclass:: StoreException
.. autoclass:: NotStored
.. autoclass:: CASFailure
.. autoclass:: NotFound
.. autoclass:: NonNumeric
.. autoclass:: RetrieveException
.. autoclass:: NoValue
