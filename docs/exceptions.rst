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

MemcachedException(Exception)
+++++++++++++++++++++++++++++

Base exception that all other exceptions inherit from.  This is never
raised directly.

UnknownProtocol(MemcachedException)
+++++++++++++++++++++++++++++++++++

An unknown protocol was specified in the memcached URI.

InvalidURI(MemcachedException)
++++++++++++++++++++++++++++++

An error was encountered in parsing the server URI.

ServerDisconnect(MemcachedException)
++++++++++++++++++++++++++++++++++++

The connection to the server closed.

NoAvailableServers(MemcachedException)
++++++++++++++++++++++++++++++++++++++

There are no servers available to cache on, probably because all are
unavailable.

StoreException(MemcachedException)
++++++++++++++++++++++++++++++++++

Base class for storage related exceptions.  Never raised directly.

NotStored(StoreException)
+++++++++++++++++++++++++

Item was not stored, but not due to an error.  Normally means the
condition for an "add" or "replace" was not met.

CASFailure(StoreException)
++++++++++++++++++++++++++

Item you are trying to store with a "cas" command has been modified
since you last fetched it (result=EXISTS).

NotFound(StoreException)
++++++++++++++++++++++++

Item you are trying to store with a "cas" command does not exist.

NonNumeric(StoreException)
++++++++++++++++++++++++++

The item you are trying to incr/decr is not numeric.

RetrieveException(MemcachedException)
+++++++++++++++++++++++++++++++++++++

Base class for retrieve related exceptions.  This is never raised directly.

NoValue(RetrieveException)
++++++++++++++++++++++++++

Server has no data associated with this key.
