#!/usr/bin/env python3

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

__author__ = 'Sean Reifschneider <jafo@tummy.com>'
__version__ = 'X.XX'
__copyright__ = 'Copyright (C) 2013 Sean Reifschneider, tummy.com, ltd.'
__license__ = 'Apache'

'''
from datetime import time
.. module:: memcached2
    :platform: Unix, Windows
    :synopsis: Next-generation memcache module for Python 2 and 3.

    .. moduleauthor:: Sean Reifschneider <jafo@tummy.com>

A re-implementation of the python-memcached module, designed to work with
Python 2 and 3.  Note: It is tested against Python 2.7 and 3.3 during
development, there may be problems running against previous versions.

See the Memcache() class and the tests for examples of use.

Developed by Sean Reifschneider <jafo@tummy.com> in 2013.

Bugs/patches/code: https://github.com/linsomniac/python-memcached2
'''

import re
import socket
import select
import sys
import time
from binascii import crc32
import collections
from bisect import bisect

PY3 = sys.version > '3'
PY33 = sys.version > '3.3'
if PY3:
    import queue
else:
    import Queue as queue
if not PY33:
    ConnectionResetError = socket.error

    class BrokenPipeError(Exception):

        '''INTERNAL: Python 2 does not define this exception, so we
        create one of our own.'''
        pass


def _from_bytes(data):
    '''INTERNAL: Convert bytes to a regular string.'''
    if PY3:
        if isinstance(data, str):
            return data
        return str(data, 'latin-1')
    return str(data)


def _to_bytes(data):
    '''INTERNAL: Convert something to bytes type.'''
    if PY3:
        if isinstance(data, bytes):
            return data
        return bytes(data, 'latin-1')
    return data


def _to_bool(s):
    '''INTERNAL: Convert a stats boolean string into boolean.'''
    if s in ['0', 'no']:
        return False
    if s in ['1', 'yes']:
        return True
    raise NotImplementedError('Unknown boolean value {0}'.format(repr(s)))


def _server_interaction(
        buffers_by_server, send_threshold, send_minimum,
        expected_keys, results, return_successful, return_failed):
    '''INTERNAL: Write and read to sockets that are ready.

    This is used by the :py:func:`~memcached2.Memcache.set_multi` code to
    interact with the server when buffers overflow or to finish sending and
    receiving data.
    '''
    return_exception = None

    read_sockets = [x for x in buffers_by_server.keys() if x.backend]

    write_sockets = [
        x[0] for x in buffers_by_server.items()
        if not send_threshold or len(x[1]) >= send_threshold]

    read_ready, write_ready = select.select(
        read_sockets, write_sockets, [])[:2]

    #  receive data from read-ready sockets
    for server in read_ready:
        try:
            server.read_from_socket()
        except ServerDisconnect:
            return_exception = _deferred_exception(
                server, results, buffers_by_server, expected_keys,
                'ServerDisconnect received, probably server died')

        while server.line_available() and expected_keys[server]:
            line = server.read_until().rstrip()
            key = expected_keys[server].pop(0)

            if line.startswith('CLIENT_ERROR'):
                return_exception = _deferred_exception(
                    server, results, buffers_by_server, expected_keys,
                    'CLIENT_ERROR received, possibly key is too long.')

            if line == 'STORED':
                if return_successful:
                    results[key] = None
            elif return_failed:
                results[key] = line

    #  send data to write-ready sockets
    for server in write_ready:
        data = buffers_by_server[server]
        if not data:
            continue
        bytes_sent = server.backend.send(data)
        del data[:bytes_sent]

    return return_exception


def _deferred_exception(
        server, results, buffers_by_server, expected_keys, message):
    '''INTERNAL: Handle an exception on a socket during set_multi.

    This is used by the :py:func:`~memcached2.Memcache.set_multi` code to
    reset socket connections and reconnect, flushing data to re-gain sync.
    '''
    server.reset()
    server.connect()
    del buffers_by_server[server][:]
    del expected_keys[server][:]
    return MultiStorageException(message, results)


def _dictionary_values_empty(d):
    '''INTERNAL: Return the values in the dictionary that are not false.
    '''
    return [x for x in d.values() if x]


def debug(msg):
    '''INTERNAL: Write a debugging message to stderr with a stack trace.
    '''
    import inspect
    import os
    stack = (
        ' -> '.join(['{0}({1}:{2})'.format(x[3],
                                           os.path.basename(x[1]), x[2])
                     for x in reversed(inspect.stack()[1:])]))
    sys.stderr.write('{0} => {1}\n'.format(msg, stack))
    sys.stderr.flush()


class MemcachedException(Exception):

    '''Base exception that all other exceptions inherit from.
    This is never raised directly.'''


class UnknownProtocol(MemcachedException):

    '''An unknown protocol was specified in the memcached URI.
    Sublcass of :class:`MemcachedException`.'''


class InvalidURI(MemcachedException):

    '''An error was encountered in parsing the server URI.
    Subclass of :class:`MemcachedException`.'''


class ServerDisconnect(MemcachedException):

    '''The connection to the server closed.
    Subclass of :class:`MemcachedException`.'''


class NoAvailableServers(MemcachedException):

    '''There are no servers available to cache on, probably because all
    are disconnected.  This exception typically occurs after the code
    which would do a reconnection is run.
    Subclass of :class:`MemcachedException`.'''


class StoreException(MemcachedException):

    '''Base class for storage related exceptions.  Never raised directly.
    Subclass of :class:`MemcachedException`.'''


class NotStored(StoreException):

    '''Item was not stored, but not due to an error.  Normally means the
    condition for an "add" or "replace" was not met..  Subclass of
    :class:`StoreException`.'''


class CASFailure(StoreException):

    '''Item you are trying to store with a "cas" command has been modified
    since you last fetched it (result=EXISTS).  Subclass of
    :class:`StoreException`.'''


class MultiStorageException(StoreException):

    '''During a SET operation the server returned CLIENT_ERROR.  This is
    probably due to too long of a key being used.  Subclass of
    :class:`StoreException`.'''
    def __init__(self, message=None, results={}):
        self.message = message
        self.results = results


class CASRefreshFailure(CASFailure):

    '''When trying to refresh a CAS from the memcached, the retrieved value
    did not match the value sent with the last update.  This may happen if
    another process has updated the value.  Subclass of
    :class:`CASFailure`.'''


class NotFound(StoreException):

    '''Item you are trying to store with a "cas" command does not exist..
    Subclass of :class:`StoreException`.'''


class NonNumeric(StoreException):

    '''The item you are trying to incr/decr is not numeric..
    Subclass of :class:`StoreException`.'''


class RetrieveException(MemcachedException):

    '''Base class for retrieve related exceptions.  This is never raised
    directly..  Subclass of :class:`MemcachedException`.'''


class NoValue(RetrieveException):

    '''Server has no data associated with this key..
    Subclass of :class:`RetrieveException`.'''


class ExceptionsAreMissesMapping(collections.MutableMapping):

    '''A dictionary-like interface which swallows server exceptions.

    This is a dictionary-like interface to memcache, but it swallows
    server exceptions, except in the case of coding errors.  This is
    meant for situations where you want to keep the code simple, and
    treat cache misses, server errors, and the like as cache misses.

    See :ref:`ExceptionsAreMissesMapping Introduction
    <exceptionsaremissesmapping-introduction>`
    and :ref:`ExceptionsAreMissesMapping Examples
    <exceptionsaremissesmapping-examples>` for more information.

    '''
    def __init__(self, servers, selector=None, hasher=None):
        ret = super(ExceptionsAreMissesMapping, self).__init__()
        self.memcache = Memcache(servers, selector, hasher)
        return ret

    def __getitem__(self, key):
        try:
            return self.memcache.get(key)
        except (NoValue, ServerDisconnect, NotStored, NotFound, CASFailure):
            raise KeyError(key)

    def __setitem__(self, key, value):
        try:
            self.memcache.set(key, value)
        except (ServerDisconnect, NotStored, NotFound, CASFailure):
            pass

    def __delitem__(self, key):
        try:
            self.memcache.delete(key)
            return True
        except (ServerDisconnect, NoAvailableServers, NotFound):
            return False

    def __iter__(self):
        raise NotImplementedError()

    def __len__(self):
        items = 0
        try:
            for server_stats in self.memcache.stats():
                items += server_stats.get('curr_items', 0)
        except (ServerDisconnect, NoAvailableServers):
            pass

        return items


class ValueSuperStr(str):

    '''Wrapper around Memcache value results.

    This acts as a string normally, containing the value read from the
    server.  However, it is augmented with additional attributes representing
    additional data received from the server: `flags`, `key`, and
    `cas_unique` (which may be None if it was not requested from the server).

    If this is constructed with the `memcache`
    :py:class:`~memcache2.ServerConnection` instance, then additional
    methods may be used to update the value via this object.  If `cas_unique`
    is given, these updates are done using the CAS value.
    '''

    def __new__(self, value, key, flags, cas_unique=None, memcache=None):
        '''Instantiate new instance.

        :param value: The memcache `value`, which is the value of this
            class when treated like a string.
        :type value: str
        :param key: The `key` associated with the `value` retrieved.
        :type key: str
        :param flags: `flags` associated with the `value` retrieved.
        :type flags: int
        :param cas_unique: The `cas_unique` value, if it was queried, or
            None if no CAS information was retrieved.
        :type cas_unique: int
        :param memcache: The memcache server instance, used for future
            operations on this key.
        :type memcache: :py:class:`~memcache2.ServerConnection`
        :returns: :py:class:`~memcached2.ValueSuperStr` instance
        '''
        data = super(ValueSuperStr, self).__new__(self, value)
        data.key = key
        data.flags = flags
        data.cas_unique = cas_unique
        data.cas_unavailable = False
        data.memcache = memcache

        return data

    def set(self, value, flags=0, exptime=0, update_cas=False):
        '''Update the value in the server.

        The key that was used to retrieve this value is updated in the server.
        If the value was retrieved from the server with `get_cas` enabled,
        then this will update using the CAS.

        :param update_cas: If `True`, a `get()` will be done after the
            `set()`, and if the result is the same as what we set,
            update the CAS value in this object.  This is so that multiple
            updates can be done with CAS set.  This may result in a
            :py:exc:`~memcached2.CASRefreshFailure`.
        :type update_cas: boolean

        See :py:method:`~memcache2.Memcache.set` for more information.

        .. note::

            This does not update this object's value.

        .. note::

            If this object was retrieved with `get_cas` set, then multiple
            updates will trigger a :py:exception:`~memcache2.CASFailure`
            unless `update_cas` is used.

        :raises: :py:exc:`~memcached2.CASFailure`,
            :py:exc:`~memcached2.CASRefreshFailure`
        '''
        if self.cas_unique:
            if self.cas_unavailable:
                raise CASFailure('CAS value already consumed.')
            self.cas_unavailable = True

        retval = self.memcache.set(
            self.key, value, flags=flags, exptime=exptime,
            cas_unique=self.cas_unique)

        if self.cas_unique and update_cas:
            result = self.memcache.get(self.key)
            if result != value:
                raise CASRefreshFailure(
                    'Value from server changed during CAS refresh')
            self.cas_unique = result.cas_unique
            self.cas_unavailable = False

        return retval

    def append(self, value):
        '''Append `value` to the data stored for this key.

        See :py:method:`~memcache2.Memcache.append` for more information.

        .. note::

            This does not update this object's value.

        :raises: :py:exc:`~memcached2.CASFailure`
        '''
        if self.cas_unique is not None:
            raise CASFailure('Not supported with CAS')
        return self.memcache.append(self.key, value)

    def prepend(self, value):
        '''Prepend `value` to the data stored for this key.

        See :py:method:`~memcache2.Memcache.prepend` for more information.

        .. note::

            This does not update this object's value.

        :raises: :py:exc:`~memcached2.CASFailure`
        '''
        if self.cas_unique is not None:
            raise CASFailure('Not supported with CAS')
        return self.memcache.prepend(self.key, value)

    def incr(self, value=1):
        '''Increment the value for this key.

        See :py:method:`~memcache2.Memcache.incr` for more information.

        .. note::

            This does not update this object's value.

        :raises: :py:exc:`~memcached2.CASFailure`
        '''
        if self.cas_unique is not None:
            raise CASFailure('Not supported with CAS')
        return self.memcache.incr(self.key, value)

    def decr(self, value=1):
        '''Decrement the value for this key.

        See :py:method:`~memcache2.Memcache.decr` for more information.

        .. note::

            This does not update this object's value.

        :raises: :py:exc:`~memcached2.CASFailure`
        '''
        if self.cas_unique is not None:
            raise CASFailure('Not supported with CAS')
        return self.memcache.decr(self.key, value)

    def delete(self):
        '''Remove this key from the server.

        See :py:method:`~memcache2.Memcache.delete` for more information.

        :raises: :py:exc:`~memcached2.CASFailure`
        '''
        if self.cas_unique is not None:
            raise CASFailure('Not supported with CAS')
        return self.memcache.delete(self.key)

    def delete_all(self):
        '''Remove this key from all of the the servers.

        See :py:method:`~memcache2.Memcache.delete_all` for more information.

        :raises: :py:exc:`~memcached2.CASFailure`
        '''
        if self.cas_unique is not None:
            raise CASFailure('Not supported with CAS')
        return self.memcache.delete_all(self.key)

    def touch(self, exptime):
        '''Update the expiration time on an item.

        See :py:method:`~memcache2.Memcache.touch` for more information.

        :raises: :py:exc:`~memcached2.CASFailure`
        '''
        if self.cas_unique is not None:
            raise CASFailure('Not supported with CAS')
        return self.memcache.touch(self.key, exptime)


class ValueDictionary(dict):

    '''Encode the response as a dictionary.

    This is a simple dictionary of the result data from the memcache
    server, including keys: "key", "value", "flags", and "cas_unique".
    This is a way of getting additional data from the memcache server
    for use in things like CAS updates.
    '''

    def __init__(self, value, key, flags, cas_unique=None, memcache=None):
        '''Instantiate new instance.

        :param value: The memcache `value`, which is the value of this
            class when treated like a string.
        :type value: str
        :param key: The `key` associated with the `value` retrieved.
        :type key: str
        :param flags: `flags` associated with the `value` retrieved.
        :type flags: int
        :param cas_unique: The `cas_unique` value, if it was queried, or
            None if no CAS information was retrieved.
        :type cas_unique: int
        :param memcache: The memcache server instance, used for future
            operations on this key.
        :type memcache: :py:class:`~memcache2.ServerConnection`
        :returns: :py:class:`~memcached2.ValueSuperStr` instance
        '''
        super(ValueDictionary, self).__init__(
            [
                ['key', key],
                ['value', value],
                ['flags', flags],
                ['cas_unique', cas_unique]
            ])


class HasherBase:

    '''Turn memcache keys into hashes, for use in server selection.

    Normally, the python-memcached2 classes will automatically select a
    hasher to use.  However, for special circumstances you may wish to
    use a different hasher or develop your own.

    This is an abstract base class, here largely for documentation purposes.
    Hasher sub-classes such as :py:class:`~memcached2.HasherZero` and
    :py:class:`~memcached2.HasherCMemcache`, implement a `hash` method
    which does all the work.

    See :py:func:`~memcached2.HasherBase.hash` for details of implementing
    a subclass.
    '''
    def hash(self, key):
        '''Hash a key into a number.

        This must persistently turn a string into the same value.  That value
        is used to determine which server to use for this key.

        :param key: memcache key
        :type key: str
        :returns: int -- Hashed version of `key`.
        '''
        raise NotImplementedError('This class is only meant to be subclassed')


class HasherZero(HasherBase):

    '''Hasher that always returns 0, useful only for
    :py:class:`~memcached2.SelectorFirst`.'''
    def hash(self, key):
        '''See :py:func:`memcached2.HasherBase.hash` for details of
        this function.
        '''
        return 0


class HasherCMemcache(HasherBase):

    '''Hasher compatible with the C memcache hash function.'''
    def hash(self, key):
        '''See :py:func:`memcached2.HasherBase.hash` for details of
        this function.
        '''
        key = _to_bytes(key)
        return ((((crc32(key) & 0xffffffff) >> 16) & 0x7fff) or 1)


class ServerPool:

    '''A pool of servers that connections can be checked in/out from.

    Implements a thread-safe pool for storing server connections.  This
    allows only the active users of sockets to be holding connections.
    Fewer connections may be needed because of this.
    '''

    def __init__(self, reconnector=None):
        '''Initialize ServerPool instance.

        :param reconnector: The reconnector instance used by the pool
                to determine when a re-connection to a server will be
                attempted.  If `None`, a default
                :py:class:`~memcached2.ReconnectorTime` will be created.
        :type reconnector: :py:class:`~memcached2.ReconnectorBase`
        '''
        self.server_pools = {}
        if reconnector is None:
            reconnector = ReconnectorTime()
        self.reconnector = reconnector

    def _add(self, server_uri):
        '''INTERNAL: Add the specified server to the pool list.
        '''
        if server_uri in self.server_pools:
            return
        self.server_pools[server_uri] = queue.Queue()

    def is_available(self, server_uri):
        '''Is the server available for use.  This checks internal state to
        determine if a server should try to be used.

        :param server_uri: The URI for the server to check.
        :type server_uri: str
        :returns: boolean -- True if the server is available for use.
        '''
        self._add(server_uri)

        if not self.server_pools[server_uri].empty():
            return True
        return self.reconnector.connectable(server_uri)

    def get(self, server_uri):
        '''Retrieve a server for use.  Either pulling it from the queue or
        making a new connection object.

        :param server_uri: The URI for the server we need a connection to.
        :type server_uri: str
        :returns: ServerConnection -- Server connection to use.
        '''
        self._add(server_uri)
        pool = self.server_pools[server_uri]

        if not pool.empty():
            try:
                return pool.get_nowait()
            except queue.Empty:
                pass

        server = ServerConnection(server_uri)
        return server

    def put(self, connection):
        '''Return a connection to the pool.

        :param connection: Connection to return to the queue.
        :type connection: :py:class:`~memcache2.ServerConnection`
        '''
        if connection.buffer:
            raise ValueError('Socket returned to pool with data waiting')
        server_uri = connection.uri
        self._add(server_uri)
        self.server_pools[server_uri].put(connection)

    def empty(self):
        '''Release all the server connections currently in the pool.'''
        for pool in self.server_pools.values():
            if pool.empty():
                continue
            try:
                connection = pool.get_nowait()
                connection.reset()
            except queue.Empty:
                pass

    def __del__(self):
        self.empty()


class SelectorBase:

    '''Select which server to use.

    These classes implement a variety of algorithms for determining which
    server to use, based on the key being stored.

    The selection is done based on a `key_hash`, as returned by the
    :py:func:`memcached2.HasherBase.hash` function.

    Normally, the python-memcached2 classes will automatically pick a
    selector to use.  However, for special circumstances you may wish to
    use a specific Selector or develop your own.

    This is an abstract base class, here largely for documentation purposes.
    Selector sub-classes such as :py:class:`~memcached2.SelectorFirst` and
    :py:class:`~memcached2.SelectorRehashDownServers`, implement a `select`
    method which does all the work.

    See :py:func:`~memcached2.SelectorBase.select` for details of implementing
    a subclass.
    '''
    def select(self, server_uri_list, hasher, key, server_pool):
        '''Select a server bashed on the `key_hash`.

        Given the list of servers and a hash of of key, determine which
        of the servers this key is associated with on.

        :param server_uri_list: A list of the server URIs to select among.
        :type server_uri_list: list of server URIs
        :param hasher: Hasher function, such as
            :py:func:`memcache2.HasherBase.hash`.
        :type hasher: :py:func:`memcache2.HasherBase.hash`.
        :param key: The key to hash.
        :type key: str
        :param server_pool: (None) A server connection pool.  If not
            specified, a global pool is used.
        :type server_pool: :py:class:`~memcache2.ServerPool` object.

        :returns: string -- The server_uri to use.
        :raises: :py:exc:`~memcached2.NoAvailableServers`
        '''
        raise NotImplementedError('This class is only meant to be subclassed')


class SelectorFirst(SelectorBase):

    '''Server selector that only returns the first server.  Useful when there
    is only one server to select amongst.
    '''
    def select(self, server_uri_list, hasher, key, server_pool):
        '''See :py:func:`memcached2.SelectorBase.select` for details of
        this function.
        '''
        server_uri = server_uri_list[0]
        if server_pool.is_available(server_uri):
            return server_uri
        raise NoAvailableServers()


class SelectorRehashDownServers(SelectorBase):

    '''Select a server, if it is down re-hash up to `hashing_retries` times.

    This was the default in the original python-memcached module.  If the
    server that a key is housed on is down, it will re-hash the key after
    adding an (ASCII) number of tries to the key and try that server.

    This is most suitable if you want to inter-operate with the old
    python-memcache client.

    If no up server is found after `hashing_retries` attempts,
    :py:exc:`memcached2.NoAvailableServers` is raised.
    '''
    def __init__(self, hashing_retries=10):
        '''
        :param hashing_retries: Retry hashing the key looking for another
            server this many times.
        :type hashing_retries: int
        '''
        self.hashing_retries = hashing_retries

    def select(self, server_uri_list, hasher, key, server_pool):
        '''See :py:func:`memcached2.SelectorBase.select` for details of
        this function.
        '''
        server_uri = server_uri_list[hasher(key) % len(server_uri_list)]
        if server_pool.is_available(server_uri):
            return server_uri

        for i in range(self.hashing_retries):
            server_uri = server_uri_list[
                hasher(key + str(i)) % len(server_uri_list)]
            if server_pool.is_available(server_uri):
                return server_uri

        raise NoAvailableServers()


class SelectorFractalSharding(SelectorBase):

    '''On a down server, re-partition that servers keyspace to other servers.

    This uses an algorithm that basically maps every key in the keyspace to
    a list of the servers that will answer queries for it.  The first
    available server in that list will be used.  The list is such that
    the keys that map to a server when it is up will get distributed across
    other servers evenly, stabally, and predictably.

    I called it Fractal because when a server is down you dig deeper and see a
    new level of complexity in the keyspace mapping.
    '''
    def select(self, server_uri_list, hasher, key, server_pool):
        '''See :py:func:`memcached2.SelectorBase.select` for details of
        this function.
        '''
        key_hash = hasher(key)
        remaining_uris = server_uri_list[:]
        for i in range(len(server_uri_list)):
            position = key_hash % len(remaining_uris)
            server_uri = server_uri_list[position]

            if server_pool.is_available(server_uri):
                return server_uri

            del(remaining_uris[position])

        raise NoAvailableServers()


class SelectorConsistentHashing(SelectorBase):

    '''Predictably select a server, even if its normal server is down.

    This implements the Consistent Hash algorithm as
    http://en.wikipedia.org/wiki/Consistent_hashing

    This is done by splitting the key-space up into a number of buckets
    (more than the number of servers but probably no more than the
    number of servers squared).  See Wikipedia for details on how this
    algorithm operates.

    The downside of this mechanism is that it requires building a fairly
    large table at startup, so it is not suited to short lived code.
    It also is fairly expensive to add and remove servers from the pool
    (not implemented in this code).  Note that it is NOT expensive to
    fail a server, only to completely remove it.
    '''
    def __init__(self, total_buckets=None):
        '''
        :param total_buckets: How many buckets to create.  Smaller values
                decrease the startup overhead, but also mean that a down
                server will tend to not evenly redistribute it's load across
                other servers.  The default value of None means the default
                value of the number of servers squared.
        :type total_buckets: int
        '''
        self.total_buckets = None
        self.buckets = None

    def _initialize_buckets(self, server_uri_list, hasher):
        '''Create the consistent-hashing set of buckets, used for determining
        what server to use.'''

        if self.buckets is not None:
            return
        len_uri_list = len(server_uri_list)
        if not self.total_buckets:
            self.total_buckets = len_uri_list * len_uri_list

        bucket_dict = {}
        for i in range(self.total_buckets):
            bucket_dict[hasher(str(i))] = i % len_uri_list

        self.buckets = sorted(bucket_dict.items())

    def select(self, server_uri_list, hasher, key, server_pool):
        '''See :py:func:`memcached2.SelectorBase.select` for details of
        this function.
        '''
        if self.buckets is None:
            self._initialize_buckets(server_uri_list, hasher)

        hashed_key = hasher(key)
        offset = bisect(self.buckets, (hashed_key, 0))
        len_buckets = len(self.buckets)
        len_uri_list = len(server_uri_list)
        already_tried_uris = set()
        for i in range(len_buckets):
            if len(already_tried_uris) == len_uri_list:
                raise NoAvailableServers()

            bucket_offset = (offset + i) % len_buckets

            #  the last bucket covers things up to the first bucket
            if bucket_offset == 0 and hashed_key < self.buckets[0][0]:
                bucket = self.buckets[-1]
            else:
                bucket = self.buckets[bucket_offset]

            uri_offset = bucket[1]
            server_uri = server_uri_list[uri_offset]
            if uri_offset in already_tried_uris:
                continue
            already_tried_uris.update([uri_offset])

            if server_pool.is_available(server_uri):
                return server_uri

        raise NoAvailableServers()


class ReconnectorBase:

    '''Track server problems and determine when to reconnect.

    This is a base class for classes that handle reconnecting to down
    servers.  This tracks down servers have been having problems and
    determining when to defer connecting and when to retry.

    The Reconnector tracks problems with servers and defers connections
    when it's been having problems.
    '''

    def __init__(self):
        pass

    def connectable(self, server_url):
        '''Is the specified server connectable?

        :returns: boolean -- Returns whether a connection should be initiated.

        :param server_url: Server to consult about connectability.
        :type server_url: string: URL of server.
        '''
        raise NotImplementedError()

    def had_error(self, server_url):
        '''Called to report to the reconnector when there is an error on
        a server.

        :param server_url: Server related to this report.
        :type server_url: string: URL of server.
        '''
        raise NotImplementedError()

    def had_success(self, server_url):
        '''Called when a successful communication occurs with a server.

        :param server_url: Server related to this report.
        :type server_url: string: URL of server.
        '''
        raise NotImplementedError()


class ReconnectorSimple(ReconnectorBase):

    '''This is a simple reconnector that immediately tries connections on
    down servers.
    '''
    def __init__(self):
        ReconnectorBase.__init__(self)
        self.server_data = {}

    def connectable(self, server_url):
        '''See :py:func:`memcached2.ReconnectorBase.connectable` for
        details of this function.
        '''
        return True

    def had_error(self, server_url):
        '''See :py:func:`memcached2.ReconnectorBase.error` for details of
        this function.
        '''
        pass

    def had_success(self, server_url):
        '''See :py:func:`memcached2.ReconnectorBase.had_success` for
        details of this function.
        '''
        pass


class ReconnectorTime(ReconnectorBase):

    '''Try reconnecting to a server after a specific number of seconds
    since the last failed operation.
    '''

    def __init__(self, timeout=30):
        '''
        :param timeout: Number of seconds since last error.
        :type timeout: float
        '''
        ReconnectorBase.__init__(self)
        self.timeout = timeout
        self.last_error = {}

    def connectable(self, server_url):
        '''See :py:func:`memcached2.ReconnectorBase.connectable` for
        details of this function.
        '''
        last_error = self.last_error.get(server_url)
        if last_error is None:
            return True
        now = time.time()
        if now - last_error >= self.timeout:
            self.had_success(server_url)
            return True
        return False

    def had_error(self, server_url):
        '''See :py:func:`memcached2.ReconnectorBase.error` for details of
        this function.
        '''
        if self.last_error.get(server_url) is not None:
            return
        self.last_error[server_url] = time.time()

    def had_success(self, server_url):
        '''See :py:func:`memcached2.ReconnectorBase.had_success` for
        details of this function.
        '''
        del self.last_error[server_url]


#  the global pool object
_global_pool = ServerPool()


class Memcache:

    '''
    Create a new memcache connection, to the specified servers.

    The list of servers, specified by URL, are consulted based on the
    hash of the key, effectively "sharding" the key space.

    This is a low-level memcache interface.  This interface will raise
    exceptions when backend connections occur, allowing a program full
    control over handling of connection problems.

    Example:

    >>> from memcached2 import *                               # noqa
    >>> mc = Memcache(['memcached://localhost:11211/'])
    >>> mc.set('foo', 'bar')
    >>> mc.get('foo')
    'bar'

    Extensive examples including demonstrations of the statistics output
    is available in the documentation for
    :ref:`Memcache Examples <memcache-examples>`

    '''

    def __init__(
            self, servers, value_wrapper=None, selector=None, hasher=None,
            server_pool=None):
        '''
        :param servers: One or more server URIs of the form:
            "memcache://hostname[:port]/"
        :type servers: list
        :param value_wrapper: (None)  This causes values returned to be
            wrapped in the passed class before being returned.  For example
            :py:class:`~memcache2.ValueSuperStr` implements many useful
            additions to the string return.
        :type value_wrapper: :py:class:`~memcache2.ValueSuperStr` or
            compatible object.
        :param selector: (None)  This code implements the server selector
            logic.  If not specified, the default is used.  The default
            is to use :py:class:`~memcached2.SelectorFirst` if only one
            server is specified, and
            :py:class:`~memcached2.SelectorRehashDownServers`
            if multiple servers are given.
        :type selector: :py:class:`~memcache2.SelectorBase`
        :param hasher: (None) A "Hash" object which takes a key and returns
            a hash for persistent server selection.  If not specified, it
            defaults to :py:class:`~memcache2.HasherZero` if there is only
            one server specified, or :py:class:`~memcache2.HasherCMemcache`
            otherwise.
        :type hasher: :py:class:`~memcache2.HasherBase`
        :param server_pool: (None) A server connection pool.  If not
            specified, a global pool is used.
        :type server_pool: :py:class:`~memcache2.ServerPool` object.
        '''

        self.server_uris = servers

        self.value_wrapper = value_wrapper

        if hasher is not None:
            self.hasher = hasher
        else:
            if len(self.server_uris) < 2:
                self.hasher = HasherZero()
            else:
                self.hasher = HasherCMemcache()

        if selector is not None:
            self.selector = selector
        else:
            if len(self.server_uris) < 2:
                self.selector = SelectorFirst()
            elif len(self.server_uris) == 2:
                self.selector = SelectorRehashDownServers()
            else:
                self.selector = SelectorFractalSharding()

        if server_pool is not None:
            self.server_pool = server_pool
        else:
            global _global_pool
            self.server_pool = _global_pool

    def __del__(self):
        self.close()

    def _select_server(self, key):
        '''INTERNAL: Given a key, find the server that serves that key.

        :returns: :py:class:`~memcached2.ServerConnection` -- The server object
            that the command was sent to.
        '''
        return self.selector.select(self.server_uris, self.hasher.hash, key,
                                    self.server_pool)

    def _send_command(self, command, key=None, server=None):
        '''INTERNAL: Send a command to a server.

        :param command: The memcache-protocol command to send to the
            server, a string terminated with CR+NL.
        :type command: str
        :param key: The key within the command, used to determine what
            server to send the command to.  If None, `server` is expected
            to be set.
        :type key: str or None
        :param server: Server to send the command on, if provided, the key
            is not used for server selection.
        :type server: :py:class:`~memcached2.ServerConnection` instance
            or None.  If None, `key` is expected to be set to select which
            server to send the command to.
        :returns: :py:class:`~memcached2.ServerConnection` -- The server object
            that the command was sent to.
        :raises: :py:exc:`~memcached2.NoAvailableServers`,
            :py:exc:`~memcached2.ServerDisconnect`
        '''
        if not server:
            server = self.server_pool.get(self._select_server(key))

        command = _to_bytes(command)
        try:
            server.send_command(command)
        except ConnectionResetError:
            server.reset()
            raise ServerDisconnect('ConnectionResetError')
        except BrokenPipeError:
            server.reset()
            raise ServerDisconnect('BrokenPipeError')

        return server

    def _keys_by_server(self, keys):
        '''Hash a bunch of keys and return them grouped by server.

        The hashing function is called on each key to determine which server
        houses it.  The keys are grouped by what server they belong to.
        This is so that commands that operate on multiple keys at the same
        time can be sent to the correct server, such as with the memcached
        "get" and "gets" commands.

        Example:

            >>> mcd._keys_by_server(('a', 'b', 'c', 'd'))    # noqa
            [(<ServerConnection 1>, ['a', 'c']),
             (<ServerConnection 2>, ['b', 'd']))

        :param keys: Keys to be hashed to determine the server they are
                associated with.
        :type keys: A list of strs.
        :returns: A list of `(server_uri,keys)` pairs, where the `keys` is a
                list of keys associated with that server.
        '''
        server_map = collections.defaultdict(list)
        for key in keys:
            server = self._select_server(key)
            server_map[server].append(key)
        return server_map.items()

    def _get_parser(self, server):
        '''Read the results of a get command from the server.

        This is meant to be called after a get/gets command and parses the
        results.  This must be called repeatedly until it returns
        `(None,None)`, at which point the results have been fully consumed.

        :returns: A tuple of `(key,value)`, or `(None,None)` if "END"
                was received.
        :raises: :py:exc:`NotImplementedError`
        '''
        data = server.read_until()
        if data == 'END\r\n':
            return None, None

        if not data.startswith('VALUE'):
            server.reset()
            raise NotImplementedError(
                'Unknown response: {0}'.format(repr(data[:30])))

        value_data = data.rstrip().split()[1:]

        key = value_data[0]
        flags = int(value_data[1])
        length = int(value_data[2])
        body = server.read_length(length)

        if len(value_data) > 3:
            cas_unique = int(value_data[3])
        else:
            cas_unique = None

        data = server.read_until()   # trailing termination
        if data != '\r\n':
            server.reset()
            raise NotImplementedError(
                'Unexpected response when looking for terminator: {0}'
                .format(data))

        if self.value_wrapper:
            return key, self.value_wrapper(
                body, key, flags, cas_unique, memcache=self)
        return key, body

    def get(self, key, get_cas=False):
        '''Retrieve the specified key from a memcache server.

        :param key: The key to lookup in the memcache server.
        :type key: str
        :param get_cas: If True, the "cas unique" is queried and the return
            object has the "cas_unique" attribute set.
        :type get_cas: bool
        :returns: String, or "value_wrapper" as specified during object
            creation such as `~memcached2.ValueSuperStr`.
        :raises: :py:exc:`~memcached2.NoValue`, :py:exc:`NotImplementedError`,
            :py:exc:`~memcached2.NoAvailableServers`
        '''

        command = 'get {0}\r\n'
        if get_cas:
            command = 'gets {0}\r\n'
        with self._send_command(
                command.format(key), key).managed_pool(
                self.server_pool) as server:
            key, value = self._get_parser(server)
            if key is None:
                server.reset()
                raise NoValue()

            data = server.read_until()
            if data != 'END\r\n':
                raise NotImplementedError(
                    'Unknown response: {0}'.format(repr(data[:30])))

        return value

    def get_multi(self, keys, get_cas=False):
        '''Retrieve the specified keys from a memcache server.

        This will determine the servers that the different keys are on, and
        send a request for all the specified keys that are on that server
        as a single request.  All the results are correlated and returned.

        :param keys: The keys to lookup in the memcache server.
        :type keys: list of str
        :param get_cas: If True, the "cas unique" is queried and the return
            object has the "cas_unique" attribute set.
        :type get_cas: bool
        :returns: Dictionary of keys, the associated value from the cache
            is str, or "value_wrapper" as specified during object creation
            such as `~memcached2.ValueSuperStr`.
        :raises: :py:exc:`NotImplementedError`,
            :py:exc:`~memcached2.NoAvailableServers`
        '''

        command = 'get {0}\r\n'
        if get_cas:
            command = 'gets {0}\r\n'

        results = {}

        for server_uri, server_keys in self._keys_by_server(keys):
            key_str = ' '.join(server_keys)
            server = self.server_pool.get(server_uri)
            self._send_command(command.format(key_str), server=server)

            while True:
                key, value = self._get_parser(server)
                if key is None:
                    break

                results[key] = value

            self.server_pool.put(server)

        return results

    def set(self, key, value, flags=0, exptime=0, cas_unique=None):
        '''Set a key to the value in the memcache server.

        :param key: Key used to store value in memcache server and hashed to
            determine which server is used.
        :type key: str
        :param value: Value stored in memcache server for this key.
        :type value: str
        :param flags: If specified, the same value will be provided on
                :func:`get`.
        :type flags: int (32 bits)
        :param exptime: If non-zero, it specifies the expriation time, in
            seconds, for this value.
        :type exptime: int
        :param cas_unique: If specified as the 64-bit integer from
            :py:func:`~memcached2.Memcache.get` with `cas_unique=True`, the
            value is only stored if the value has not been updated since
            the :py:func:`~memcached2.Memcache.get` call.
        :type cas_unique: int (64 bits)
        '''
        if cas_unique is not None:
            command = 'cas {0} {1} {2} {3} {4}\r\n'.format(
                key, flags, exptime, len(value),
                cas_unique) + value + '\r\n'
        else:
            command = 'set {0} {1} {2} {3}\r\n'.format(
                key, flags, exptime, len(value)) + value + '\r\n'
        self._storage_command(command, key)

    def _buffer_data(
            self, output_buffers, to_send, base_options,
            nonblocking_servers, expected_keys):
        '''INTERNAL: Add data to the output buffer.

        This is used by the :py:func:`~memcached2.Memcache.set_multi` code to
        buffer multi data for the servers.
        '''

        #  set up
        key = to_send[0]
        value = to_send[1]
        if len(to_send) == 2:
            key_options = base_options
        else:
            key_options = base_options.copy()
            base_options.update(to_send[2])

        #  find server for command
        server_uri = self._select_server(key)
        if not server_uri in nonblocking_servers:
            server = self.server_pool.get(self._select_server(key))
            server.connect()
            nonblocking_servers[server_uri] = server
            server.setblocking(False)
            output_buffers[server] = bytearray()
            expected_keys[server] = []
        server = nonblocking_servers[server_uri]

        #  add command to output buffer
        output_buffer = output_buffers[server]
        expected_keys[server].append(key)
        if key_options['cas_unique'] is not None:
            output_buffer.extend(_to_bytes(
                'cas {0} {1} {2} {3} {4}\r\n'.format(
                    key, key_options['flags'], key_options['exptime'],
                    len(value),
                    key_options['cas_unique'])))
        else:
            output_buffer.extend(_to_bytes(
                'set {0} {1} {2} {3}\r\n'.format(
                    key, key_options['flags'], key_options['exptime'],
                    len(value))))
        output_buffer.extend(_to_bytes(value))
        output_buffer.extend(_to_bytes('\r\n'))

    def set_multi(
            self, data, options={},
            return_successful=True, return_failed=True):
        '''Set many key/value pairs at once.
        This produces pipelining of the multiple set operations, to get
        maximum performance.

        *Note*: If a client error occurs, such as too long a key being sent,
        the remainder of that data block will be discarded and the connection
        to the server reset.

        :param data: A list of (key,value) pairs, for example as produced
                by dict.items().  Optionally, a third element may be a
                dictionary containing the options for this key, as in the
                `options` argument.
        :type data: list of tuples
        :param options: Default options for all keys set, a dictionary with
                keys: 'flags', 'exptime', and 'cas_unique'.  See
                :py:func:`~memcached2.Memcache.set` for descriptions of these
                items.
        :type options: dict
        :param return_successful: If True, the returned dictionary includes
                keys that were successfully set (with the value `None`).
                Default is True.
        :type return_successful: boolean
        :param return_failed: If True, the returned dictionary includes
                keys that received a non-successful storage result.  The
                value in the return data is the server response.
                Default is True.
        :type return_successful: boolean
        :returns: dict -- Dictionary of keys that were stored and the
                result code, depending on the values of `return_successful`
                and `return_failed`.

        :raises: :py:exc:`MultiStorageException`
        '''

        output_buffers = {}
        expected_keys = {}
        results = {}
        nonblocking_servers = {}
        send_threshold = 180224
        send_minimum = send_threshold
        deferred_exception = None

        base_options = {
            'flags': 0,
            'exptime': 0,
            'cas_unique': None}
        base_options.update(options)

        for to_send in data:
            self._buffer_data(
                output_buffers, to_send, base_options,
                nonblocking_servers, expected_keys)

            #  send data and read any ready data
            if [
                    x for x in output_buffers.values()
                    if len(x) >= send_threshold]:
                had_exception = _server_interaction(
                    output_buffers, send_threshold, send_minimum,
                    expected_keys, results,
                    return_successful, return_failed)
                if had_exception:
                    deferred_exception = had_exception

        #  complete interaction with servers
        while (_dictionary_values_empty(output_buffers)
                or _dictionary_values_empty(expected_keys)):
            had_exception = _server_interaction(
                output_buffers, 0, 0, expected_keys, results,
                return_successful, return_failed)
            if had_exception:
                deferred_exception = had_exception

        for server in nonblocking_servers.values():
            server.setblocking(True)
            self.server_pool.put(server)

        if deferred_exception:
            raise deferred_exception

        return results

    def cache(self, key, function, *args, **kwargs):
        '''Cached wrapper around function.

        Check for `key` in the cache, and if it's not there, call
        `function(key)`, store the return value in the cache under `key`.

        :param key: Key in the memcache server(s) for get/set.
        :type key: str
        :param function: A function or (other callable) which will be
            called with `key` as an argument if the key is not able to be
            looked up in the memcache.
        :type function: callable
        :param *args: Additional arguments for `function`.
        :type *args: Arguments
        :param **kwargs: Additional keyword arguments for `function`.
        :type **kwargs: Keyword arguments
        :returns: str -- Data from cache, or by calling the function.
        '''
        try:
            return self.get(key)
        except NoValue:
            data = function(key, *args, **kwargs)
            self.set(key, data)
            return data

    def add(self, key, value, flags=0, exptime=0):
        '''Store, but only if the server doesn't already hold data for it.

        :param key: Key used to store value in memcache server and hashed to
            determine which server is used.
        :type key: str
        :param value: Value stored in memcache server for this key.
        :type value: str
        :param flags: If specified, the same value will be provided on
                :func:`get`.
        :type flags: int (32 bits)
        :param exptime: If non-zero, it specifies the expriation time, in
            seconds, for this value.
        :type exptime: int
        '''
        command = 'add {0} {1} {2} {3}\r\n'.format(
            key, flags, exptime, len(value)) + value + '\r\n'
        self._storage_command(command, key)

    def replace(self, key, value, flags=0, exptime=0):
        '''Store data, but only if the server already holds data for it.

        :param key: Key used to store value in memcache server and hashed to
            determine which server is used.
        :type key: str
        :param value: Value stored in memcache server for this key.
        :type value: str
        :param flags: If specified, the same value will be provided on
                :func:`get`.
        :type flags: int (32 bits)
        :param exptime: If non-zero, it specifies the expriation time, in
            seconds, for this value.
        :type exptime: int
        '''
        command = 'replace {0} {1} {2} {3}\r\n'.format(
            key, flags, exptime, len(value)) + value + '\r\n'
        self._storage_command(command, key)

    def append(self, key, value):
        '''Store data after existing data associated with this key.

        :param key: Key used to store value in memcache server and hashed to
            determine which server is used.
        :type key: str
        :param value: Value stored in memcache server for this key.
        :type value: str
        '''
        command = 'append {0} 0 0 {1}\r\n'.format(
            key, len(value)) + value + '\r\n'
        self._storage_command(command, key)

    def prepend(self, key, value):
        '''Store data before existing data associated with this key.

        :param key: Key used to store value in memcache server and hashed to
            determine which server is used.
        :type key: str
        :param value: Value stored in memcache server for this key.
        :type value: str
        '''
        command = 'prepend {0} 0 0 {1}\r\n'.format(
            key, len(value)) + value + '\r\n'
        self._storage_command(command, key)

    def delete(self, key):
        '''Delete the key if it exists.

        :param key: Key used to store value in memcache server and hashed to
            determine which server is used.
        :type key: str
        :returns: Boolean indicating if key was deleted.
        :raises: :py:exc:`NotImplementedError`,
            :py:exc:`~memcached2.NoAvailableServers`
        '''
        command = 'delete {0}\r\n'.format(key)

        with self._send_command(
                command, key).managed_pool(self.server_pool) as server:
            server = self._send_command(command, key)
            data = server.read_until()

        if data == 'DELETED\r\n':
            return True
        if data == 'NOT_FOUND\r\n':
            return False

        raise NotImplementedError(
            'Unknown return data from server: "{0}"'.format(repr(data)))

    def _all_available_server_uris(self):
        '''INTERNAL: Return a list of all server URIs that are available.
        '''
        return [x for x in self.server_uris
                if self.server_pool.is_available(x)]

    def delete_all(self, key):
        '''Delete the key from all servers if it exists.

        This might be used in the case where you want to ensure that any
        future topology changes will be less likely to pick up any old data.

        If the key is not found, :py:exc:`~memcached2.NotFound` is raised.

        :param key: Key used to store value in memcache servers.
        :type key: str
        :returns: Boolean indicating if key was deleted.
        :raises: :py:exc:`~memcached2.NotFound`, :py:exc:`NotImplementedError`,
            :py:exc:`~memcached2.NoAvailableServers`
        '''
        command = 'delete {0}\r\n'.format(key)

        found_key = False
        for server in [self.server_pool.get(x)
                       for x in self._all_available_server_uris()]:
            self._send_command(command, server=server)

            data = server.read_until()
            self.server_pool.put(server)

            if data == 'DELETED\r\n':
                found_key = True
            elif data == 'NOT_FOUND\r\n':
                pass
            else:
                raise NotImplementedError(
                    'Unknown return data from server: "{0}"'
                    .format(repr(data)))

        return found_key

    def touch(self, key, exptime):
        '''Update the expiration time on an item.

        :param key: Key used to store value in memcache server and hashed to
            determine which server is used.
        :type key: str
        :param exptime: If non-zero, it specifies the expriation time, in
            seconds, for this value.  Note that setting exptime=0 causes the
            item to not expire based on time.
        :type exptime: int
        :raises: :py:exc:`~memcached2.NotFound`, :py:exc:`NotImplementedError`,
            :py:exc:`~memcached2.NoAvailableServers`
        '''
        command = 'touch {0} {1}\r\n'.format(key, exptime)

        server = self._send_command(command, key)
        data = server.read_until()
        self.server_pool.put(server)

        if data == 'TOUCHED\r\n':
            return
        if data == 'NOT_FOUND\r\n':
            raise NotFound()

        raise NotImplementedError(
            'Unknown return data from server: "{0}"'.format(repr(data)))

    def _reconnect_all(self):
        '''INTERNAL: Attempt to connect to all backend servers.'''
        for server in [self.server_pool.get(x)
                       for x in self._all_available_server_uris()]:
            server.connect()
            self.server_pool.put(server)

    def flush_all(self):
        '''Flush the memcache servers.

        .. note::

            An attempt is made to connect to all backend servers
            before running this command.

        :raises: :py:exc:`NotImplementedError`
        '''
        self._reconnect_all()
        for server in [self.server_pool.get(x)
                       for x in self._all_available_server_uris()]:
            server.send_command('flush_all\r\n')
            data = server.read_until()
            self.server_pool.put(server)

            if data != 'OK\r\n':
                raise NotImplementedError(
                    'Unknown return data from server: "{0}"'
                    .format(repr(data)))

    def _run_multi_server(self, function):
        '''INTERNAL: Run statistics `function()` on each server.

        This function iterates over all servers, and gathers statistics
        via the `function` function if the server is up.  If it is not
        up, the value `None` is used for that servers statistics.

        :param function: A function which takes a
            :py:class:`~memcached2.ServerConnection` object as an argument,
            and returns statistics data for that server.
        :type function: :py:class:`~memcached2.ServerConnection` object
        :returns: list -- A list where each element represents the statistics
            from the server specified in the same list position when the
            :py:class:`~memcached2.Memcache` object was created.  If the
            server is down, `None` is put in place of that servers statistics.
        '''
        results = []
        self._reconnect_all()
        for server in [self.server_pool.get(x)
                       for x in self._all_available_server_uris()]:
            stats = None
            if server.backend:
                stats = function(server)
            self.server_pool.put(server)
            results.append(stats)
        return results

    def stats(self):
        '''Get general statistics about memcache servers.

        Examples of the results of this function is available in the
        documentation as
        :ref:`Memcache Statistics Examples <memcached-stats-examples>`

        .. note::

            An attempt is made to connect to all servers before issuing
            this command.

        :returns: list --
            The statistics data is a dictionary of key/value pairs representing
            information about the server.

            This data is returned as a list of statistics, one item for
            each server.  If the server is not connected, `None` is returned
            for its position, otherwise data as mentioned above.
        '''
        def query(server):
            command = 'stats\r\n'
            server.send_command(command)
            stats = {}
            while True:
                data = _from_bytes(server.read_until())
                if data == 'END\r\n':
                    break
                prefix, key, value = data.strip().split()
                if prefix != 'STAT':
                    raise NotImplementedError(
                        'Unknown stats data: {0}'.format(repr(data)))
                if key in (
                        [
                            'pid', 'uptime', 'time', 'pointer_size',
                            'curr_items', 'total_items', 'bytes',
                            'curr_connections', 'total_connections',
                            'connection_structures', 'reserved_fds',
                            'cmd_get', 'cmd_set', 'cmd_flush',
                            'cmd_hits', 'cmd_misses', 'delete_misses',
                            'delete_hits', 'incr_misses', 'incr_hits',
                            'decr_misses', 'decr_hits', 'cas_misses',
                            'cas_hits', 'cas_badval', 'touch_hits',
                            'touch_misses', 'auth_cmds', 'auth_errors',
                            'evictions', 'reclaimed', 'bytes_read',
                            'bytes_written', 'limit_maxbytes', 'threads',
                            'conn_yields', 'hash_power_level', 'hash_bytes',
                            'expired_unfetched', 'evicted_unfetched',
                            'slabs_moved'
                        ]):
                    value = int(value)
                if key in ['rusage_user', 'rusage_system']:
                    value = float(value)
                stats[key] = value

            return stats

        return self._run_multi_server(query)

    def stats_items(self):
        '''Get statistics about item storage per slab class from the
        memcache servers.

        Examples of the results of this function is available in the
        documentation as
        :ref:`Memcache Statistics Examples <memcached-stats-examples>`

        .. note::

            An attempt is made to connect to all servers before issuing
            this command.

        :returns: list --
            The statistic information is a dictionary keyed by the "slab
            class", with the value another dictionary of key/value pairs
            representing the slab information.

            This data is returned as a list of statistics, one item for
            each server.  If the server is not connected, None is returned
            for its position, otherwise data as mentioned above.
        '''
        def query(server):
            command = 'stats items\r\n'
            server.send_command(command)
            stats = {}
            while True:
                data = _from_bytes(server.read_until())
                if data == 'END\r\n':
                    break
                prefix, key, value = data.strip().split()
                if prefix != 'STAT':
                    raise NotImplementedError(
                        'Unknown stats data: {0}'.format(repr(data)))
                prefix, slab_key, stat_key = key.split(':')
                if prefix != 'items':
                    raise NotImplementedError(
                        'Unknown stats item: {0}'.format(repr(key)))
                if not slab_key in stats:
                    stats[slab_key] = {}
                if stat_key in (
                        [
                            'number', 'age', 'evicted', 'evicted_nonzero',
                            'evicted_time', 'outofmemory', 'tailrepairs',
                            'reclaimed', 'expired_unfetched',
                            'evicted_unfetched'
                        ]):
                    value = int(value)
                stats[slab_key][stat_key] = value

            return stats

        return self._run_multi_server(query)

    def stats_slabs(self):
        '''Gets information about each of the slabs created during memcached
        runtime.  Returns a dictionary of slab IDs, each contains a dictionary
        of key/value pairs for that slab.

        Examples of the results of this function is available in the
        documentation as
        :ref:`Memcache Statistics Examples <memcached-stats-examples>`

        .. note::

            An attempt is made to connect to all servers before issuing
            this command.

        :returns: list --
            The statistic information is a dictionary keyed by the "slab
            class", with the value another dictionary of key/value pairs
            representing statistic information about each of the slabs
            created during the memcace runtime.

            This data is returned as a list of statistics, one item
            for each server.  If the server is not connected, None is
            returned for its position, otherwise data as mentioned above.
        '''
        def query(server):
            command = 'stats slabs\r\n'
            server.send_command(command)
            stats = {'slabs': {}}
            while True:
                data = _from_bytes(server.read_until())
                if data == 'END\r\n':
                    break
                prefix, key, value = data.strip().split()
                if prefix != 'STAT':
                    raise NotImplementedError(
                        'Unknown stats data: {0}'.format(repr(data)))

                if ':' in key:
                    slab_key, stat_key = key.split(':')
                    if not slab_key in stats['slabs']:
                        stats['slabs'][slab_key] = {}
                    if stat_key in (
                            [
                                'chunk_size', 'chunks_per_page',
                                'total_pages', 'total_chunks',
                                'used_chunks', 'free_chunks',
                                'free_chunks_end', 'mem_requested',
                                'get_hits', 'cmd_set', 'delete_hits',
                                'incr_hits', 'decr_hits', 'cas_hits',
                                'cas_badval', 'touch_hits'
                            ]):
                        value = int(value)
                    stats['slabs'][slab_key][stat_key] = value
                else:
                    if key in ['active_slabs', 'total_malloced']:
                        value = int(value)
                    stats[key] = value

            return stats

        return self._run_multi_server(query)

    def stats_settings(self):
        '''Gets statistics about settings (primarily from processing
        command-line arguments).

        Examples of the results of this function is available in the
        documentation as
        :ref:`Memcache Statistics Examples <memcached-stats-examples>`

        .. note::

            An attempt is made to connect to all servers before issuing
            this command.

        :returns: list --
            The statistic information is a dictionary of key/value pairs.

            This data is returned as a list of statistics, one item for
            each server.  If the server is not connected, None is returned
            for its position, otherwise data as mentioned above.
        '''
        def query(server):
            command = 'stats settings\r\n'
            server.send_command(command)
            stats = {}
            while True:
                data = _from_bytes(server.read_until())
                if data == 'END\r\n':
                    break
                prefix, key, value = data.strip().split()
                if prefix != 'STAT':
                    raise NotImplementedError(
                        'Unknown stats data: {0}'.format(repr(data)))
                if key in (
                        [
                            'maxbytes', 'maxconns', 'tcpport', 'udpport',
                            'verbosity', 'oldest', 'umask', 'chunk_size',
                            'num_threads', 'num_threads_per_udp',
                            'reqs_per_event', 'tcp_backlog',
                            'item_size_max', 'hashpower_init'
                        ]):
                    value = int(value)
                if key in ['growth_factor']:
                    value = float(value)
                if key in (
                        [
                            'maxconns_fast', 'slab_reassign',
                            'slab_automove', 'detail_enabled', 'cas_enabled'
                        ]):
                    value = _to_bool(value)
                stats[key] = value

            return stats

        return self._run_multi_server(query)

    def stats_sizes(self):
        '''Get statistics about object sizes.

        Examples of the results of this function is available in the
        documentation as
        :ref:`Memcache Statistics Examples <memcached-stats-examples>`

        .. warning::

            This operation locks the cache while it iterates over all
            objects.  Returns a list of (size,count) tuples received
            from the server.

        .. note::

            An attempt is made to connect to all servers before issuing
            this command.

        :returns: list --
            Each statistic is a dictionary of of size:count where the size is
            rounded up to 32-byte ranges.

            This data is returned as a list of statistics, one item for
            each server.  If the server is not connected, None is returned
            for its position, otherwise data as mentioned above.
        '''
        def query(server):
            command = 'stats sizes\r\n'
            server.send_command(command)
            stats = []
            while True:
                data = _from_bytes(server.read_until())
                if data == 'END\r\n':
                    break
                prefix, key, value = data.strip().split()
                if prefix != 'STAT':
                    raise NotImplementedError(
                        'Unknown stats data: {0}'.format(repr(data)))
                stats.append((int(key), int(value)))

            return stats

        return self._run_multi_server(query)

    def incr(self, key, value=1):
        '''Increment the value for the key, treated as a 64-bit unsigned value.

        :param key: Key used to store value in memcache server and hashed to
            determine which server is used.
        :type key: str
        :param value: A numeric value (default=1) to add to the existing value.
        :type value: int (64 bit)
        :returns: int -- (64 bits) The new value after the increment.
        :raises: :py:exc:`~memcached2.NotFound`,
            :py:exc:`~memcached2.NonNumeric`, :py:exc:`NotImplementedError`
        '''
        command = 'incr {0} {1}\r\n'.format(key, value)
        return self._incrdecr_command(command, key)

    def decr(self, key, value=1):
        '''Decrement the value for the key, treated as a 64-bit unsigned value.

        :param key: Key used to store value in memcache server and hashed to
            determine which server is used.
        :type key: str
        :param value: A numeric value (default=1) to add to the existing value.
        :type value: int (64 bit)
        :returns: int -- (64 bits) The new value after the decrement.
        :raises: :py:exc:`~memcached2.NotFound`,
            :py:exc:`~memcached2.NonNumeric`, :py:exc:`NotImplementedError`
        '''
        command = 'decr {0} {1}\r\n'.format(key, value)
        return self._incrdecr_command(command, key)

    def _incrdecr_command(self, command, key):
        '''INTERNAL: Increment/decrement command back-end.

        :param command: The memcache-protocol command to send to the
            server, a string terminated with CR+NL.
        :type command: str
        :param key: The key within the command, used to determine what
            server to send the command to.
        :type key: str
        :raises: :py:exc:`~memcached2.NotFound`,
            :py:exc:`~memcached2.NonNumeric`, :py:exc:`NotImplementedError`,
            :py:exc:`~memcached2.NoAvailableServers`
        '''
        server = self._send_command(command, key)
        data = server.read_until()
        self.server_pool.put(server)

        #  <NEW_VALUE>\r\n
        if data[0] in '0123456789':
            return int(data.strip())
        if data == 'NOT_FOUND\r\n':
            raise NotFound()
        client_error = (
            'CLIENT_ERROR cannot increment or decrement '
            'non-numeric value\r\n')
        if data == client_error:
            raise NonNumeric()

        raise NotImplementedError(
            'Unknown return data from server: "{0}"'.format(repr(data)))

    def _storage_command(self, command, key):
        '''INTERNAL: Send storage command to server and parse results.

        :param command: The memcache-protocol command to send to the
            server, a string terminated with CR+NL.
        :type command: str
        :param key: The key within the command, used to determine what
            server to send the command to.
        :type key: str
        :raises: :py:exc:`~memcached2.NotFound`,
            :py:exc:`~memcached2.NotStored`, :py:exc:`~memcached2.CASFailure`,
            :py:exc:`NotImplementedError`,
            :py:exc:`~memcached2.NoAvailableServers`
        '''
        server = self._send_command(command, key)
        data = server.read_until()
        self.server_pool.put(server)

        if data == 'STORED\r\n':
            return
        if data == 'NOT_STORED\r\n':
            raise NotStored()
        if data == 'EXISTS\r\n':
            raise CASFailure()
        if data == 'NOT FOUND\r\n':
            raise NotFound()

        raise NotImplementedError(
            'Unknown return data from server: "{0}"'.format(repr(data)))

    def close(self):
        '''Close the connection to all the backend servers.
        '''

        for server in [self.server_pool.get(x)
                       for x in self._all_available_server_uris()]:
            server.reset()
            self.server_pool.put(server)


class ExceptionsAreMissesMemcache(Memcache):

    '''
    A :py:class:`~memcached.Memcache` wrapper class which swallows
    server exceptions, except in the case of coding errors.  This is
    meant for situations where you want to keep the code simple, and
    treat cache misses, server errors, and the like as cache misses.
    See :py:func:`memcached2.Memcache` for details of the use of this
    class, exceptions to that are noted here.

    The methods that are protected against exceptions are those
    documented in this class.  Everything should otherwise act like
    a :py:class:`~memcached2.Memcache` instance.
    '''
    def get(self, *args, **kwargs):
        '''Retrieve the specified key from a memcache server.

        Exceptions are swallowed and treated as memcached misses.
        See :py:func:`~memcached2.Memcache.get` for details on this
        method.  Changes from the base function are:

        :returns: None if no value or exception, String, or "value_wrapper"
            as specified during object creation such as
            `~memcached2.ValueSuperStr`.
        :raises: Exceptions are swallowed and treated a misses.
        '''
        try:
            return Memcache.get(self, *args, **kwargs)
        except (NoValue, ServerDisconnect, NotStored, NotFound, CASFailure):
            return None

    def set(self, *args, **kwargs):
        '''Update the value in the server.
        See :py:func:`~memcached2.Memcache.set` for details on this
        method.  Changes from the base function are:

        Exceptions are swallowed and treated as memcached misses.
        See :py:func:`~memcached2.Memcache.set` for details on this
        method.  Changes from the base function are:

        :raises: Exceptions are swallowed and treated a misses.
        '''
        try:
            return Memcache.set(self, *args, **kwargs)
        except (ServerDisconnect, NotStored, NotFound, CASFailure):
            return None

    def set_multi(self, *args, **kwargs):
        '''Update multiple values in the server.
        See :py:func:`~memcached2.Memcache.set_multi` for details on this
        method.  Changes from the base function are:

        Exceptions are swallowed and treated as memcached misses.
        See :py:func:`~memcached2.Memcache.set` for details on this
        method.  Changes from the base function are:

        :raises: Exceptions are swallowed and treated a misses.
        '''
        try:
            return Memcache.set_multi(self, *args, **kwargs)
        #  ServerDisconnect here too?  Maybe should happen in set_multi.
        except MultiStorageException as e:
            return e.results

    def delete(self, *args, **kwargs):
        '''Remove this key from the server.

        Exceptions are swallowed and treated as memcached misses.
        See :py:func:`~memcached2.Memcache.delete` for details on this
        method.  Changes from the base function are:

        :raises: Exceptions are swallowed and treated a misses.
        '''
        try:
            return Memcache.delete(self, *args, **kwargs)
        except (ServerDisconnect, NoAvailableServers):
            return False


class ServerConnection:

    '''Low-level communication with the memcached server.

    Data should be passed in as strings, and that is converted to `bytes`
    for sending to the backend, encoded as ASCII, if necessary.  Data
    returned is likewise converted from `bytes`, also encoded as ASCII,
    if necessary.

    This implments the connection to a server, sending messages and reading
    responses.  This is largely intended to be used by other modules in the
    memcached2 module such as :py:class:`~memcached2.Memcache()` rather than
    directly by consumers.

    Note that this class buffers data read from the server, so you should
    **never** read data directly from the underlying socket, as it may
    confuse other software which uses this interface.
    '''

    def __init__(self, uri):
        '''
        :param uri: The URI of the backend server.
        :type uri: str
        '''

        self.uri = uri
        self.parsed_uri = self.parse_uri()
        self.backend = None
        self.buffer_readsize = 10000
        self.is_blocking = True
        self.reset()

    def is_up(self):
        '''Is the connection to the backend up?

        :returns: boolean -- If the connection to the server is ok.
        '''
        return self.backend is not None

    def reset(self):
        '''Reset the connection.

        Flushes buffered data and closes the backend connection.
        '''

        self.buffer = ''
        if self.backend:
            self.backend.close()
        self.backend = None

    def consume_from_buffer(self, length):
        '''Retrieve the specified number of bytes from the buffer.

        :param length: Number of bytes of data to consume from buffer.
        :type length: int
        :returns: str -- Data from buffer.
        '''

        data = self.buffer[:length]
        self.buffer = self.buffer[length:]
        return data

    def parse_uri(self):
        '''Parse a server connection URI.

        Parses the `uri` attribute of this object.

        Currently, the only supported URI format is of the form:

            * memcached://<hostname>[:port]/ -- A TCP socket connection to \
                    the host, optionally on the specified port.  If \
                    `port` is not specified, port 11211 is used.

        :returns: dict -- A dictionary with the key `protocol` and other
            protocol-specific keys.  For `memcached` protocol the keys
            include `host`, and `port`.
        :raises: :py:exc:`~memcached2.InvalidURI`
        '''

        m = re.match(
            r'memcached://(?P<host>[^:]+)(:(?P<port>[0-9]+))?/',
            self.uri)
        if m:
            group = m.groupdict()
            port = group.get('port')
            if not port:
                port = 11211
            port = int(port)
            return {'protocol': 'memcached', 'host': group.get('host'),
                    'port': port}

        raise InvalidURI('Invalid URI: {0}'.format(self.uri))

    def connect(self):
        '''Connect to memcached server.

        If already connected, this function returns immmediately.  Otherwise,
        the connection is reset and a connection is made to the backend.

        :raises: :py:exc:`~memcached2.UnknownProtocol`
        '''

        if self.backend:
            return

        self.reset()
        if self.parsed_uri['protocol'] == 'memcached':
            self.backend = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                self.backend.connect(
                    (self.parsed_uri['host'], self.parsed_uri['port']))
                debug('*** OPENING: {0}'.format(repr(self.backend)))
            except socket.gaierror as ex:
                raise InvalidURI('Error connecting to "{0}" '
                                 '(host={1}, port={2}): {3}'.format(
                                     self.uri, repr(self.parsed_uri['host']),
                                     self.parsed_uri['port'], str(ex)))
            if not self.backend.setblocking:
                self.backend.setblocking(self.is_blocking)
            return

        raise UnknownProtocol(
            'Unknown connection protocol: {0}'
            .format(self.parsed_uri['protocol']))

    def send_command(self, command):
        '''Write an ASCII command to the memcached server.

        :param command: Data that is sent to the server.  This is converted
            to a `bytes` type with ASCII encoding if necessary for sending
            across the socket.
        :type command: str

        :raises: :py:exc:`~memcached2.ServerDisconnect`
        '''

        if not self.backend:
            self.connect()

        try:
            self.backend.send(_to_bytes(command))
        except ConnectionResetError:
            raise ServerDisconnect('ConnectionResetError')
        except BrokenPipeError:
            raise ServerDisconnect('BrokenPipeError')

    def line_available(self):
        '''Is a line (including termination) available in the server buffer?

        :returns: boolean -- Is a line of data available in the server buffer?
        '''
        return '\r\n' in self.buffer

    def read_until(self, search='\r\n'):
        '''Read data from the server until "search" is found.

        Data is read in blocks, any remaining data beyond `search` is held
        in a buffer to be consumed in the future.

        :param search: Read data from the server until `search` is found.
                This defaults to '\r\n', so it acts like readline().
        :type search: str

        :returns: str -- Data read, up to and including `search`.  Converted
            from `bytes` (as read from backend) with ASCII encoding, if
            necessary.
        :raises: :py:exc:`~memcached2.ServerDisconnect`
        '''
        start = 0
        search_len = len(search)

        while True:
            if self.buffer:
                pos = self.buffer.find(search, start)
                if pos >= 0:
                    return self.consume_from_buffer(pos + search_len)
                else:
                    start = max(0, len(self.buffer) - search_len)

            self.read_from_socket()

    def read_from_socket(self):
        '''Read data from the socket, storing into buffer.

        A single read operation from the socket, storing data into the buffer.
        '''
        try:
            data = _from_bytes(self.backend.recv(self.buffer_readsize))
        except ConnectionResetError:
            raise ServerDisconnect('ConnectionResetError')
        except BrokenPipeError:
            raise ServerDisconnect('BrokenPipeError')
        if not data:
            raise ServerDisconnect('Zero-length read in read_until()')
        self.buffer += data

    def read_length(self, length):
        '''Read the specified number of bytes.

        :param length: Number of bytes of data to read.
        :type length: int

        :returns: str -- Data read from socket.  Converted from `bytes`
            (as read from backend) with ASCII encoding, if necessary.
        :raises: :py:exc:`~memcached2.ServerDisconnect`
        '''
        while len(self.buffer) < length:
            try:
                data = _from_bytes(self.backend.recv(self.buffer_readsize))
            except ConnectionResetError:
                raise ServerDisconnect('During recv() in read_length()')
            if not data:
                raise ServerDisconnect('Zero-length read in read_length()')
            self.buffer += data

        return self.consume_from_buffer(length)

    def setblocking(self, blocking):
        '''Set the socket to blocking or non-blocking mode.

        :param blocking: If `True`, the socket is set to blocking operation,
                otherwise it is set to non-blocking.
        :type blocking: boolean
        '''
        self.is_blocking = blocking
        if self.backend:
            self.backend.setblocking(blocking)

    def fileno(self):
        '''Return the socket file descriptor.

        :returns: int -- File descriptor of the associated socket.
        '''
        if not self.backend:
            return None
        return self.backend.fileno()

    def managed_pool(self, pool=None):
        self.pool = pool
        return self

    def __enter__(self):
        debug('Entering pool: {0}'.format(repr(self.backend)))
        if not hasattr(self, 'pool'):
            raise NotImplementedError(
                'Context manager needs to be used with managed_pool() method')

        return self

    def __exit__(self, ext, exv, trb):
        '''Close backend if exception, otherwise return to pool.'''
        if ext:
            self.reset()
        elif self.backend and self.pool:
            self.pool.put(self)
        delattr(self, 'pool')

    def __repr__(self):
        return '<ServerConnection to {0}>'.format(self.uri)
