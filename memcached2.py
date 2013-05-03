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

'''
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
import sys
from binascii import crc32
import collections

PY3 = sys.version > '3'
if not PY3:
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
        return str(data, 'ascii')
    return str(data)


def _to_bytes(data):
    '''INTERNAL: Convert something to bytes type.'''
    if PY3:
        if isinstance(data, bytes):
            return data
        return bytes(data, 'ascii')
    return data


def _to_bool(s):
    '''INTERNAL: Convert a stats boolean string into boolean.'''
    if s in ['0', 'no']:
        return False
    if s in ['1', 'yes']:
        return True
    raise NotImplementedError('Unknown boolean value {0}'
            .format(repr(s)))


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
    '''Base class for storage related exceptions.  Never raised directly..
    Subclass of :class:`MemcachedException`.'''


class NotStored(StoreException):
    '''Item was not stored, but not due to an error.  Normally means the
    condition for an "add" or "replace" was not met..  Subclass of
    :class:`StoreException`.'''


class CASFailure(StoreException):
    '''Item you are trying to store with a "cas" command has been modified
    since you last fetched it (result=EXISTS)..  Subclass of
    :class:`StoreException`.'''


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


class MemcacheValue(str):
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
        :returns: :py:class:`~memcached2.MemcacheValue` instance
        '''
        data = super(MemcacheValue, self).__new__(self, value)
        data.key = key
        data.flags = flags
        data.cas_unique = cas_unique
        data.memcache = memcache

        return data

    def set(self, value, flags=0, exptime=0):
        '''Update the value in the server.

        The key that was used to retrieve this value is updated in the server.
        If the value was retrieved from the server with `get_cas` enabled,
        then this will update using the CAS.

        See :py:method:`~memcache2.Memcache.set` for more information.

        .. note::

            This does not update this object's value.

        .. note::

            If this object was retrieved with `get_cas` set, then multiple
            updates will trigger a :py:exception:`~memcache2.CASFailure`.
        '''
        return self.memcache.set(self.key, value, flags=flags, exptime=exptime,
                cas_unique=self.cas_unique)


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


class SelectorBase:
    '''Select which server to use and reconnect to down servers.

    These classes implement both the algorithm for selecting the server and
    for reconnecting to servers that are down.

    The selection is done based on a `key_hash`, as returned by the
    :py:func:`memcached2.HasherBase.hash` function.  Servers that are
    down must be reconnected to, as all servers initially start off
    disconnected.

    Normally, the python-memcached2 classes will automatically pick a
    selector to use.  However, for special circumstances you may wish to
    use a different selector or develop your own.

    This is an abstract base class, here largely for documentation purposes.
    Selector sub-classes such as :py:class:`~memcached2.SelectorFirst` and
    :py:class:`~memcached2.SelectorAvailableServers`, implement a `select`
    method which does all the work.

    See :py:func:`~memcached2.SelectorBase.select` for details of implementing
    a subclass.
    '''
    def select(self, server_list, key_hash):
        '''Select a server bashed on the `key_hash`.

        Given the list of servers and a hash of of key, determine which
        of the servers this key is stored on.

        This often makes use of the `backend` attribute of the `server_list`
        elements, if it is None, the server is not currently connected
        to.

        :param server_list: A list of the servers to select among.
        :type server_list: list of :py:class:`~memcache2.ServerConnection`.
        :param key_hash: Hash of the key, as returned by
            :py:func:`memcache2.HasherBase.hash`.
        :type key_hash: int
        :returns: ServerConnection -- The server to use for the specified key.
        :raises: :py:exc:`~memcached2.NoAvailableServers`
        '''
        raise NotImplementedError('This class is only meant to be subclassed')


class SelectorFirst(SelectorBase):
    '''Server selector that only returns the first server.  Useful when there
    is only one server to select amongst.

    If the server is down, an attempt to reconnect will be made.
    '''
    def select(self, server_list, key_hash):
        '''See :py:func:`memcached2.SelectorBase.select` for details of
        this function.
        '''
        server = server_list[0]
        if not server.backend:
            server.connect()
        return server


class SelectorAvailableServers(SelectorBase):
    '''Select among all "up" server connections.

    After a specified number of operations, and at the first operation, an
    attempt will be made to connect to any servers that are not currently
    up.
    '''
    def __init__(self, reconnect_frequency=100):
        '''
        :param reconnect_frequency: Every this many operations, attempt
            reconecting to all down servers.
        :type reconnect_frequency: int
        '''
        self.reconnect_frequency = reconnect_frequency
        self.operations_to_next_reconnect = 0

    def select(self, server_list, key_hash):
        '''See :py:func:`memcached2.SelectorBase.select` for details of
        this function.
        '''
        if self.operations_to_next_reconnect < 1:
            self.operations_to_next_reconnect = self.reconnect_frequency
            for server in [x for x in server_list if not x.backend]:
                server.connect()
        else:
            self.operations_to_next_reconnect -= 1

        up_server_list = [x for x in server_list if x.backend]
        if not up_server_list:
            raise NoAvailableServers()

        return up_server_list[key_hash % len(up_server_list)]


class Memcache:
    '''
    Create a new memcache connection, to the specified servers.

    The list of servers, specified by URL, are consulted based on the
    hash of the key, effectively "sharding" the key space.

    This is a low-level memcache interface.  This interface will raise
    exceptions when backend connections occur, allowing a program full
    control over handling of connection problems.

    Example:

    >>> from memcached2 import *
    >>> mc = Memcache(['memcached://localhost:11211/'])
    >>> mc.set('foo', 'bar')
    >>> mc.get('foo')
    'bar'

    Extensive examples including demonstrations of the statistics output
    is available in the documentation for
    :ref:`Memcache Examples <memcache-examples>`

    '''

    def __init__(self, servers, selector=None, hasher=None):
        '''
        :param servers: One or more server URIs of the form:
            "memcache://hostname[:port]/"
        :type servers: list
        :param selector: (None) A "Selector" class object.  This code
            implements the server selector logic.  If not specified, the
            default is used.  The default is to use
            :py:class:`~memcached2.SelectorFirst` if only one server is
            specified, and :py:class:`~memcached2.SelectorAvailableServers`
            if multiple servers are given.
        :type selector: "Selector" class object.
        :param hasher: (None) A "Hash" object which takes a key and returns
            a hash for persistent server selection.  If not specified, it
            defaults to :py:class:`~memcache2.HasherZero` if there is only
            one server specified, or :py:class:`~memcache2.HasherCMemcache`
            otherwise.
        :type hasher: "Hash" class object.
        '''

        self.servers = [ServerConnection(x) for x in servers]

        self.hasher = hasher

        if selector != None:
            self.selector = selector
        else:
            if len(self.servers) < 2:
                self.selector = SelectorFirst()
                if hasher == None:
                    self.hasher = HasherZero()
            else:
                self.selector = SelectorAvailableServers()
                if hasher == None:
                    self.hasher = HasherCMemcache()

    def __del__(self):
        self.close()

    def _send_command(self, command, key):
        '''INTERNAL: Send a command to a server.

        :param command: The memcache-protocol command to send to the
            server, a string terminated with CR+NL.
        :type command: str
        :param key: The key within the command, used to determine what
            server to send the command to.
        :type key: str
        :returns: :py:class:`~memcached2.ServerConnection` -- The server object
            that the command was sent to.
        :raises: :py:exc:`~memcached2.NoAvailableServers`,
            :py:exc:`~memcached2.ServerDisconnect`
        '''
        command = _to_bytes(command)
        server = self.selector.select(self.servers, self.hasher.hash(key))

        try:
            server.send_command(command)
        except ConnectionResetError:
            raise ServerDisconnect('ConnectionResetError')
        except BrokenPipeError:
            raise ServerDisconnect('BrokenPipeError')

        return server

    def get(self, key, get_cas=False):
        '''Retrieve the specified key from a memcache server.

        :param key: The key to lookup in the memcache server.
        :type key: str
        :param get_cas: If True, the "cas unique" is queried and the return
            object has the "cas_unique" attribute set.
        :type get_cas: bool
        :returns: :py:class:`~memcached2.MemcacheValue` -- The value read from
            the server, which includes attributes specifying the key and
            flags, otherwise it acts like a string.
        :raises: :py:exc:`~memcached2.NoValue`, :py:exc:`NotImplementedError`,
            :py:exc:`~memcached2.NoAvailableServers`
        '''

        if get_cas:
            server = self._send_command('gets {0}\r\n'.format(key), key)
        else:
            server = self._send_command('get {0}\r\n'.format(key), key)

        data = server.read_until('\r\n')
        if data == 'END\r\n':
            raise NoValue()

        if not data.startswith('VALUE'):
            raise NotImplementedError(
                    'Unknown response: {0}'.format(repr(data)))
        split_data = data.rstrip().split()[1:]
        key = split_data[0]
        flags = int(split_data[1])
        length = int(split_data[2])
        if len(split_data) > 3:
            cas_unique = int(split_data[3])
        else:
            cas_unique = None
        body = server.read_length(length)

        data = server.read_until('\r\n')   # trailing termination
        if data != '\r\n':
            raise NotImplementedError('Unexpected response when looking for '
                    'terminator: {0}'.format(data))

        data = server.read_until('\r\n')
        if data != 'END\r\n':
            raise NotImplementedError(
                    'Unknown response: {0}'.format(repr(data)))

        return MemcacheValue(body, key, flags, cas_unique, memcache=self)

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
        if cas_unique:
            command = 'cas {0} {1} {2} {3} {4}\r\n'.format(key,
                    flags, exptime, len(value), cas_unique) + value + '\r\n'
        else:
            command = 'set {0} {1} {2} {3}\r\n'.format(key,
                    flags, exptime, len(value)) + value + '\r\n'
        self._storage_command(command, key)

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
        command = 'add {0} {1} {2} {3}\r\n'.format(key,
                flags, exptime, len(value)) + value + '\r\n'
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
        command = 'replace {0} {1} {2} {3}\r\n'.format(key,
                flags, exptime, len(value)) + value + '\r\n'
        self._storage_command(command, key)

    def append(self, key, value):
        '''Store data after existing data associated with this key.

        :param key: Key used to store value in memcache server and hashed to
            determine which server is used.
        :type key: str
        :param value: Value stored in memcache server for this key.
        :type value: str
        '''
        command = 'append {0} 0 0 {1}\r\n'.format(key,
                len(value)) + value + '\r\n'
        self._storage_command(command, key)

    def prepend(self, key, value):
        '''Store data before existing data associated with this key.

        :param key: Key used to store value in memcache server and hashed to
            determine which server is used.
        :type key: str
        :param value: Value stored in memcache server for this key.
        :type value: str
        '''
        command = 'prepend {0} 0 0 {1}\r\n'.format(key,
                len(value)) + value + '\r\n'
        self._storage_command(command, key)

    def delete(self, key):
        '''Delete the key if it exists.

        :param key: Key used to store value in memcache server and hashed to
            determine which server is used.
        :type key: str
        :raises: :py:exc:`~memcached2.NotFound`, :py:exc:`NotImplementedError`,
            :py:exc:`~memcached2.NoAvailableServers`
        '''
        command = 'delete {0}\r\n'.format(key)

        server = self._send_command(command, key)

        data = server.read_until('\r\n')

        if data == 'DELETED\r\n':
            return
        if data == 'NOT_FOUND\r\n':
            raise NotFound()

        raise NotImplementedError('Unknown return data from server: "{0}"'
                .format(repr(data)))

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
        data = server.read_until('\r\n')

        if data == 'TOUCHED\r\n':
            return
        if data == 'NOT_FOUND\r\n':
            raise NotFound()

        raise NotImplementedError('Unknown return data from server: "{0}"'
                .format(repr(data)))

    def _reconnect_all(self):
        '''INTERNAL: Attempt to connect to all backend servers.'''
        for server in [x for x in self.servers if not x.backend]:
            server.connect()

    def flush_all(self):
        '''Flush the memcache servers.

        .. note::

            An attempt is made to connect to all backend servers
            before running this command.

        :raises: :py:exc:`NotImplementedError`
        '''
        command = 'flush_all\r\n'

        self._reconnect_all()
        for server in [x for x in self.servers if x.backend]:
            server.send_command(command)
            data = server.read_until('\r\n')

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
        for server in self.servers:
            stats = None
            if server.backend:
                stats = function(server)
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
                data = _from_bytes(server.read_until('\r\n'))
                if data == 'END\r\n':
                    break
                prefix, key, value = data.strip().split()
                if prefix != 'STAT':
                    raise NotImplementedError('Unknown stats data: {0}'
                            .format(repr(data)))
                if key in ['pid', 'uptime', 'time', 'pointer_size',
                        'curr_items', 'total_items', 'bytes',
                        'curr_connections', 'total_connections',
                        'connection_structures', 'reserved_fds', 'cmd_get',
                        'cmd_set', 'cmd_flush', 'cmd_hits', 'cmd_misses',
                        'delete_misses', 'delete_hits', 'incr_misses',
                        'incr_hits', 'decr_misses', 'decr_hits', 'cas_misses',
                        'cas_hits', 'cas_badval', 'touch_hits',
                        'touch_misses', 'auth_cmds', 'auth_errors',
                        'evictions', 'reclaimed', 'bytes_read',
                        'bytes_written', 'limit_maxbytes', 'threads',
                        'conn_yields', 'hash_power_level', 'hash_bytes',
                        'expired_unfetched', 'evicted_unfetched',
                        'slabs_moved']:
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
                data = _from_bytes(server.read_until('\r\n'))
                if data == 'END\r\n':
                    break
                prefix, key, value = data.strip().split()
                if prefix != 'STAT':
                    raise NotImplementedError('Unknown stats data: {0}'
                            .format(repr(data)))
                prefix, slab_key, stat_key = key.split(':')
                if prefix != 'items':
                    raise NotImplementedError('Unknown stats item: {0}'
                            .format(repr(key)))
                if not slab_key in stats:
                    stats[slab_key] = {}
                if stat_key in ['number', 'age', 'evicted', 'evicted_nonzero',
                        'evicted_time', 'outofmemory', 'tailrepairs',
                        'reclaimed', 'expired_unfetched', 'evicted_unfetched']:
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
                data = _from_bytes(server.read_until('\r\n'))
                if data == 'END\r\n':
                    break
                prefix, key, value = data.strip().split()
                if prefix != 'STAT':
                    raise NotImplementedError('Unknown stats data: {0}'
                            .format(repr(data)))

                if ':' in key:
                    slab_key, stat_key = key.split(':')
                    if not slab_key in stats['slabs']:
                        stats['slabs'][slab_key] = {}
                    if stat_key in ['chunk_size', 'chunks_per_page',
                            'total_pages', 'total_chunks', 'used_chunks',
                            'free_chunks', 'free_chunks_end', 'mem_requested',
                            'get_hits', 'cmd_set', 'delete_hits', 'incr_hits',
                            'decr_hits', 'cas_hits', 'cas_badval',
                            'touch_hits']:
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
                data = _from_bytes(server.read_until('\r\n'))
                if data == 'END\r\n':
                    break
                prefix, key, value = data.strip().split()
                if prefix != 'STAT':
                    raise NotImplementedError('Unknown stats data: {0}'
                            .format(repr(data)))
                if key in ['maxbytes', 'maxconns', 'tcpport', 'udpport',
                        'verbosity', 'oldest', 'umask', 'chunk_size',
                        'num_threads', 'num_threads_per_udp', 'reqs_per_event',
                        'tcp_backlog', 'item_size_max', 'hashpower_init']:
                    value = int(value)
                if key in ['growth_factor']:
                    value = float(value)
                if key in ['maxconns_fast', 'slab_reassign', 'slab_automove',
                        'detail_enabled', 'cas_enabled']:
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
                data = _from_bytes(server.read_until('\r\n'))
                if data == 'END\r\n':
                    break
                prefix, key, value = data.strip().split()
                if prefix != 'STAT':
                    raise NotImplementedError('Unknown stats data: {0}'
                            .format(repr(data)))
                stats.append((int(key), int(value)))

            return stats

        return self._run_multi_server(query)

    def incr(self, key, value):
        '''Increment the value for the key, treated as a 64-bit unsigned value.

        :param key: Key used to store value in memcache server and hashed to
            determine which server is used.
        :type key: str
        :param value: A numeric value to add to the existing value.
        :type value: int (64 bit)
        :returns: int -- (64 bits) The new value after the increment.
        :raises: :py:exc:`~memcached2.NotFound`,
            :py:exc:`~memcached2.NonNumeric`, :py:exc:`NotImplementedError`
        '''
        command = 'incr {0} {1}\r\n'.format(key, value)
        return self._incrdecr_command(command, key)

    def decr(self, key, value):
        '''Decrement the value for the key, treated as a 64-bit unsigned value.

        :param key: Key used to store value in memcache server and hashed to
            determine which server is used.
        :type key: str
        :param value: A numeric value to add to the existing value.
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
        data = server.read_until('\r\n')

        #  <NEW_VALUE>\r\n
        if data[0] in '0123456789':
            return int(data.strip())
        if data == 'NOT_FOUND\r\n':
            raise NotFound()
        client_error = ('CLIENT_ERROR cannot increment or decrement '
                'non-numeric value\r\n')
        if data == client_error:
            raise NonNumeric()

        raise NotImplementedError('Unknown return data from server: "{0}"'
                .format(repr(data)))

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

        data = server.read_until('\r\n')

        if data == 'STORED\r\n':
            return
        if data == 'NOT_STORED\r\n':
            raise NotStored()
        if data == 'EXISTS\r\n':
            raise CASFailure()
        if data == 'NOT FOUND\r\n':
            raise NotFound()

        raise NotImplementedError('Unknown return data from server: "{0}"'
                .format(repr(data)))

    def close(self):
        '''Close the connection to all the backend servers.
        '''

        for server in self.servers:
            server.reset()


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
        self.reset()

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

        m = re.match(r'memcached://(?P<host>[^:]+)(:(?P<port>[0-9]+))?/',
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
            self.backend.connect((self.parsed_uri['host'],
                    self.parsed_uri['port']))
            return

        raise UnknownProtocol('Unknown connection protocol: {0}'
                .format(self.parsed_uri['protocol']))

    def send_command(self, command):
        '''Write an ASCII command to the memcached server.

        :param command: Data that is sent to the server.  This is converted
            to a `bytes` type with ASCII encoding if necessary for sending
            across the socket.
        :type command: str

        :raises: :py:exc:`~memcached2.ServerDisconnect`
        '''

        try:
            self.backend.send(_to_bytes(command))
        except ConnectionResetError:
            raise ServerDisconnect('ConnectionResetError')
        except BrokenPipeError:
            raise ServerDisconnect('BrokenPipeError')

    def read_until(self, search):
        '''Read data from the server until "search" is found.

        Data is read in blocks, any remaining data beyond `search` is held
        in a buffer to be consumed in the future.

        :param search: Read data from the server until `search` is found.
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
