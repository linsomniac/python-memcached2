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

PY3 = sys.version > '3'
if not PY3:
    ConnectionResetError = socket.error


def _from_bytes(data):
    '''INTERNAL: Convert bytes to a regular string.'''
    if PY3:
        if isinstance(data, str):
            return data
        return str(data, 'ascii')
    return str(data)


def _to_bytes(data):
    '''Internal: Convert something to bytes type.'''
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


class Memcached2Exception(Exception):
    '''Base exception that all other exceptions inherit from'''


class UnknownProtocol(Memcached2Exception):
    '''An unknown protocol was specified in the memcached URI'''


class InvalidURI(Memcached2Exception):
    '''An error was encountered in parsing the URI'''


class BackendDisconnect(Memcached2Exception):
    '''The backend connection closed'''


class RetrieveException(Memcached2Exception):
    '''Base class for retrieve related exceptions.'''


class NoValue(RetrieveException):
    '''No value retrieved.'''


class StoreException(Memcached2Exception):
    '''Base class for storage related exceptions.'''


class NoAvailableServers(Memcached2Exception):
    '''There are no servers available to cache on, probably because all are
    unavailable.'''


class NotStored(StoreException):
    '''Item was not stored, but not due to an error.  Normally means the
    condition for an "add" or "replace" was not met'''


class CASFailure(StoreException):
    '''Item you are trying to store with a "cas" command has been modified
    since you last fetched it (result=EXISTS)'''


class NotFound(StoreException):
    '''Item you are trying to store with a "cas" command does not exist'''


class NonNumeric(StoreException):
    '''The item you are trying to incr/decr is not numeric.'''


class MemcacheValue(str):
    '''Wrapper around Memcache value results, to augment the return data to
    include the additional information (flags, key, cas_unique)'''

    def __new__(self, value, key, flags, cas_unique=None):
        data = super(MemcacheValue, self).__new__(self, value)
        data.key = key
        data.flags = flags
        data.cas_unique = cas_unique

        return data


class HasherNone:
    '''Hasher that always returns None, useful only for SelectorFirst.'''
    def hash(self, key):
        return None


class HasherCMemcache:
    '''Hasher compatible with the C memcache hash function'''
    def hash(self, key):
        key = _to_bytes(key)
        return ((((crc32(key) & 0xffffffff) >> 16) & 0x7fff) or 1)


class SelectorFirst:
    '''Server selector that only returns the first server.  Useful when there
    is only one server to select amongst.'''
    def select(self, server_list, key_hash):
        server = server_list[0]
        if not server.backend:
            server.connect()
        return server


class SelectorAvailableServers:
    '''Select among all "up" server connections, reconnecting to down servers
    periodically.'''
    def __init__(self, reconnect_frequency=100):
        '''Selector that attempts to reconnect to down servers every
        "reconnect_frequency" operations (default: 100).'''
        self.reconnect_frequency = reconnect_frequency
        self.operations_to_next_reconnect = 0

    def select(self, server_list, key_hash):
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
    '''Basic memcache interface.  This interface will raise exceptions when
    backend connections occur, allowing a program full control over handling
    of connection problems.

    An "error swallowing" wrapper will provide functionality similar to the
    previous python-memcached module.

    Example:

    >>> from memcached2 import *
    >>> mc = Memcache(['memcached://localhost:11211/'])
    >>> mc.set('foo', 'bar')
    >>> mc.get('foo')
    'bar'

    '''

    def __init__(self, servers, selector=None, hasher=None):
        '''Create a new Memcache connection, to the specified servers.
        The list of servers, specified by URL, are consulted based on the
        hash of the key, effectively "sharding" the key space.

        The "selector" is the algorithm that selects the backend server,
        making decisions based on which servers are available and attempting
        reconnecting.  If not specified, it defaults to SelectorFirst() if
        there is only one server, or SelectorAvailableServers() if multiple
        servers are given.

        The "hasher" is a hash function which takes a key and returns a hash
        for persistent server selection.  If not specified, it defaults to
        HasherNone() if there is only one server specified, or
        HasherCMemcache() otherwise.
        '''

        self.servers = [ServerConnection(x) for x in servers]

        self.hasher = hasher

        if selector != None:
            self.selector = selector
        else:
            if len(self.servers) < 2:
                self.selector = SelectorFirst()
                if hasher == None:
                    self.hasher = HasherNone()
            else:
                self.selector = SelectorAvailableServers()
                if hasher == None:
                    self.hasher = HasherCMemcache()

    def __del__(self):
        self.close()

    def _send_command(self, command, key):
        '''Send a command to a server.  "key" is used to determine the server
        to send the command to.'''
        command = _to_bytes(command)
        server = self.selector.select(self.servers, self.hasher.hash(key))
        server.send_command(command)
        return server

    def get(self, key, get_cas=False):
        '''Retrieve the specified key from a memcache server.  Returns
        the value read from the server, as a "MemcacheValue" object
        which includes attributes specifying the key and flags, otherwise
        it acts like a string.  If "get_cas" is true, the "cas_unique"
        value is queried and stored in the return data.
        '''

        if get_cas:
            server = self._send_command('gets {0}\r\n'.format(key), key)
        else:
            server = self._send_command('get {0}\r\n'.format(key), key)

        data = server.read_until('\r\n')
        if data == 'END\r\n':
            raise NoValue()

        if not data.startswith('VALUE'):
            raise ValueError('Unknown response: {0}'.format(repr(data)))
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
            raise ValueError('Unexpected response when looking for '
                    'terminator: {0}'.format(data))

        data = server.read_until('\r\n')
        if data != 'END\r\n':
            raise ValueError('Unknown response: {0}'.format(repr(data)))

        return MemcacheValue(body, key, flags, cas_unique)

    def set(self, key, value, flags=0, exptime=0, cas_unique=None):
        '''Set a key to the value in the memcache server.  If the "flags"
        are specified, those same flags will be provided on return.  If
        "exptime" is set to non-zero, it specifies the expriation time, in
        seconds, that this key's data expires.  If "cas_unique" is given,
        it is a 64-bit integer from get(key, get_cas=True), the set is only
        done if the value has not been updated since the get.
        '''
        if cas_unique:
            command = 'cas {0} {1} {2} {3} {4}\r\n'.format(key,
                    flags, exptime, len(value), cas_unique) + value + '\r\n'
        else:
            command = 'set {0} {1} {2} {3}\r\n'.format(key,
                    flags, exptime, len(value)) + value + '\r\n'
        return self._storage_command(command, key)

    def add(self, key, value, flags=0, exptime=0):
        '''Store, but only if the server doesn't already hold data for it.
        If the "flags" are specified, those same flags will be provided
        on return.  If "exptime" is set to non-zero, it specifies the
        expriation time, in seconds, that this key's data expires.
        '''
        command = 'add {0} {1} {2} {3}\r\n'.format(key,
                flags, exptime, len(value)) + value + '\r\n'
        return self._storage_command(command, key)

    def replace(self, key, value, flags=0, exptime=0):
        '''Store data, but only if the server already holds data for it.
        If the "flags" are specified, those same flags will be provided
        on return.  If "exptime" is set to non-zero, it specifies the
        expriation time, in seconds, that this key's data expires.
        '''
        command = 'replace {0} {1} {2} {3}\r\n'.format(key,
                flags, exptime, len(value)) + value + '\r\n'
        return self._storage_command(command, key)

    def append(self, key, value):
        '''Store data after existing data associated with this key.
        '''
        command = 'append {0} 0 0 {1}\r\n'.format(key,
                len(value)) + value + '\r\n'
        return self._storage_command(command, key)

    def prepend(self, key, value):
        '''Store data before existing data associated with this key.
        '''
        command = 'prepend {0} 0 0 {1}\r\n'.format(key,
                len(value)) + value + '\r\n'
        return self._storage_command(command, key)

    def delete(self, key):
        '''Delete the key if it exists.
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
        '''Update the expiration time on an item.  Note that setting
        exptime=0 causes the item not to expire based on time.
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
        '''Flush the memcache server.  An attempt is made to connect to all
        backend servers before running this command.
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
        '''INTERNAL: Run statistics function() on each server, return a
        list of the results from each server.'''
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

        The statistics data is a dictionary of key/value pairs representing
        information about the server.

        This data is returned as a list of statistics, one item for
        each server.  If the server is not connected, None is returned
        for its position, otherwise data as mentioned above.

        An attempt is made to connect to all servers before issuing
        this command.
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

        The statistic information is a dictionary keyed by the "slab class",
        with the value another dictionary of key/value pairs representing
        the slab information.

        This data is returned as a list of statistics, one item for
        each server.  If the server is not connected, None is returned
        for its position, otherwise data as mentioned above.

        An attempt is made to connect to all servers before issuing
        this command.
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

        The statistic information is a dictionary keyed by the "slab class",
        with the value another dictionary of key/value pairs representing
        statistic information about each of the slabs created during the
        memcace runtime.

        This data is returned as a list of statistics, one item for
        each server.  If the server is not connected, None is returned
        for its position, otherwise data as mentioned above.

        An attempt is made to connect to all servers before issuing
        this command.
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

        The statistic information is a dictionary of key/value pairs.

        This data is returned as a list of statistics, one item for
        each server.  If the server is not connected, None is returned
        for its position, otherwise data as mentioned above.

        An attempt is made to connect to all servers before issuing
        this command.
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

        Each statistic is a dictionary of of size:count where the size is
        rounded up to 32-byte ranges.

        WARNING: This operation locks the cache while it iterates
        over all objects.  Returns a list of (size,count) tuples received
        from the server.

        This data is returned as a list of statistics, one item for
        each server.  If the server is not connected, None is returned
        for its position, otherwise data as mentioned above.

        An attempt is made to connect to all servers before issuing
        this command.
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
        Return the new value.
        '''
        command = 'incr {0} {1}\r\n'.format(key, value)
        return self._incrdecr_command(command, key)

    def decr(self, key, value):
        '''Decrement the value for the key, treated as a 64-bit unsigned value.
        Return the new value.
        '''
        command = 'decr {0} {1}\r\n'.format(key, value)
        return self._incrdecr_command(command, key)

    def _incrdecr_command(self, command, key):
        '''INTERNAL: Increment/decrement command back-end.
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
        '''INTERNAL: Storage command back-end.
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
        '''Close the connection to the backend servers.
        '''

        for server in self.servers:
            server.reset()


class ServerConnection:
    '''Low-level communication with the memcached server.  This implments
    the connection to the server, sending messages and parsing responses.'''

    def __init__(self, uri, timeout=None):
        self.uri = uri
        self.parsed_uri = self.parse_uri()
        self.timeout = timeout
        self.backend = None
        self.buffer_readsize = 10000
        self.reset()

    def reset(self):
        '''Reset the connection including flushing buffered data and closing
        the backend connection.'''

        self.buffer = ''
        if self.backend:
            self.backend.close()
        self.backend = None

    def consume_from_buffer(self, length):
        '''Retrieve the specified number of bytes from the buffer'''

        data = self.buffer[:length]
        self.buffer = self.buffer[length:]
        return data

    def parse_uri(self):
        '''Parse a server connection URI.  Returns a dictionary with the
        connection information, including a 'protocol' key and other
        protocol-specific keys.'''

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
        '''Connect to memcached server.  If already connected,
        this function returns immmediately.'''

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
        '''Write an ASCII command to the memcached server.'''

        self.backend.send(_to_bytes(command))

    def read_until(self, search):
        '''Read data from the server until "search" is found.  Return data read
        including the first occurrence of "search".'''
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
                raise BackendDisconnect('During recv() in read_until()')
            if not data:
                raise BackendDisconnect('Zero-length read in read_until()')
            self.buffer += data

    def read_length(self, length):
        '''Read the specified number of bytes.  Return data read.'''
        while len(self.buffer) < length:
            try:
                data = _from_bytes(self.backend.recv(self.buffer_readsize))
            except ConnectionResetError:
                raise BackendDisconnect('During recv() in read_length()')
            if not data:
                raise BackendDisconnect('Zero-length read in read_length()')
            self.buffer += data

        return self.consume_from_buffer(length)
