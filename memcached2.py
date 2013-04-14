#!/usr/bin/env python3

'''
A re-implementation of the python-memcached module, designed to work with
Python 2 and 3.  Note: It is tested against Python 2.7 and 3.3 during
development, there may be problems running against previous versions.

See the Memcache() class for an example of use.

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
        return str(data, 'ascii')
    return str(data)


def _to_bytes(data):
    '''Internal: Convert something to bytes type.'''
    if PY3:
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


class MemcacheValue(bytes):
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
    def select(self, server_list, hasher):
        server = server_list[0]
        if not server.backend:
            server.connect()
        return server


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
        there is only one server, or NotImplemented otherwise.

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
                raise NotImplementedError('Only one server supported')
                if hasher == None:
                    self.hasher = HasherCMemcache()

    def __del__(self):
        self.close()

    def _send_command(self, command):
        command = _to_bytes(command)
        server = self.selector.select(self.servers, self.hasher)
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
            server = self._send_command('gets {0}\r\n'.format(key))
        else:
            server = self._send_command('get {0}\r\n'.format(key))

        data = server.read_until(b'\r\n')
        if data == b'END\r\n':
            raise NoValue()

        if not data.startswith(b'VALUE'):
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

        data = server.read_until(b'\r\n')   # trailing termination
        if data != b'\r\n':
            raise ValueError('Unexpected response when looking for '
                    'terminator: {0}'.format(data))

        data = server.read_until(b'\r\n')
        if data != b'END\r\n':
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
        return self._storage_command(command)

    def add(self, key, value, flags=0, exptime=0):
        '''Store, but only if the server doesn't already hold data for it.
        If the "flags" are specified, those same flags will be provided
        on return.  If "exptime" is set to non-zero, it specifies the
        expriation time, in seconds, that this key's data expires.
        '''
        command = 'add {0} {1} {2} {3}\r\n'.format(key,
                flags, exptime, len(value)) + value + '\r\n'
        return self._storage_command(command)

    def replace(self, key, value, flags=0, exptime=0):
        '''Store data, but only if the server already holds data for it.
        If the "flags" are specified, those same flags will be provided
        on return.  If "exptime" is set to non-zero, it specifies the
        expriation time, in seconds, that this key's data expires.
        '''
        command = 'replace {0} {1} {2} {3}\r\n'.format(key,
                flags, exptime, len(value)) + value + '\r\n'
        return self._storage_command(command)

    def append(self, key, value):
        '''Store data after existing data associated with this key.
        '''
        command = 'append {0} 0 0 {1}\r\n'.format(key,
                len(value)) + value + '\r\n'
        return self._storage_command(command)

    def prepend(self, key, value):
        '''Store data before existing data associated with this key.
        '''
        command = 'prepend {0} 0 0 {1}\r\n'.format(key,
                len(value)) + value + '\r\n'
        return self._storage_command(command)

    def delete(self, key):
        '''Delete the key if it exists.
        '''
        command = 'delete {0}\r\n'.format(key)

        server = self._send_command(command)
        data = server.read_until(b'\r\n')

        if data == b'DELETED\r\n':
            return
        if data == b'NOT_FOUND\r\n':
            raise NotFound()

        raise NotImplementedError('Unknown return data from server: "{0}"'
                .format(repr(data)))

    def touch(self, key, exptime):
        '''Update the expiration time on an item.  Note that setting
        exptime=0 causes the item not to expire based on time.
        '''
        command = 'touch {0} {1}\r\n'.format(key, exptime)

        server = self._send_command(command)
        data = server.read_until(b'\r\n')

        if data == b'TOUCHED\r\n':
            return
        if data == b'NOT_FOUND\r\n':
            raise NotFound()

        raise NotImplementedError('Unknown return data from server: "{0}"'
                .format(repr(data)))

    def flush_all(self):
        '''Flush the memcache server.
        '''
        command = 'flush_all\r\n'

        server = self._send_command(command)
        data = server.read_until(b'\r\n')

        if data == b'OK\r\n':
            return

        raise NotImplementedError('Unknown return data from server: "{0}"'
                .format(repr(data)))

    def stats(self):
        '''Get statistics from the memcache server, returns a dictionary of
        key/value pairs received from the server.
        '''
        command = 'stats\r\n'

        server = self._send_command(command)
        stats = {}
        while True:
            data = _from_bytes(server.read_until(b'\r\n'))
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

    def stats_items(self):
        '''Gets statistics about items storage per slab class.  Returns a
        dictionary of slab classes, each containing key/value pairs received
        from the server.
        '''
        command = 'stats items\r\n'

        server = self._send_command(command)
        stats = {}
        while True:
            data = _from_bytes(server.read_until(b'\r\n'))
            if data == 'END\r\n':
                break
            prefix, key, value = data.strip().split()
            if prefix != 'STAT':
                raise NotImplementedError('Unknown stats data: {0}'
                        .format(repr(data)))
            prefix, slab_key, stat_key = key.split(':')
            if prefix != 'items':
                raise NotImplementedError('Unknown stats item key: {0}'
                        .format(repr(key)))
            if not slab_key in stats:
                stats[slab_key] = {}
            if stat_key in ['number', 'age', 'evicted', 'evicted_nonzero',
                    'evicted_time', 'outofmemory', 'tailrepairs',
                    'reclaimed', 'expired_unfetched', 'evicted_unfetched']:
                value = int(value)
            stats[slab_key][stat_key] = value

        return stats

    def stats_slabs(self):
        '''Gets information about each of the slabs created during memcached
        runtime.  Returns a dictionary of slab IDs, each contains a dictionary
        of key/value pairs for that slab.
        '''
        command = 'stats slabs\r\n'

        server = self._send_command(command)
        stats = {'slabs': {}}
        while True:
            data = _from_bytes(server.read_until(b'\r\n'))
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

    def stats_settings(self):
        '''Gets statistics about settings (primarily from processing
        command-line arguments), returns a dictionary of key/value pairs
        received from the server.
        '''
        command = 'stats settings\r\n'

        server = self._send_command(command)
        stats = {}
        while True:
            data = _from_bytes(server.read_until(b'\r\n'))
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

    def stats_sizes(self):
        '''Get statistics about object sizes, sizes grouped by chunks of
        32-bits.  WARNING: This operation locks the cache while it iterates
        over all objects.  Returns a list of (size,count) tuples received
        from the server.
        '''
        command = 'stats sizes\r\n'

        server = self._send_command(command)
        stats = []
        while True:
            data = _from_bytes(server.read_until(b'\r\n'))
            if data == 'END\r\n':
                break
            prefix, key, value = data.strip().split()
            if prefix != 'STAT':
                raise NotImplementedError('Unknown stats data: {0}'
                        .format(repr(data)))
            stats.append((int(key), int(value)))

        return stats

    def incr(self, key, value):
        '''Increment the value for the key, treated as a 64-bit unsigned value.
        Return the new value.
        '''
        command = 'incr {0} {1}\r\n'.format(key, value)
        return self._incrdecr_command(command)

    def decr(self, key, value):
        '''Decrement the value for the key, treated as a 64-bit unsigned value.
        Return the new value.
        '''
        command = 'decr {0} {1}\r\n'.format(key, value)
        return self._incrdecr_command(command)

    def _incrdecr_command(self, command):
        '''INTERNAL: Increment/decrement command back-end.
        '''
        server = self._send_command(command)
        data = server.read_until(b'\r\n')

        #  <NEW_VALUE>\r\n
        if data[0] in b'0123456789':
            return int(data.strip())
        if data == b'NOT_FOUND\r\n':
            raise NotFound()
        client_error = (b'CLIENT_ERROR cannot increment or decrement '
                b'non-numeric value\r\n')
        if data == client_error:
            raise NonNumeric()

        raise NotImplementedError('Unknown return data from server: "{0}"'
                .format(repr(data)))

    def _storage_command(self, command):
        '''INTERNAL: Storage command back-end.
        '''
        server = self._send_command(command)

        data = server.read_until(b'\r\n')

        if data == b'STORED\r\n':
            return
        if data == b'NOT_STORED\r\n':
            raise NotStored()
        if data == b'EXISTS\r\n':
            raise CASFailure()
        if data == b'NOT FOUND\r\n':
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

        self.buffer = b''
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

        self.backend.send(command)

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
                data = self.backend.recv(self.buffer_readsize)
            except ConnectionResetError:
                raise BackendDisconnect('During recv() in read_until()')
            if not data:
                raise BackendDisconnect('Zero-length read in read_until()')
            self.buffer += data

    def read_length(self, length):
        '''Read the specified number of bytes.  Return data read.'''
        while len(self.buffer) < length:
            try:
                data = self.backend.recv(self.buffer_readsize)
            except ConnectionResetError:
                raise BackendDisconnect('During recv() in read_length()')
            if not data:
                raise BackendDisconnect('Zero-length read in read_length()')
            self.buffer += data

        return self.consume_from_buffer(length)
