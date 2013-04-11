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


class Memcached2Exception(Exception):
    '''Base exception that all other exceptions inherit from'''


class UnknownProtocol(Memcached2Exception):
    '''An unknown protocol was specified in the memcached URI'''


class InvalidURI(Memcached2Exception):
    '''An error was encountered in parsing the URI'''


class BackendDisconnect(Memcached2Exception):
    '''The backend connection closed'''


class StoreException(Memcached2Exception):
    '''Base class for storage related exceptions.'''


class NotStored(StoreException):
    '''Item was not stored, but not due to an error.  Normally means the
    condition for an "add" or "replace" was not met'''


class Exists(StoreException):
    '''Item you are trying to store with a "cas" command has been modified
    since you last fetched it'''


class NotFound(StoreException):
    '''Item you are trying to store with a "cas" command does not exist'''


class MemcacheValue(bytes):
    '''Wrapper around Memcache value results, to augment the return data to
    include the additional information (flags, key, cas_unique)'''

    def __new__(self, value, key, flags, cas_unique=None):
        data = super(MemcacheValue, self).__new__(self, value)
        self.key = key
        self.flags = flags
        self.cas_unique = cas_unique

        return data


class HasherNone:
    '''Hasher that always returns None, useful only for SelectorFirst.'''
    def __init__(self):
        pass

    def hash(self, key):
        return None


class HasherCMemcache:
    '''Hasher compatible with the C memcache hash function'''
    def __init__(self):
        pass

    def hash(self, key):
        if PY3:
            key = bytes(key, 'ascii')
        return ((((crc32(key) & 0xffffffff) >> 16) & 0x7fff) or 1)


class SelectorFirst:
    '''Server selector that only returns the first server.  Useful when there
    is only one server to select amongst.'''
    def __init__(self):
        pass

    def select(self, server_list, hasher):
        return server_list[0]


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

    def get(self, key):
        '''Retrieve the specified key from a memcache server.  Returns
        the value read from the server, as a "MemcacheValue" object
        which includes attributes specifying the key and flags, otherwise
        it acts like a string.
        '''

        if PY3:
            command = bytes('get {0}\r\n'.format(key), 'ascii')
        else:
            command = bytes('get {0}\r\n'.format(key))

        server = self.selector.select(self.servers, self.hasher)

        server.send_command(command)
        data = server.read_until(b'\r\n').rstrip().split()
        if data[0] != b'VALUE':
            raise ValueError('Unknown response: {0}'.format(repr(data)))
        key, flags, length = data[1:]
        length = int(length)
        flags = int(flags)
        body = server.read_length(length)
        data = server.read_until(b'\r\n')   # trailing termination
        data = server.read_until(b'\r\n')
        if data != b'END\r\n':
            raise ValueError('Unknown response: {0}'.format(repr(data)))

        return MemcacheValue(body, key, flags)

    def set(self, key, value, flags=0, exptime=0):
        '''Set a key to the value in the memcache server.  If the "flags"
        are specified, those same flags will be provided on return.  If
        "exptime" is set to non-zero, it specifies the expriation time, in
        seconds, that this key's data expires.
        '''

        server = self.selector.select(self.servers, self.hasher)
        if not server.backend:
            server.connect()

        if PY3:
            command = bytes('set {0} {1} {2} {3}\r\n'.format(key, flags,
                    exptime, len(value)), 'ascii') + bytes(value,
                    'ascii') + b'\r\n'
        else:
            command = bytes('set {0} {1} {2} {3}\r\n'.format(key, flags,
                    exptime, len(value))) + bytes(value) + b'\r\n'
        server.send_command(command)

        data = server.read_until(b'\r\n')

        if data == b'STORED\r\n':
            return
        if data == b'NOT STORED\r\n':
            raise NotStored('During set()')
        if data == b'EXISTS\r\n':
            raise Exists('During set()')
        if data == b'NOT FOUND\r\n':
            raise NotFound('During set()')

        raise NotImplementedError('Unknown return data from server: "{0}"'
                .format(data))

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
