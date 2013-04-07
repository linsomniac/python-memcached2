#!/usr/bin/env python3

import re
import socket
import sys

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


class Memcached2StoreException(Memcached2Exception):
    '''Base class for storage related exceptions.'''


class Memcached2NotStored(Memcached2StoreException):
    '''Item was not stored, but not due to an error.  Normally means the
    condition for an "add" or "replace" was not met'''


class Memcached2Exists(Memcached2StoreException):
    '''Item you are trying to store with a "cas" command has been modified
    since you last fetched it'''


class Memcached2NotFound(Memcached2StoreException):
    '''Item you are trying to store with a "cas" command does not exist'''


class Memcache:
    def __init__(self, servers):
        self.servers = [ServerConnection(x) for x in servers]

    def set(self, key, value, flags=0, exptime=0):
        if not self.servers[0].backend:
            self.servers[0].connect()

        if PY3:
            command = bytes('set {0} {1} {2} {3}\r\n'.format(key, flags,
                    exptime, len(value)), 'ascii') + bytes(value,
                    'ascii') + b'\r\n'
        else:
            command = bytes('set {0} {1} {2} {3}\r\n'.format(key, flags,
                    exptime, len(value))) + bytes(value) + b'\r\n'
        self.servers[0].send_command(command)

        data = self.servers[0].read_until(b'\r\n')

        if data == 'STORED\r\n':
            return
        if data == 'NOT STORED\r\n':
            return
        if data == 'EXISTS\r\n':
            return
        if data == 'NOT FOUND\r\n':
            return

    def close(self):
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
