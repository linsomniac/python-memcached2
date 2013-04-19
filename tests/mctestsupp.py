#!/usr/bin/env python

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

import sys

PY3 = sys.version > '3'


def flush_local_memcache(test):
    import socket
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(('127.0.0.1', 11211))
    s.send(b'flush_all\nquit\n')
    results = s.recv(1000)
    test.assertIn(b'OK', results)
    s.close()


class FakeMemcacheServer:
    '''A simple socket server so that specific error conditions can be tested.
    This must be subclassed and implment the "server()" method.'''

    def __init__(self):
        import socket
        import os
        import signal

        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.s.listen(1)
        self.port = self.s.getsockname()[1]
        self.pid = os.fork()

        if self.pid != 0:
            self.s.close()
            del self.s
        else:
            def alarm(signum, frame):
                os._exit(0)

            count = 0
            signal.signal(signal.SIGALRM, alarm)
            signal.alarm(5)
            while True:
                connection, addr = self.s.accept()
                self.server(self.s, connection, count)
                count += 1


RECEIVE = None


class CommandServer(FakeMemcacheServer):
    def __init__(self, commands):
        self.commands = commands
        FakeMemcacheServer.__init__(self)

    def server(self, sock, conn, count):
        for command in self.commands:
            if command == RECEIVE:
                conn.recv(1000)
            else:
                if PY3:
                    conn.send(bytes(command, 'ascii'))
                else:
                    conn.send(bytes(command))
        conn.close()
