#!/usr/bin/env python


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
                conn, addr = self.s.accept()
                self.server(self.s, conn, count)
                count += 1
