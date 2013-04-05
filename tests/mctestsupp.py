#!/usr/bin/env python


def flush_local_memcache(test):
    import socket
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(('127.0.0.1', 11211))
    s.send(b'flush_all\nquit\n')
    results = s.recv(1000)
    test.assertIn(b'OK', results)
    s.close()
