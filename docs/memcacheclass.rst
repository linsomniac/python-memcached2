Low-Level Memcache() Interface
******************************

Introduction
------------

This is a low-level interface to a group of memcache servers.  This code
tends to either return the requested data, or raises an exception if the
data is not available or there is any sort of an error.  If you want
high level control, this is probably the interface for you.  However, if
you want something easy, like the old python-memcached module, you will
want to wait for the higher level interfaces to be implemented.

.. _memcache-examples:

Examples
--------

Basic :py:func:`~memcached2.Memcache.get` and exception example:

>>> import memcached2
>>> memcache = memcached2.Memcache(('memcached://localhost/',))
>>> try:
...    result = memcache.get('session_id')
...    print('Got cached results: {0}'.format(repr(result)))
... except memcached2.NoValue:
...    print('Cached value not available, need to recompute it')
... 
Cached value not available, need to recompute it

Demonstrating :py:func:`~memcached2.Memcache.set`,
:py:func:`~memcached2.Memcache.get` and
:py:class:`~memcached2.MemcacheValue`:

>>> memcache.set('session_id', 'TEST SESSSION DATA')
>>> result = memcache.get('session_id')
>>> print('Got cached results: {0}'.format(repr(result)))
Got cached results: 'TEST SESSSION DATA'
>>> result.key
'session_id'
>>> result.flags
0

Showing flags and expiration time and :py:func:`~memcached2.Memcache.touch`:

>>> memcache.set('foo', 'xXx', flags=12, exptime=30)
>>> result = memcache.get('foo')
>>> result
'xXx'
>>> result.key
'foo'
>>> result.flags
12
>>> import time
>>> time.sleep(30)
>>> result = memcache.get('foo')
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
  File "memcached2.py", line 411, in get
    raise NoValue()
memcached2.NoValue
>>> memcache.set('foo', 'bar', exptime=1)
>>> memcache.touch('foo', exptime=30)
>>> time.sleep(2)
>>> memcache.get('foo')
'bar'

Usage of :py:func:`~memcached2.Memcache.replace`,
:py:func:`~memcached2.Memcache.append`, and
:py:func:`~memcached2.Memcache.prepend`:

>>> memcache.replace('unset_key', 'xyzzy')
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
  File "memcached2.py", line 502, in replace
    self._storage_command(command, key)
  File "memcached2.py", line 945, in _storage_command
    raise NotStored()
memcached2.NotStored
>>> memcache.set('unset_key', 'old_data', exptime=30)
>>> memcache.replace('unset_key', 'xyzzy')
>>> memcache.get('unset_key')
'xyzzy'
>>> memcache.append('unset_key', '>>>')
>>> memcache.prepend('unset_key', '<<<')
>>> memcache.get('unset_key')
'<<<xyzzy>>>'

Example of using CAS (Check And Set) atomic operations:

>>> memcache.set('foo', 'bar')
>>> result = memcache.get('foo', get_cas=True)
>>> result.cas_unique
5625
>>> memcache.set('foo', 'baz', cas_unique=result.cas_unique)
>>> memcache.get('foo', get_cas=True)
'baz'
>>> memcache.set('foo', 'qux', cas_unique=result.cas_unique)
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
  File "memcached2.py", line 464, in set
    self._storage_command(command, key)
  File "memcached2.py", line 947, in _storage_command
    raise CASFailure()
memcached2.CASFailure
>>> memcache.get('foo', get_cas=True)
'baz'

Usage of
:py:func:`~memcached2.Memcache.incr`/:py:func:`~memcached2.Memcache.decr`:

>>> memcache.incr('incrtest', 1)
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
  File "memcached2.py", line 878, in incr
    return self._incrdecr_command(command, key)
  File "memcached2.py", line 915, in _incrdecr_command
    raise NotFound()
memcached2.NotFound
>>> memcache.set('incrtest', 'a')
>>> memcache.incr('incrtest', 1)
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
  File "memcached2.py", line 878, in incr
    return self._incrdecr_command(command, key)
  File "memcached2.py", line 919, in _incrdecr_command
    raise NonNumeric()
memcached2.NonNumeric
>>> memcache.set('incrtest', '1')
>>> memcache.incr('incrtest', 1)
2
>>> memcache.decr('incrtest', 1)
1
>>> memcache.get('incrtest')
'1'

.. _memcached-stats-examples:

Statistics sample information:

>>> import pprint
>>> pprint.pprint(memcache.stats())
[{'accepting_conns': '1',
  'auth_cmds': 0,
  'auth_errors': 0,
  'bytes': 201,
  'bytes_read': 173542,
  'bytes_written': 516341,
  'cas_badval': 49,
  'cas_hits': 49,
  'cas_misses': 0,
  'cmd_flush': 1154,
  'cmd_get': 880,
  'cmd_set': 5778,
  'cmd_touch': '148',
  'conn_yields': 0,
  'connection_structures': 9,
  'curr_connections': 5,
  'curr_items': 3,
  'decr_hits': 49,
  'decr_misses': 48,
  'delete_hits': 49,
  'delete_misses': 49,
  'evicted_unfetched': 0,
  'evictions': 0,
  'expired_unfetched': 0,
  'get_hits': '681',
  'get_misses': '199',
  'hash_bytes': 262144,
  'hash_is_expanding': '0',
  'hash_power_level': 16,
  'incr_hits': 49,
  'incr_misses': 49,
  'libevent': '2.0.19-stable',
  'limit_maxbytes': 67108864,
  'listen_disabled_num': '0',
  'pid': 22356,
  'pointer_size': 32,
  'reclaimed': 0,
  'reserved_fds': 20,
  'rusage_system': 7.568473,
  'rusage_user': 8.904556,
  'threads': 4,
  'time': 1366722131,
  'total_connections': 1545,
  'total_items': 5634,
  'touch_hits': 98,
  'touch_misses': 50,
  'uptime': 370393,
  'version': '1.4.14'}]
>>> pprint.pprint(memcache.stats_settings())
[{'auth_enabled_sasl': 'no',
  'binding_protocol': 'auto-negotiate',
  'cas_enabled': True,
  'chunk_size': 48,
  'detail_enabled': False,
  'domain_socket': 'NULL',
  'evictions': 'on',
  'growth_factor': 1.25,
  'hashpower_init': 0,
  'inter': '127.0.0.1',
  'item_size_max': 1048576,
  'maxbytes': 67108864,
  'maxconns': 1024,
  'maxconns_fast': False,
  'num_threads': 4,
  'num_threads_per_udp': 4,
  'oldest': 216734,
  'reqs_per_event': 20,
  'slab_automove': False,
  'slab_reassign': False,
  'stat_key_prefix': ':',
  'tcp_backlog': 1024,
  'tcpport': 11211,
  'udpport': 11211,
  'umask': 700,
  'verbosity': 0}]
>>> pprint.pprint(memcache.stats_items())
[{'1': {'age': 766,
        'evicted': 0,
        'evicted_nonzero': 0,
        'evicted_time': 0,
        'evicted_unfetched': 0,
        'expired_unfetched': 0,
        'number': 3,
        'outofmemory': 0,
        'reclaimed': 0,
        'tailrepairs': 0}}]
>>> pprint.pprint(memcache.stats_sizes())
[[(64, 1), (96, 2)]]
>>> pprint.pprint(memcache.stats_slabs())
[{'active_slabs': 1,
  'slabs': {'1': {'cas_badval': 49,
                  'cas_hits': 49,
                  'chunk_size': 80,
                  'chunks_per_page': 13107,
                  'cmd_set': 5778,
                  'decr_hits': 49,
                  'delete_hits': 49,
                  'free_chunks': 13104,
                  'free_chunks_end': 0,
                  'get_hits': 681,
                  'incr_hits': 49,
                  'mem_requested': 201,
                  'total_chunks': 13107,
                  'total_pages': 1,
                  'touch_hits': 98,
                  'used_chunks': 3}},
  'total_malloced': 1048560}]

How to :py:func:`~memcached2.Memcache.delete`,
:py:func:`~memcached2.Memcache.flush_all`, and
:py:func:`~memcached2.Memcache.close` the connection:

>>> memcache.delete('foo')
>>> memcache.flush_all()
>>> memcache.close()

Object Documentation
--------------------

.. autoclass:: memcached2.Memcache
   :members: get, set, add, replace, append, prepend, delete, touch,
      incr, decr, flush_all, close,
      stats, stats_items, stats_slabs, stats_settings, stats_sizes
   :private-members: _send_command, _reconnect_all, _run_multi_server,
      _incrdecr_command, _storage_command
