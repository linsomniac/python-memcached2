Low-Level Memcache() Interface
******************************

.. autoclass:: memcached2.Memcache
   :members: get, set, add, replace, append, prepend, delete, touch,
      incr, decr, flush_all, close,
      stats, stats_items, stats_slabs, stats_settings, stats_sizes
   :private-members: _send_command, _reconnect_all, _run_multi_server,
      _incrdecr_command, _storage_command
