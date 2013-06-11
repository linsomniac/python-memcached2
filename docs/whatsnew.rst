What's New in python-memcachee2
*******************************

Sun Jun 10, 2013

  * Tagging as 0.2 as the functionality here is usable and stabilized and I
    want to start working on some significant changes that may break things
    for a while.

Sun Jun 09, 2013

  * Added :py:class:`~memcached2.SelectorConsistentHashing` that implements
    this algorithm for server selection.

Sun Jun 08, 2013

  * Removed :py:class:`~memcached2.SelectorRehashOnDownServer` and replaced
    it with the better
    :py:class:`~memcached2.SelectorRestirOnDownServer`.

Tue Jun 04, 2013

  * Fixing a bug if Memcache(selector+XXX) is used, hasher was not being
    set.

  * Created SelectorRehashOnDownServer which will hash to the same server,
    unless that server is down in which case it will rehash among only the
    up servers.

Wed May 28, 2013

  * Adding :py:func:`memcached2.Memcache.delete_all` and
    :py:func:`memcached2.ValueSuperStr.sdelete_allet`.

Wed May 27, 2013

  * SelectorAvailableServers now can flush all servers when the topology
    changes.  That's the situation it is most suited for, though it's also
    ideal for 2 server clusters.

Wed May 25, 2013

  * Added get_multi which can get multiple keys at once.

Wed May 22, 2013

  * Memcache now has a get_multi() method that will get multiple keys at
    once.

Wed May 20, 2013

  * Memcache.cache() now takes varargs and kwargs, optionally, which will
    be passed to the compute function.

Wed May 19, 2013

  * Memcache.cache() added which will call a function on a cache miss,
    then put the result in the cache.

Wed May 18, 2013

  * Now have a ExceptionsAreMissesMemcache() class for lower-level access
    that treats exceptions as misses.

Wed May 15, 2013

  * ValueSuperStr can now do a CAS refresh on
    :py:func:`memcached2.ValueSuperStr.set`.

Wed May 8, 2013

  * MemcacheValue is now called ValueSuperStr, and it is no longer the
    default return type in Memcache().  It can be defined by passing
    ValueMemcache to Memcache() as the "value_wrapper".  There's also
    a ValueDictionary now.
  * Adding ValueDictionary class.
  * Memcache() class no longer returns MemcacheValue class.
    It returns a normal string, unless you have specified a value_wrapper
    attribute during the creation of the Memcache object.

Tue May 7, 2013

  * Adding MANIFEST.in file.
  * Adding CASFailure to MemcacheValue methods.

Fri May 3, 2013

  * I did a short performance test against the python-memcached
    library that this is meant to replace.  This new module is around 10%
    faster (using the Memcache() class) at retrieving 10 byte values, and
    16% faster at 1KB values.  I was expecting more, but I also haven't
    done any performance tuning.  If I just return normal strings instead
    of ValueSuperStr, that goes up to 23% faster, so that may be a point
    of optimization.
  * Adding remaining methods to MemcacheValue.

Thu May 2, 2013

  * MemcacheValue now has "set()" method.

Wed May 1, 2013

  * I'm tagging a 0.2 but still not going to release to pypi
    yet.  Server failure testing, related to ExceptionsAreMissesMapping,
    have located several exceptions that weren't being caught and
    translated into local module exceptions.  Current functionality is
    solid, but I want to add a MemcacheCASValue class, which is kind of
    an API change.
  * Improving Python 2 BrokenPipeError
  * Catching more exceptions, more tests.

    Added more extensive testing to ExceptionsAsMissesMapping, including
    in the cases where the server disconnects.  Through that, found places
    where more exceptions needed to be caught.

Tue Apr 30, 2013

  * Trapping ServerDisconnected exception.

Mon Apr 29, 2013

  * ObliviousMapping renamed ExceptionsAreMissesMapping

    ExceptionsAreMissesMapping suggested by Wes Winham.  Thanks!

Sat Apr 27, 2013

  * The module is usable, but if you do you
    should expect that the interfaces may change.  The high level
    :py:class:`~memcached2.ExceptionsAreMissesMapping code is usable but
    not fully tested and the exceptions aren't all caught.  The low-level
    :py:class:`~memcached2.Memcache` code is basically complete, documented,
    and well tested.
  * Bringing back KeyError because d.get() is preferable.
  * Renaming ObliviousDict to ObliviousMapping.

Fri Apr 26, 2013

  * Adding ObliviousDict() tests and fixing "in".
