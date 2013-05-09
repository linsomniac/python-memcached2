Python Memcached2
=================

A new 100% Python memcache client library.  This is targeted at Python 3.3+
and Python 2.7, though it may be possible for it to work with Python 2.6 as
well.

**I am looking for feedback on this module, its design and architecture.**

**2013-05-03**: I did a short performance test against the python-memcached
library that this is meant to replace.  This new module is around 10%
faster (using the Memcache() class) at retrieving 10 byte values, and 16%
faster at 1KB values.  I was expecting more, but I also haven't done any
performance tuning.  If I just return normal strings instead of
ValueMemcache, that goes up to 23% faster, so that may be a point of
optimization.

**2013-05-01**: I'm tagging a 0.2 but still not going to release to pypi
yet.  Server failure testing, related to ExceptionsAreMissesMapping, have
located several exceptions that weren't being caught and translated into
local module exceptions.  Current functionality is solid, but I want to
add a MemcacheCASValue class, which is kind of an API change.

**2013-04-27**: The module is usable, but if you do you
should expect that the interfaces may change.  The high level
:py:class:`~memcached2.ExceptionsAreMissesMapping code is usable but
not fully tested and the exceptions aren't all caught.  The low-level
:py:class:`~memcached2.Memcache` code is basically complete, documented,
and well tested.

Example
-------

The dictionary/mapping interface looks like this:

    >>> import memcached2
    >>> mcd = memcached2.ExceptionsAreMissesMapping(('memcached://localhost/',))
    >>> 'foo' in mcd
    False
    >>> mcd['foo'] = 'hello'
    >>> 'foo' in mcd
    True
    >>> mcd['foo']
    'hello'
    >>> len(mcd)
    1
    >>> del(mcd['foo'])
    >>> len(mcd)
    0

The lower-level :py:class:`~memcached2.Memcache` interface raises
exceptions in the cases of unexpected results or server connection issues.
Here's a small example, many more examples are available in the
documentation.

    >>> import memcached2
    >>> memcache = memcached2.Memcache(('memcached://localhost/',))
    >>> try:
    >>>     data = memcache.get('session', data)
    >>> except Memcached2.RetrieveException:
    >>>     data = read_session_from_database()
    >>> try:
    >>>     memcache.set('session', data)
    >>> except Memcached2.StoretrieveException:
    >>>     pass

Documentation
-------------

The documentation, including extensive examples, is available at
[python-memcached2 on Read The Docs](https://python-memcached2.readthedocs.org/en/latest/)

Helping Out
-----------

I'd really appreciate review of the design and implementation, and in
particular how it deals with a threaded environment.

I do need to document my current thoughts on the architecture, but here
are my thoughts about it:

I'd also like to have a comprehensive test suite for it.  The old package
had a basic test suite, but it was missing a *lot* of test cases.  I'd
like ot have a test case that demonstrates the failure before looking at a
patch to fix it.

Thoughts?  If you know anyone who is interested in Python and Memcached,
please direct them to this project, I'd like the input early.

The previous module uses "readline()", which does a system call for every
character in the input.  This is a lot of overhead, so I'm switching to
a buffered input wrapper and using recv() to read large blocks of data,
then search for the newline or other data.  Should be a big performance
boost, especially on large data stores.

License
-------

python-memcached2 is licensed under version 2.0 of the Apache License.
See the file "LICENSE" in the top level directory or the Copyright notice
in the software.

Background
----------

The existing python-memcached module has a long history, but also is a very
complicated set of code that I did not write and has very few tests.  It
was written long long ago, and so I've felt that there was a benefit both
to starting over from a clean slate, using lessons learned, and to using
more modern software-development practices.  Plus, it's a good opportunity
for supporting Python 3 and Python 2 (though there is a Python 3 patch for
python-memcached).

The current code is deployed widely, so doing extensive changes to it is
fraught with peril.

So in early April I began experimenting with a new code-base for
python-memcached.  I've been exploring a few different things to see where
they go, and so far I think it looks promising.
