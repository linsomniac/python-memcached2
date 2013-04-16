Python Memcached2
=================

A new 100% Python memcache client library.  This is targeted at Python 3.3+
and Python 2.7, though it may be possible for it to work with Python 2.6 as
well.

2013-04-15: I'm considering the Memcache() class to mostly be done.  I do
need to add a multi-backend Selector.  However, this class is kind of the
lowest level, primarily in that it raises exceptions which requires all
accesses to catch exceptions.

2013-04-01: I've just started working on this, and it's a side project for a
side project, so I have no timeline.  Use the regular python-memcached.
I assure you it's not an April fools joke though.

Example
-------

Right now, the only interface is an "error exposing" one that needs to
be protected by try/except.

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

Background
----------

I've been wanting to rewrite the Python memcached module for a while and
have finally started.  The original python-memcached code was written by
someone not very familiar with Python.  I've since been maintaining it felt
like starting over would help with getting both the Python 3 support and
just cleaning up the code over all.

I've been maintaining it for years, but currently a lot of people are
using it in production and so i can't really change the API.  I'd like
to just start from a clean slate and see where it goes.
