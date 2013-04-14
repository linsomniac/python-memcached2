Python Memcached2
=================

A new 100% Python memcache client library.  This is targeted at Python 3,
but I anticipate using 3to2 to convert it to Python 2 and work there.

2013-04-14: I've implemented much of the lowest-level class, Memcache().
This implements many of the commands for the memcache server.  This exposes
all errors via exceptions, so it's kind of lower level, but most functions
are implemented.  A few more are left do complete.

2013-04-01: I've just started working on this, and it's a side project for a
side project, so I have no timeline.  Use the regular python-memcached.
I assure you it's not an April fools joke though.

Details
-------

I've been wanting to rewrite the Python memcached module for a while and
have finally started. I now have the basic connection object, it's a
very long way from being done, but I can now do a flush of the server.

I'd really appreciate review of the design and implementation, and in
particular how it deals with a threaded environment.

I do need to document my current thoughts on the architecture, but here
are my thoughts about it:

The original python-memcached module was written by someone who doesn't
really know Python, as a quick and dirty hack.  I've been maintaining
it for years, but currently a lot of people are using it in production
and so i can't really change the API.  I'd like to just start from a
clean slate and see where it goes.

I'm planning to write it for Python 3, but usable on Python 2.  I
probably won't go any further than testing it against Python 2.7 and 3.3,
but I'm open to supporting other platforms.

The previous module uses "readline()", which does a system call for every
character in the input.  This is a lot of overhead, so I'm switching to
a buffered input wrapper and using recv() to read large blocks of data,
then search for the newline or other data.  Should be a big performance
boost, especially on large data stores.

I'd also like to have a comprehensive test suite for it.  The old package
had a basic test suite, but it was missing a *LOT* of test cases.  I'd
like ot have a test case that demonstrates the failure before looking at a
patch to fix it.

Thoughts?  If you know anyone who is interested in Python and Memcached,
please direct them to this project, I'd like the input early.
