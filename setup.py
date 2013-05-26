#!/usr/bin/env python

from setuptools import setup
import memcache2

setup(name='python-memcached',
      version=memcache2.__version__,
      description='Pure python memcached client',
      long_description=open('README.md').read(),
      author='Sean Reifschneider',
      author_email='jafo@tummy.com',
      maintainer='Sean Reifschneider',
      maintainer_email='jafo@tummy.com',
      url='http://www.tummy.com/Community/software/python-memcached2/',
      download_url='ftp://ftp.tummy.com/pub/python-memcached2/',
      py_modules=['memcache2'],
      classifiers=[
        #'Development Status :: 5 - Production/Stable',
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Internet',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.3',
        ])

