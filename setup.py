#!/usr/bin/env python

import os

from setuptools import setup, find_packages

__email__ = "me@chelseau.com"
__license__ = "GPL"
__copyright__ = "Copyright 2015, Chelsea Urquhart"
__author__ = "Chelsea Urquhart"

here = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(here, 'README.md')) as f:
    README = f.read()

requires = [
    'pyrax==1.9.5',
]

setup(
    name='Threaded Object Deleter',
    version="2.0.0",
    author=__author__,
    author_email=__email__,
    description='A lightweight, extremely fast deleter for various objects.',
    license=__license__,
    keywords='cloudfiles threading',
    url='https://github.com/chelseau/threadedobjectdeleter',
    packages=find_packages(),
    long_description=README,
    classifiers=[
        "Development Status :: 4 - Beta",
        "Topic :: Utilities",
    ],
    install_requires=requires,
)
