#!/usr/bin/env python

"""delete.py: The main deletion script. This is the one to execute.

This is designed to be an extremely lightweight script in order to utilize
as many threads as possible to delete from an object store as quickly as
possible. There is no error checking during object deletions and deletions will
keep retrying until there is nothing more to delete. This is so that temporary
failures won't halt everything.

Use at your own risk!
"""

__author__ = "Chelsea Urquhart"
__copyright__ = "Copyright 2015, Chelsea Urquhart"
__license__ = "GPL"
__email__ = "info@chelseau.com"

# Settings

# Maximum threads to use at once
max_threads = 64
queue_size = max_threads * 256 * 2

# Should we output while we go or be silent?
verbose = True

# An array of prefixes to delete. The default '' will delete everything.
prefixes = ['']

# Credentials
rs_username = ''
rs_apikey = ''
rs_region = 'DFW'
rs_loginurl = 'https://identity.api.rackspacecloud.com/v2.0/'

# Maximum size of a bulk deletion request
rs_bulksize = queue_size / max_threads / 2

from cloudfiles import CloudFiles
from threadeddeleter import ThreadedDeleter

# Initialize Cloud Files, login
cf = CloudFiles(rs_username, rs_apikey, rs_region, rs_loginurl, rs_bulksize)

# Initialize threaded deleter
deleter = ThreadedDeleter(cf, queue_size, max_threads, verbose)

with deleter:
    deleter.delete(prefixes)