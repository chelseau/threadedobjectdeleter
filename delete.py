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

from threadeddeleter import ThreadedDeleter
from objectstore import ObjectStore
import ast
import imp
from ConfigParser import ConfigParser
import os
import sys


# Configuration defaults
class Settings:
    store = ''
    prefixes = list()
    bulk_size = 100
    verbose = True
    max_threads = 64
    queue_size = 25000

pwd = os.path.abspath(os.path.dirname(__file__))

# Load config
parser = ConfigParser()
parser.read([os.path.join(pwd, 'app.ini'),
             os.path.expanduser('~/.objectdeleter.ini')])

if not parser.has_section('deleter'):
    print('Invalid config file')
    sys.exit(1)

# Process config
for key in dict(parser.items('deleter')):
    if hasattr(Settings, key):
        default = getattr(Settings, key)

        value = parser.get('deleter', key)

        # Is this a data type we need to convert/validate?
        for datatype in [list, bool, int, None]:
            if datatype is not None and isinstance(default, datatype):
                if len(value) == 0:
                    # Empty value of data type
                    value = datatype()
                    datatype = None
                break

        if datatype is not None:

            # Evaluate non-string data
            try:
                value = ast.literal_eval(value)
            except SyntaxError:
                print("Failed to parse {key}. Aborting execution.".format(
                    key=key))
                sys.exit(1)

            # Validate data type
            if not isinstance(value, datatype):
                print("Invalid data type of {key}. Expecting {type}".format(
                    key=key, type=str(datatype)))
                sys.exit(1)

        # Override default option
        setattr(Settings, key, value)

# @TODO: Validate options

try:
    module = imp.load_source('store',
                             os.path.join(pwd, 'stores',
                                          str(Settings.store).lower()) + '.py')
    if not hasattr(module, 'Store') or not issubclass(module.Store,
                                                      ObjectStore):
        raise ImportError("Malformed object store module")
except ImportError as e:
    print("Failed to load {store} object store. Ending script execution."
          .format(store=str(Settings.store).lower()))
    sys.exit(1)

# Initialize object store
store = module.Store(parser)

# Initialize threaded deleter
deleter = ThreadedDeleter(store, Settings)

with deleter:
    deleter.delete(Settings.prefixes)
