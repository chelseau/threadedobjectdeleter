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
import re
import sys


# Configuration defaults
class Settings:
    store = ''
    prefixes = list()
    verbose = True
    max_threads = 64
    queue_size = 25000

pwd = os.path.abspath(os.path.dirname(__file__))


def main(argv):
    """
    Main
    :param argv: a list of arguments
    :return: The code to exit with
    """
    global pwd

    # Load config
    parser = ConfigParser()
    parser.read([os.path.join(pwd, 'app.ini'),
                 os.path.expanduser('~/.objectdeleter.ini')])

    if not parser.has_section('deleter'):
        print('Invalid config file')
        return 1

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
                    return 1

                # Validate data type
                if not isinstance(value, datatype):
                    print("Invalid data type of {key}. Expecting {type}"
                          .format(key=key, type=str(datatype)))
                    return 1

            # Override default option
            setattr(Settings, key, value)

    # Validate options

    # Validate store. This is just responsible for making sure arbitrary data
    # can't be injected here. Actually loading will happen later.
    Settings.store = re.sub(r'[^\w\s]', '', Settings.store)
    if len(Settings.store) == 0:
        print("Object store module not specified. Ending script execution.")
        return 1

    if Settings.max_threads <= 0:
        print("Maximum threads is too low. It must be at least 1."
              " Ending script execution.")
        return 1

    if Settings.queue_size < 1:
        print("Maximum queue size is too low. It must be at least 1."
              " Ending script execution.")
        return 1

    try:
        module = imp.load_source('store',
                                 os.path.join(pwd, 'stores',
                                              str(Settings.store).lower()) +
                                 '.py')
        if not hasattr(module, 'Store') or not issubclass(module.Store,
                                                          ObjectStore):
            raise ImportError("Malformed object store module")
    except ImportError as e:
        print("Failed to load {store} store: {err}. Ending script execution."
              .format(store=str(Settings.store).lower(), err=str(e)))
        return 1

    # Initialize object store
    try:
        store = module.Store(parser)
    except Exception as e:
        print(e.message)
        return 1

    # Initialize threaded deleter
    deleter = ThreadedDeleter(store, Settings)

    with deleter:
        deleter.delete(Settings.prefixes)

    return 0

if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
