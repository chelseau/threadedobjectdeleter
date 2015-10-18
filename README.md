Threaded Object Deleter
=======================

A Multi-threaded object deleter. It currently supports Rackspace Cloud Files
and Amazon S3.

Issues
------
Please report any issues and send any pull requests to [threadedobjectdeleter](https://github.com/chelseau/threadedobjectdeleter)

I'll also gladly accept feature requests.

Usage
-----
The delete.py script will work almost out of the box. You'll need to configure
app.ini (see sample.ini for example usage) with your credentials, prefixes you
want to delete (default is to delete everything), and if desired adjust the max
threads.

Feel free to use this for whatever you want! Hopefully it'll be useful to
someone!
