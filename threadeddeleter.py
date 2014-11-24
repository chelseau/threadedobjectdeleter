"""threadeddeleter.py: Contains the threaded deleter class."""

__author__ = "Chelsea Urquhart"
__copyright__ = "Copyright 2014, Chelsea Urquhart"
__license__ = "GPL"
__email__ = "info@chelseau.com"

import threading
import Queue
import time


class ThreadedDeleter:
    """A class for managing and controlling deletion threads."""

    @staticmethod
    def output(text):
        ThreadedDeleter.output_lock.acquire()
        print '[%s] %s' % (time.ctime(), text)
        ThreadedDeleter.output_lock.release()


    def __init__(self, object_store, queue_size, max_threads, verbose=False):
        """
        Initializes a threaded deleter class.
        :param object_store: The object store we're working with
        :param max_threads: The maximum number of threads to use at once
        :param verbose: Enable output?
        :return: None
        """
        self.object_store = object_store
        self.queue_size = queue_size
        self.max_threads = max_threads
        self.verbose = verbose

        self.lock = threading.Lock()
        self.queue = Queue.Queue(queue_size)
        self.finished = False
        self.deleted_objects = 0
        self.threads = []

    def delete_object(self, thread_id):
        """
        The polling function for each thread. This will read the queue and
        call the object store deletion function as necessary.
        :param thread_id: The numeric ID of the currently running thread
        :return: None
        """
        # Setup a cURL instance for this thread
        local = threading.local()
        self.object_store.init_local(local)
        # Is there more data to process?
        while not self.finished:
            self.lock.acquire()
            if not self.queue.empty():
                container, object = self.queue.get()
                self.lock.release()
                if self.verbose:
                    ThreadedDeleter.output('[Thread %s] Deleting %s...' % (
                        thread_id, object))
                try:
                    self.object_store.delete_object(container, object, local)
                except Exception:
                    self.lock.release()
                    self.finish()
                    raise
            else:
                self.lock.release()
                time.sleep(1)
                # Sleep for 1 second
        self.object_store.cleanup_local(local)

    def add_to_queue(self, data):
        """
        Adds the given container, filename tuple to the deletion queue
        :param data: A tuple containing container, filename
        :return: None
        """
        # Acquire a lock
        self.lock.acquire()

        # Iterate the array of data. Put it in the queue.
        for item in data:
            try:
                self.queue.put(item, False)
                self.deleted_objects += 1
            except Queue.Full:
                # Is the queue full? Lets sleep for a bit to let the threads
                # catch up.
                self.lock.release()
                while self.queue.full():
                    # Let some threads catch up!
                    time.sleep(1)

                # The queue isn't full anymore so we can acquire a new lock
                # and put the item in again. This time we're going to use a
                # blocking request because we cannot have this fail.
                self.lock.acquire()
                self.queue.put(item)
                self.deleted_objects += 1
        self.lock.release()

    def delete(self, prefixes):
        # Login
        if self.verbose:
            ThreadedDeleter.output('Logging in...')
        try:
            self.object_store.login()
        except Exception:
            self.finish()
            raise

        # Fetch matching containers
        if self.verbose:
            ThreadedDeleter.output('Fetching containers...')
        try:
            containers = self.object_store.list_containers(prefixes)
        except Exception:
            self.finish()
            raise

        # Initialize and start up threads 1-max_threads
        for index in range(1, self.max_threads):
            thread = threading.Thread(target=self.delete_object, args=[index])
            thread.start()
            self.threads.append(thread)

        data = []
        start_time = time.time()

        # Iterate the list of containers to delete everything
        for final in [False, True]:
            for container in containers:
                if self.verbose:
                    ThreadedDeleter.output('Processing %s...' % container)
                final = False
                while True:
                    # Keep trying until we run out of files for object stores
                    # that don't return everything at once.
                    try:
                        files = self.object_store.list_objects(container, final)
                    except Exception:
                        self.finish()
                        raise

                    if len(files) == 0:
                        break
                    for file in files:
                        data.append((container, file))
                        # continue
                        if len(data) > self.max_threads / 2:
                            # We've got enough of a buffer to get going. Lets
                            # do it!
                            self.add_to_queue(data)
                            data = []
                    while self.queue.qsize() > self.max_threads / 2:
                        # Sleep for a second before we retry this container.
                        # There were likely errors on some files, so we'll want
                        # to retry those.
                        time.sleep(1)
                    # Add any leftovers to the queue
                    if len(data) > 0:
                        self.add_to_queue(data)
                        data = []
                if final:
                    ThreadedDeleter.output(
                        'Finished Processing %s...' % container)
                    # All out of files!
            # Wait for all the data to be processed before we continue.
            while not self.queue.empty():
                time.sleep(1 / 10)
                pass

        # Set the finished variable so all the threads will die when they're
        # done working
        self.finish()

        # Iterate the containers again and delete them.
        for container in containers:
            if self.verbose:
                ThreadedDeleter.output('Deleting %s...' % container)
            try:
                self.object_store.delete_container(container)
            except Exception:
                self.finish()
                raise

        # Calculate Duration
        end_time = time.time()

        if len(containers) == 0 and self.verbose:
            ThreadedDeleter.output('There are no containers!')
        elif self.verbose:
            # Output status
            ThreadedDeleter.output(
                'Deleted %s objects from %s containers in %s seconds' % (
                    self.deleted_objects, len(containers),
                    (end_time - start_time)))

    def finish(self):
        """
        Sets our state to finished and waits for all threads to finish up
        :return: None
        """
        if not self.finished:
            self.finished = True

            # Wait for all the threads to finish working
            for thread in self.threads:
                thread.join()


ThreadedDeleter.output_lock = threading.Lock()