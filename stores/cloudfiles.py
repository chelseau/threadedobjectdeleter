"""cloudfiles.py: Contains a CloudFiles implementation of ObjectStore."""

__author__ = "Chelsea Urquhart"
__copyright__ = "Copyright 2015, Chelsea Urquhart"
__credits__ = "Rackspace [http://tinyurl.com/os95vts]"
__license__ = "GPL"
__email__ = "info@chelseau.com"

import os
import pyrax
import sys
from objectstore import ObjectStore
from threadeddeleter import ThreadedDeleter


class Store(ObjectStore):
    """A ObjectStore class for Rackspace Cloud Files"""

    @classmethod
    def get_retry_text(cls, retries):
        """
        Returns retry text based on the number of retries
        :param retries: The number of retries
        :return: A string
        """
        if retries == 0:
            return ' All retries exhausted.'
        else:
            return ' Retrying {} more times.'.format(retries)

    def __init__(self, parser):
        """
        Initialize all our variables
        :param parser: Our config parser object
        :return: None
        :throws: Exception on validation error
        """

        # Store arguments
        self.marker = dict()
        self.rax = None
        self.region = ''
        self.bulk_size = 0
        self.username = ''
        self.api_key = ''

        options = ['region', 'bulk_size', 'username', 'api_key']
        optional = ['bulk_size']

        if not parser.has_section('cloudfiles'):
            raise Exception('CloudFiles configuration is missing')

        for option in options:
            if not parser.has_option('cloudfiles', option):
                if option not in optional:
                    raise Exception('Missing cloudfiles option: {}'.format(
                        option))
            else:
                setattr(self, option, parser.get('cloudfiles', option))

        # Ensure data type
        self.bulk_size = int(self.bulk_size)

        # Validate options
        if len(self.region) == 0:
            raise Exception('No region specified')
        if len(self.username) == 0:
            raise Exception('No username specified')
        if len(self.api_key) == 0:
            raise Exception('No API key specified')

        # Set identity type
        pyrax.settings.set('identity_type', 'rackspace')

    def login(self):
        """
        Logs into cloud files. Note that this is on the main thread.
        init_thread is responsible for initializing individual threads.
        :return: True on success, false on failure
        """

        try:
            pyrax.set_credentials(username=self.username,
                                  api_key=self.api_key)
            self.rax = pyrax.connect_to_cloudfiles(self.region, True)
            if self.rax is None:
                ThreadedDeleter.output('Unknown error occured while connecting'
                                       ' to CloudFiles.')
                return False
        except pyrax.exceptions.AuthenticationFailed as e:
            ThreadedDeleter.output('Authentication failed: {msg}'.format(
                msg=str(e)))
            return False
        except pyrax.exceptions.PyraxException as e:
            ThreadedDeleter.output('Unknown error occurred: {msg}'.format(
                msg=str(e)))
            return False

        return True

    def list_containers(self, prefixes, retry=2):
        """
        Lists containers beginning with any of the provided prefixes
        :param prefixes: The (list of) prefixes to get containers for
        :param retry: The number of retries to use
        :return: A list of containers or False on error
        """
        containers = list()
        if len(prefixes) == 0:
            prefixes = [None]

        for prefix in prefixes:
            try:
                containers_ = self.rax.list(prefix=prefix)
            except Exception as e:
                ThreadedDeleter.output('List containers failed: {msg}.{retry}'
                                       .format(msg=str(e),
                                               retry=self.get_retry_text(
                                                   retry)))
                if retry == 0:
                    return False

                # Retry
                return self.list_containers(prefixes, retry - 1)

            if containers_ is not None:
                for container in containers_:
                    containers.append(container.name)

        return containers

    def list_objects(self, container_name, retry=2):
        """
        Lists objects in a given container
        :param container_name: The name of the container to get objects from
        :param retry: The number of retries to use
        :return: A list of objects or False on error
        """
        marker = None
        if container_name in self.marker:
            marker = self.marker.get(container_name)

        try:
            container = self.rax.get_container(container_name)
            objects_ = container.list(marker=marker)
        except Exception as e:
            ThreadedDeleter.output('List objects failed: {msg}.{retry}'
                                   .format(msg=str(e),
                                           retry=self.get_retry_text(retry)))
            if retry == 0:
                return False

            # Retry
            return self.list_objects(container_name, retry - 1)

        if len(objects_) == 0:
            return objects_

        objects = list()
        for object in objects_:
            objects.append(object.name)
        self.marker[container_name] = objects[-1]

        return objects

    def delete_objects_bulk(self, local):
        if local.size > 0:
            for container, objects in local.data.iteritems()\
                    if hasattr(local.data, 'iteritems')\
                    else local.data.items():
                self.rax.bulk_delete(container, objects)
        local.size = 0
        local.data = dict()

    def delete_object(self, container, object, local):
        """
        Deletes an object from a given container
        :param container: The name of the container to get objects from
        :param object: The name of the object to delete
        :param local: A Local class object for storing thread-specific
         variables in.
        :return: None
        """
        if self.bulk_size <= 1:
            self.rax.delete_object(container, object)
        else:
            if container not in local.data:
                local.data[container] = list()
            local.data[container].append(object)
            local.size += 1
            if local.size >= self.bulk_size:
                self.delete_objects_bulk(local)

    def init_thread(self, local):
        """
        Initialize thread-specific RAX connection & data list
        :param local: The Local object
        :return: None
        """
        local.rax = pyrax.connect_to_cloudfiles(self.region, True)
        local.data = dict()
        local.size = 0

    def cleanup_thread(self, local):
        """
        Cleanup thread-specific RAX connection
        :param local: The Local object
        :return: None
        """

        # Delete any remaining objects first if using bulk deletions
        self.delete_objects_bulk(local)

    def delete_container(self, container, retry=2):
        """
        Deletes a container
        :param container: The name of the container to get objects from
        :param retry: The number of retries to use
        :return: None
        """
        try:
            self.rax.delete_container(container, del_objects=True)
            return True
        except Exception as e:
            ThreadedDeleter.output('Delete container failed: {msg}.{retry}'
                                   .format(msg=str(e),
                                           retry=self.get_retry_text(retry)))
            if retry == 0:
                return False

            # Retry
            return self.delete_container(container, retry - 1)
