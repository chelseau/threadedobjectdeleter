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

    def __init__(self, parser):
        """
        Initialize all our variables
        :param parser: Our config parser object
        :return: None
        """

        # Store arguments
        self.force_delete = False
        self.marker = dict()
        self.api_endpoint = None
        self.rax = None
        self.region = parser.get('cloudfiles', 'region')
        self.bulk_size = parser.get('cloudfiles', 'bulk_size')
        self.username = parser.get('cloudfiles', 'username')
        self.api_key = parser.get('cloudfiles', 'api_key')
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
                msg=e.message))
            return False
        except pyrax.exceptions.PyraxException as e:
            ThreadedDeleter.output('Unknown error occurred: {msg}'.format(
                msg=e.message))
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
        for prefix in prefixes:
            try:
                containers_ = self.rax.list(prefix=prefix)
            except Exception as e:
                ThreadedDeleter.output('List containers failed: {msg}'
                                       .format(msg=e.message,
                                               retry='retrying.' if retry != 0
                                               else ''))
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
            ThreadedDeleter.output('List objects failed: {msg}'
                                   .format(msg=e.message,
                                           retry='retrying.' if retry != 0
                                           else ''))
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
            for container, objects in local.data.iteritems():
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
            if local.size >= self.bulk_size or self.force_delete:
                self.force_delete = False
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
            ThreadedDeleter.output('Delete container failed: {msg}'
                                   .format(msg=e.message,
                                           retry='retrying.' if retry != 0
                                           else ''))
            if retry == 0:
                return False

            # Retry
            return self.delete_container(container, retry - 1)
