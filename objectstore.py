"""objectstore.py: An abstract ObjectStore class."""

__author__ = "Chelsea Urquhart"
__copyright__ = "Copyright 2015, Chelsea Urquhart"
__license__ = "GPL"
__email__ = "info@chelseau.com"

from abc import ABCMeta, abstractmethod


class ObjectStore:
    """An abstract ObjectStore class for accessing various object stores"""
    __metaclass__ = ABCMeta

    @abstractmethod
    def login(self):
        """
        Log in to the object store service and retrieve anything necessary
        to delete objects and containers.
        :return: None
        """

    @abstractmethod
    def list_containers(self, prefixes):
        """
        Lists containers begining with any of the provided prefixes
        :param prefixes: The (list of) prefixes to get containers for
        :return: A list of containers
        """

    @abstractmethod
    def list_objects(self, container):
        """
        Lists objects in a given container
        :param container: The name of the container to get objects from
        :return: A list of objects
        """

    @abstractmethod
    def delete_object(self, container, object_, local):
        """
        Deletes an object from a given container
        :param container: The name of the container to get objects from
        :param object_: The name of the object to delete
        :param local: A Local class object for storing thread-specific
         variables in.
        :return: None
        """

    @abstractmethod
    def init_thread(self, local):
        """
        Initialize anything needed in the Local object for a thread
        :param local: The Local object
        :return: None
        """

    @abstractmethod
    def cleanup_thread(self, local):
        """
        Cleanup anything that needs cleaning up in the Local object for a thread
        :param local: The Local object
        :return: None
        """

    @abstractmethod
    def delete_container(self, container):
        """
        Deletes a container
        :param container: The name of the container to get objects from
        :return: None
        """
