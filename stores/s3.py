"""s3.py: Contains a Amazon S3 implementation of ObjectStore."""

__author__ = "Chelsea Urquhart"
__copyright__ = "Copyright 2015, Chelsea Urquhart"
__license__ = "GPL"
__email__ = "info@chelseau.com"

import os
from boto3.session import Session
import sys
from objectstore import ObjectStore
from threadeddeleter import ThreadedDeleter


class Store(ObjectStore):
    """A ObjectStore class for Amazon S3"""

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
        self.objects = dict()
        self.aws = None
        self.region = ''
        self.access_key_id = ''
        self.access_key_secret = ''
        self.page_size = 10000

        options = ['access_key_id', 'access_key_secret', 'region', 'page_size']
        optional = []

        if not parser.has_section('s3'):
            raise Exception('S3 configuration is missing')

        for option in options:
            if not parser.has_option('s3', option):
                if option not in optional:
                    raise Exception('Missing S3 option: {}'.format(
                        option))
            else:
                setattr(self, option, parser.get('s3', option))

        # Ensure data type
        self.page_size = int(self.page_size)

        # Validate options
        if len(self.region) == 0:
            raise Exception('No region specified')
        if len(self.access_key_id) == 0:
            raise Exception('No API key specified')
        if len(self.access_key_secret) == 0:
            raise Exception('No API key secret specified')
        if self.page_size <= 0:
            raise Exception('Invalid page size specified')

    def login(self):
        """
        Logs into S3. Note that this is on the main thread.
        init_thread is responsible for initializing individual threads.
        :return: True on success, false on failure
        """

        try:
            session = Session(aws_access_key_id=self.access_key_id,
                              aws_secret_access_key=self.access_key_secret,
                              region_name=self.region)
            self.aws = session.resource('s3')
        except Exception as e:
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

        try:
            for prefix in prefixes:
                for bucket in self.aws.buckets.filter(Prefix=prefix):
                    containers.append(bucket.name)
        except Exception as e:
            ThreadedDeleter.output('List containers failed: {msg}.{retry}'
                                   .format(msg=str(e),
                                           retry=self.get_retry_text(
                                               retry)))

            if retry == 0:
                return False

            # Retry
            return self.list_containers(prefixes, retry - 1)

        return containers

    def list_objects(self, container_name, retry=2):
        """
        Lists objects in a given container
        :param container_name: The name of the container to get objects from
        :param retry: The number of retries to use
        :return: A list of objects or False on error
        """
        if container_name in self.objects:
            objects = self.objects.get(container_name)
        else:
            objects = None

        objects_ = list()

        try:
            if objects is None:
                bucket = self.aws.Bucket(container_name)
                objects = iter(bucket.objects.page_size(self.page_size))
                self.objects[container_name] = objects

            for i in range(0, self.page_size):
                try:
                    object_ = next(objects)
                    objects_.append(object_.key)
                except StopIteration as e:
                    # Just ignore this. We're out of files.
                    pass

        except Exception as e:
            ThreadedDeleter.output('List objects failed: {msg}.{retry}'
                                   .format(msg=str(e),
                                           retry=self.get_retry_text(retry)))
            if retry == 0:
                return False

            # Retry
            return self.list_objects(container_name, retry - 1)

        return objects_

    def delete_object(self, container, object, local):
        """
        Deletes an object from a given container
        :param container: The name of the container to get objects from
        :param object: The name of the object to delete
        :param local: A Local class object for storing thread-specific
         variables in.
        :return: None
        """
        try:
            bucket = self.aws.Bucket(container)
            object = bucket.Object(object)
            object.delete()
        except Exception as e:
            ThreadedDeleter.output('Delete object failed: {msg}.'
                                   .format(msg=str(e)))

    def init_thread(self, local):
        """
        Initialize thread-specific RAX connection & data list
        :param local: The Local object
        :return: None
        """
        session = Session(aws_access_key_id=self.access_key_id,
                          aws_secret_access_key=self.access_key_secret,
                          region_name=self.region)
        local.aws = session.resource('s3')
        local.data = dict()
        local.size = 0

    def cleanup_thread(self, local):
        """
        Cleanup thread-specific RAX connection
        :param local: The Local object
        :return: None
        """

    def delete_container(self, container, retry=2):
        """
        Deletes a container
        :param container: The name of the container to get objects from
        :param retry: The number of retries to use
        :return: None
        """
        try:
            bucket = self.aws.Bucket(container)
            bucket.delete()
            return True
        except Exception as e:
            ThreadedDeleter.output('Delete container failed: {msg}.{retry}'
                                   .format(msg=str(e),
                                           retry=self.get_retry_text(retry)))
            if retry == 0:
                return False

            # Retry
            return self.delete_container(container, retry - 1)
