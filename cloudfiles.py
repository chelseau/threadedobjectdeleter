"""cloudfiles.py: Contains a CloudFiles implementation of ObjectStore."""

__author__ = "Chelsea Urquhart"
__copyright__ = "Copyright 2014, Chelsea Urquhart"
__credits__ = "Rackspace [http://tinyurl.com/os95vts]"
__license__ = "GPL"
__email__ = "info@chelseau.com"

from StringIO import StringIO
from urllib import quote

import pycurl
from lxml import builder, etree

from objectstore import ObjectStore


class CloudFiles(ObjectStore):
    """A ObjectStore class for Rackspace Cloud Files"""

    def __init__(self, username, api_key, region, login_url, bulk_size):
        """
        Initialize all our variables
        :return: None
        """

        # Store arguments
        self.username = username
        self.api_key = api_key
        self.region = region
        self.login_url = login_url
        self.bulk_size = bulk_size
        self.force_delete = False
        self.marker = {}

        self.last_headers = {}
        self.last_code = None
        self.last_status = None
        self.auth_token = None
        self.api_endpoint = None

    def curl_headers(self, header_line):
        """
        Callback for cURL to pass headers to
        :param header_line: The line of the header being processed
        :return: None
        """
        header_line = header_line.decode('iso-8859-1')
        if ':' in header_line:
            name, value = header_line.split(':', 1)
            self.last_headers[name.strip()] = value.strip()
        else:
            try:
                name, last_code, last_status = header_line.split(' ', 2)
                self.last_code = int(last_code)
                self.last_status = last_status
            except ValueError:
                # Not the line we're looking for!
                pass

    def curl_request(self, url, curl_handle, request=None, headers=None,
                     method='POST', read_headers=True):
        """
        Function to send a cURL request. This does not close connections so they
        can be reused.
        :param url: The url to retrieve
        :param ch: The cURL handle to use
        :param request: The request data to send (string)
        :param headers: Any headers to send with the request (array)
        :param method: The request method to use
        :return: Tuple of headers, content
        """
        self.last_headers = {}
        self.last_code = None
        self.last_status = None
        buffer = StringIO()
        curl_handle.setopt(curl_handle.URL, url)
        if headers is not None:
            curl_handle.setopt(curl_handle.HTTPHEADER, headers)
        curl_handle.setopt(curl_handle.WRITEDATA, buffer)
        if request is not None:
            curl_handle.setopt(curl_handle.POSTFIELDS, request)
        curl_handle.setopt(curl_handle.CUSTOMREQUEST, method)
        if read_headers:
            # We only look at the response on the main thread. The delete
            # object threads set this to False
            curl_handle.setopt(curl_handle.HEADERFUNCTION, self.curl_headers)
        curl_handle.perform()
        return buffer.getvalue()

    def login_v2(self):
        """
        Logs into cloud files and obtains/stores an authorization token
        Uses API v2
        :return: None
        """
        xml = builder.E.auth(
            builder.E.apiKeyCredentials(
                xmlns='http://docs.rackspace.com/identity/api/ext/RAX-KSKEY'
                      '/v1.0',
                username=self.username,
                apiKey=self.api_key)
        )
        namespace = 'http://docs.openstack.org/identity/api/v2.0'
        request = '<?xml version="1.0" encoding="UTF-8"?>' + etree.tostring(xml)
        request_headers = ['Content-Type: application/xml',
                           'Accept: application/xml']
        ch = pycurl.Curl()
        response = self.curl_request(self.login_url + '/tokens', ch, request,
                                     request_headers)
        ch.close()
        response = etree.fromstring(response)
        auth_errno = response.xpath('//ns:unauthorized/@code',
                                    namespaces={'ns': namespace})
        auth_error = response.xpath('//ns:unauthorized/ns:message',
                                    namespaces={'ns': namespace})
        if len(auth_errno) > 0 and len(auth_error) > 0:
            raise Exception(
                'Login error [%s]: %s' % (auth_errno[0], auth_error[0].text))
        try:
            self.auth_token = response.xpath('//ns:access/ns:token/@id',
                                             namespaces={'ns': namespace})[0]
            self.api_endpoint = response.xpath(
                '//ns:access/ns:serviceCatalog/ns:service['
                '@name="cloudFiles"]/ns:endpoint[@region="' + self.region +
                '"]/@publicURL',
                namespaces={'ns': namespace})[0]
        except IndexError:
            raise Exception(
                'Bad response received on login request: %s' % etree.tostring(
                    response))

    def login(self):
        """
        Logs into cloud files and obtains/stores an authorization token
        :return: None
        """

        if '2.0' in self.login_url:
            return self.login_v2()

        namespace = 'http://docs.rackspacecloud.com/auth/api/v1.1'
        xml = builder.E.credentials(
            xmlns=namespace,
            username=self.username,
            key=self.api_key)
        request = '<?xml version="1.0" encoding="UTF-8"?>' + etree.tostring(xml)
        request_headers = ['Content-Type: application/xml',
                           'Accept: application/xml']
        ch = pycurl.Curl()
        response = self.curl_request(self.login_url, ch, request,
                                     request_headers)
        ch.close()
        response = etree.fromstring(response)
        auth_errno = response.xpath('//ns:unauthorized/@code',
                                    namespaces={'ns': namespace})
        auth_error = response.xpath('//ns:unauthorized/ns:message',
                                    namespaces={'ns': namespace})
        if len(auth_errno) > 0 and len(auth_error) > 0:
            raise Exception(
                'Login error [%s]: %s' % (auth_errno[0], auth_error[0].text))
        try:
            self.auth_token = response.xpath('//ns:auth/ns:token/@id',
                                             namespaces={'ns': namespace})[0]
            self.api_endpoint = response.xpath(
                '//ns:auth/ns:serviceCatalog/ns:service['
                '@name="cloudFiles"]/ns:endpoint[@region="' + self.region +
                '"]/@publicURL',
                namespaces={'ns': namespace})[0]
        except IndexError:
            raise Exception(
                'Bad response received on login request: %s' % etree.tostring(
                    response))

    def list_containers(self, prefixes):
        """
        Lists containers begining with any of the provided prefixes
        :param prefixes: The (list of) prefixes to get containers for
        :return: A list of containers
        """
        request_headers = ['X-Auth-Token: ' + self.auth_token]
        ch = pycurl.Curl()
        response = self.curl_request(self.api_endpoint + '/', ch, None,
                                     request_headers,
                                     'GET')
        ch.close()
        if self.last_code < 200 or self.last_code > 299:
            raise Exception('HTTP Error (List Containers) %s: %s' % (
                self.last_code, self.last_status))
        containers = []
        for container in filter(None, response.split('\n')):
            for prefix in prefixes:
                if container.startswith(prefix):
                    containers.append(container)
        return containers

    def list_objects(self, container, recheck=False):
        """
        Lists objects in a given container
        :param container: The name of the container to get objects from
        :return: A list of objects
        """
        self.force_delete = True
        request_headers = ['X-Auth-Token: ' + self.auth_token]
        ch = pycurl.Curl()
        params = ''
        if container in self.marker:
            params = '?marker=' + quote(self.marker[container])
        response = self.curl_request(
            self.api_endpoint + '/' + quote(container) + params, ch, None,
            request_headers, 'GET')
        ch.close()
        if self.last_code < 200 or self.last_code > 299:
            raise Exception(
                ('HTTP Error (List Objects) %s: %s\n%s', self.last_headers) % (
                    self.last_code, self.last_status))
        response = filter(None, response.split('\n'))
        if len(response) > 0:
            self.marker[container] = response[-1]
            return response
        else:
            if recheck:
                return response
            if container in self.marker:
                del self.marker[container]
            return self.list_objects(container, True)

    def delete_objects_bulk(self, local):
        if len(local.data) > 0:
            local.processed += len(local.data)
            if local.processed % (self.bulk_size * 2) == 0:
                # Connections can easily become unusable. We don't want a thread
                # to become unusable for long. Lets reset our cURL connection
                # if it is unitialized, or has already been used 19 times.
                local.ch = pycurl.Curl()
            request_headers = ['X-Auth-Token: ' + self.auth_token,
                               'Content-Type: text/plain']
            self.curl_request(
                self.api_endpoint + '/?bulk-delete', local.ch,
                '\n'.join(local.data), request_headers, 'DELETE', False)
            local.data = []


    def delete_object(self, container, object, local):
        """
        Deletes an object from a given container
        :param container: The name of the container to get objects from
        :param object: The name of the object to delete
        :param local: A Local class object for storing thread-specific
         variables in.
        :return: None
        """
        request_headers = ['X-Auth-Token: ' + self.auth_token]
        if self.bulk_size <= 1:
            local.processed += 1
            if local.processed % 20 == 0:
                # Connections can easily become unusable. We don't want a thread
                # to become unusable for long. Lets reset our cURL connection
                # if it is unitialized, or has already been used 19 times.
                local.ch = pycurl.Curl()
            self.curl_request(
                self.api_endpoint + '/' + quote(
                    container) + '/' + quote(object), local.ch,
                None, request_headers, 'DELETE', False)
        else:
            local.data.append('/' + container + '/' + object)
            if len(local.data) >= self.bulk_size or self.force_delete:
                self.force_delete = False
                self.delete_objects_bulk(local)
                # We can't really do error checking on these because
                # self.last_code
                # and self.last_status are not thread safe. If this becomes
                # an issue,
                # I will implement that via the Local object

    def init_local(self, local):
        """
        Initialize thread-specific cURL connection & processed count
        :param local: The Local object
        :return: None
        """
        local.processed = 0
        local.ch = pycurl.Curl()
        local.data = []

    def cleanup_local(self, local):
        """
        Cleanup thread-specific cURL connection
        :param local: The Local object
        :return: None
        """

        # Delete any remaining objects first if using bulk deletions
        self.delete_objects_bulk(local)
        local.ch.close()

    def delete_container(self, container):
        """
        Deletes a container
        :param container: The name of the container to get objects from
        :return: None
        """
        request_headers = ['X-Auth-Token: ' + self.auth_token]
        ch = pycurl.Curl()
        self.curl_request(
            self.api_endpoint + '/' + container, ch, None,
            request_headers, 'DELETE')
        if self.last_code < 200 or self.last_code > 299:
            raise Exception('HTTP Error (Delete Container) %s: %s' % (
                self.last_code, self.last_status))
        ch.close()