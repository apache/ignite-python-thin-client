# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from collections import OrderedDict
import socket
from typing import Union

from pyignite.constants import *
from pyignite.exceptions import (
    HandshakeError, ParameterError, SocketError, connection_errors,
)
from pyignite.datatypes import Byte, Int, Short, String, UUIDObject
from pyignite.datatypes.internal import Struct

from .handshake import HandshakeRequest
from .ssl import wrap
from ..stream import BinaryStream, READ_BACKWARD


class Connection:
    """
    This is a `pyignite` class, that represents a connection to Ignite
    node. It serves multiple purposes:

     * socket wrapper. Detects fragmentation and network errors. See also
       https://docs.python.org/3/howto/sockets.html,
     * binary protocol connector. Incapsulates handshake and failover reconnection.
    """

    _socket = None
    _failed = None

    client = None
    host = None
    port = None
    timeout = None
    username = None
    password = None
    ssl_params = {}
    uuid = None

    @staticmethod
    def _check_ssl_params(params):
        expected_args = [
            'use_ssl',
            'ssl_version',
            'ssl_ciphers',
            'ssl_cert_reqs',
            'ssl_keyfile',
            'ssl_keyfile_password',
            'ssl_certfile',
            'ssl_ca_certfile',
        ]
        for param in params:
            if param not in expected_args:
                raise ParameterError((
                    'Unexpected parameter for connection initialization: `{}`'
                ).format(param))

    def __init__(
        self, client: 'Client', timeout: float = 2.0,
        username: str = None, password: str = None, **ssl_params
    ):
        """
        Initialize connection.

        For the use of the SSL-related parameters see
        https://docs.python.org/3/library/ssl.html#ssl-certificates.

        :param client: Ignite client object,
        :param timeout: (optional) sets timeout (in seconds) for each socket
         operation including `connect`. 0 means non-blocking mode, which is
         virtually guaranteed to fail. Can accept integer or float value.
         Default is None (blocking mode),
        :param use_ssl: (optional) set to True if Ignite server uses SSL
         on its binary connector. Defaults to use SSL when username
         and password has been supplied, not to use SSL otherwise,
        :param ssl_version: (optional) SSL version constant from standard
         `ssl` module. Defaults to TLS v1.1, as in Ignite 2.5,
        :param ssl_ciphers: (optional) ciphers to use. If not provided,
         `ssl` default ciphers are used,
        :param ssl_cert_reqs: (optional) determines how the remote side
         certificate is treated:

         * `ssl.CERT_NONE` − remote certificate is ignored (default),
         * `ssl.CERT_OPTIONAL` − remote certificate will be validated,
           if provided,
         * `ssl.CERT_REQUIRED` − valid remote certificate is required,

        :param ssl_keyfile: (optional) a path to SSL key file to identify
         local (client) party,
        :param ssl_keyfile_password: (optional) password for SSL key file,
         can be provided when key file is encrypted to prevent OpenSSL
         password prompt,
        :param ssl_certfile: (optional) a path to ssl certificate file
         to identify local (client) party,
        :param ssl_ca_certfile: (optional) a path to a trusted certificate
         or a certificate chain. Required to check the validity of the remote
         (server-side) certificate,
        :param username: (optional) user name to authenticate to Ignite
         cluster,
        :param password: (optional) password to authenticate to Ignite cluster.
        """
        self.client = client
        self.timeout = timeout
        self.username = username
        self.password = password
        self._check_ssl_params(ssl_params)
        if self.username and self.password and 'use_ssl' not in ssl_params:
            ssl_params['use_ssl'] = True
        self.ssl_params = ssl_params
        self._failed = False

    @property
    def closed(self) -> bool:
        """ Tells if socket is closed. """
        return self._socket is None

    @property
    def failed(self) -> bool:
        """ Tells if connection is failed. """
        return self._failed

    @failed.setter
    def failed(self, value):
        self._failed = value

    @property
    def alive(self) -> bool:
        """ Tells if connection is up and no failure detected. """
        return not self.failed and not self.closed

    def __repr__(self) -> str:
        return '{}:{}'.format(self.host or '?', self.port or '?')

    _wrap = wrap

    def get_protocol_version(self):
        """
        Returns the tuple of major, minor, and revision numbers of the used
        thin protocol version, or None, if no connection to the Ignite cluster
        was yet established.
        """
        return self.client.protocol_version

    def read_response(self) -> Union[dict, OrderedDict]:
        """
        Processes server's response to the handshake request.

        :return: handshake data.
        """
        response_start = Struct([
            ('length', Int),
            ('op_code', Byte),
        ])
        with BinaryStream(self, self.recv()) as stream:
            start_class = response_start.parse(stream)
            start = stream.read_ctype(start_class, direction=READ_BACKWARD)
            data = response_start.to_python(start)
            response_end = None
            if data['op_code'] == 0:
                response_end = Struct([
                    ('version_major', Short),
                    ('version_minor', Short),
                    ('version_patch', Short),
                    ('message', String),
                ])
            elif self.get_protocol_version() >= (1, 4, 0):
                response_end = Struct([
                    ('node_uuid', UUIDObject),
                ])
            if response_end:
                end_class = response_end.parse(stream)
                end = stream.read_ctype(end_class, direction=READ_BACKWARD)
                data.update(response_end.to_python(end))
            return data

    def connect(
        self, host: str = None, port: int = None
    ) -> Union[dict, OrderedDict]:
        """
        Connect to the given server node with protocol version fallback.

        :param host: Ignite server node's host name or IP,
        :param port: Ignite server node's port number.
        """
        detecting_protocol = False

        # choose highest version first
        if self.client.protocol_version is None:
            detecting_protocol = True
            self.client.protocol_version = max(PROTOCOLS)

        try:
            result = self._connect_version(host, port)
        except HandshakeError as e:
            if e.expected_version in PROTOCOLS:
                self.client.protocol_version = e.expected_version
                result = self._connect_version(host, port)
            else:
                raise e
        except connection_errors:
            # restore undefined protocol version
            if detecting_protocol:
                self.client.protocol_version = None
            raise

        # connection is ready for end user
        self.uuid = result.get('node_uuid', None)  # version-specific (1.4+)

        self.failed = False
        return result

    def _connect_version(
        self, host: str = None, port: int = None,
    ) -> Union[dict, OrderedDict]:
        """
        Connect to the given server node using protocol version
        defined on client.

        :param host: Ignite server node's host name or IP,
        :param port: Ignite server node's port number.
        """

        host = host or IGNITE_DEFAULT_HOST
        port = port or IGNITE_DEFAULT_PORT

        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.settimeout(self.timeout)
        self._socket = self._wrap(self._socket)
        self._socket.connect((host, port))

        protocol_version = self.client.protocol_version

        hs_request = HandshakeRequest(
            protocol_version,
            self.username,
            self.password
        )

        with BinaryStream(self) as stream:
            hs_request.from_python(stream)
            self.send(stream.getbuffer())

        hs_response = self.read_response()
        if hs_response['op_code'] == 0:
            self.close()

            error_text = 'Handshake error: {}'.format(hs_response['message'])
            # if handshake fails for any reason other than protocol mismatch
            # (i.e. authentication error), server version is 0.0.0
            if any([
                hs_response['version_major'],
                hs_response['version_minor'],
                hs_response['version_patch'],
            ]):
                error_text += (
                    ' Server expects binary protocol version '
                    '{version_major}.{version_minor}.{version_patch}. Client '
                    'provides {client_major}.{client_minor}.{client_patch}.'
                ).format(
                    client_major=protocol_version[0],
                    client_minor=protocol_version[1],
                    client_patch=protocol_version[2],
                    **hs_response
                )
            raise HandshakeError((
                hs_response['version_major'],
                hs_response['version_minor'],
                hs_response['version_patch'],
            ), error_text)
        self.host, self.port = host, port
        return hs_response

    def reconnect(self):
        # do not reconnect if connection is already working
        # or was closed on purpose
        if not self.failed:
            return

        self.close()

        # connect and silence the connection errors
        try:
            self.connect(self.host, self.port)
        except connection_errors:
            pass

    def send(self, data: Union[bytes, bytearray, memoryview], flags=None):
        """
        Send data down the socket.

        :param data: bytes to send,
        :param flags: (optional) OS-specific flags.
        """
        if self.closed:
            raise SocketError('Attempt to use closed connection.')

        kwargs = {}
        if flags is not None:
            kwargs['flags'] = flags

        try:
            self._socket.sendall(data, **kwargs)
        except connection_errors:
            self.failed = True
            self.reconnect()
            raise

    def recv(self, flags=None) -> bytearray:
        def _recv(buffer, num_bytes):
            bytes_to_receive = num_bytes
            while bytes_to_receive > 0:
                try:
                    bytes_rcvd = self._socket.recv_into(buffer, bytes_to_receive, **kwargs)
                    if bytes_rcvd == 0:
                        raise SocketError('Connection broken.')
                except connection_errors:
                    self.failed = True
                    self.reconnect()
                    raise

                buffer = buffer[bytes_rcvd:]
                bytes_to_receive -= bytes_rcvd

        if self.closed:
            raise SocketError('Attempt to use closed connection.')

        kwargs = {}
        if flags is not None:
            kwargs['flags'] = flags

        data = bytearray(4)
        _recv(memoryview(data), 4)
        response_len = int.from_bytes(data, PROTOCOL_BYTE_ORDER)

        data.extend(bytearray(response_len))
        _recv(memoryview(data)[4:], response_len)
        return data

    def close(self):
        """
        Try to mark socket closed, then unlink it. This is recommended but
        not required, since sockets are automatically closed when
        garbage-collected.
        """
        if self._socket:
            try:
                self._socket.shutdown(socket.SHUT_RDWR)
                self._socket.close()
            except connection_errors:
                pass

            self._socket = None
