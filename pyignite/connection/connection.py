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

from pyignite.constants import PROTOCOLS, IGNITE_DEFAULT_HOST, IGNITE_DEFAULT_PORT, PROTOCOL_BYTE_ORDER
from pyignite.exceptions import HandshakeError, SocketError, connection_errors, AuthenticationError
from .bitmask_feature import BitmaskFeature

from .handshake import HandshakeRequest, HandshakeResponse
from .protocol_context import ProtocolContext
from .ssl import wrap, check_ssl_params
from ..stream import BinaryStream

CLIENT_STATUS_AUTH_FAILURE = 2000


class BaseConnection:
    def __init__(self, client, host: str = None, port: int = None, username: str = None, password: str = None,
                 **ssl_params):
        self.client = client
        self.host = host if host else IGNITE_DEFAULT_HOST
        self.port = port if port else IGNITE_DEFAULT_PORT
        self.username = username
        self.password = password
        self.uuid = None

        check_ssl_params(ssl_params)

        if self.username and self.password and 'use_ssl' not in ssl_params:
            ssl_params['use_ssl'] = True

        self.ssl_params = ssl_params
        self._failed = False

    @property
    def closed(self) -> bool:
        """ Tells if socket is closed. """
        raise NotImplementedError

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

    @property
    def protocol_context(self):
        """
        Returns protocol context, or None, if no connection to the Ignite
        cluster was yet established.
        """
        return self.client.protocol_context

    def _process_handshake_error(self, response):
        error_text = f'Handshake error: {response.message}'
        # if handshake fails for any reason other than protocol mismatch
        # (i.e. authentication error), server version is 0.0.0
        protocol_version = self.client.protocol_context.version
        server_version = (response.version_major, response.version_minor, response.version_patch)

        if any(server_version):
            error_text += f' Server expects binary protocol version ' \
                          f'{server_version[0]}.{server_version[1]}.{server_version[2]}. ' \
                          f'Client provides ' \
                          f'{protocol_version[0]}.{protocol_version[1]}.{protocol_version[2]}.'
        elif response.client_status == CLIENT_STATUS_AUTH_FAILURE:
            raise AuthenticationError(error_text)
        raise HandshakeError(server_version, error_text)


class Connection(BaseConnection):
    """
    This is a `pyignite` class, that represents a connection to Ignite
    node. It serves multiple purposes:

     * socket wrapper. Detects fragmentation and network errors. See also
       https://docs.python.org/3/howto/sockets.html,
     * binary protocol connector. Encapsulates handshake and failover reconnection.
    """

    def __init__(self, client: 'Client', host: str, port: int, timeout: float = None,
                 username: str = None, password: str = None, **ssl_params):
        """
        Initialize connection.

        For the use of the SSL-related parameters see
        https://docs.python.org/3/library/ssl.html#ssl-certificates.

        :param client: Ignite client object,
        :param host: Ignite server node's host name or IP,
        :param port: Ignite server node's port number,
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
        super().__init__(client, host, port, username, password, **ssl_params)
        self.timeout = timeout
        self._socket = None

    @property
    def closed(self) -> bool:
        return self._socket is None

    def connect(self):
        """
        Connect to the given server node with protocol version fallback.
        """
        detecting_protocol = False

        # choose highest version first
        if self.client.protocol_context is None:
            detecting_protocol = True
            self.client.protocol_context = ProtocolContext(max(PROTOCOLS), BitmaskFeature.all_supported())

        try:
            result = self._connect_version()
        except HandshakeError as e:
            if e.expected_version in PROTOCOLS:
                self.client.protocol_context.version = e.expected_version
                result = self._connect_version()
            else:
                raise e
        except connection_errors:
            # restore undefined protocol version
            if detecting_protocol:
                self.client.protocol_context = None
            raise

        # connection is ready for end user
        features = BitmaskFeature.from_array(result.get('features', None))
        self.client.protocol_context.features = features
        self.uuid = result.get('node_uuid', None)  # version-specific (1.4+)
        self.failed = False

    def _connect_version(self) -> Union[dict, OrderedDict]:
        """
        Connect to the given server node using protocol version
        defined on client.
        """

        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.settimeout(self.timeout)
        self._socket = wrap(self._socket, self.ssl_params)
        self._socket.connect((self.host, self.port))

        protocol_context = self.client.protocol_context

        hs_request = HandshakeRequest(
            protocol_context,
            self.username,
            self.password
        )

        with BinaryStream(self.client) as stream:
            hs_request.from_python(stream)
            self.send(stream.getvalue(), reconnect=False)

        with BinaryStream(self.client, self.recv(reconnect=False)) as stream:
            hs_response = HandshakeResponse.parse(stream, self.protocol_context)

            if hs_response.op_code == 0:
                self.close()
                self._process_handshake_error(hs_response)

            return hs_response

    def reconnect(self):
        if self.alive:
            return

        self.close()

        # connect and silence the connection errors
        try:
            self.connect()
        except connection_errors:
            pass

    def request(self, data: Union[bytes, bytearray], flags=None) -> bytearray:
        """
        Perform request.

        :param data: bytes to send,
        :param flags: (optional) OS-specific flags.
        """
        self.send(data, flags=flags)
        return self.recv()

    def send(self, data: Union[bytes, bytearray], flags=None, reconnect=True):
        """
        Send data down the socket.

        :param data: bytes to send,
        :param flags: (optional) OS-specific flags.
        :param reconnect: (optional) reconnect on failure, default True.
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
            if reconnect:
                self.reconnect()
            raise

    def recv(self, flags=None, reconnect=True) -> bytearray:
        """
        Receive data from the socket.

        :param flags: (optional) OS-specific flags.
        :param reconnect: (optional) reconnect on failure, default True.
        """
        if self.closed:
            raise SocketError('Attempt to use closed connection.')

        kwargs = {}
        if flags is not None:
            kwargs['flags'] = flags

        data = bytearray(1024)
        buffer = memoryview(data)
        bytes_total_received, bytes_to_receive = 0, 0
        while True:
            try:
                bytes_received = self._socket.recv_into(buffer, len(buffer), **kwargs)
                if bytes_received == 0:
                    raise SocketError('Connection broken.')
                bytes_total_received += bytes_received
            except connection_errors:
                self.failed = True
                if reconnect:
                    self.reconnect()
                raise

            if bytes_total_received < 4:
                continue
            elif bytes_to_receive == 0:
                response_len = int.from_bytes(data[0:4], PROTOCOL_BYTE_ORDER)
                bytes_to_receive = response_len

                if response_len + 4 > len(data):
                    buffer.release()
                    data.extend(bytearray(response_len + 4 - len(data)))
                    buffer = memoryview(data)[bytes_total_received:]
                    continue

            if bytes_total_received >= bytes_to_receive:
                buffer.release()
                break

            buffer = buffer[bytes_received:]

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
