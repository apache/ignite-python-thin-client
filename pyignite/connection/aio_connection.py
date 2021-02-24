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

import asyncio
from asyncio import Lock, AbstractEventLoop
from collections import OrderedDict
from io import BytesIO
from typing import Union

from pyignite.constants import PROTOCOLS, IGNITE_DEFAULT_HOST, IGNITE_DEFAULT_PORT, PROTOCOL_BYTE_ORDER
from pyignite.exceptions import HandshakeError, SocketError, connection_errors, AuthenticationError
from pyignite.datatypes import Byte, Int, Short, String, UUIDObject
from pyignite.datatypes.internal import Struct
from .connection import CLIENT_STATUS_AUTH_FAILURE

from .handshake import HandshakeRequest, HandshakeResponse
from .ssl import create_ssl_context, check_ssl_params
from ..stream import BinaryStream, READ_BACKWARD


class AioConnection:
    """
    """

    _reader = None
    _writer = None
    _failed = None
    _loop = None
    _mux = None

    client = None
    host = None
    port = None
    username = None
    password = None
    ssl_params = {}
    uuid = None

    def __init__(self, client: 'Client', username: str = None, password: str = None,
                 loop: AbstractEventLoop = None, **ssl_params):
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
        self.username = username
        self.password = password
        check_ssl_params(ssl_params)

        if self.username and self.password and 'use_ssl' not in ssl_params:
            ssl_params['use_ssl'] = True

        self.ssl_params = ssl_params

        if loop:
            self._loop = loop
        else:
            self._loop = asyncio.get_event_loop()

        self.ssl_params = ssl_params
        self._mux = Lock()
        self._failed = False

    @property
    def closed(self) -> bool:
        """ Tells if socket is closed. """
        return self._writer is None

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

    def get_protocol_version(self):
        """
        Returns the tuple of major, minor, and revision numbers of the used
        thin protocol version, or None, if no connection to the Ignite cluster
        was yet established.
        """
        return self.client.protocol_version

    async def connect(self, host: str = None, port: int = None) -> Union[dict, OrderedDict]:
        """
        Connect to the given server node with protocol version fallback.

        :param host: Ignite server node's host name or IP,
        :param port: Ignite server node's port number.
        """
        async with self._mux:
            return await self._connect(host, port)

    async def _connect(self, host: str = None, port: int = None) -> Union[dict, OrderedDict]:
        detecting_protocol = False

        # choose highest version first
        if self.client.protocol_version is None:
            detecting_protocol = True
            self.client.protocol_version = max(PROTOCOLS)

        try:
            result = await self._connect_version(host, port)
        except HandshakeError as e:
            if e.expected_version in PROTOCOLS:
                self.client.protocol_version = e.expected_version
                result = await self._connect_version(host, port)
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

    async def _connect_version(
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

        ssl_context = create_ssl_context(self.ssl_params)
        self._reader, self._writer = await asyncio.open_connection(host, port, ssl=ssl_context, loop=self._loop)

        protocol_version = self.client.protocol_version

        hs_request = HandshakeRequest(
            protocol_version,
            self.username,
            self.password
        )

        with BinaryStream(self) as stream:
            await hs_request.from_python_async(stream)
            await self._send(stream.getbuffer(), reconnect=False)

        with BinaryStream(self, await self._recv(reconnect=False)) as stream:
            hs_response = await HandshakeResponse.parse_async(stream, self.get_protocol_version())

            if hs_response.op_code == 0:
                self._close()

                error_text = f'Handshake error: {hs_response.message}'
                # if handshake fails for any reason other than protocol mismatch
                # (i.e. authentication error), server version is 0.0.0
                server_version = (hs_response.version_major, hs_response.version_minor, hs_response.version_patch)

                if any(server_version):
                    error_text += f' Server expects binary protocol version ' \
                                  f'{server_version[0]}.{server_version[1]}.{server_version[2]}. ' \
                                  f'Client provides ' \
                                  f'{protocol_version[0]}.{protocol_version[1]}.{protocol_version[2]}.'
                elif hs_response.client_status == CLIENT_STATUS_AUTH_FAILURE:
                    raise AuthenticationError(error_text)
                raise HandshakeError(server_version, error_text)
            self.host, self.port = host, port
            return hs_response

    async def reconnect(self):
        with self._mux:
            return await self._reconnect()

    async def _reconnect(self):
        # do not reconnect if connection is already working
        # or was closed on purpose
        if not self.failed:
            return

        self._close()

        # connect and silence the connection errors
        try:
            await self._connect(self.host, self.port)
        except connection_errors:
            pass

    async def send(self, data: Union[bytes, bytearray, memoryview]):
        async with self._mux:
            return await self._send(data)

    async def _send(self, data: Union[bytes, bytearray, memoryview], reconnect=True):
        if self.closed:
            raise SocketError('Attempt to use closed connection.')

        try:
            self._writer.write(data)
            await self._writer.drain()
        except connection_errors:
            self.failed = True
            if reconnect:
                await self._reconnect()
            raise

    async def recv(self) -> bytearray:
        async with self._mux:
            return await self._recv()

    async def _recv(self, reconnect=True) -> bytearray:
        if self.closed:
            raise SocketError('Attempt to use closed connection.')

        with BytesIO() as stream:
            try:
                buf = await self._reader.readexactly(4)
                response_len = int.from_bytes(buf, PROTOCOL_BYTE_ORDER)

                stream.write(buf)

                stream.write(await self._reader.readexactly(response_len))
            except connection_errors:
                self.failed = True
                if reconnect:
                    await self._reconnect()
                raise

            return bytearray(stream.getbuffer())

    async def close(self):
        async with self._mux:
            self._close()

    def _close(self):
        """
        Close connection.
        """
        if self._writer:
            try:
                self._writer.close()
            except connection_errors:
                pass

            self._writer, self._reader = None, None
