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
from asyncio import Lock
from collections import OrderedDict
from io import BytesIO
from typing import Union

from pyignite.constants import PROTOCOLS, PROTOCOL_BYTE_ORDER
from pyignite.exceptions import HandshakeError, SocketError, connection_errors
from .bitmask_feature import BitmaskFeature
from .connection import BaseConnection

from .handshake import HandshakeRequest, HandshakeResponse
from .protocol_context import ProtocolContext
from .ssl import create_ssl_context
from ..stream import AioBinaryStream


class AioConnection(BaseConnection):
    """
    Asyncio connection to Ignite node. It serves multiple purposes:

    * wrapper of asyncio streams. See also https://docs.python.org/3/library/asyncio-stream.html
    * encapsulates handshake and reconnection.
    """

    def __init__(self, client: 'AioClient', host: str, port: int, username: str = None, password: str = None,
                 **ssl_params):
        """
        Initialize connection.

        For the use of the SSL-related parameters see
        https://docs.python.org/3/library/ssl.html#ssl-certificates.

        :param client: Ignite client object,
        :param host: Ignite server node's host name or IP,
        :param port: Ignite server node's port number,
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
        self._mux = Lock()
        self._reader = None
        self._writer = None

    @property
    def closed(self) -> bool:
        """ Tells if socket is closed. """
        return self._writer is None

    async def connect(self) -> Union[dict, OrderedDict]:
        """
        Connect to the given server node with protocol version fallback.
        """
        async with self._mux:
            return await self._connect()

    async def _connect(self) -> Union[dict, OrderedDict]:
        detecting_protocol = False

        # choose highest version first
        if self.client.protocol_context is None:
            detecting_protocol = True
            self.client.protocol_context = ProtocolContext(max(PROTOCOLS), BitmaskFeature.all_supported())

        try:
            result = await self._connect_version()
        except HandshakeError as e:
            if e.expected_version in PROTOCOLS:
                self.client.protocol_context.version = e.expected_version
                result = await self._connect_version()
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
        return result

    async def _connect_version(self) -> Union[dict, OrderedDict]:
        """
        Connect to the given server node using protocol version
        defined on client.
        """

        ssl_context = create_ssl_context(self.ssl_params)
        self._reader, self._writer = await asyncio.open_connection(self.host, self.port, ssl=ssl_context)

        protocol_context = self.client.protocol_context

        hs_request = HandshakeRequest(
            protocol_context,
            self.username,
            self.password
        )

        with AioBinaryStream(self.client) as stream:
            await hs_request.from_python_async(stream)
            await self._send(stream.getbuffer(), reconnect=False)

        with AioBinaryStream(self.client, await self._recv(reconnect=False)) as stream:
            hs_response = await HandshakeResponse.parse_async(stream, self.protocol_context)

            if hs_response.op_code == 0:
                self._close()
                self._process_handshake_error(hs_response)

            return hs_response

    async def reconnect(self):
        async with self._mux:
            await self._reconnect()

    async def _reconnect(self):
        if self.alive:
            return

        self._close()

        # connect and silence the connection errors
        try:
            await self._connect()
        except connection_errors:
            pass

    async def request(self, data: Union[bytes, bytearray, memoryview]) -> bytearray:
        """
        Perform request.

        :param data: bytes to send.
        """
        async with self._mux:
            await self._send(data)
            return await self._recv()

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
