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
import logging
import socket
import struct
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from pyignite import Client, AioClient
from pyignite import monitoring
from pyignite.exceptions import ReconnectError, ParameterError
from pyignite.monitoring import HandshakeFailedEvent

logger = logging.getLogger('fake_ignite')
logger.setLevel(logging.DEBUG)

DEFAULT_HOST = '127.0.0.1'
DEFAULT_PORT = 10800


class FakeIgniteProtocol(asyncio.Protocol):
    def __init__(self, server):
        self._transport = None
        self._server = server
        self._buf = bytearray()
        self._done_handshake = False

    def connection_made(self, transport):
        sock = transport.get_extra_info('socket')
        if sock is not None:
            logger.debug('Connecting from %s', sock)
        self._server.add_client(transport)
        self._transport = transport

    def _handshake_response(self, error=True):
        if error:
            return struct.pack('<lbhhhbll', 16, 0, 1, 3, 0, 9, 0, 0)
        else:
            return struct.pack('<lb', 1, 1)

    def _parse_handshake_request(self, buf):
        return struct.unpack('<lbhhhb', buf)

    def data_received(self, data):
        logger.debug(f'Data received: {data if data else b""}')

        if self._server.do_handshake and not self._done_handshake:
            self._buf += data

            if len(self._buf) < 12:
                return

            req = self._parse_handshake_request(self._buf[0:12])

            if req[1] == 1 and (req[2], req[3], req[4]) > (1, 3, 0):
                response = self._handshake_response(True)
                logger.debug(f'Writing handshake response {response}')
                self._transport.write(response)
                self._transport.close()
            else:
                response = self._handshake_response(False)
                logger.debug(f'Writing handshake response {response}')
                self._transport.write(response)
                self._done_handshake = True
            self._buf = bytearray()


class FakeIgniteServer:
    def __init__(self, do_handshake=False):
        self.clients = []
        self.server = None
        self.do_handshake = do_handshake
        self.loop = asyncio.get_event_loop()

    async def start(self):
        self.server = await self.loop.create_server(lambda: FakeIgniteProtocol(self), DEFAULT_HOST, DEFAULT_PORT)

    def add_client(self, client):
        self.clients.append(client)

    async def close(self):
        for client in self.clients:
            client.close()

        if self.server:
            self.server.close()
            await self.server.wait_closed()


class HandshakeTimeoutListener(monitoring.ConnectionEventListener):
    def __init__(self):
        self.events = []

    def on_handshake_fail(self, event: HandshakeFailedEvent):
        self.events.append(event)


@pytest.fixture
async def server():
    server = FakeIgniteServer()
    try:
        await server.start()
        yield server
    finally:
        await server.close()


@pytest.fixture
async def server_with_handshake():
    server = FakeIgniteServer(do_handshake=True)
    try:
        await server.start()
        yield server
    finally:
        await server.close()


@pytest.mark.asyncio
async def test_handshake_timeout(server, event_loop):
    def sync_client_connect():
        hs_to_listener = HandshakeTimeoutListener()
        client = Client(handshake_timeout=3.0, event_listeners=[hs_to_listener])
        start = time.monotonic()
        try:
            client.connect(DEFAULT_HOST, DEFAULT_PORT)
        except Exception as e:
            return time.monotonic() - start, hs_to_listener.events, e
        return time.monotonic() - start, hs_to_listener.events, None

    duration, events, err = await event_loop.run_in_executor(ThreadPoolExecutor(), sync_client_connect)

    assert isinstance(err, ReconnectError)
    assert 3.0 <= duration < 4.0
    assert len(events) > 0
    for ev in events:
        assert isinstance(ev, HandshakeFailedEvent)
        assert 'timed out' in ev.error_msg


@pytest.mark.asyncio
async def test_handshake_timeout_async(server):
    hs_to_listener = HandshakeTimeoutListener()
    client = AioClient(handshake_timeout=3.0, event_listeners=[hs_to_listener])
    with pytest.raises(ReconnectError):
        start = time.monotonic()
        await client.connect(DEFAULT_HOST, DEFAULT_PORT)

    assert 3.0 <= time.monotonic() - start < 4.0
    assert len(hs_to_listener.events) > 0
    for ev in hs_to_listener.events:
        assert isinstance(ev, HandshakeFailedEvent)
        assert 'timed out' in ev.error_msg


@pytest.mark.asyncio
async def test_socket_timeout_applied_sync(server_with_handshake, event_loop):
    def sync_client_connect():
        hs_to_listener = HandshakeTimeoutListener()
        client = Client(timeout=5.0, handshake_timeout=3.0, event_listeners=[hs_to_listener])
        start = time.monotonic()
        try:
            client.connect(DEFAULT_HOST, DEFAULT_PORT)
            assert all(n.alive for n in client._nodes)
            client.get_cache_names()
        except Exception as e:
            return time.monotonic() - start, hs_to_listener.events, e
        return time.monotonic() - start, hs_to_listener.events, None

    duration, events, err = await event_loop.run_in_executor(ThreadPoolExecutor(), sync_client_connect)

    assert isinstance(err, socket.timeout)
    assert 5.0 <= duration < 6.0
    assert len(events) == 0


@pytest.mark.asyncio
async def test_handshake_timeout_not_affected_for_others_requests_async(server_with_handshake):
    hs_to_listener = HandshakeTimeoutListener()
    client = AioClient(handshake_timeout=3.0, event_listeners=[hs_to_listener])
    with pytest.raises(asyncio.TimeoutError):
        await client.connect(DEFAULT_HOST, DEFAULT_PORT)
        assert all(n.alive for n in client._nodes)
        await asyncio.wait_for(client.get_cache_names(), 5.0)


@pytest.mark.parametrize(
    'handshake_timeout',
    [0.0, -10.0, -0.01]
)
@pytest.mark.asyncio
async def test_handshake_timeout_param_validation(handshake_timeout):
    with pytest.raises(ParameterError):
        await AioClient(handshake_timeout=handshake_timeout).connect(DEFAULT_HOST, DEFAULT_PORT)

    with pytest.raises(ParameterError):
        Client(handshake_timeout=handshake_timeout).connect(DEFAULT_HOST, DEFAULT_PORT)
