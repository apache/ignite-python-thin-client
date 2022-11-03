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
import sys
from asyncio import TimeoutError, InvalidStateError

import pytest

from pyignite import AioClient
from pyignite.aio_cache import AioCache
from pyignite.datatypes.key_value import PeekModes
from tests.util import start_ignite_gen


@pytest.fixture(scope='module', autouse=True)
def server1():
    yield from start_ignite_gen(idx=1)


@pytest.fixture(autouse=True)
async def proxy(event_loop, server1, cache):
    proxy = ProxyServer(("127.0.0.1", 10802), ("127.0.0.1", 10801))
    try:
        await proxy.start()
        yield proxy
    finally:
        await proxy.close()


@pytest.fixture(scope='module', autouse=True)
async def cache(server1):
    c = AioClient(partition_aware=False)
    async with c.connect("127.0.0.1", 10801):
        try:
            cache = await c.get_or_create_cache("test")
            yield cache
        finally:
            await cache.destroy()


@pytest.fixture(autouse=True)
def invalid_states_errors():
    errors = []

    def trace(_, event, arg):
        if event == 'exception':
            etype, _, _ = arg
            if etype is InvalidStateError:
                errors.append(arg)

        return trace

    try:
        sys.settrace(trace)
        yield errors
    finally:
        sys.settrace(None)


cache_method_params = (
    "method,targs",
    [
        (AioCache.get, (1,)),
        (AioCache.put, (1, 1)),
        (AioCache.get_all, ([1, 1],)),
        (AioCache.put_all, ({1: 1},)),
        (AioCache.replace, (1, 1)),
        (AioCache.clear, ()),
        (AioCache.clear_key, (1,)),
        (AioCache.clear_keys, ([1, 1],)),
        (AioCache.contains_key, (1,)),
        (AioCache.contains_keys, ([1, 1],)),
        (AioCache.get_and_put, (1, 1)),
        (AioCache.get_and_put_if_absent, (1, 1)),
        (AioCache.put_if_absent, (1, 1)),
        (AioCache.get_and_remove, (1,)),
        (AioCache.get_and_replace, (1, 1)),
        (AioCache.remove_key, (1,)),
        (AioCache.remove_keys, ([1, 1],)),
        (AioCache.remove_all, ()),
        (AioCache.remove_if_equals, (1, 1)),
        (AioCache.replace_if_equals, (1, 1, 1)),
        (AioCache.get_size, ([PeekModes.PRIMARY, PeekModes.BACKUP],)),
        (AioCache.get_size, ()),
        (AioCache.settings, ())
    ]
)


@pytest.mark.parametrize(*cache_method_params)
@pytest.mark.asyncio
async def test_cancellation_on_slow_response(event_loop, proxy, invalid_states_errors,
                                             method, targs):
    c = AioClient(partition_aware=False)
    async with c.connect("127.0.0.1", 10802):
        cache = await c.get_cache("test")

        proxy.slow_response = True
        with pytest.raises(TimeoutError):
            await method(cache, *targs, timeout=0.1)

        proxy.slow_response = False
        assert len(invalid_states_errors) == 0


@pytest.mark.parametrize(*cache_method_params)
@pytest.mark.asyncio
async def test_cancellation_on_disconnect(event_loop, proxy, invalid_states_errors,
                                          method, targs):
    c = AioClient(partition_aware=False)
    async with c.connect("127.0.0.1", 10802):
        cache = await c.get_cache("test")
        proxy.discard_response = True

        result = asyncio.ensure_future(method(cache, *targs, timeout=0.1))
        await asyncio.sleep(0.2)
        await proxy.disconnect_peers()

        with pytest.raises(TimeoutError):
            await result

    assert len(invalid_states_errors) == 0


class ProxyServer:
    """
    Proxy for simulating discarding response or slow response from ignite server
    Set `discard_response` or `slow_response` to `True` to simulate this condition.
    Set `slow_response_timeout` to change sleep time interval for slow responses.
    Call `disconnect_peers()` in order to simulate lost connection to Ignite server.
    """
    def __init__(self, local_host, remote_host):
        self.local_host = local_host
        self.remote_host = remote_host
        self.peers = {}
        self.discard_response, self.slow_response = False, False
        self.slow_response_timeout = 0.1
        self.server = None

    async def start(self):
        loop = asyncio.get_event_loop()
        host, port = self.local_host
        self.server = await loop.create_server(
            lambda: ProxyTcpProtocol(self), host=host, port=port)

    async def disconnect_peers(self):
        peers = dict(self.peers)
        for k, v in peers.items():
            if not v:
                return

            local, remote = v
            if local:
                await remote.close()
            if remote:
                await local.close()

    async def close(self):
        try:
            await self.disconnect_peers()
        except TimeoutError:
            pass

        self.server.close()


class ProxyTcpProtocol(asyncio.Protocol):
    def __init__(self, proxy):
        self.addr, self.port = proxy.remote_host
        self.proxy = proxy
        self.transport, self.remote_protocol = None, None
        self.conn_info, self.close_fut = None, None
        super().__init__()

    def connection_made(self, transport):
        self.transport = transport
        self.conn_info = transport.get_extra_info("peername")

    def data_received(self, data):
        if self.remote_protocol and self.remote_protocol.transport:
            self.remote_protocol.transport.write(data)
            return

        loop = asyncio.get_event_loop()
        self.remote_protocol = RemoteTcpProtocol(self.proxy, self, data)
        coro = loop.create_connection(lambda: self.remote_protocol, host=self.addr, port=self.port)
        asyncio.ensure_future(coro)

        self.proxy.peers[self.conn_info] = (self, self.remote_protocol)

    async def close(self):
        if not self.transport:
            return

        self.close_fut = asyncio.get_event_loop().create_future()
        self.transport.close()

        try:
            await asyncio.wait_for(self.close_fut, 0.1)
        except TimeoutError:
            pass

    def connection_lost(self, exc):
        if self.close_fut:
            self.close_fut.done()


class RemoteTcpProtocol(asyncio.Protocol):
    def __init__(self, proxy, proxy_protocol, data):
        self.proxy = proxy
        self.proxy_protocol = proxy_protocol
        self.data = data
        self.transport, self.close_fut = None, None
        self.queue = []
        self.loop_task = asyncio.ensure_future(self.send_loop())
        self.stop_flag = False
        super().__init__()

    def connection_made(self, transport):
        self.transport = transport
        self.transport.write(self.data)

    async def close(self):
        if not self.transport:
            return

        self.stop_flag = True
        self.close_fut = asyncio.get_event_loop().create_future()
        self.transport.close()
        try:
            await asyncio.wait_for(self.loop_task, 0.1)
        except TimeoutError:
            pass

        try:
            await asyncio.wait_for(self.close_fut, 0.1)
        except TimeoutError:
            pass

    def connection_lost(self, exc):
        if self.close_fut:
            self.close_fut.done()

    async def send_loop(self):
        while not self.stop_flag:
            if not self.queue:
                await asyncio.sleep(0.01)
                continue

            packet = self.queue.pop()
            if packet:
                if self.proxy.slow_response:
                    await asyncio.sleep(self.proxy.slow_response_timeout)
                self.proxy_protocol.transport.write(packet)

    def data_received(self, data):
        if self.proxy.discard_response:
            return

        self.queue.append(data)
