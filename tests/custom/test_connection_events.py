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
import pytest

from pyignite import Client, AioClient
from pyignite.datatypes.cache_config import CacheMode
from pyignite.datatypes.prop_codes import PROP_NAME, PROP_CACHE_MODE
from pyignite.monitoring import ConnectionEventListener, ConnectionLostEvent, ConnectionClosedEvent, \
    HandshakeSuccessEvent, HandshakeFailedEvent, HandshakeStartEvent

from tests.util import start_ignite_gen, kill_process_tree


@pytest.fixture(autouse=True)
def server1():
    yield from start_ignite_gen(idx=1)


@pytest.fixture(autouse=True)
def server2():
    yield from start_ignite_gen(idx=2)


events = []


def teardown_function():
    events.clear()


class RecordingConnectionEventListener(ConnectionEventListener):
    def on_handshake_start(self, event):
        events.append(event)

    def on_handshake_success(self, event):
        events.append(event)

    def on_handshake_fail(self, event):
        events.append(event)

    def on_authentication_fail(self, event):
        events.append(event)

    def on_connection_closed(self, event):
        events.append(event)

    def on_connection_lost(self, event):
        events.append(event)


def test_events(request, server2):
    client = Client(event_listeners=[RecordingConnectionEventListener()])
    with client.connect([('127.0.0.1', 10800 + idx) for idx in range(1, 3)]):
        protocol_context = client.protocol_context
        nodes = {conn.port: conn for conn in client._nodes}
        cache = client.get_or_create_cache({
            PROP_NAME: request.node.name,
            PROP_CACHE_MODE: CacheMode.REPLICATED,
        })

        kill_process_tree(server2.pid)

        for _ in range(0, 100):
            try:
                cache.put(1, 1)
            except: # noqa 13
                pass

            if any(isinstance(e, ConnectionLostEvent) for e in events):
                break

    __assert_events(nodes, protocol_context)


@pytest.mark.asyncio
async def test_events_async(request, server2):
    client = AioClient(event_listeners=[RecordingConnectionEventListener()])
    async with client.connect([('127.0.0.1', 10800 + idx) for idx in range(1, 3)]):
        protocol_context = client.protocol_context
        nodes = {conn.port: conn for conn in client._nodes}
        cache = await client.get_or_create_cache({
            PROP_NAME: request.node.name,
            PROP_CACHE_MODE: CacheMode.REPLICATED,
        })
        kill_process_tree(server2.pid)

        for _ in range(0, 100):
            try:
                await cache.put(1, 1)
            except: # noqa 13
                pass

            if any(isinstance(e, ConnectionLostEvent) for e in events):
                break

    __assert_events(nodes, protocol_context)


def __assert_events(nodes, protocol_context):
    assert len([e for e in events if isinstance(e, ConnectionLostEvent)]) == 1
    # ConnectionLostEvent is a subclass of ConnectionClosedEvent
    assert 1 <= len([e for e in events if type(e) == ConnectionClosedEvent and e.node_uuid]) <= 2
    assert len([e for e in events if isinstance(e, HandshakeSuccessEvent)]) == 2

    for ev in events:
        assert ev.host == '127.0.0.1'
        if isinstance(ev, ConnectionLostEvent):
            assert ev.port == 10802
            assert ev.node_uuid == str(nodes[ev.port].uuid)
            assert ev.error_msg
        elif isinstance(ev, HandshakeStartEvent):
            assert ev.port in {10801, 10802}
        elif isinstance(ev, HandshakeFailedEvent):
            assert ev.port == 10802
            assert ev.protocol_context == protocol_context
            assert ev.error_msg
        elif isinstance(ev, HandshakeSuccessEvent):
            assert ev.port in {10801, 10802}
            assert ev.node_uuid == str(nodes[ev.port].uuid)
            assert ev.protocol_context == protocol_context
        elif isinstance(ev, ConnectionClosedEvent):
            assert ev.port in {10801, 10802}
            if ev.node_uuid:  # Possible if protocol negotiation occurred.
                assert ev.node_uuid == str(nodes[ev.port].uuid)
