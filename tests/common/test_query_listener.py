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
from pyignite.exceptions import CacheError
from pyignite.monitoring import QueryEventListener, QueryStartEvent, QueryFailEvent, QuerySuccessEvent
from pyignite.queries.op_codes import OP_CACHE_PUT, OP_CACHE_PARTITIONS, OP_CACHE_GET_NAMES

events = []


class QueryRouteListener(QueryEventListener):
    def on_query_start(self, event):
        if event.op_code != OP_CACHE_PARTITIONS:
            events.append(event)

    def on_query_fail(self, event):
        if event.op_code != OP_CACHE_PARTITIONS:
            events.append(event)

    def on_query_success(self, event):
        if event.op_code != OP_CACHE_PARTITIONS:
            events.append(event)


@pytest.fixture
def client():
    client = Client(event_listeners=[QueryRouteListener()])
    try:
        client.connect('127.0.0.1', 10801)
        yield client
    finally:
        client.close()
        events.clear()


@pytest.fixture
async def async_client(event_loop):
    client = AioClient(event_listeners=[QueryRouteListener()])
    try:
        await client.connect('127.0.0.1', 10801)
        yield client
    finally:
        await client.close()
        events.clear()


def test_query_fail_events(request, client):
    with pytest.raises(CacheError):
        cache = client.get_cache(request.node.name)
        cache.put(1, 1)

    __assert_fail_events(client)


@pytest.mark.asyncio
async def test_query_fail_events_async(request, async_client):
    with pytest.raises(CacheError):
        cache = await async_client.get_cache(request.node.name)
        await cache.put(1, 1)

    __assert_fail_events(async_client)


def __assert_fail_events(client):
    assert len(events) == 2
    conn = client._nodes[0]
    for ev in events:
        if isinstance(ev, QueryStartEvent):
            assert ev.op_code == OP_CACHE_PUT
            assert ev.op_name == 'OP_CACHE_PUT'
            assert ev.host == conn.host
            assert ev.port == conn.port
            assert ev.node_uuid == str(conn.uuid if conn.uuid else '')

        if isinstance(ev, QueryFailEvent):
            assert ev.op_code == OP_CACHE_PUT
            assert ev.op_name == 'OP_CACHE_PUT'
            assert ev.host == conn.host
            assert ev.port == conn.port
            assert ev.node_uuid == str(conn.uuid if conn.uuid else '')
            assert 'Cache does not exist' in ev.err_msg
            assert ev.duration >= 0


def test_query_success_events(client):
    client.get_cache_names()
    __assert_success_events(client)


@pytest.mark.asyncio
async def test_query_success_events_async(async_client):
    await async_client.get_cache_names()
    __assert_success_events(async_client)


def __assert_success_events(client):
    assert len(events) == 2
    conn = client._nodes[0]
    for ev in events:
        if isinstance(ev, QueryStartEvent):
            assert ev.op_code == OP_CACHE_GET_NAMES
            assert ev.op_name == 'OP_CACHE_GET_NAMES'
            assert ev.host == conn.host
            assert ev.port == conn.port
            assert ev.node_uuid == str(conn.uuid if conn.uuid else '')

        if isinstance(ev, QuerySuccessEvent):
            assert ev.op_code == OP_CACHE_GET_NAMES
            assert ev.op_name == 'OP_CACHE_GET_NAMES'
            assert ev.host == conn.host
            assert ev.port == conn.port
            assert ev.node_uuid == str(conn.uuid if conn.uuid else '')
            assert ev.duration >= 0
