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
import contextlib
from collections import OrderedDict, deque
import random

import pytest

from pyignite import GenericObjectMeta, AioClient, Client
from pyignite.aio_cache import AioCache
from pyignite.connection import Connection, AioConnection
from pyignite.constants import PROTOCOL_BYTE_ORDER
from pyignite.datatypes import String, LongObject
from pyignite.datatypes.cache_config import CacheMode
from pyignite.datatypes.prop_codes import PROP_NAME, PROP_BACKUPS_NUMBER, PROP_CACHE_KEY_CONFIGURATION, PROP_CACHE_MODE
from tests.util import wait_for_condition, wait_for_condition_async, start_ignite, kill_process_tree

try:
    from contextlib import asynccontextmanager
except ImportError:
    from async_generator import asynccontextmanager

requests = deque()
old_send = Connection.send
old_send_async = AioConnection._send


def patched_send(self, *args, **kwargs):
    """Patched send function that push to queue idx of server to which request is routed."""
    buf = args[0]
    if buf and len(buf) >= 6:
        op_code = int.from_bytes(buf[4:6], byteorder=PROTOCOL_BYTE_ORDER)
        # Filter only caches operation.
        if 1000 <= op_code < 1100:
            requests.append(self.port % 100)
    return old_send(self, *args, **kwargs)


async def patched_send_async(self, *args, **kwargs):
    """Patched send function that push to queue idx of server to which request is routed."""
    buf = args[0]
    if buf and len(buf) >= 6:
        op_code = int.from_bytes(buf[4:6], byteorder=PROTOCOL_BYTE_ORDER)
        # Filter only caches operation.
        if 1000 <= op_code < 1100:
            requests.append(self.port % 100)
    return await old_send_async(self, *args, **kwargs)


def setup_function():
    requests.clear()
    Connection.send = patched_send
    AioConnection._send = patched_send_async


def teardown_function():
    Connection.send = old_send
    AioConnection.send = old_send_async


def wait_for_affinity_distribution(cache, key, node_idx, timeout=30):
    real_node_idx = 0

    def check_grid_idx():
        nonlocal real_node_idx
        try:
            cache.get(key)
            real_node_idx = requests.pop()
        except (OSError, IOError):
            return False
        return real_node_idx == node_idx

    res = wait_for_condition(check_grid_idx, timeout=timeout)

    if not res:
        raise TimeoutError(f"failed to wait for affinity distribution, expected node_idx {node_idx},"
                           f"got {real_node_idx} instead")


async def wait_for_affinity_distribution_async(cache, key, node_idx, timeout=30):
    real_node_idx = 0

    async def check_grid_idx():
        nonlocal real_node_idx
        try:
            await cache.get(key)
            real_node_idx = requests.pop()
        except (OSError, IOError):
            return False
        return real_node_idx == node_idx

    res = await wait_for_condition_async(check_grid_idx, timeout=timeout)

    if not res:
        raise TimeoutError(f"failed to wait for affinity distribution, expected node_idx {node_idx},"
                           f"got {real_node_idx} instead")


@pytest.mark.parametrize("key,grid_idx", [(1, 1), (2, 2), (3, 3), (4, 1), (5, 1), (6, 2), (11, 1), (13, 1), (19, 1)])
@pytest.mark.parametrize("backups", [0, 1, 2, 3])
def test_cache_operation_on_primitive_key_routes_request_to_primary_node(request, key, grid_idx, backups, client):
    cache = client.get_or_create_cache({
        PROP_NAME: request.node.name + str(backups),
        PROP_BACKUPS_NUMBER: backups,
    })
    try:
        __perform_operations_on_primitive_key(client, cache, key, grid_idx)
    finally:
        cache.destroy()


@pytest.mark.parametrize("key,grid_idx", [(1, 1), (2, 2), (3, 3), (4, 1), (5, 1), (6, 2), (11, 1), (13, 1), (19, 1)])
@pytest.mark.parametrize("backups", [0, 1, 2, 3])
@pytest.mark.asyncio
async def test_cache_operation_on_primitive_key_routes_request_to_primary_node_async(
        request, key, grid_idx, backups, async_client):
    cache = await async_client.get_or_create_cache({
        PROP_NAME: request.node.name + str(backups),
        PROP_BACKUPS_NUMBER: backups,
    })
    try:
        await __perform_operations_on_primitive_key(async_client, cache, key, grid_idx)
    finally:
        await cache.destroy()


def __perform_operations_on_primitive_key(client, cache, key, grid_idx):
    operations = [
        ('get', 1), ('put', 2), ('replace', 2), ('clear_key', 1), ('contains_key', 1), ('get_and_put', 2),
        ('get_and_put_if_absent', 2), ('put_if_absent', 2), ('get_and_remove', 1), ('get_and_replace', 2),
        ('remove_key', 1), ('remove_if_equals', 2), ('replace', 2), ('replace_if_equals', 3)
    ]

    def inner():
        cache.put(key, key)
        wait_for_affinity_distribution(cache, key, grid_idx)

        for op_name, param_nums in operations:
            op = getattr(cache, op_name)
            args = [random.randint(-100, 100) for _ in range(0, param_nums - 1)]
            op(key, *args)
            assert requests.pop() == grid_idx

    async def inner_async():
        await cache.put(key, key)
        await wait_for_affinity_distribution_async(cache, key, grid_idx)

        for op_name, param_nums in operations:
            op = getattr(cache, op_name)
            args = [random.randint(-100, 100) for _ in range(0, param_nums - 1)]
            await op(key, *args)

            assert requests.pop() == grid_idx

    return inner_async() if isinstance(client, AioClient) else inner()


@pytest.mark.skip(reason="Custom key objects are not supported yet")
def test_cache_operation_on_complex_key_routes_request_to_primary_node():
    pass


@pytest.mark.parametrize("key,grid_idx", [(1, 2), (2, 1), (3, 1), (4, 2), (5, 2), (6, 3)])
@pytest.mark.skip(reason="Custom key objects are not supported yet")
def test_cache_operation_on_custom_affinity_key_routes_request_to_primary_node(request, client, key, grid_idx):
    class AffinityTestType1(
        metaclass=GenericObjectMeta,
        type_name='AffinityTestType1',
        schema=OrderedDict([
            ('test_str', String),
            ('test_int', LongObject)
        ])
    ):
        pass

    cache_config = {
        PROP_NAME: request.node.name,
        PROP_CACHE_KEY_CONFIGURATION: [
            {
                'type_name': 'AffinityTestType1',
                'affinity_key_field_name': 'test_int',
            },
        ],
    }
    cache = client.create_cache(cache_config)

    # noinspection PyArgumentList
    key_obj = AffinityTestType1(
        test_str="abc",
        test_int=key
    )

    cache.put(key_obj, 1)
    cache.put(key_obj, 2)

    assert requests.pop() == grid_idx


client_routed_connection_string = [('127.0.0.1', 10800 + idx) for idx in range(1, 5)]


@pytest.fixture
def client_routed():
    client = Client(partition_aware=True)
    try:
        client.connect(client_routed_connection_string)
        yield client
    finally:
        client.close()


@pytest.fixture
def client_routed_cache(client_routed, request):
    yield client_routed.get_or_create_cache(request.node.name)


@pytest.fixture
async def async_client_routed():
    client = AioClient(partition_aware=True)
    try:
        await client.connect(client_routed_connection_string)
        yield client
    finally:
        await client.close()


@pytest.fixture
async def async_client_routed_cache(async_client_routed, request):
    cache = await async_client_routed.get_or_create_cache(request.node.name)
    yield cache


def test_cache_operation_routed_to_new_cluster_node(client_routed_cache):
    __perform_cache_operation_routed_to_new_node(client_routed_cache)


@pytest.mark.asyncio
async def test_cache_operation_routed_to_new_cluster_node_async(async_client_routed_cache):
    await __perform_cache_operation_routed_to_new_node(async_client_routed_cache)


def __perform_cache_operation_routed_to_new_node(cache):
    key = 12

    def inner():
        wait_for_affinity_distribution(cache, key, 3)
        cache.put(key, key)
        cache.put(key, key)
        assert requests.pop() == 3

        srv = start_ignite(idx=4)
        try:
            # Wait for rebalance and partition map exchange
            wait_for_affinity_distribution(cache, key, 4)

            # Response is correct and comes from the new node
            res = cache.get_and_remove(key)
            assert res == key
            assert requests.pop() == 4
        finally:
            kill_process_tree(srv.pid)

    async def inner_async():
        await wait_for_affinity_distribution_async(cache, key, 3)
        await cache.put(key, key)
        await cache.put(key, key)
        assert requests.pop() == 3

        srv = start_ignite(idx=4)
        try:
            # Wait for rebalance and partition map exchange
            await wait_for_affinity_distribution_async(cache, key, 4)

            # Response is correct and comes from the new node
            res = await cache.get_and_remove(key)
            assert res == key
            assert requests.pop() == 4
        finally:
            kill_process_tree(srv.pid)

    return inner_async() if isinstance(cache, AioCache) else inner()


@pytest.fixture
def replicated_cache(request, client):
    cache = client.get_or_create_cache({
        PROP_NAME: request.node.name,
        PROP_CACHE_MODE: CacheMode.REPLICATED,
    })
    try:
        yield cache
    finally:
        cache.destroy()


@pytest.fixture
async def async_replicated_cache(request, async_client):
    cache = await async_client.get_or_create_cache({
        PROP_NAME: request.node.name,
        PROP_CACHE_MODE: CacheMode.REPLICATED,
    })
    try:
        yield cache
    finally:
        await cache.destroy()


def test_replicated_cache_operation_routed_to_random_node(replicated_cache):
    verify_random_node(replicated_cache)


@pytest.mark.asyncio
async def test_replicated_cache_operation_routed_to_random_node_async(async_replicated_cache):
    await verify_random_node(async_replicated_cache)


def verify_random_node(cache):
    key = 1

    def inner():
        cache.put(key, key)

        idx1 = requests.pop()
        idx2 = idx1

        # Try 10 times - random node may end up being the same
        for _ in range(1, 10):
            cache.put(key, key)
            idx2 = requests.pop()
            if idx2 != idx1:
                break
        assert idx1 != idx2

    async def inner_async():
        await cache.put(key, key)

        idx1 = requests.pop()

        idx2 = idx1

        # Try 10 times - random node may end up being the same
        for _ in range(1, 10):
            await cache.put(key, key)
            idx2 = requests.pop()

            if idx2 != idx1:
                break
        assert idx1 != idx2

    return inner_async() if isinstance(cache, AioCache) else inner()


@contextlib.contextmanager
def create_caches(client):
    caches = []
    try:
        caches = [client.create_cache(f'test_cache_{i}') for i in range(0, 10)]
        yield caches
    finally:
        for cache in caches:
            try:
                cache.destroy()
            except:  # noqa: 13
                cache.destroy()  # Retry if connection failed.
                pass


@asynccontextmanager
async def create_caches_async(client):
    caches = []
    try:
        caches = await asyncio.gather(*[client.create_cache(f'test_cache_{i}') for i in range(0, 10)])
        yield caches
    finally:
        for cache in caches:
            try:
                await cache.destroy()
            except:  # noqa: 13
                await cache.destroy()  # Retry if connection failed.
                pass


def test_new_registered_cache_affinity(client):
    with create_caches(client) as caches:
        key = 12
        test_cache = random.choice(caches)
        test_cache.put(key, key)
        wait_for_affinity_distribution(test_cache, key, 3)

        caches.append(client.create_cache('new_cache'))

        for cache in caches:
            cache.get(key)
            assert requests.pop() == 3


@pytest.mark.asyncio
async def test_new_registered_cache_affinity_async(async_client):
    async with create_caches_async(async_client) as caches:
        key = 12
        test_cache = random.choice(caches)
        await test_cache.put(key, key)
        await wait_for_affinity_distribution_async(test_cache, key, 3)

        caches.append(await async_client.create_cache('new_cache'))

        for cache in caches:
            await cache.get(key)
            assert requests.pop() == 3


def test_all_registered_cache_updated_on_new_server(client_routed):
    with create_caches(client_routed) as caches:
        key = 12
        test_cache = random.choice(caches)
        wait_for_affinity_distribution(test_cache, key, 3)
        test_cache.put(key, key)
        assert requests.pop() == 3

        srv = start_ignite(idx=4)
        try:
            # Wait for rebalance and partition map exchange
            wait_for_affinity_distribution(test_cache, key, 4)

            for cache in caches:
                cache.get(key)
                assert requests.pop() == 4
        finally:
            kill_process_tree(srv.pid)


@pytest.mark.asyncio
async def test_all_registered_cache_updated_on_new_server_async(async_client_routed):
    async with create_caches_async(async_client_routed) as caches:
        key = 12
        test_cache = random.choice(caches)
        await wait_for_affinity_distribution_async(test_cache, key, 3)
        await test_cache.put(key, key)
        assert requests.pop() == 3

        srv = start_ignite(idx=4)
        try:
            # Wait for rebalance and partition map exchange
            await wait_for_affinity_distribution_async(test_cache, key, 4)

            for cache in caches:
                await cache.get(key)
                assert requests.pop() == 4
        finally:
            kill_process_tree(srv.pid)


@pytest.mark.asyncio
async def test_update_affinity_concurrently(async_client):
    async with create_caches_async(async_client) as caches:
        key = 12
        await asyncio.gather(*[cache.put(key, key) for cache in caches])

        for cache in caches:
            await cache.get(key)
            assert requests.pop() == 3
