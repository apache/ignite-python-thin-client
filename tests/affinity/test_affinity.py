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
import decimal
from datetime import datetime, timedelta
from uuid import UUID, uuid4

import pytest

from pyignite import GenericObjectMeta, AioClient
from pyignite.api import (
    cache_get_node_partitions, cache_get_node_partitions_async, cache_local_peek, cache_local_peek_async
)
from pyignite.constants import MAX_INT
from pyignite.datatypes import (
    BinaryObject, ByteArray, ByteObject, IntObject, ShortObject, LongObject, FloatObject, DoubleObject, BoolObject,
    CharObject, String, UUIDObject, DecimalObject, TimestampObject, TimeObject
)
from pyignite.datatypes.cache_config import CacheMode
from pyignite.datatypes.prop_codes import PROP_NAME, PROP_CACHE_MODE, PROP_CACHE_KEY_CONFIGURATION
from tests.util import wait_for_condition, wait_for_condition_async


def test_get_node_partitions(client, caches):
    cache_ids = [cache.cache_id for cache in caches]
    mappings = __get_mappings(client, cache_ids)
    __check_mappings(mappings, cache_ids)


@pytest.mark.asyncio
async def test_get_node_partitions_async(async_client, async_caches):
    cache_ids = [cache.cache_id for cache in async_caches]
    mappings = await __get_mappings(async_client, cache_ids)
    __check_mappings(mappings, cache_ids)


def __wait_for_ready_affinity(client, cache_ids):
    def inner():
        def condition():
            result = __get_mappings(client, cache_ids)
            return len(result.value['partition_mapping']) == len(cache_ids)

        wait_for_condition(condition)

    async def inner_async():
        async def condition():
            result = await __get_mappings(client, cache_ids)
            return len(result.value['partition_mapping']) == len(cache_ids)

        await wait_for_condition_async(condition)

    return inner_async() if isinstance(client, AioClient) else inner()


def __get_mappings(client, cache_ids):
    def inner():
        conn = client.random_node
        result = cache_get_node_partitions(conn, cache_ids)
        assert result.status == 0, result.message
        return result

    async def inner_async():
        conn = await client.random_node()
        result = await cache_get_node_partitions_async(conn, cache_ids)
        assert result.status == 0, result.message
        return result

    return inner_async() if isinstance(client, AioClient) else inner()


def __check_mappings(result, cache_ids):
    partition_mapping = result.value['partition_mapping']

    for i, cache_id in enumerate(cache_ids):
        cache_mapping = partition_mapping[cache_id]
        assert 'is_applicable' in cache_mapping

        # Check replicated cache
        if i == 3:
            assert not cache_mapping['is_applicable']
            assert 'node_mapping' not in cache_mapping
            assert cache_mapping['number_of_partitions'] == 0
        else:
            # Check cache config
            if i == 2:
                assert cache_mapping['cache_config']

            assert cache_mapping['is_applicable']
            assert cache_mapping['node_mapping']
            assert cache_mapping['number_of_partitions'] == 1024


@pytest.fixture
def caches(client):
    yield from __create_caches_fixture(client)


@pytest.fixture
async def async_caches(async_client):
    async for caches in __create_caches_fixture(async_client):
        yield caches


def __create_caches_fixture(client):
    caches_to_create = []
    for i in range(0, 5):
        cache_name = f'test_cache_{i}'
        if i == 2:
            caches_to_create.append((
                cache_name,
                {
                    PROP_NAME: cache_name,
                    PROP_CACHE_KEY_CONFIGURATION: [
                        {
                            'type_name': ByteArray.type_name,
                            'affinity_key_field_name': 'byte_affinity',
                        }
                    ]
                }))
        elif i == 3:
            caches_to_create.append((
                cache_name,
                {
                    PROP_NAME: cache_name,
                    PROP_CACHE_MODE: CacheMode.REPLICATED
                }
            ))
        else:
            caches_to_create.append((cache_name, None))

    def generate_caches():
        caches = []
        for name, config in caches_to_create:
            if config:
                cache = client.get_or_create_cache(config)
            else:
                cache = client.get_or_create_cache(name)
            caches.append(cache)
        return asyncio.gather(*caches) if isinstance(client, AioClient) else caches

    def inner():
        caches = []
        try:
            caches = generate_caches()
            __wait_for_ready_affinity(client, [cache.cache_id for cache in caches])
            yield caches
        finally:
            for cache in caches:
                cache.destroy()

    async def inner_async():
        caches = []
        try:
            caches = await generate_caches()
            await __wait_for_ready_affinity(client, [cache.cache_id for cache in caches])
            yield caches
        finally:
            await asyncio.gather(*[cache.destroy() for cache in caches])

    return inner_async() if isinstance(client, AioClient) else inner()


@pytest.fixture
def cache(client):
    cache = client.get_or_create_cache({
        PROP_NAME: 'test_cache_1',
        PROP_CACHE_MODE: CacheMode.PARTITIONED,
    })
    try:
        __wait_for_ready_affinity(client, [cache.cache_id])
        yield cache
    finally:
        cache.destroy()


@pytest.fixture
async def async_cache(async_client):
    cache = await async_client.get_or_create_cache({
        PROP_NAME: 'test_cache_1',
        PROP_CACHE_MODE: CacheMode.PARTITIONED,
    })
    try:
        await __wait_for_ready_affinity(async_client, [cache.cache_id])
        yield cache
    finally:
        await cache.destroy()


affinity_primitives_params = [
    # integers
    (42, None),
    (43, ByteObject),
    (-44, ByteObject),
    (45, IntObject),
    (-46, IntObject),
    (47, ShortObject),
    (-48, ShortObject),
    (49, LongObject),
    (MAX_INT - 50, LongObject),
    (MAX_INT + 51, LongObject),

    # floating point
    (5.2, None),
    (5.354, FloatObject),
    (-5.556, FloatObject),
    (-57.58, DoubleObject),

    # boolean
    (True, None),
    (True, BoolObject),
    (False, BoolObject),

    # char
    ('A', CharObject),
    ('Z', CharObject),
    ('⅓', CharObject),
    ('á', CharObject),
    ('ы', CharObject),
    ('カ', CharObject),
    ('Ø', CharObject),
    ('ß', CharObject),

    # string
    ('This is a test string', None),
    ('Кириллица', None),
    ('Little Mary had a lamb', String),

    # UUID
    (UUID('12345678123456789876543298765432'), None),
    (UUID('74274274274274274274274274274274'), UUIDObject),
    (uuid4(), None),

    # decimal (long internal representation in Java)
    (decimal.Decimal('-234.567'), None),
    (decimal.Decimal('200.0'), None),
    (decimal.Decimal('123.456'), DecimalObject),
    (decimal.Decimal('1.0'), None),
    (decimal.Decimal('0.02'), None),

    # decimal (BigInteger internal representation in Java)
    (decimal.Decimal('12345671234567123.45671234567'), None),
    (decimal.Decimal('-845678456.7845678456784567845'), None),

    # date and time
    (datetime(1980, 1, 1), None),
    ((datetime(1980, 1, 1), 999), TimestampObject),
    (timedelta(days=99), TimeObject)
]


@pytest.mark.parametrize('key, key_hint', affinity_primitives_params)
def test_affinity(client, cache, key, key_hint):
    __check_best_node_calculation(client, cache, key, 42, key_hint=key_hint)


@pytest.mark.parametrize('key, key_hint', affinity_primitives_params)
@pytest.mark.asyncio
async def test_affinity_async(async_client, async_cache, key, key_hint):
    await __check_best_node_calculation(async_client, async_cache, key, 42, key_hint=key_hint)


@pytest.fixture
def key_generic_object():
    class KeyClass(
        metaclass=GenericObjectMeta,
        schema={
            'NO': IntObject,
            'NAME': String,
        },
    ):
        pass

    key = KeyClass()
    key.NO = 1
    key.NAME = 'test_string'
    yield key


@pytest.mark.parametrize('with_type_hint', [True, False])
def test_affinity_for_generic_object(client, cache, key_generic_object, with_type_hint):
    key_hint = BinaryObject if with_type_hint else None
    __check_best_node_calculation(client, cache, key_generic_object, 42, key_hint=key_hint)


@pytest.mark.parametrize('with_type_hint', [True, False])
@pytest.mark.asyncio
async def test_affinity_for_generic_object_async(async_client, async_cache, key_generic_object, with_type_hint):
    key_hint = BinaryObject if with_type_hint else None
    await __check_best_node_calculation(async_client, async_cache, key_generic_object, 42, key_hint=key_hint)


def __check_best_node_calculation(client, cache, key, value, key_hint=None):
    def check_peek_value(node, best_node, result):
        if node is best_node:
            assert result.value == value, f'Affinity calculation error for {key}'
        else:
            assert result.value is None, f'Affinity calculation error for {key}'

    def inner():
        cache.put(key, value, key_hint=key_hint)
        best_node = client.get_best_node(cache, key, key_hint=key_hint)

        for node in filter(lambda n: n.alive, client._nodes):
            result = cache_local_peek(node, cache.cache_info, key, key_hint=key_hint)

            check_peek_value(node, best_node, result)

    async def inner_async():
        await cache.put(key, value, key_hint=key_hint)
        best_node = await client.get_best_node(cache, key, key_hint=key_hint)

        for node in filter(lambda n: n.alive, client._nodes):
            result = await cache_local_peek_async(node, cache.cache_info, key, key_hint=key_hint)

            check_peek_value(node, best_node, result)

    return inner_async() if isinstance(client, AioClient) else inner()
