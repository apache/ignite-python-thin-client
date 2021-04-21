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

from inspect import getmembers

import pyignite
import pytest

from pyignite.datatypes.cache_config import (
    CacheMode, CacheAtomicityMode, WriteSynchronizationMode, PartitionLossPolicy, RebalanceMode
)
from pyignite.datatypes.prop_codes import (
    PROP_NAME, PROP_CACHE_KEY_CONFIGURATION, PROP_CACHE_MODE, PROP_CACHE_ATOMICITY_MODE, PROP_BACKUPS_NUMBER,
    PROP_WRITE_SYNCHRONIZATION_MODE, PROP_COPY_ON_READ, PROP_READ_FROM_BACKUP, PROP_DATA_REGION_NAME,
    PROP_IS_ONHEAP_CACHE_ENABLED, PROP_GROUP_NAME, PROP_DEFAULT_LOCK_TIMEOUT, PROP_MAX_CONCURRENT_ASYNC_OPERATIONS,
    PROP_PARTITION_LOSS_POLICY, PROP_EAGER_TTL, PROP_STATISTICS_ENABLED, PROP_REBALANCE_MODE, PROP_REBALANCE_DELAY,
    PROP_REBALANCE_TIMEOUT, PROP_REBALANCE_BATCH_SIZE, PROP_REBALANCE_BATCHES_PREFETCH_COUNT, PROP_REBALANCE_ORDER,
    PROP_REBALANCE_THROTTLE, PROP_QUERY_ENTITIES, PROP_QUERY_PARALLELISM, PROP_QUERY_DETAIL_METRIC_SIZE,
    PROP_SQL_SCHEMA, PROP_SQL_INDEX_INLINE_MAX_SIZE, PROP_SQL_ESCAPE_ALL, PROP_MAX_QUERY_ITERATORS, PROP_EXPIRY_POLICY
)
from pyignite.exceptions import CacheError

cache_name = 'config_cache'


@pytest.fixture
def test_cache_settings(expiry_policy_supported):
    settings = {
        PROP_NAME: cache_name,
        PROP_CACHE_MODE: CacheMode.PARTITIONED,
        PROP_CACHE_ATOMICITY_MODE: CacheAtomicityMode.TRANSACTIONAL,
        PROP_BACKUPS_NUMBER: 2,
        PROP_WRITE_SYNCHRONIZATION_MODE: WriteSynchronizationMode.FULL_SYNC,
        PROP_COPY_ON_READ: True,
        PROP_READ_FROM_BACKUP: True,
        PROP_DATA_REGION_NAME: 'SmallDataRegion',
        PROP_IS_ONHEAP_CACHE_ENABLED: True,
        PROP_QUERY_ENTITIES: [{
            'table_name': cache_name + '_table',
            'key_field_name': 'KEY',
            'key_type_name': 'java.lang.String',
            'value_field_name': 'VAL',
            'value_type_name': 'java.lang.String',
            'field_name_aliases': [
                {'alias': 'val', 'field_name': 'VAL'},
                {'alias': 'key', 'field_name': 'KEY'}
            ],
            'query_fields': [
                {
                    'name': 'KEY',
                    'type_name': 'java.lang.String'
                },
                {
                    'name': 'VAL',
                    'type_name': 'java.lang.String'
                }
            ],
            'query_indexes': []
        }],
        PROP_QUERY_PARALLELISM: 20,
        PROP_QUERY_DETAIL_METRIC_SIZE: 10,
        PROP_SQL_SCHEMA: 'PUBLIC',
        PROP_SQL_INDEX_INLINE_MAX_SIZE: 1024,
        PROP_SQL_ESCAPE_ALL: True,
        PROP_MAX_QUERY_ITERATORS: 200,
        PROP_REBALANCE_MODE: RebalanceMode.SYNC,
        PROP_REBALANCE_DELAY: 1000,
        PROP_REBALANCE_TIMEOUT: 5000,
        PROP_REBALANCE_BATCH_SIZE: 100,
        PROP_REBALANCE_BATCHES_PREFETCH_COUNT: 10,
        PROP_REBALANCE_ORDER: 3,
        PROP_REBALANCE_THROTTLE: 10,
        PROP_GROUP_NAME: cache_name + '_group',
        PROP_CACHE_KEY_CONFIGURATION: [
            {
                'type_name': 'java.lang.String',
                'affinity_key_field_name': 'abc1234',
            }
        ],
        PROP_DEFAULT_LOCK_TIMEOUT: 3000,
        PROP_MAX_CONCURRENT_ASYNC_OPERATIONS: 100,
        PROP_PARTITION_LOSS_POLICY: PartitionLossPolicy.READ_WRITE_ALL,
        PROP_EAGER_TTL: True,
        PROP_STATISTICS_ENABLED: True
    }

    if expiry_policy_supported:
        settings[PROP_EXPIRY_POLICY] = None
    elif 'PROP_EXPIRY_POLICY' in ALL_PROPS:
        del ALL_PROPS['PROP_EXPIRY_POLICY']

    return settings


@pytest.fixture
def cache(client):
    cache = client.get_or_create_cache(cache_name)
    yield cache
    cache.destroy()


@pytest.fixture
async def async_cache(async_client):
    cache = await async_client.get_or_create_cache(cache_name)
    yield cache
    await cache.destroy()


@pytest.fixture
def cache_with_config(client, test_cache_settings):
    cache = client.get_or_create_cache(test_cache_settings)
    yield cache
    cache.destroy()


@pytest.fixture
async def async_cache_with_config(async_client, test_cache_settings):
    cache = await async_client.get_or_create_cache(test_cache_settings)
    yield cache
    await cache.destroy()


def test_cache_get_configuration(client, cache):
    assert cache_name in client.get_cache_names()
    assert cache.settings[PROP_NAME] == cache_name


@pytest.mark.asyncio
async def test_cache_get_configuration_async(async_client, async_cache):
    assert cache_name in (await async_client.get_cache_names())
    assert (await async_cache.settings())[PROP_NAME] == cache_name


def test_get_or_create_with_config_existing(client, cache_with_config, test_cache_settings):
    assert cache_name in client.get_cache_names()

    with pytest.raises(CacheError):
        client.create_cache(test_cache_settings)

    cache = client.get_or_create_cache(test_cache_settings)
    assert cache.settings == cache_with_config.settings


@pytest.mark.asyncio
async def test_get_or_create_with_config_existing_async(async_client, async_cache_with_config, test_cache_settings):
    assert cache_name in (await async_client.get_cache_names())

    with pytest.raises(CacheError):
        await async_client.create_cache(test_cache_settings)

    cache = await async_client.get_or_create_cache(test_cache_settings)
    assert (await cache.settings()) == (await async_cache_with_config.settings())

ALL_PROPS = {name: value for name, value in getmembers(pyignite.datatypes.prop_codes) if name.startswith('PROP')}


def test_get_or_create_with_config_new(client, test_cache_settings):
    assert cache_name not in client.get_cache_names()
    cache = client.get_or_create_cache(test_cache_settings)
    try:
        assert cache_name in client.get_cache_names()
        real_cache_settings = cache.settings
        assert real_cache_settings == test_cache_settings
        assert set(real_cache_settings.keys()) == set(ALL_PROPS.values())
    finally:
        cache.destroy()


@pytest.mark.asyncio
async def test_get_or_create_with_config_new_async(async_client, test_cache_settings):
    assert cache_name not in (await async_client.get_cache_names())

    cache = await async_client.get_or_create_cache(test_cache_settings)
    try:
        assert cache_name in (await async_client.get_cache_names())
        real_cache_settings = await cache.settings()
        assert real_cache_settings == test_cache_settings
        assert set(real_cache_settings.keys()) == set(ALL_PROPS.values())
    finally:
        await cache.destroy()
