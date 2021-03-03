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

from pyignite.datatypes.prop_codes import PROP_NAME, PROP_CACHE_KEY_CONFIGURATION
from pyignite.exceptions import CacheError

cache_name = 'config_cache'


@pytest.fixture
def cache_config():
    return {
        PROP_NAME: cache_name,
        PROP_CACHE_KEY_CONFIGURATION: [
            {
                'type_name': 'blah',
                'affinity_key_field_name': 'abc1234',
            }
        ],
    }


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
def cache_with_config(client, cache_config):
    cache = client.get_or_create_cache(cache_config)
    yield cache
    cache.destroy()


@pytest.fixture
async def async_cache_with_config(async_client, cache_config):
    cache = await async_client.get_or_create_cache(cache_config)
    yield cache
    await cache.destroy()


def test_cache_get_configuration(client, cache):
    assert cache_name in client.get_cache_names()
    assert cache.settings[PROP_NAME] == cache_name


@pytest.mark.asyncio
async def test_cache_get_configuration_async(async_client, async_cache):
    assert cache_name in (await async_client.get_cache_names())
    assert (await async_cache.settings())[PROP_NAME] == cache_name


def test_get_or_create_with_config_existing(client, cache_with_config, cache_config):
    assert cache_name in client.get_cache_names()

    with pytest.raises(CacheError):
        client.create_cache(cache_config)

    cache = client.get_or_create_cache(cache_config)
    assert cache.settings == cache_with_config.settings


@pytest.mark.asyncio
async def test_get_or_create_with_config_existing_async(async_client, async_cache_with_config, cache_config):
    assert cache_name in (await async_client.get_cache_names())

    with pytest.raises(CacheError):
        await async_client.create_cache(cache_config)

    cache = await async_client.get_or_create_cache(cache_config)
    assert (await cache.settings()) == (await async_cache_with_config.settings())


def test_get_or_create_with_config_new(client, cache_config):
    assert cache_name not in client.get_cache_names()
    cache = client.get_or_create_cache(cache_config)
    try:
        assert cache_name in client.get_cache_names()
        assert cache.settings[PROP_NAME] == cache_name
    finally:
        cache.destroy()


@pytest.mark.asyncio
async def test_get_or_create_with_config_new_async(async_client, cache_config):
    assert cache_name not in (await async_client.get_cache_names())

    cache = await async_client.get_or_create_cache(cache_config)
    try:
        assert cache_name in (await async_client.get_cache_names())
        assert (await cache.settings())[PROP_NAME] == cache_name
    finally:
        await cache.destroy()
