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

from collections import OrderedDict
from decimal import Decimal

import pytest

from pyignite import GenericObjectMeta
from pyignite.datatypes import BoolObject, DecimalObject, FloatObject, IntObject, String
from pyignite.datatypes.prop_codes import PROP_NAME, PROP_CACHE_KEY_CONFIGURATION
from pyignite.exceptions import CacheError, ParameterError


def test_cache_create(client):
    cache = client.get_or_create_cache('my_oop_cache')
    try:
        assert cache.name == cache.settings[PROP_NAME] == 'my_oop_cache'
    finally:
        cache.destroy()


@pytest.mark.asyncio
async def test_cache_create_async(async_client):
    cache = await async_client.get_or_create_cache('my_oop_cache')
    try:
        assert cache.name == (await cache.settings())[PROP_NAME] == 'my_oop_cache'
    finally:
        await cache.destroy()


def test_get_cache(client):
    my_cache = client.get_or_create_cache('my_cache')
    try:
        assert my_cache.settings[PROP_NAME] == 'my_cache'
    finally:
        my_cache.destroy()

    my_cache = client.get_cache('my_cache')
    with pytest.raises(CacheError):
        _ = my_cache.settings[PROP_NAME]


@pytest.mark.asyncio
async def test_get_cache_async(async_client):
    my_cache = await async_client.get_or_create_cache('my_cache')
    try:
        assert (await my_cache.settings())[PROP_NAME] == 'my_cache'
    finally:
        await my_cache.destroy()

    my_cache = await async_client.get_cache('my_cache')
    with pytest.raises(CacheError):
        _ = (await my_cache.settings())[PROP_NAME]


@pytest.fixture
def cache_config():
    yield {
        PROP_NAME: 'my_oop_cache',
        PROP_CACHE_KEY_CONFIGURATION: [
            {
                'type_name': 'blah',
                'affinity_key_field_name': 'abc1234',
            },
        ],
    }


def test_cache_config(client, cache_config):
    client.create_cache(cache_config)
    cache = client.get_or_create_cache('my_oop_cache')
    try:
        assert cache.name == cache_config[PROP_NAME]
        assert cache.settings[PROP_CACHE_KEY_CONFIGURATION] == cache_config[PROP_CACHE_KEY_CONFIGURATION]
    finally:
        cache.destroy()


@pytest.mark.asyncio
async def test_cache_config_async(async_client, cache_config):
    await async_client.create_cache(cache_config)
    cache = await async_client.get_or_create_cache('my_oop_cache')
    try:
        assert cache.name == cache_config[PROP_NAME]
        assert (await cache.settings())[PROP_CACHE_KEY_CONFIGURATION] == cache_config[PROP_CACHE_KEY_CONFIGURATION]
    finally:
        await cache.destroy()


@pytest.fixture
def binary_type_fixture():
    class TestBinaryType(
        metaclass=GenericObjectMeta,
        schema=OrderedDict([
            ('test_bool', BoolObject),
            ('test_str', String),
            ('test_int', IntObject),
            ('test_decimal', DecimalObject),
        ]),
    ):
        pass

    return TestBinaryType(
        test_bool=True,
        test_str='This is a test',
        test_int=42,
        test_decimal=Decimal('34.56'),
    )


def test_cache_binary_get_put(cache, binary_type_fixture):
    cache.put('my_key', binary_type_fixture)
    value = cache.get('my_key')
    assert value.test_bool == binary_type_fixture.test_bool
    assert value.test_str == binary_type_fixture.test_str
    assert value.test_int == binary_type_fixture.test_int
    assert value.test_decimal == binary_type_fixture.test_decimal


@pytest.mark.asyncio
async def test_cache_binary_get_put_async(async_cache, binary_type_fixture):
    await async_cache.put('my_key', binary_type_fixture)

    value = await async_cache.get('my_key')
    assert value.test_bool == binary_type_fixture.test_bool
    assert value.test_str == binary_type_fixture.test_str
    assert value.test_int == binary_type_fixture.test_int
    assert value.test_decimal == binary_type_fixture.test_decimal


@pytest.fixture
def binary_type_schemas_fixture():
    schemas = [
        OrderedDict([
            ('TEST_BOOL', BoolObject),
            ('TEST_STR', String),
            ('TEST_INT', IntObject),
        ]),
        OrderedDict([
            ('TEST_BOOL', BoolObject),
            ('TEST_STR', String),
            ('TEST_INT', IntObject),
            ('TEST_FLOAT', FloatObject),
        ]),
        OrderedDict([
            ('TEST_BOOL', BoolObject),
            ('TEST_STR', String),
            ('TEST_INT', IntObject),
            ('TEST_DECIMAL', DecimalObject),
        ])
    ]
    yield 'TestBinaryType', schemas


def test_get_binary_type(client, binary_type_schemas_fixture):
    type_name, schemas = binary_type_schemas_fixture

    for schema in schemas:
        client.put_binary_type(type_name, schema=schema)

    binary_type_info = client.get_binary_type('TestBinaryType')
    assert len(binary_type_info['schemas']) == 3

    binary_type_info = client.get_binary_type('NonExistentType')
    assert binary_type_info['type_exists'] is False
    assert len(binary_type_info) == 1


@pytest.mark.asyncio
async def test_get_binary_type_async(async_client, binary_type_schemas_fixture):
    type_name, schemas = binary_type_schemas_fixture

    for schema in schemas:
        await async_client.put_binary_type(type_name, schema=schema)

    binary_type_info = await async_client.get_binary_type('TestBinaryType')
    assert len(binary_type_info['schemas']) == 3

    binary_type_info = await async_client.get_binary_type('NonExistentType')
    assert binary_type_info['type_exists'] is False
    assert len(binary_type_info) == 1


def test_get_cache_errors(client):
    cache = client.get_cache('missing-cache')

    with pytest.raises(CacheError, match=r'Cache does not exist \[cacheId='):
        cache.put(1, 1)

    with pytest.raises(ParameterError, match="You should supply at least cache name"):
        client.create_cache(None)


@pytest.mark.asyncio
async def test_get_cache_errors_async(async_client):
    cache = await async_client.get_cache('missing-cache')

    with pytest.raises(CacheError, match=r'Cache does not exist \[cacheId='):
        await cache.put(1, 1)

    with pytest.raises(ParameterError, match="You should supply at least cache name"):
        await async_client.create_cache(None)
