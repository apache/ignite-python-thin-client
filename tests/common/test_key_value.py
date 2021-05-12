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

from datetime import datetime

import pytest

from pyignite import GenericObjectMeta
from pyignite.datatypes import CollectionObject, IntObject, MapObject, TimestampObject, String


def test_put_get(cache):
    cache.put('my_key', 5)

    assert cache.get('my_key') == 5


@pytest.mark.asyncio
async def test_put_get_async(async_cache):
    await async_cache.put('my_key', 5)

    assert await async_cache.get('my_key') == 5


def test_get_all(cache):
    assert cache.get_all(['key_1', 2, (3, IntObject)]) == {}

    cache.put('key_1', 4)
    cache.put(3, 18, key_hint=IntObject)

    assert cache.get_all(['key_1', 2, (3, IntObject)]) == {'key_1': 4, 3: 18}


@pytest.mark.asyncio
async def test_get_all_async(async_cache):
    assert await async_cache.get_all(['key_1', 2, (3, IntObject)]) == {}

    await async_cache.put('key_1', 4)
    await async_cache.put(3, 18, key_hint=IntObject)

    assert await async_cache.get_all(['key_1', 2, (3, IntObject)]) == {'key_1': 4, 3: 18}


def test_put_all(cache):
    test_dict = {
        1: 2,
        'key_1': 4,
        (3, IntObject): 18,
    }
    cache.put_all(test_dict)

    result = cache.get_all(list(test_dict.keys()))

    assert len(result) == len(test_dict)
    for k, v in test_dict.items():
        k = k[0] if isinstance(k, tuple) else k
        assert result[k] == v


@pytest.mark.asyncio
async def test_put_all_async(async_cache):
    test_dict = {
        1: 2,
        'key_1': 4,
        (3, IntObject): 18,
    }
    await async_cache.put_all(test_dict)

    result = await async_cache.get_all(list(test_dict.keys()))

    assert len(result) == len(test_dict)
    for k, v in test_dict.items():
        k = k[0] if isinstance(k, tuple) else k
        assert result[k] == v


def test_contains_key(cache):
    cache.put('test_key', 42)

    assert cache.contains_key('test_key')
    assert not cache.contains_key('non-existent-key')


@pytest.mark.asyncio
async def test_contains_key_async(async_cache):
    await async_cache.put('test_key', 42)

    assert await async_cache.contains_key('test_key')
    assert not await async_cache.contains_key('non-existent-key')


def test_contains_keys(cache):
    cache.put(5, 6)
    cache.put('test_key', 42)

    assert cache.contains_keys([5, 'test_key'])
    assert not cache.contains_keys([5, 'non-existent-key'])


@pytest.mark.asyncio
async def test_contains_keys_async(async_cache):
    await async_cache.put(5, 6)
    await async_cache.put('test_key', 42)

    assert await async_cache.contains_keys([5, 'test_key'])
    assert not await async_cache.contains_keys([5, 'non-existent-key'])


def test_get_and_put(cache):
    assert cache.get_and_put('test_key', 42) is None
    assert cache.get('test_key') == 42
    assert cache.get_and_put('test_key', 1234) == 42
    assert cache.get('test_key') == 1234


@pytest.mark.asyncio
async def test_get_and_put_async(async_cache):
    assert await async_cache.get_and_put('test_key', 42) is None
    assert await async_cache.get('test_key') == 42
    assert await async_cache.get_and_put('test_key', 1234) == 42
    assert await async_cache.get('test_key') == 1234


def test_get_and_replace(cache):
    assert cache.get_and_replace('test_key', 42) is None
    assert cache.get('test_key') is None
    cache.put('test_key', 42)
    assert cache.get_and_replace('test_key', 1234) == 42


@pytest.mark.asyncio
async def test_get_and_replace_async(async_cache):
    assert await async_cache.get_and_replace('test_key', 42) is None
    assert await async_cache.get('test_key') is None
    await async_cache.put('test_key', 42)
    assert await async_cache.get_and_replace('test_key', 1234) == 42


def test_get_and_remove(cache):
    assert cache.get_and_remove('test_key') is None
    cache.put('test_key', 42)
    assert cache.get_and_remove('test_key') == 42
    assert cache.get_and_remove('test_key') is None


@pytest.mark.asyncio
async def test_get_and_remove_async(async_cache):
    assert await async_cache.get_and_remove('test_key') is None
    await async_cache.put('test_key', 42)
    assert await async_cache.get_and_remove('test_key') == 42
    assert await async_cache.get_and_remove('test_key') is None


def test_put_if_absent(cache):
    assert cache.put_if_absent('test_key', 42)
    assert not cache.put_if_absent('test_key', 1234)


@pytest.mark.asyncio
async def test_put_if_absent_async(async_cache):
    assert await async_cache.put_if_absent('test_key', 42)
    assert not await async_cache.put_if_absent('test_key', 1234)


def test_get_and_put_if_absent(cache):
    assert cache.get_and_put_if_absent('test_key', 42) is None
    assert cache.get_and_put_if_absent('test_key', 1234) == 42
    assert cache.get_and_put_if_absent('test_key', 5678) == 42
    assert cache.get('test_key') == 42


@pytest.mark.asyncio
async def test_get_and_put_if_absent_async(async_cache):
    assert await async_cache.get_and_put_if_absent('test_key', 42) is None
    assert await async_cache.get_and_put_if_absent('test_key', 1234) == 42
    assert await async_cache.get_and_put_if_absent('test_key', 5678) == 42
    assert await async_cache.get('test_key') == 42


def test_replace(cache):
    assert cache.replace('test_key', 42) is False
    cache.put('test_key', 1234)
    assert cache.replace('test_key', 42) is True
    assert cache.get('test_key') == 42


@pytest.mark.asyncio
async def test_replace_async(async_cache):
    assert await async_cache.replace('test_key', 42) is False
    await async_cache.put('test_key', 1234)
    assert await async_cache.replace('test_key', 42) is True
    assert await async_cache.get('test_key') == 42


def test_replace_if_equals(cache):
    assert cache.replace_if_equals('my_test', 42, 1234) is False
    cache.put('my_test', 42)
    assert cache.replace_if_equals('my_test', 42, 1234) is True
    assert cache.get('my_test') == 1234


@pytest.mark.asyncio
async def test_replace_if_equals_async(async_cache):
    assert await async_cache.replace_if_equals('my_test', 42, 1234) is False
    await async_cache.put('my_test', 42)
    assert await async_cache.replace_if_equals('my_test', 42, 1234) is True
    assert await async_cache.get('my_test') == 1234


def test_clear(cache):
    cache.put('my_test', 42)
    cache.clear()
    assert cache.get('my_test') is None


@pytest.mark.asyncio
async def test_clear_async(async_cache):
    await async_cache.put('my_test', 42)
    await async_cache.clear()
    assert await async_cache.get('my_test') is None


def test_clear_key(cache):
    cache.put('my_test', 42)
    cache.put('another_test', 24)

    cache.clear_key('my_test')

    assert cache.get('my_test') is None
    assert cache.get('another_test') == 24


@pytest.mark.asyncio
async def test_clear_key_async(async_cache):
    await async_cache.put('my_test', 42)
    await async_cache.put('another_test', 24)

    await async_cache.clear_key('my_test')

    assert await async_cache.get('my_test') is None
    assert await async_cache.get('another_test') == 24


def test_clear_keys(cache):
    cache.put('my_test_key', 42)
    cache.put('another_test', 24)

    cache.clear_keys(['my_test_key', 'nonexistent_key'])

    assert cache.get('my_test_key') is None
    assert cache.get('another_test') == 24


@pytest.mark.asyncio
async def test_clear_keys_async(async_cache):
    await async_cache.put('my_test_key', 42)
    await async_cache.put('another_test', 24)

    await async_cache.clear_keys(['my_test_key', 'nonexistent_key'])

    assert await async_cache.get('my_test_key') is None
    assert await async_cache.get('another_test') == 24


def test_remove_key(cache):
    cache.put('my_test_key', 42)
    assert cache.remove_key('my_test_key') is True
    assert cache.remove_key('non_existent_key') is False


@pytest.mark.asyncio
async def test_remove_key_async(async_cache):
    await async_cache.put('my_test_key', 42)
    assert await async_cache.remove_key('my_test_key') is True
    assert await async_cache.remove_key('non_existent_key') is False


def test_remove_if_equals(cache):
    cache.put('my_test', 42)
    assert cache.remove_if_equals('my_test', 1234) is False
    assert cache.remove_if_equals('my_test', 42) is True
    assert cache.get('my_test') is None


@pytest.mark.asyncio
async def test_remove_if_equals_async(async_cache):
    await async_cache.put('my_test', 42)
    assert await async_cache.remove_if_equals('my_test', 1234) is False
    assert await async_cache.remove_if_equals('my_test', 42) is True
    assert await async_cache.get('my_test') is None


def test_remove_keys(cache):
    cache.put('my_test', 42)

    cache.put('another_test', 24)
    cache.remove_keys(['my_test', 'non_existent'])

    assert cache.get('my_test') is None
    assert cache.get('another_test') == 24


@pytest.mark.asyncio
async def test_remove_keys_async(async_cache):
    await async_cache.put('my_test', 42)

    await async_cache.put('another_test', 24)
    await async_cache.remove_keys(['my_test', 'non_existent'])

    assert await async_cache.get('my_test') is None
    assert await async_cache.get('another_test') == 24


def test_remove_all(cache):
    cache.put('my_test', 42)
    cache.put('another_test', 24)
    cache.remove_all()

    assert cache.get('my_test') is None
    assert cache.get('another_test') is None


@pytest.mark.asyncio
async def test_remove_all_async(async_cache):
    await async_cache.put('my_test', 42)
    await async_cache.put('another_test', 24)
    await async_cache.remove_all()

    assert await async_cache.get('my_test') is None
    assert await async_cache.get('another_test') is None


def test_cache_get_size(cache):
    cache.put('my_test', 42)
    assert cache.get_size() == 1


@pytest.mark.asyncio
async def test_cache_get_size_async(async_cache):
    await async_cache.put('my_test', 42)
    assert await async_cache.get_size() == 1


class Value(
    metaclass=GenericObjectMeta,
    schema={
        'id': IntObject,
        'name': String,
    }
):
    pass


collection_params = [
    [
        'simple',
        (CollectionObject.ARR_LIST, [
            (123, IntObject), 678, None, 55.2, ((datetime(year=1996, month=3, day=1), 0), TimestampObject)
        ]),
        (CollectionObject.ARR_LIST, [123, 678, None, 55.2, (datetime(year=1996, month=3, day=1), 0)])
    ],
    [
        'nested',
        (CollectionObject.ARR_LIST, [
            123, ((1, [456, 'inner_test_string', 789]), CollectionObject), 'outer_test_string'
        ]),
        (CollectionObject.ARR_LIST, [123, (1, [456, 'inner_test_string', 789]), 'outer_test_string'])
    ],
    [
        'binary',
        (CollectionObject.ARR_LIST, [Value(id=i, name=f'val_{i}') for i in range(0, 10)]),
        (CollectionObject.ARR_LIST, [Value(id=i, name=f'val_{i}') for i in range(0, 10)]),
    ],
    [
        'hash_map',
        (
            MapObject.HASH_MAP,
            {
                (123, IntObject): 'test_data',
                456: ((1, [456, 'inner_test_string', 789]), CollectionObject),
                'test_key': 32.4,
                'simple_strings': ['string_1', 'string_2']
            }
        ),
        (
            MapObject.HASH_MAP,
            {
                123: 'test_data',
                456: (1, [456, 'inner_test_string', 789]),
                'test_key': 32.4,
                'simple_strings': ['string_1', 'string_2']
            }
        )
    ],
    [
        'linked_hash_map',
        (
            MapObject.LINKED_HASH_MAP,
            {
                'test_data': 12345,
                456: ['string_1', 'string_2'],
                'test_key': 32.4
            }
        ),
        (
            MapObject.LINKED_HASH_MAP,
            {
                'test_data': 12345,
                456: ['string_1', 'string_2'],
                'test_key': 32.4
            }
        )
    ],
    [
        'binary_map',
        (MapObject.HASH_MAP, {i: Value(id=i, name=f"val_{i}") for i in range(10)}),
        (MapObject.HASH_MAP, {i: Value(id=i, name=f"val_{i}") for i in range(10)})
    ]
]


@pytest.mark.parametrize(['key', 'hinted_value', 'value'], collection_params)
def test_put_get_collection(cache, key, hinted_value, value):
    cache.put(key, hinted_value)
    assert cache.get(key) == value


@pytest.mark.parametrize(['key', 'hinted_value', 'value'], collection_params)
@pytest.mark.asyncio
async def test_put_get_collection_async(async_cache, key, hinted_value, value):
    await async_cache.put(key, hinted_value)
    assert await async_cache.get(key) == value


@pytest.fixture
def complex_map():
    return {"test" + str(i): ((MapObject.HASH_MAP,
                               {"key_1": ((CollectionObject.ARR_LIST, ["value_1", 1.0]), CollectionObject),
                                "key_2": ((CollectionObject.ARR_LIST, [["value_2_1", "1.0"], ["value_2_2", "0.25"]]),
                                          CollectionObject),
                                "key_3": ((CollectionObject.ARR_LIST, [["value_3_1", "1.0"], ["value_3_2", "0.25"]]),
                                          CollectionObject),
                                "key_4": ((CollectionObject.ARR_LIST, [["value_4_1", "1.0"], ["value_4_2", "0.25"]]),
                                          CollectionObject),
                                'key_5': False,
                                "key_6": "value_6"}), MapObject) for i in range(10000)}


def test_put_all_large_complex_map(cache, complex_map):
    cache.put_all(complex_map)
    values = cache.get_all(complex_map.keys())
    assert len(values) == len(complex_map)


@pytest.mark.asyncio
async def test_put_all_large_complex_map_async(async_cache, complex_map):
    await async_cache.put_all(complex_map)
    values = await async_cache.get_all(complex_map.keys())
    assert len(values) == len(complex_map)
