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

import pytest

from pyignite import GenericObjectMeta
from pyignite.api import resource_close, resource_close_async
from pyignite.connection import AioConnection
from pyignite.datatypes import IntObject, String
from pyignite.exceptions import CacheError


class SimpleObject(
    metaclass=GenericObjectMeta,
    type_name='SimpleObject',
    schema=OrderedDict([
        ('id', IntObject),
        ('str', String),
    ])
):
    pass


page_size = 10


@pytest.fixture
def test_objects_data():
    yield {i: SimpleObject(id=i, str=f'str_{i}') for i in range(page_size * 2)}


@pytest.mark.asyncio
def test_scan_objects(cache, test_objects_data):
    cache.put_all(test_objects_data)

    for p_sz in [page_size, page_size * 2, page_size * 3, page_size + 5]:
        with cache.scan(p_sz) as cursor:
            result = {k: v for k, v in cursor}
            assert result == test_objects_data

        __check_cursor_closed(cursor)

        with pytest.raises(Exception):
            with cache.scan(p_sz) as cursor:
                for _ in cursor:
                    raise Exception

        __check_cursor_closed(cursor)

    cursor = cache.scan(page_size)
    assert {k: v for k, v in cursor} == test_objects_data
    __check_cursor_closed(cursor)


@pytest.mark.asyncio
async def test_scan_objects_async(async_cache, test_objects_data):
    await async_cache.put_all(test_objects_data)

    for p_sz in [page_size, page_size * 2, page_size * 3, page_size + 5]:
        async with async_cache.scan(p_sz) as cursor:
            result = {k: v async for k, v in cursor}
            assert result == test_objects_data

        await __check_cursor_closed(cursor)

        with pytest.raises(Exception):
            async with async_cache.scan(p_sz) as cursor:
                async for _ in cursor:
                    raise Exception

    await __check_cursor_closed(cursor)

    cursor = await async_cache.scan(page_size)
    assert {k: v async for k, v in cursor} == test_objects_data

    await __check_cursor_closed(cursor)


@pytest.fixture
def cache_scan_data():
    yield {
        1: 'This is a test',
        2: 'One more test',
        3: 'Foo',
        4: 'Buzz',
        5: 'Bar',
        6: 'Lorem ipsum',
        7: 'dolor sit amet',
        8: 'consectetur adipiscing elit',
        9: 'Nullam aliquet',
        10: 'nisl at ante',
        11: 'suscipit',
        12: 'ut cursus',
        13: 'metus interdum',
        14: 'Nulla tincidunt',
        15: 'sollicitudin iaculis',
    }


@pytest.mark.parametrize('page_size', range(1, 17, 5))
def test_cache_scan(cache, cache_scan_data, page_size):
    cache.put_all(cache_scan_data)

    with cache.scan(page_size=page_size) as cursor:
        assert {k: v for k, v in cursor} == cache_scan_data


@pytest.mark.parametrize('page_size', range(1, 17, 5))
@pytest.mark.asyncio
async def test_cache_scan_async(async_cache, cache_scan_data, page_size):
    await async_cache.put_all(cache_scan_data)

    async with async_cache.scan(page_size=page_size) as cursor:
        assert {k: v async for k, v in cursor} == cache_scan_data


def test_uninitialized_cursor(cache, test_objects_data):
    cache.put_all(test_objects_data)

    cursor = cache.scan(page_size)
    for _ in cursor:
        break

    cursor.close()
    __check_cursor_closed(cursor)


@pytest.mark.asyncio
async def test_uninitialized_cursor_async(async_cache, test_objects_data):
    await async_cache.put_all(test_objects_data)

    # iterating of non-awaited cursor.
    with pytest.raises(CacheError):
        cursor = async_cache.scan(page_size)
        assert {k: v async for k, v in cursor} == test_objects_data

    cursor = await async_cache.scan(page_size)
    assert {k: v async for k, v in cursor} == test_objects_data
    await __check_cursor_closed(cursor)


def __check_cursor_closed(cursor):
    async def check_async():
        result = await resource_close_async(cursor.connection, cursor.cursor_id)
        assert result.status != 0

    def check():
        result = resource_close(cursor.connection, cursor.cursor_id)
        assert result.status != 0

    return check_async() if isinstance(cursor.connection, AioConnection) else check()
