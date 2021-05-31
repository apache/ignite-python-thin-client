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


@pytest.fixture
def client():
    client = Client(partition_aware=True)
    try:
        client.connect('127.0.0.1', 10801)
        yield client
    finally:
        client.close()


@pytest.fixture
async def async_client(event_loop):
    client = AioClient(partition_aware=True)
    try:
        await client.connect('127.0.0.1', 10801)
        yield client
    finally:
        await client.close()


def test_all_cache_operations_with_partition_aware_client_on_single_server(request, client):
    cache = client.get_or_create_cache(request.node.name)
    key = 1
    key2 = 2

    # Put/Get
    cache.put(key, key)
    assert cache.get(key) == key

    # Replace
    res = cache.replace(key, key2)
    assert res
    assert cache.get(key) == key2

    # Clear
    cache.put(key2, key2)
    cache.clear_key(key2)
    assert cache.get(key2) is None

    # ContainsKey
    assert cache.contains_key(key)
    assert not cache.contains_key(key2)

    # GetAndPut
    cache.put(key, key)
    res = cache.get_and_put(key, key2)
    assert res == key
    assert cache.get(key) == key2

    # GetAndPutIfAbsent
    cache.clear_key(key)
    res = cache.get_and_put_if_absent(key, key)
    res2 = cache.get_and_put_if_absent(key, key2)
    assert res is None
    assert res2 == key
    assert cache.get(key) == key

    # PutIfAbsent
    cache.clear_key(key)
    res = cache.put_if_absent(key, key)
    res2 = cache.put_if_absent(key, key2)
    assert res
    assert not res2
    assert cache.get(key) == key

    # GetAndRemove
    cache.put(key, key)
    res = cache.get_and_remove(key)
    assert res == key
    assert cache.get(key) is None

    # GetAndReplace
    cache.put(key, key)
    res = cache.get_and_replace(key, key2)
    assert res == key
    assert cache.get(key) == key2

    # RemoveKey
    cache.put(key, key)
    cache.remove_key(key)
    assert cache.get(key) is None

    # RemoveIfEquals
    cache.put(key, key)
    res = cache.remove_if_equals(key, key2)
    res2 = cache.remove_if_equals(key, key)
    assert not res
    assert res2
    assert cache.get(key) is None

    # Replace
    cache.put(key, key)
    cache.replace(key, key2)
    assert cache.get(key) == key2

    # ReplaceIfEquals
    cache.put(key, key)
    res = cache.replace_if_equals(key, key2, key2)
    res2 = cache.replace_if_equals(key, key, key2)
    assert not res
    assert res2
    assert cache.get(key) == key2


@pytest.mark.asyncio
async def test_all_cache_operations_with_partition_aware_client_on_single_server_async(request, async_client):
    cache = await async_client.get_or_create_cache(request.node.name)
    key = 1
    key2 = 2

    # Put/Get
    await cache.put(key, key)
    assert await cache.get(key) == key

    # Replace
    res = await cache.replace(key, key2)
    assert res
    assert await cache.get(key) == key2

    # Clear
    await cache.put(key2, key2)
    await cache.clear_key(key2)
    assert await cache.get(key2) is None

    # ContainsKey
    assert await cache.contains_key(key)
    assert not await cache.contains_key(key2)

    # GetAndPut
    await cache.put(key, key)
    res = await cache.get_and_put(key, key2)
    assert res == key
    assert await cache.get(key) == key2

    # GetAndPutIfAbsent
    await cache.clear_key(key)
    res = await cache.get_and_put_if_absent(key, key)
    res2 = await cache.get_and_put_if_absent(key, key2)
    assert res is None
    assert res2 == key
    assert await cache.get(key) == key

    # PutIfAbsent
    await cache.clear_key(key)
    res = await cache.put_if_absent(key, key)
    res2 = await cache.put_if_absent(key, key2)
    assert res
    assert not res2
    assert await cache.get(key) == key

    # GetAndRemove
    await cache.put(key, key)
    res = await cache.get_and_remove(key)
    assert res == key
    assert await cache.get(key) is None

    # GetAndReplace
    await cache.put(key, key)
    res = await cache.get_and_replace(key, key2)
    assert res == key
    assert await cache.get(key) == key2

    # RemoveKey
    await cache.put(key, key)
    await cache.remove_key(key)
    assert await cache.get(key) is None

    # RemoveIfEquals
    await cache.put(key, key)
    res = await cache.remove_if_equals(key, key2)
    res2 = await cache.remove_if_equals(key, key)
    assert not res
    assert res2
    assert await cache.get(key) is None

    # Replace
    await cache.put(key, key)
    await cache.replace(key, key2)
    assert await cache.get(key) == key2

    # ReplaceIfEquals
    await cache.put(key, key)
    res = await cache.replace_if_equals(key, key2, key2)
    res2 = await cache.replace_if_equals(key, key, key2)
    assert not res
    assert res2
    assert await cache.get(key) == key2
