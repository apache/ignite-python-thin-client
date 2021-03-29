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
def connection_param():
    return [('127.0.0.1', 10800 + i) for i in range(1, 4)]


@pytest.mark.parametrize('partition_aware', ['with_partition_aware', 'wo_partition_aware'])
def test_connection_context(connection_param, partition_aware):
    is_partition_aware = partition_aware == 'with_partition_aware'
    client = Client(partition_aware=is_partition_aware)

    # Check context manager
    with client.connect(connection_param):
        __check_open(client, is_partition_aware)
    __check_closed(client)

    # Check standard way
    try:
        client.connect(connection_param)
        __check_open(client, is_partition_aware)
    finally:
        client.close()
        __check_closed(client)


@pytest.mark.asyncio
@pytest.mark.parametrize('partition_aware', ['with_partition_aware', 'wo_partition_aware'])
async def test_connection_context_async(connection_param, partition_aware):
    is_partition_aware = partition_aware == 'with_partition_aware'
    client = AioClient(partition_aware=is_partition_aware)

    # Check async context manager.
    async with client.connect(connection_param):
        await __check_open(client, is_partition_aware)
    __check_closed(client)

    # Check standard way.
    try:
        await client.connect(connection_param)
        await __check_open(client, is_partition_aware)
    finally:
        await client.close()
        __check_closed(client)


def __check_open(client, is_partition_aware):
    def inner_sync():
        if is_partition_aware:
            assert client.random_node.alive
        else:
            all(n.alive for n in client._nodes)

    async def inner_async():
        if is_partition_aware:
            random_node = await client.random_node()
            assert random_node.alive
        else:
            all(n.alive for n in client._nodes)

    return inner_sync() if isinstance(client, Client) else inner_async()


def __check_closed(client):
    assert all(not n.alive for n in client._nodes)
