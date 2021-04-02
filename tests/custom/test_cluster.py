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

from pyignite.exceptions import CacheError
from tests.util import kill_process_tree, clear_ignite_work_dir

from pyignite.datatypes.cluster_state import ClusterState


@pytest.fixture(params=['with-persistence', 'without-persistence'])
def with_persistence(request):
    yield request.param == 'with-persistence'


@pytest.fixture(autouse=True)
def cleanup():
    clear_ignite_work_dir()
    yield None
    clear_ignite_work_dir()


def test_cluster_set_active(start_ignite_server, start_client, with_persistence, cleanup):
    key = 42
    val = 42

    server1 = start_ignite_server(idx=1, use_persistence=with_persistence)
    server2 = start_ignite_server(idx=2, use_persistence=with_persistence)

    start_state = ClusterState.INACTIVE if with_persistence else ClusterState.ACTIVE
    try:
        client = start_client(timeout=0)
        with client.connect([("127.0.0.1", 10801), ("127.0.0.1", 10802)]):
            cluster = client.get_cluster()
            assert cluster.get_state() == start_state

            cluster.set_state(ClusterState.ACTIVE)
            assert cluster.get_state() == ClusterState.ACTIVE

            cache = client.get_or_create_cache("test_cache")
            cache.put(key, val)
            assert cache.get(key) == val

            cluster.set_state(ClusterState.ACTIVE_READ_ONLY)
            assert cluster.get_state() == ClusterState.ACTIVE_READ_ONLY

            assert cache.get(key) == val
            with pytest.raises(CacheError):
                cache.put(key, val + 1)

            cluster.set_state(ClusterState.INACTIVE)
            assert cluster.get_state() == ClusterState.INACTIVE

            with pytest.raises(CacheError):
                cache.get(key)

            with pytest.raises(CacheError):
                cache.put(key, val + 1)

            cluster.set_state(ClusterState.ACTIVE)
            assert cluster.get_state() == ClusterState.ACTIVE

            cache.put(key, val + 2)
            assert cache.get(key) == val + 2
    finally:
        kill_process_tree(server1.pid)
        kill_process_tree(server2.pid)


@pytest.mark.asyncio
async def test_cluster_set_active_async(start_ignite_server, start_async_client, with_persistence, cleanup):
    key = 42
    val = 42

    server1 = start_ignite_server(idx=1, use_persistence=with_persistence)
    server2 = start_ignite_server(idx=2, use_persistence=with_persistence)

    start_state = ClusterState.INACTIVE if with_persistence else ClusterState.ACTIVE
    try:
        client = start_async_client()
        async with client.connect([("127.0.0.1", 10801), ("127.0.0.1", 10802)]):
            cluster = client.get_cluster()
            assert await cluster.get_state() == start_state

            await cluster.set_state(ClusterState.ACTIVE)
            assert await cluster.get_state() == ClusterState.ACTIVE

            cache = await client.get_or_create_cache("test_cache")
            await cache.put(key, val)
            assert await cache.get(key) == val

            await cluster.set_state(ClusterState.ACTIVE_READ_ONLY)
            assert await cluster.get_state() == ClusterState.ACTIVE_READ_ONLY

            assert await cache.get(key) == val
            with pytest.raises(CacheError):
                await cache.put(key, val + 1)

            await cluster.set_state(ClusterState.INACTIVE)
            assert await cluster.get_state() == ClusterState.INACTIVE

            with pytest.raises(CacheError):
                await cache.get(key)

            with pytest.raises(CacheError):
                await cache.put(key, val + 1)

            await cluster.set_state(ClusterState.ACTIVE)
            assert await cluster.get_state() == ClusterState.ACTIVE

            await cache.put(key, val + 2)
            assert await cache.get(key) == val + 2
    finally:
        kill_process_tree(server1.pid)
        kill_process_tree(server2.pid)
