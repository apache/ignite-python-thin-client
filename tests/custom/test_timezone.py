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

from tests.util import kill_process_tree

from pyignite.datatypes.cluster_state import ClusterState


def test_cluster_set_active(start_ignite_server, start_client):
    server1 = start_ignite_server(idx=1)
    server2 = start_ignite_server(idx=2)
    try:
        client = start_client()

        with client.connect([("127.0.0.1", 10801), ("127.0.0.1", 10802)]):
            cluster = client.get_cluster()

            assert cluster.get_state() == ClusterState.ACTIVE

            cluster.set_state(ClusterState.ACTIVE_READ_ONLY)
            assert cluster.get_state() == ClusterState.ACTIVE_READ_ONLY

            cluster.set_state(ClusterState.INACTIVE)
            assert cluster.get_state() == ClusterState.INACTIVE

            cluster.set_state(ClusterState.ACTIVE)
            assert cluster.get_state() == ClusterState.ACTIVE
    finally:
        kill_process_tree(server1.pid)
        kill_process_tree(server2.pid)


@pytest.mark.asyncio
async def test_cluster_set_active_async(start_ignite_server, start_async_client):
    server1 = start_ignite_server(idx=1)
    server2 = start_ignite_server(idx=2)
    try:
        client = await start_async_client()
        with await client.connect([("127.0.0.1", 10801), ("127.0.0.1", 10802)]):
            cluster = await client.get_cluster()

            assert await cluster.get_state() == ClusterState.ACTIVE

            await cluster.set_state(ClusterState.ACTIVE_READ_ONLY)
            assert await cluster.get_state() == ClusterState.ACTIVE_READ_ONLY

            await cluster.set_state(ClusterState.INACTIVE)
            assert await cluster.get_state() == ClusterState.INACTIVE

            await cluster.set_state(ClusterState.ACTIVE)
            assert await cluster.get_state() == ClusterState.ACTIVE
    finally:
        kill_process_tree(server1.pid)
        kill_process_tree(server2.pid)
