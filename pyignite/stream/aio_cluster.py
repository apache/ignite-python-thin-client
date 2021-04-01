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

"""
This module contains `AioCluster` that lets you get info and change state of the
whole cluster.
"""
from pyignite import AioClient
from pyignite.api.cluster import cluster_get_state_async, cluster_set_state_async


class AioCluster:
    """
    Ignite cluster abstraction. Users should never use this class directly,
    but construct its instances with
    :py:meth:`~pyignite.aio_client.AioClient.get_cluster` method instead.
    """

    def __init__(self, client: 'AioClient'):
        self._client = client

    async def get_state(self):
        """
        Gets current cluster state.

        :return: Current cluster state. This is one of ClusterState.INACTIVE,
         ClusterState.ACTIVE or ClusterState.ACTIVE_READ_ONLY.
        """
        return await cluster_get_state_async(await self._client.random_node())

    async def set_state(self, state):
        """
        Changes current cluster state to the given.

        Note: Deactivation clears in-memory caches (without persistence)
         including the system caches.

        :param state: New cluster state. This is one of ClusterState.INACTIVE,
         ClusterState.ACTIVE or ClusterState.ACTIVE_READ_ONLY.
        """
        return await cluster_set_state_async(await self._client.random_node(), state)
