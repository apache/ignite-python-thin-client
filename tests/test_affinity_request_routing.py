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

from collections import OrderedDict, deque
import pytest

from pyignite import *
from pyignite.connection import Connection
from pyignite.datatypes import *
from pyignite.datatypes.cache_config import CacheMode
from pyignite.datatypes.prop_codes import *
from tests.util import *


requests = deque()
old_send = Connection.send


def patched_send(self, *args, **kwargs):
    """Patched send function that push to queue idx of server to which request is routed."""
    requests.append(self.port % 100)
    return old_send(self, *args, **kwargs)


def setup_function():
    requests.clear()
    Connection.send = patched_send


def teardown_function():
    Connection.send = old_send


def wait_for_affinity_distribution(cache, key, node_idx, timeout=30):
    real_node_idx = 0

    def check_grid_idx():
        nonlocal real_node_idx
        try:
            cache.get(key)
            real_node_idx = requests.pop()
        except (OSError, IOError):
            return False
        return real_node_idx == node_idx

    res = wait_for_condition(check_grid_idx, timeout=timeout)

    if not res:
        raise TimeoutError(f"failed to wait for affinity distribution, expected node_idx {node_idx},"
                           f"got {real_node_idx} instead")


@pytest.mark.parametrize("key,grid_idx", [(1, 1), (2, 2), (3, 3), (4, 1), (5, 1), (6, 2), (11, 1), (13, 1), (19, 1)])
@pytest.mark.parametrize("backups", [0, 1, 2, 3])
def test_cache_operation_on_primitive_key_routes_request_to_primary_node(
        request, key, grid_idx, backups, client_partition_aware):

    cache = client_partition_aware.get_or_create_cache({
        PROP_NAME: request.node.name + str(backups),
        PROP_BACKUPS_NUMBER: backups,
    })

    cache.put(key, key)
    wait_for_affinity_distribution(cache, key, grid_idx)

    # Test
    cache.get(key)
    assert requests.pop() == grid_idx

    cache.put(key, key)
    assert requests.pop() == grid_idx

    cache.replace(key, key + 1)
    assert requests.pop() == grid_idx

    cache.clear_key(key)
    assert requests.pop() == grid_idx

    cache.contains_key(key)
    assert requests.pop() == grid_idx

    cache.get_and_put(key, 3)
    assert requests.pop() == grid_idx

    cache.get_and_put_if_absent(key, 4)
    assert requests.pop() == grid_idx

    cache.put_if_absent(key, 5)
    assert requests.pop() == grid_idx

    cache.get_and_remove(key)
    assert requests.pop() == grid_idx

    cache.get_and_replace(key, 6)
    assert requests.pop() == grid_idx

    cache.remove_key(key)
    assert requests.pop() == grid_idx

    cache.remove_if_equals(key, -1)
    assert requests.pop() == grid_idx

    cache.replace(key, -1)
    assert requests.pop() == grid_idx

    cache.replace_if_equals(key, 10, -10)
    assert requests.pop() == grid_idx


@pytest.mark.skip(reason="Custom key objects are not supported yet")
def test_cache_operation_on_complex_key_routes_request_to_primary_node():
    pass


@pytest.mark.parametrize("key,grid_idx", [(1, 2), (2, 1), (3, 1), (4, 2), (5, 2), (6, 3)])
@pytest.mark.skip(reason="Custom key objects are not supported yet")
def test_cache_operation_on_custom_affinity_key_routes_request_to_primary_node(
        request, client_partition_aware, key, grid_idx):
    class AffinityTestType1(
        metaclass=GenericObjectMeta,
        type_name='AffinityTestType1',
        schema=OrderedDict([
            ('test_str', String),
            ('test_int', LongObject)
        ])
    ):
        pass

    cache_config = {
        PROP_NAME: request.node.name,
        PROP_CACHE_KEY_CONFIGURATION: [
            {
                'type_name': 'AffinityTestType1',
                'affinity_key_field_name': 'test_int',
            },
        ],
    }
    cache = client_partition_aware.create_cache(cache_config)

    # noinspection PyArgumentList
    key_obj = AffinityTestType1(
        test_str="abc",
        test_int=key
    )

    cache.put(key_obj, 1)
    cache.put(key_obj, 2)

    assert requests.pop() == grid_idx


def test_cache_operation_routed_to_new_cluster_node(request, start_ignite_server, start_client):
    client = start_client(partition_aware=True)
    client.connect([("127.0.0.1", 10801), ("127.0.0.1", 10802), ("127.0.0.1", 10803), ("127.0.0.1", 10804)])
    cache = client.get_or_create_cache(request.node.name)
    key = 12
    wait_for_affinity_distribution(cache, key, 3)
    cache.put(key, key)
    cache.put(key, key)
    assert requests.pop() == 3

    srv = start_ignite_server(4)
    try:
        # Wait for rebalance and partition map exchange
        wait_for_affinity_distribution(cache, key, 4)

        # Response is correct and comes from the new node
        res = cache.get_and_remove(key)
        assert res == key
        assert requests.pop() == 4
    finally:
        kill_process_tree(srv.pid)


def test_replicated_cache_operation_routed_to_random_node(request, client_partition_aware):
    cache = client_partition_aware.get_or_create_cache({
        PROP_NAME: request.node.name,
        PROP_CACHE_MODE: CacheMode.REPLICATED,
    })

    verify_random_node(cache)


def verify_random_node(cache):
    key = 1
    cache.put(key, key)

    idx1 = requests.pop()
    idx2 = idx1

    # Try 10 times - random node may end up being the same
    for _ in range(1, 10):
        cache.put(key, key)
        idx2 = requests.pop()
        if idx2 != idx1:
            break
    assert idx1 != idx2
