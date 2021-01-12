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

from pyignite import *
from pyignite.datatypes import *
from pyignite.datatypes.cache_config import CacheMode
from pyignite.datatypes.prop_codes import *
from tests.util import *


@pytest.mark.parametrize("key,grid_idx", [(1, 3), (2, 1), (3, 1), (4, 3), (5, 1), (6, 3), (11, 2), (13, 2), (19, 2)])
@pytest.mark.parametrize("backups", [0, 1, 2, 3])
def test_cache_operation_on_primitive_key_routes_request_to_primary_node(
        request, key, grid_idx, backups, client_partition_aware):

    cache = client_partition_aware.get_or_create_cache({
        PROP_NAME: request.node.name + str(backups),
        PROP_BACKUPS_NUMBER: backups,
    })

    # Warm up affinity map
    cache.put(key, key)
    get_request_grid_idx()

    # Test
    cache.get(key)
    assert get_request_grid_idx() == grid_idx

    cache.put(key, key)
    assert get_request_grid_idx("Put") == grid_idx

    cache.replace(key, key + 1)
    assert get_request_grid_idx("Replace") == grid_idx

    cache.clear_key(key)
    assert get_request_grid_idx("ClearKey") == grid_idx

    cache.contains_key(key)
    assert get_request_grid_idx("ContainsKey") == grid_idx

    cache.get_and_put(key, 3)
    assert get_request_grid_idx("GetAndPut") == grid_idx

    cache.get_and_put_if_absent(key, 4)
    assert get_request_grid_idx("GetAndPutIfAbsent") == grid_idx

    cache.put_if_absent(key, 5)
    assert get_request_grid_idx("PutIfAbsent") == grid_idx

    cache.get_and_remove(key)
    assert get_request_grid_idx("GetAndRemove") == grid_idx

    cache.get_and_replace(key, 6)
    assert get_request_grid_idx("GetAndReplace") == grid_idx

    cache.remove_key(key)
    assert get_request_grid_idx("RemoveKey") == grid_idx

    cache.remove_if_equals(key, -1)
    assert get_request_grid_idx("RemoveIfEquals") == grid_idx

    cache.replace(key, -1)
    assert get_request_grid_idx("Replace") == grid_idx

    cache.replace_if_equals(key, 10, -10)
    assert get_request_grid_idx("ReplaceIfEquals") == grid_idx


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

    assert get_request_grid_idx("Put") == grid_idx


def test_cache_operation_routed_to_new_cluster_node(request):
    client = Client(partition_aware=True)
    client.connect([("127.0.0.1", 10801), ("127.0.0.1", 10802), ("127.0.0.1", 10803), ("127.0.0.1", 10804)])
    cache = client.get_or_create_cache(request.node.name)
    key = 12
    cache.put(key, key)
    cache.put(key, key)
    assert get_request_grid_idx("Put") == 3

    srv = start_ignite(4)
    try:
        # Wait for rebalance and partition map exchange
        def check_grid_idx():
            cache.get(key)
            return get_request_grid_idx() == 4
        wait_for_condition(check_grid_idx)

        # Response is correct and comes from the new node
        res = cache.get_and_remove(key)
        assert res == key
        assert get_request_grid_idx("GetAndRemove") == 4
    finally:
        kill_process_tree(srv.pid)


def test_unsupported_affinity_cache_operation_routed_to_random_node(client_partition_aware):
    verify_random_node(client_partition_aware.get_cache("custom-affinity"))


def test_replicated_cache_operation_routed_to_random_node(request, client_partition_aware):
    cache = client_partition_aware.get_or_create_cache({
        PROP_NAME: request.node.name,
        PROP_CACHE_MODE: CacheMode.REPLICATED,
    })

    verify_random_node(cache)


def verify_random_node(cache):
    key = 1
    cache.put(key, key)

    idx1 = get_request_grid_idx("Put")
    idx2 = idx1

    # Try 10 times - random node may end up being the same
    for _ in range(1, 10):
        cache.put(key, key)
        idx2 = get_request_grid_idx("Put")
        if idx2 != idx1:
            break
    assert idx1 != idx2
