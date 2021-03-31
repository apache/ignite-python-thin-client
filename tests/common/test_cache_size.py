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

from pyignite.datatypes.cache_config import WriteSynchronizationMode
from pyignite.datatypes.key_value import PeekModes
from pyignite.datatypes.prop_codes import (
    PROP_NAME, PROP_IS_ONHEAP_CACHE_ENABLED, PROP_BACKUPS_NUMBER, PROP_WRITE_SYNCHRONIZATION_MODE
)
from tests.util import get_or_create_cache, get_or_create_cache_async

test_params = [
    [
        {
            PROP_NAME: 'cache_onheap_backups_2',
            PROP_IS_ONHEAP_CACHE_ENABLED: True,
            PROP_BACKUPS_NUMBER: 2,
            PROP_WRITE_SYNCHRONIZATION_MODE: WriteSynchronizationMode.FULL_SYNC
        },
        [
            [None, 1],
            [PeekModes.PRIMARY, 1],
            [PeekModes.BACKUP, 2],
            [PeekModes.ALL, 3],
            [[PeekModes.PRIMARY, PeekModes.BACKUP], 3],
            [PeekModes.ONHEAP, 1],
            [PeekModes.OFFHEAP, 1]
        ]
    ]
]


@pytest.mark.parametrize("cache_settings, cache_sizes", test_params)
def test_cache_size(client, cache_settings, cache_sizes):
    with get_or_create_cache(client, cache_settings) as cache:
        cache.put(1, 1)

        for props, exp_value in cache_sizes:
            value = cache.get_size(props)
            assert value == exp_value, f"expected {exp_value} for {props}, got {value} instead."


@pytest.mark.asyncio
@pytest.mark.parametrize("cache_settings, cache_sizes", test_params)
async def test_cache_size_async(async_client, cache_settings, cache_sizes):
    async with get_or_create_cache_async(async_client, cache_settings) as cache:
        await cache.put(1, 1)

        for props, exp_value in cache_sizes:
            value = await cache.get_size(props)
            assert value == exp_value, f"expected {exp_value} for {props}, got {value} instead."
