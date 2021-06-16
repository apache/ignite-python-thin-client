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
import asyncio
import time
from datetime import timedelta

import pytest

from pyignite.datatypes import ExpiryPolicy
from pyignite.datatypes.prop_codes import PROP_NAME, PROP_EXPIRY_POLICY


@pytest.mark.skip_if_no_expiry_policy
def test_expiry_policy(cache):
    ttl, num_retries = timedelta(seconds=0.6), 10
    cache_eternal = cache.with_expire_policy(create=ExpiryPolicy.ETERNAL)
    cache_created = cache.with_expire_policy(create=ttl)
    cache_updated = cache.with_expire_policy(update=ttl)
    cache_accessed = cache.with_expire_policy(access=ttl)

    for _ in range(num_retries):
        cache.clear()

        start = time.time()

        cache_eternal.put(0, 0)
        cache_created.put(1, 1)
        cache_updated.put(2, 2)
        cache_accessed.put(3, 3)

        time.sleep(ttl.total_seconds() * 2 / 3)

        result = [cache.contains_key(k) for k in range(4)]

        if time.time() - start >= ttl.total_seconds():
            continue

        assert all(result)

        start = time.time()

        cache_created.put(1, 2)  # Check that update doesn't matter for created policy
        cache_created.get(1)  # Check that access doesn't matter for created policy
        cache_updated.put(2, 3)  # Check that update policy works.
        cache_accessed.get(3)   # Check that access policy works.

        time.sleep(ttl.total_seconds() * 2 / 3)

        result = [cache.contains_key(k) for k in range(4)]

        if time.time() - start >= ttl.total_seconds():
            continue

        assert result == [True, False, True, True]

        time.sleep(ttl.total_seconds() * 2 / 3)

        cache_updated.get(2)  # Check that access doesn't matter for updated policy.

        time.sleep(ttl.total_seconds() * 2 / 3)

        result = [cache.contains_key(k) for k in range(0, 4)]
        assert result == [True, False, False, False]


@pytest.mark.asyncio
@pytest.mark.skip_if_no_expiry_policy
async def test_expiry_policy_async(async_cache):
    ttl, num_retries = timedelta(seconds=0.6), 10
    cache_eternal = async_cache.with_expire_policy(create=ExpiryPolicy.ETERNAL)
    cache_created = async_cache.with_expire_policy(create=ttl)
    cache_updated = async_cache.with_expire_policy(update=ttl)
    cache_accessed = async_cache.with_expire_policy(access=ttl)

    for _ in range(num_retries):
        await async_cache.clear()

        start = time.time()

        await asyncio.gather(
            cache_eternal.put(0, 0),
            cache_created.put(1, 1),
            cache_updated.put(2, 2),
            cache_accessed.put(3, 3)
        )

        await asyncio.sleep(ttl.total_seconds() * 2 / 3)

        result = await asyncio.gather(*[async_cache.contains_key(k) for k in range(4)])

        if time.time() - start >= ttl.total_seconds():
            continue

        assert all(result)

        start = time.time()

        await asyncio.gather(
            cache_created.put(1, 2),  # Check that update doesn't matter for created policy
            cache_created.get(1),  # Check that access doesn't matter for created policy
            cache_updated.put(2, 3),  # Check that update policy works.
            cache_accessed.get(3)   # Check that access policy works.
        )

        await asyncio.sleep(ttl.total_seconds() * 2 / 3)

        result = await asyncio.gather(*[async_cache.contains_key(k) for k in range(4)])

        if time.time() - start >= ttl.total_seconds():
            continue

        assert result == [True, False, True, True]

        await asyncio.sleep(ttl.total_seconds() * 2 / 3)

        await cache_updated.get(2)  # Check that access doesn't matter for updated policy.

        await asyncio.sleep(ttl.total_seconds() * 2 / 3)

        result = await asyncio.gather(*[async_cache.contains_key(k) for k in range(4)])
        assert result == [True, False, False, False]

create_cache_with_expiry_params = (
    'expiry_policy',
    [
        None,
        ExpiryPolicy(),
        ExpiryPolicy(create=ExpiryPolicy.ETERNAL),
        ExpiryPolicy(create=2000, update=4000, access=6000)
    ]
)


@pytest.mark.parametrize(*create_cache_with_expiry_params)
@pytest.mark.skip_if_no_expiry_policy
def test_create_cache_with_expiry_policy(client, expiry_policy):
    cache = client.create_cache({
        PROP_NAME: 'expiry_cache',
        PROP_EXPIRY_POLICY: expiry_policy
    })
    try:
        settings = cache.settings
        assert settings[PROP_EXPIRY_POLICY] == expiry_policy
    finally:
        cache.destroy()


@pytest.mark.parametrize(*create_cache_with_expiry_params)
@pytest.mark.skip_if_no_expiry_policy
@pytest.mark.asyncio
async def test_create_cache_with_expiry_policy_async(async_client, expiry_policy):
    cache = await async_client.create_cache({
        PROP_NAME: 'expiry_cache',
        PROP_EXPIRY_POLICY: expiry_policy
    })
    try:
        settings = await cache.settings()
        assert settings[PROP_EXPIRY_POLICY] == expiry_policy
    finally:
        await cache.destroy()


@pytest.mark.skip_if_no_expiry_policy
@pytest.mark.parametrize(
    'params',
    [
        {'create': timedelta(seconds=-1), 'update': timedelta(seconds=-1), 'delete': timedelta(seconds=-1)},
        {'create': 0.6},
        {'create': -3}
    ]
)
def test_expiry_policy_param_validation(params):
    with pytest.raises((TypeError, ValueError)):
        ExpiryPolicy(**params)
