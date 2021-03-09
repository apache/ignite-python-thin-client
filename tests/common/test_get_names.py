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

import pytest


def test_get_names(client):
    bucket_names = {'my_bucket', 'my_bucket_2', 'my_bucket_3'}
    for name in bucket_names:
        client.get_or_create_cache(name)

    assert set(client.get_cache_names()) == bucket_names


@pytest.mark.asyncio
async def test_get_names_async(async_client):
    bucket_names = {'my_bucket', 'my_bucket_2', 'my_bucket_3'}
    await asyncio.gather(*[async_client.get_or_create_cache(name) for name in bucket_names])

    assert set(await async_client.get_cache_names()) == bucket_names
