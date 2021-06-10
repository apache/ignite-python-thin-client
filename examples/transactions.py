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

from pyignite import AioClient, Client
from pyignite.datatypes import TransactionIsolation, TransactionConcurrency
from pyignite.datatypes.prop_codes import PROP_CACHE_ATOMICITY_MODE, PROP_NAME
from pyignite.datatypes.cache_config import CacheAtomicityMode
from pyignite.exceptions import CacheError


async def async_example():
    client = AioClient()
    async with client.connect('127.0.0.1', 10800):
        cache = await client.get_or_create_cache({
            PROP_NAME: 'tx_cache',
            PROP_CACHE_ATOMICITY_MODE: CacheAtomicityMode.TRANSACTIONAL
        })

        # starting transaction
        async with client.tx_start(
                isolation=TransactionIsolation.REPEATABLE_READ, concurrency=TransactionConcurrency.PESSIMISTIC
        ) as tx:
            await cache.put(1, 'success')
            await tx.commit()

        # key=1 value=success
        print(f"key=1 value={await cache.get(1)}")

        # rollback transaction.
        try:
            async with client.tx_start(
                    isolation=TransactionIsolation.REPEATABLE_READ, concurrency=TransactionConcurrency.PESSIMISTIC
            ):
                await cache.put(1, 'fail')
                raise RuntimeError('test')
        except RuntimeError:
            pass

        # key=1 value=success
        print(f"key=1 value={await cache.get(1)}")

        # rollback transaction on timeout.
        try:
            async with client.tx_start(timeout=1.0, label='long-tx') as tx:
                await cache.put(1, 'fail')
                await asyncio.sleep(2.0)
                await tx.commit()
        except CacheError as e:
            # Cache transaction timed out: GridNearTxLocal[...timeout=1000, ... label=long-tx]
            print(e)

        # key=1 value=success
        print(f"key=1 value={await cache.get(1)}")

        # destroy cache
        await cache.destroy()


def sync_example():
    client = Client()
    with client.connect('127.0.0.1', 10800):
        cache = client.get_or_create_cache({
            PROP_NAME: 'tx_cache',
            PROP_CACHE_ATOMICITY_MODE: CacheAtomicityMode.TRANSACTIONAL
        })

        # starting transaction
        with client.tx_start(
                isolation=TransactionIsolation.REPEATABLE_READ, concurrency=TransactionConcurrency.PESSIMISTIC
        ) as tx:
            cache.put(1, 'success')
            tx.commit()

        # key=1 value=success
        print(f"key=1 value={cache.get(1)}")

        # rollback transaction.
        try:
            with client.tx_start(
                    isolation=TransactionIsolation.REPEATABLE_READ, concurrency=TransactionConcurrency.PESSIMISTIC
            ):
                cache.put(1, 'fail')
                raise RuntimeError('test')
        except RuntimeError:
            pass

        # key=1 value=success
        print(f"key=1 value={cache.get(1)}")

        # rollback transaction on timeout.
        try:
            with client.tx_start(timeout=1.0, label='long-tx') as tx:
                cache.put(1, 'fail')
                time.sleep(2.0)
                tx.commit()
        except CacheError as e:
            # Cache transaction timed out: GridNearTxLocal[...timeout=1000, ... label=long-tx]
            print(e)

        # key=1 value=success
        print(f"key=1 value={cache.get(1)}")

        # destroy cache
        cache.destroy()


if __name__ == '__main__':
    print("Starting sync example")
    sync_example()

    print("Starting async example")
    loop = asyncio.get_event_loop()
    loop.run_until_complete(async_example())
