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
import itertools
import sys
import time

import pytest

from pyignite import AioClient, Client
from pyignite.datatypes import TransactionIsolation, TransactionConcurrency
from pyignite.datatypes.cache_config import CacheAtomicityMode
from pyignite.datatypes.prop_codes import PROP_NAME, PROP_CACHE_ATOMICITY_MODE
from pyignite.exceptions import CacheError
from pyignite.transaction import Transaction, AioTransaction


@pytest.fixture
def connection_param():
    return [('127.0.0.1', 10800 + i) for i in range(1, 4)]


@pytest.fixture(params=['with-partition-awareness', 'without-partition-awareness'])
async def async_client(request, connection_param, event_loop):
    client = AioClient(partition_aware=request.param == 'with-partition-awareness')
    try:
        await client.connect(connection_param)
        if not client.protocol_context.is_transactions_supported():
            pytest.skip(f'skipped {request.node.name}, transaction api is not supported.')
        elif sys.version_info < (3, 7):
            pytest.skip(f'skipped {request.node.name}, transaction api is not supported'
                        f'for async client on python {sys.version}')
        else:
            yield client
    finally:
        await client.close()


@pytest.fixture(params=['with-partition-awareness', 'without-partition-awareness'])
def client(request, connection_param):
    client = Client(partition_aware=request.param == 'with-partition-awareness')
    try:
        client.connect(connection_param)
        if not client.protocol_context.is_transactions_supported():
            pytest.skip(f'skipped {request.node.name}, transaction api is not supported.')
        else:
            yield client
    finally:
        client.close()


@pytest.fixture
def tx_cache(client):
    cache = client.get_or_create_cache({
        PROP_NAME: 'tx_cache',
        PROP_CACHE_ATOMICITY_MODE: CacheAtomicityMode.TRANSACTIONAL
    })
    time.sleep(1.0)  # Need to sleep because of https://issues.apache.org/jira/browse/IGNITE-14868
    yield cache
    cache.destroy()


@pytest.fixture
async def async_tx_cache(async_client):
    cache = await async_client.get_or_create_cache({
        PROP_NAME: 'tx_cache',
        PROP_CACHE_ATOMICITY_MODE: CacheAtomicityMode.TRANSACTIONAL
    })
    await asyncio.sleep(1.0)  # Need to sleep because of https://issues.apache.org/jira/browse/IGNITE-14868
    yield cache
    await cache.destroy()


@pytest.mark.parametrize(
    ['iso_level', 'concurrency'],
    itertools.product(
        [iso_level for iso_level in TransactionIsolation],
        [concurrency for concurrency in TransactionConcurrency]
    )
)
def test_simple_transaction(client, tx_cache, iso_level, concurrency):
    with client.tx_start(isolation=iso_level, concurrency=concurrency) as tx:
        tx_cache.put(1, 1)
        tx.commit()

    assert tx_cache.get(1) == 1

    with client.tx_start(isolation=iso_level, concurrency=concurrency) as tx:
        tx_cache.put(1, 10)
        tx.rollback()

    assert tx_cache.get(1) == 1

    with client.tx_start(isolation=iso_level, concurrency=concurrency) as tx:
        tx_cache.put(1, 10)

    assert tx_cache.get(1) == 1


@pytest.mark.parametrize(
    ['iso_level', 'concurrency'],
    itertools.product(
        [iso_level for iso_level in TransactionIsolation],
        [concurrency for concurrency in TransactionConcurrency]
    )
)
@pytest.mark.asyncio
async def test_simple_transaction_async(async_client, async_tx_cache, iso_level, concurrency):
    async with async_client.tx_start(isolation=iso_level, concurrency=concurrency) as tx:
        await async_tx_cache.put(1, 1)
        await tx.commit()

    assert await async_tx_cache.get(1) == 1

    async with async_client.tx_start(isolation=iso_level, concurrency=concurrency) as tx:
        await async_tx_cache.put(1, 10)
        await tx.rollback()

    assert await async_tx_cache.get(1) == 1

    async with async_client.tx_start(isolation=iso_level, concurrency=concurrency) as tx:
        await async_tx_cache.put(1, 10)

    assert await async_tx_cache.get(1) == 1


def test_transactions_timeout(client, tx_cache):
    with client.tx_start(timeout=2000, label='tx-sync') as tx:
        tx_cache.put(1, 1)
        time.sleep(3.0)
        with pytest.raises(CacheError) as to_error:
            tx.commit()
            assert 'tx-sync' in str(to_error) and 'timed out' in str(to_error)


@pytest.mark.asyncio
async def test_transactions_timeout_async(async_client, async_tx_cache):
    async def update(i, timeout):
        async with async_client.tx_start(
                label=f'tx-{i}', timeout=timeout, isolation=TransactionIsolation.READ_COMMITTED,
                concurrency=TransactionConcurrency.PESSIMISTIC
        ) as tx:
            k1, k2 = (1, 2) if i % 2 == 0 else (2, 1)
            v = f'value-{i}'

            await async_tx_cache.put(k1, v)
            await async_tx_cache.put(k2, v)

            await tx.commit()

    task = asyncio.gather(*[update(i, 2000) for i in range(20)], return_exceptions=True)
    await asyncio.sleep(5.0)
    assert task.done()  # Check that all transactions completed or rolled-back on timeout
    for i, ex in enumerate(task.result()):
        if ex:
            assert 'TransactionTimeoutException' in str(ex) or \
                'Cache transaction timed out'  # check that transaction was rolled back.
            assert f'tx-{i}' in str(ex)  # check that tx label presents in error


@pytest.mark.asyncio
@pytest.mark.parametrize('iso_level', [iso_level for iso_level in TransactionIsolation])
async def test_concurrent_pessimistic_transactions_same_key(async_client, async_tx_cache, iso_level):
    async def update(i):
        async with async_client.tx_start(
                label=f'tx_lbl_{i}', isolation=iso_level, concurrency=TransactionConcurrency.PESSIMISTIC
        ) as tx:
            await async_tx_cache.put(1, f'test-{i}')
            await tx.commit()

    res = await asyncio.gather(*[update(i) for i in range(20)], return_exceptions=True)
    assert not any(res)  # Checks that all transactions proceeds


@pytest.mark.asyncio
async def test_concurrent_optimistic_transactions_no_deadlock(async_client, async_tx_cache, event_loop):
    """
    Check that optimistic transactions are deadlock safe.
    """
    async def update(i):
        async with async_client.tx_start(
                label=f'tx-{i}', isolation=TransactionIsolation.SERIALIZABLE,
                concurrency=TransactionConcurrency.OPTIMISTIC
        ) as tx:
            k1, k2 = (1, 2) if i % 2 == 0 else (2, 1)
            v = f'value-{i}'

            await async_tx_cache.put(k1, v)
            await async_tx_cache.put(k2, v)

            await tx.commit()

    task = asyncio.gather(*[update(i) for i in range(20)], return_exceptions=True)
    await asyncio.sleep(2.0)
    assert task.done()  # Check that there are not any deadlock.
    assert not all(task.result())  # Check that some (or all) transactions proceeds.
    for i, ex in enumerate(task.result()):
        if ex:
            assert 'lock conflict' in str(ex)  # check optimistic prepare phase failed
            assert f'tx-{i}' in str(ex)  # check that tx label presents in error


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ['iso_level', 'concurrency'],
    itertools.product(
        [iso_level for iso_level in TransactionIsolation],
        [concurrency for concurrency in TransactionConcurrency]
    )
)
async def test_concurrent_transactions(async_client, async_tx_cache, iso_level, concurrency):
    async def update(i):
        async with async_client.tx_start(isolation=iso_level, concurrency=concurrency) as tx:
            await async_tx_cache.put(i, f'test-{i}')
            if i % 2 == 0:
                await tx.commit()
            else:
                await tx.rollback()

    await asyncio.gather(*[update(i) for i in range(20)], return_exceptions=True)
    assert await async_tx_cache.get_all(list(range(20))) == {i: f'test-{i}' for i in range(20) if i % 2 == 0}


@pytest.mark.parametrize(
    "params",
    [
        {'isolation': 25},
        {'concurrency': 45},
        {'timeout': 2.0},
        {'timeout': -10},
        {'label': 100500}
    ]
)
def test_tx_parameter_validation(params):
    with pytest.raises((TypeError, ValueError)):
        Transaction(None, **params)

    with pytest.raises((TypeError, ValueError)):
        AioTransaction(None, **params)
