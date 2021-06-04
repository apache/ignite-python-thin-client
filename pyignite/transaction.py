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
import math
from typing import Union

from pyignite.api.tx_api import tx_end, tx_start, tx_end_async, tx_start_async
from pyignite.datatypes import TransactionIsolation, TransactionConcurrency
from pyignite.exceptions import CacheError
from pyignite.utils import status_to_exception


def _convert_to_millis(timeout: Union[int, float]) -> int:
    if isinstance(timeout, float):
        return math.floor(timeout * 1000)
    return timeout


class Transaction:
    def __init__(self, client, concurrency=TransactionConcurrency.PESSIMISTIC,
                 isolation=TransactionIsolation.REPEATABLE_READ, timeout=0, label=None):
        self.client, self.concurrency = client, concurrency
        self.isolation, self.timeout = isolation, _convert_to_millis(timeout)
        self.label, self.closed = label, False

        self.tx_id = tx_start(self.client.random_node, self.concurrency, self.isolation, self.timeout, self.label)

    @status_to_exception(CacheError)
    def commit(self):
        return tx_end(self.tx_id, True)

    def rollback(self):
        self.close()

    @status_to_exception(CacheError)
    def close(self):
        self.closed = True
        return tx_end(self.tx_id, False)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class AioTransaction:
    def __init__(self, client, concurrency=TransactionConcurrency.PESSIMISTIC,
                 isolation=TransactionIsolation.REPEATABLE_READ, timeout=0, label=None):
        self.client, self.concurrency = client, concurrency
        self.isolation, self.timeout = isolation, _convert_to_millis(timeout)
        self.label, self.closed = label, False

    def __await__(self):
        return (yield from self.__aenter__().__await__())

    async def commit(self):
        if not self.closed:
            self.closed = True
            return await self.__end_tx(True)

    async def rollback(self):
        await self.close()

    async def close(self):
        if not self.closed:
            self.closed = True
            return await self.__end_tx(False)

    async def __aenter__(self):
        self.tx_id = await self.__start_tx()
        self.closed = False
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    @status_to_exception(CacheError)
    async def __start_tx(self):
        conn = await self.client.random_node()
        return await tx_start_async(conn, self.concurrency, self.isolation, self.timeout, self.label)

    @status_to_exception(CacheError)
    async def __end_tx(self, committed):
        return await tx_end_async(self.tx_id, committed)