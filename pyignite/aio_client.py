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
import random
from itertools import chain
from typing import Iterable, Type, Union, Any

from .api.binary import get_binary_type_async, put_binary_type_async
from .api.cache_config import cache_get_names_async
from .client import BaseClient
from .cursors import AioSqlFieldsCursor
from .aio_cache import AioCache, get_cache, create_cache, get_or_create_cache
from .connection import AioConnection
from .constants import IGNITE_DEFAULT_HOST, IGNITE_DEFAULT_PORT
from .datatypes import BinaryObject
from .exceptions import BinaryTypeError, CacheError, ReconnectError, connection_errors
from .stream import AioBinaryStream, READ_BACKWARD
from .utils import cache_id, entity_id, status_to_exception, is_iterable, is_wrapped


__all__ = ['AioClient']


class _ConnectionContextManager:
    def __init__(self, client, nodes):
        self.client = client
        self.nodes = nodes

    def __await__(self):
        return (yield from self.__aenter__().__await__())

    async def __aenter__(self):
        await self.client._connect(self.nodes)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.client.close()


class AioClient(BaseClient):
    """
    Asynchronous Client implementation.
    """

    def __init__(self, compact_footer: bool = None, partition_aware: bool = False, **kwargs):
        """
        Initialize client.

        :param compact_footer: (optional) use compact (True, recommended) or
         full (False) schema approach when serializing Complex objects.
         Default is to use the same approach the server is using (None).
         Apache Ignite binary protocol documentation on this topic:
         https://apacheignite.readme.io/docs/binary-client-protocol-data-format#section-schema
        :param partition_aware: (optional) try to calculate the exact data
         placement from the key before to issue the key operation to the
         server node:
         https://cwiki.apache.org/confluence/display/IGNITE/IEP-23%3A+Best+Effort+Affinity+for+thin+clients
         The feature is in experimental status, so the parameter is `False`
         by default. This will be changed later.
        """
        super().__init__(compact_footer, partition_aware, **kwargs)
        self._registry_mux = asyncio.Lock()

    def connect(self, *args):
        """
        Connect to Ignite cluster node(s).

        :param args: (optional) host(s) and port(s) to connect to.
        """
        nodes = self._process_connect_args(*args)
        return _ConnectionContextManager(self, nodes)

    async def _connect(self, nodes):
        for i, node in enumerate(nodes):
            host, port = node
            conn = AioConnection(self, host, port, **self._connection_args)

            if not self.partition_aware:
                try:
                    if self.protocol_version is None:
                        # open connection before adding to the pool
                        await conn.connect()

                        # do not try to open more nodes
                        self._current_node = i

                except connection_errors:
                    conn.failed = True

            self._nodes.append(conn)

        if self.partition_aware:
            connect_results = await asyncio.gather(
                *[conn.connect() for conn in self._nodes],
                return_exceptions=True
            )

            reconnect_coro = []
            for i, res in enumerate(connect_results):
                if isinstance(res, Exception):
                    if isinstance(res, connection_errors):
                        reconnect_coro.append(self._nodes[i].reconnect())
                    else:
                        raise res

            await asyncio.gather(*reconnect_coro, return_exceptions=True)

        if self.protocol_version is None:
            raise ReconnectError('Can not connect.')

    async def close(self):
        await asyncio.gather(*[conn.close() for conn in self._nodes], return_exceptions=True)
        self._nodes.clear()

    async def random_node(self) -> AioConnection:
        """
        Returns random usable node.

        This method is not a part of the public API. Unless you wish to
        extend the `pyignite` capabilities (with additional testing, logging,
        examining connections, et c.) you probably should not use it.
        """
        if self.partition_aware:
            # if partition awareness is used just pick a random connected node
            return await self._get_random_node()
        else:
            # if partition awareness is not used then just return the current
            # node if it's alive or the next usable node if connection with the
            # current is broken
            node = self._nodes[self._current_node]
            if node.alive:
                return node

            # close current (supposedly failed) node
            await self._nodes[self._current_node].close()

            # advance the node index
            self._current_node += 1
            if self._current_node >= len(self._nodes):
                self._current_node = 0

            # prepare the list of node indexes to try to connect to
            for i in chain(range(self._current_node, len(self._nodes)), range(self._current_node)):
                node = self._nodes[i]
                try:
                    await node.connect()
                except connection_errors:
                    pass
                else:
                    return node

            # no nodes left
            raise ReconnectError('Can not reconnect: out of nodes.')

    async def _get_random_node(self, reconnect=True):
        alive_nodes = [n for n in self._nodes if n.alive]
        if alive_nodes:
            return random.choice(alive_nodes)
        elif reconnect:
            await asyncio.gather(*[n.reconnect() for n in self._nodes], return_exceptions=True)
            return await self._get_random_node(reconnect=False)
        else:
            # cannot choose from an empty sequence
            raise ReconnectError('Can not reconnect: out of nodes.') from None

    @status_to_exception(BinaryTypeError)
    async def get_binary_type(self, binary_type: Union[str, int]) -> dict:
        """
        Gets the binary type information from the Ignite server. This is quite
        a low-level implementation of Ignite thin client protocol's
        `OP_GET_BINARY_TYPE` operation. You would probably want to use
        :py:meth:`~pyignite.client.Client.query_binary_type` instead.

        :param binary_type: binary type name or ID,
        :return: binary type description − a dict with the following fields:

         - `type_exists`: True if the type is registered, False otherwise. In
           the latter case all the following fields are omitted,
         - `type_id`: Complex object type ID,
         - `type_name`: Complex object type name,
         - `affinity_key_field`: string value or None,
         - `is_enum`: False in case of Complex object registration,
         - `schemas`: a list, containing the Complex object schemas in format:
           OrderedDict[field name: field type hint]. A schema can be empty.
        """
        conn = await self.random_node()
        result = await get_binary_type_async(conn, binary_type)
        return self._process_get_binary_type_result(result)

    @status_to_exception(BinaryTypeError)
    async def put_binary_type(self, type_name: str, affinity_key_field: str = None, is_enum=False, schema: dict = None):
        """
        Registers binary type information in cluster. Do not update binary
        registry. This is a literal implementation of Ignite thin client
        protocol's `OP_PUT_BINARY_TYPE` operation. You would probably want
        to use :py:meth:`~pyignite.client.Client.register_binary_type` instead.

        :param type_name: name of the data type being registered,
        :param affinity_key_field: (optional) name of the affinity key field,
        :param is_enum: (optional) register enum if True, binary object
         otherwise. Defaults to False,
        :param schema: (optional) when register enum, pass a dict
         of enumerated parameter names as keys and an integers as values.
         When register binary type, pass a dict of field names: field types.
         Binary type with no fields is OK.
        """
        conn = await self.random_node()
        return await put_binary_type_async(conn, type_name, affinity_key_field, is_enum, schema)

    async def register_binary_type(self, data_class: Type, affinity_key_field: str = None):
        """
        Register the given class as a representation of a certain Complex
        object type. Discards autogenerated or previously registered class.

        :param data_class: Complex object class,
        :param affinity_key_field: (optional) affinity parameter.
        """
        if not await self.query_binary_type(data_class.type_id, data_class.schema_id):
            await self.put_binary_type(data_class.type_name, affinity_key_field, schema=data_class.schema)

        self._registry[data_class.type_id][data_class.schema_id] = data_class

    async def query_binary_type(self, binary_type: Union[int, str], schema: Union[int, dict] = None):
        """
        Queries the registry of Complex object classes.

        :param binary_type: Complex object type name or ID,
        :param schema: (optional) Complex object schema or schema ID,
        :return: found dataclass or None, if `schema` parameter is provided,
         a dict of {schema ID: dataclass} format otherwise.
        """
        type_id = entity_id(binary_type)

        result = self._get_from_registry(type_id, schema)

        if not result:
            async with self._registry_mux:
                result = self._get_from_registry(type_id, schema)

                if not result:
                    type_info = await self.get_binary_type(type_id)
                    self._sync_binary_registry(type_id, type_info)
                    return self._get_from_registry(type_id, schema)

        return result

    async def unwrap_binary(self, value: Any) -> Any:
        """
        Detects and recursively unwraps Binary Object.

        :param value: anything that could be a Binary Object,
        :return: the result of the Binary Object unwrapping with all other data
         left intact.
        """
        if is_wrapped(value):
            blob, offset = value
            with AioBinaryStream(self, blob) as stream:
                data_class = await BinaryObject.parse_async(stream)
                return await BinaryObject.to_python_async(stream.read_ctype(data_class, direction=READ_BACKWARD), self)
        return value

    async def create_cache(self, settings: Union[str, dict]) -> 'AioCache':
        """
        Creates Ignite cache by name. Raises `CacheError` if such a cache is
        already exists.

        :param settings: cache name or dict of cache properties' codes
         and values. All cache properties are documented here:
         :ref:`cache_props`. See also the
         :ref:`cache creation example <sql_cache_create>`,
        :return: :class:`~pyignite.cache.Cache` object.
        """
        return await create_cache(self, settings)

    async def get_or_create_cache(self, settings: Union[str, dict]) -> 'AioCache':
        """
        Creates Ignite cache, if not exist.

        :param settings: cache name or dict of cache properties' codes
         and values. All cache properties are documented here:
         :ref:`cache_props`. See also the
         :ref:`cache creation example <sql_cache_create>`,
        :return: :class:`~pyignite.cache.Cache` object.
        """
        return await get_or_create_cache(self, settings)

    async def get_cache(self, settings: Union[str, dict]) -> 'AioCache':
        """
        Creates Cache object with a given cache name without checking it up
        on server. If such a cache does not exist, some kind of exception
        (most probably `CacheError`) may be raised later.

        :param settings: cache name or cache properties (but only `PROP_NAME`
         property is allowed),
        :return: :class:`~pyignite.cache.Cache` object.
        """
        return await get_cache(self, settings)

    @status_to_exception(CacheError)
    async def get_cache_names(self) -> list:
        """
        Gets existing cache names.

        :return: list of cache names.
        """
        conn = await self.random_node()
        return await cache_get_names_async(conn)

    def sql(
        self, query_str: str, page_size: int = 1024,
        query_args: Iterable = None, schema: str = 'PUBLIC',
        statement_type: int = 0, distributed_joins: bool = False,
        local: bool = False, replicated_only: bool = False,
        enforce_join_order: bool = False, collocated: bool = False,
        lazy: bool = False, include_field_names: bool = False,
        max_rows: int = -1, timeout: int = 0,
        cache: Union[int, str, 'AioCache'] = None
    ):
        """
        Runs an SQL query and returns its result.

        :param query_str: SQL query string,
        :param page_size: (optional) cursor page size. Default is 1024, which
         means that client makes one server call per 1024 rows,
        :param query_args: (optional) query arguments. List of values or
         (value, type hint) tuples,
        :param schema: (optional) schema for the query. Defaults to `PUBLIC`,
        :param statement_type: (optional) statement type. Can be:

         * StatementType.ALL − any type (default),
         * StatementType.SELECT − select,
         * StatementType.UPDATE − update.

        :param distributed_joins: (optional) distributed joins. Defaults
         to False,
        :param local: (optional) pass True if this query should be executed
         on local node only. Defaults to False,
        :param replicated_only: (optional) whether query contains only
         replicated tables or not. Defaults to False,
        :param enforce_join_order: (optional) enforce join order. Defaults
         to False,
        :param collocated: (optional) whether your data is co-located or not.
         Defaults to False,
        :param lazy: (optional) lazy query execution. Defaults to False,
        :param include_field_names: (optional) include field names in result.
         Defaults to False,
        :param max_rows: (optional) query-wide maximum of rows. Defaults to -1
         (all rows),
        :param timeout: (optional) non-negative timeout value in ms.
         Zero disables timeout (default),
        :param cache (optional) Name or ID of the cache to use to infer schema.
         If set, 'schema' argument is ignored,
        :return: generator with result rows as a lists. If
         `include_field_names` was set, the first row will hold field names.
        """

        c_id = cache.cache_id if isinstance(cache, AioCache) else cache_id(cache)

        if c_id != 0:
            schema = None

        return AioSqlFieldsCursor(self, c_id, query_str, page_size, query_args, schema, statement_type,
                                  distributed_joins, local, replicated_only, enforce_join_order, collocated,
                                  lazy, include_field_names, max_rows, timeout)
