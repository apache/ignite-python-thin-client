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
from typing import Any, Dict, Iterable, Optional, Union

from .constants import AFFINITY_RETRIES, AFFINITY_DELAY
from .connection import AioConnection
from .datatypes import prop_codes
from .datatypes.base import IgniteDataType
from .datatypes.internal import AnyDataObject
from .exceptions import CacheCreationError, CacheError, ParameterError, connection_errors
from .utils import cache_id, status_to_exception
from .api.cache_config import (
    cache_create_async, cache_get_or_create_async, cache_destroy_async, cache_get_configuration_async,
    cache_create_with_config_async, cache_get_or_create_with_config_async
)
from .api.key_value import (
    cache_get_async, cache_contains_key_async, cache_clear_key_async, cache_clear_keys_async, cache_clear_async,
    cache_replace_async, cache_put_all_async, cache_get_all_async, cache_put_async, cache_contains_keys_async,
    cache_get_and_put_async, cache_get_and_put_if_absent_async, cache_put_if_absent_async, cache_get_and_remove_async,
    cache_get_and_replace_async, cache_remove_key_async, cache_remove_keys_async, cache_remove_all_async,
    cache_remove_if_equals_async, cache_replace_if_equals_async, cache_get_size_async,
)
from .cursors import AioScanCursor
from .api.affinity import cache_get_node_partitions_async
from .cache import __parse_settings, BaseCacheMixin


async def get_cache(client: 'AioClient', settings: Union[str, dict]) -> 'AioCache':
    name, settings = __parse_settings(settings)
    if settings:
        raise ParameterError('Only cache name allowed as a parameter')

    return AioCache(client, name)


async def create_cache(client: 'AioClient', settings: Union[str, dict]) -> 'AioCache':
    name, settings = __parse_settings(settings)

    conn = await client.random_node()
    if settings:
        result = await cache_create_with_config_async(conn, settings)
    else:
        result = await cache_create_async(conn, name)

    if result.status != 0:
        raise CacheCreationError(result.message)

    return AioCache(client, name)


async def get_or_create_cache(client: 'AioClient', settings: Union[str, dict]) -> 'AioCache':
    name, settings = __parse_settings(settings)

    conn = await client.random_node()
    if settings:
        result = await cache_get_or_create_with_config_async(conn, settings)
    else:
        result = await cache_get_or_create_async(conn, name)

    if result.status != 0:
        raise CacheCreationError(result.message)

    return AioCache(client, name)


class AioCache(BaseCacheMixin):
    """
    Ignite cache abstraction. Users should never use this class directly,
    but construct its instances with
    :py:meth:`~pyignite.client.Client.create_cache`,
    :py:meth:`~pyignite.client.Client.get_or_create_cache` or
    :py:meth:`~pyignite.client.Client.get_cache` methods instead. See
    :ref:`this example <create_cache>` on how to do it.
    """
    def __init__(self, client: 'AioClient', name: str):
        """
        Initialize async cache object. For internal use.

        :param client: Async Ignite client,
        :param name: Cache name.
        """
        self._client = client
        self._name = name
        self._cache_id = cache_id(self._name)
        self._settings = None
        self._affinity_query_mux = asyncio.Lock()
        self.affinity = {'version': (0, 0)}

    async def settings(self) -> Optional[dict]:
        """
        Lazy Cache settings. See the :ref:`example <sql_cache_read>`
        of reading this property.

        All cache properties are documented here: :ref:`cache_props`.

        :return: dict of cache properties and their values.
        """
        if self._settings is None:
            conn = await self.get_best_node()
            config_result = await cache_get_configuration_async(conn, self._cache_id)

            if config_result.status == 0:
                self._settings = config_result.value
            else:
                raise CacheError(config_result.message)

        return self._settings

    async def name(self) -> str:
        """
        Lazy cache name.

        :return: cache name string.
        """
        if self._name is None:
            settings = await self.settings()
            self._name = settings[prop_codes.PROP_NAME]

        return self._name

    @property
    def client(self) -> 'AioClient':
        """
        Ignite :class:`~pyignite.aio_client.AioClient` object.

        :return: Async client object, through which the cache is accessed.
        """
        return self._client

    @property
    def cache_id(self) -> int:
        """
        Cache ID.

        :return: integer value of the cache ID.
        """
        return self._cache_id

    @status_to_exception(CacheError)
    async def destroy(self):
        """
        Destroys cache with a given name.
        """
        conn = await self.get_best_node()
        return await cache_destroy_async(conn, self._cache_id)

    @status_to_exception(CacheError)
    async def _get_affinity(self, conn: 'AioConnection') -> Dict:
        """
        Queries server for affinity mappings. Retries in case
        of an intermittent error (most probably “Getting affinity for topology
        version earlier than affinity is calculated”).

        :param conn: connection to Igneite server,
        :return: OP_CACHE_PARTITIONS operation result value.
        """
        for _ in range(AFFINITY_RETRIES or 1):
            result = await cache_get_node_partitions_async(conn, self._cache_id)
            if result.status == 0 and result.value['partition_mapping']:
                break
            await asyncio.sleep(AFFINITY_DELAY)

        return result

    async def get_best_node(self, key: Any = None, key_hint: 'IgniteDataType' = None) -> 'AioConnection':
        """
        Returns the node from the list of the nodes, opened by client, that
        most probably contains the needed key-value pair. See IEP-23.

        This method is not a part of the public API. Unless you wish to
        extend the `pyignite` capabilities (with additional testing, logging,
        examining connections, et c.) you probably should not use it.

        :param key: (optional) pythonic key,
        :param key_hint: (optional) Ignite data type, for which the given key
         should be converted,
        :return: Ignite connection object.
        """
        conn = await self._client.random_node()

        if self.client.partition_aware and key is not None:
            if self.__should_update_mapping():
                async with self._affinity_query_mux:
                    while self.__should_update_mapping():
                        try:
                            full_affinity = await self._get_affinity(conn)
                            self._update_affinity(full_affinity)

                            asyncio.ensure_future(
                                asyncio.gather(
                                    *[conn.reconnect() for conn in self.client._nodes if not conn.alive],
                                    return_exceptions=True
                                )
                            )

                            break
                        except connection_errors:
                            # retry if connection failed
                            conn = await self._client.random_node()
                            pass
                        except CacheError:
                            # server did not create mapping in time
                            return conn

            parts = self.affinity.get('number_of_partitions')

            if not parts:
                return conn

            key, key_hint = self._get_affinity_key(key, key_hint)

            hashcode = await key_hint.hashcode_async(key, self._client)

            best_node = self._get_node_by_hashcode(hashcode, parts)
            if best_node:
                return best_node

        return conn

    def __should_update_mapping(self):
        return self.affinity['version'] < self._client.affinity_version

    @status_to_exception(CacheError)
    async def get(self, key, key_hint: object = None) -> Any:
        """
        Retrieves a value from cache by key.

        :param key: key for the cache entry. Can be of any supported type,
        :param key_hint: (optional) Ignite data type, for which the given key
         should be converted,
        :return: value retrieved.
        """
        if key_hint is None:
            key_hint = AnyDataObject.map_python_type(key)

        conn = await self.get_best_node(key, key_hint)
        result = await cache_get_async(conn, self._cache_id, key, key_hint=key_hint)
        result.value = await self.client.unwrap_binary(result.value)
        return result

    @status_to_exception(CacheError)
    async def put(self, key, value, key_hint: object = None, value_hint: object = None):
        """
        Puts a value with a given key to cache (overwriting existing value
        if any).

        :param key: key for the cache entry. Can be of any supported type,
        :param value: value for the key,
        :param key_hint: (optional) Ignite data type, for which the given key
         should be converted,
        :param value_hint: (optional) Ignite data type, for which the given
         value should be converted.
        """
        if key_hint is None:
            key_hint = AnyDataObject.map_python_type(key)

        conn = await self.get_best_node(key, key_hint)
        return await cache_put_async(conn, self._cache_id, key, value, key_hint=key_hint, value_hint=value_hint)

    @status_to_exception(CacheError)
    async def get_all(self, keys: list) -> list:
        """
        Retrieves multiple key-value pairs from cache.

        :param keys: list of keys or tuples of (key, key_hint),
        :return: a dict of key-value pairs.
        """
        conn = await self.get_best_node()
        result = await cache_get_all_async(conn, self._cache_id, keys)
        if result.value:
            keys = list(result.value.keys())
            values = await asyncio.gather(*[self.client.unwrap_binary(value) for value in result.value.values()])

            for i, key in enumerate(keys):
                result.value[key] = values[i]
        return result

    @status_to_exception(CacheError)
    async def put_all(self, pairs: dict):
        """
        Puts multiple key-value pairs to cache (overwriting existing
        associations if any).

        :param pairs: dictionary type parameters, contains key-value pairs
         to save. Each key or value can be an item of representable
         Python type or a tuple of (item, hint),
        """
        conn = await self.get_best_node()
        return await cache_put_all_async(conn, self._cache_id, pairs)

    @status_to_exception(CacheError)
    async def replace(self, key, value, key_hint: object = None, value_hint: object = None):
        """
        Puts a value with a given key to cache only if the key already exist.

        :param key: key for the cache entry. Can be of any supported type,
        :param value: value for the key,
        :param key_hint: (optional) Ignite data type, for which the given key
         should be converted,
        :param value_hint: (optional) Ignite data type, for which the given
         value should be converted.
        """
        if key_hint is None:
            key_hint = AnyDataObject.map_python_type(key)

        conn = await self.get_best_node(key, key_hint)
        result = await cache_replace_async(conn, self._cache_id, key, value, key_hint=key_hint, value_hint=value_hint)
        result.value = await self.client.unwrap_binary(result.value)
        return result

    @status_to_exception(CacheError)
    async def clear(self, keys: Optional[list] = None):
        """
        Clears the cache without notifying listeners or cache writers.

        :param keys: (optional) list of cache keys or (key, key type
         hint) tuples to clear (default: clear all).
        """
        conn = await self.get_best_node()
        if keys:
            return await cache_clear_keys_async(conn, self._cache_id, keys)
        else:
            return await cache_clear_async(conn, self._cache_id)

    @status_to_exception(CacheError)
    async def clear_key(self, key, key_hint: object = None):
        """
        Clears the cache key without notifying listeners or cache writers.

        :param key: key for the cache entry,
        :param key_hint: (optional) Ignite data type, for which the given key
         should be converted,
        """
        if key_hint is None:
            key_hint = AnyDataObject.map_python_type(key)

        conn = await self.get_best_node(key, key_hint)
        return await cache_clear_key_async(conn, self._cache_id, key, key_hint=key_hint)

    @status_to_exception(CacheError)
    async def clear_keys(self, keys: Iterable):
        """
        Clears the cache key without notifying listeners or cache writers.

        :param keys: a list of keys or (key, type hint) tuples
        """
        conn = await self.get_best_node()
        return await cache_clear_keys_async(conn, self._cache_id, keys)

    @status_to_exception(CacheError)
    async def contains_key(self, key, key_hint=None) -> bool:
        """
        Returns a value indicating whether given key is present in cache.

        :param key: key for the cache entry. Can be of any supported type,
        :param key_hint: (optional) Ignite data type, for which the given key
         should be converted,
        :return: boolean `True` when key is present, `False` otherwise.
        """
        if key_hint is None:
            key_hint = AnyDataObject.map_python_type(key)

        conn = await self.get_best_node(key, key_hint)
        return await cache_contains_key_async(conn, self._cache_id, key, key_hint=key_hint)

    @status_to_exception(CacheError)
    async def contains_keys(self, keys: Iterable) -> bool:
        """
        Returns a value indicating whether all given keys are present in cache.

        :param keys: a list of keys or (key, type hint) tuples,
        :return: boolean `True` when all keys are present, `False` otherwise.
        """
        conn = await self.get_best_node()
        return await cache_contains_keys_async(conn, self._cache_id, keys)

    @status_to_exception(CacheError)
    async def get_and_put(self, key, value, key_hint=None, value_hint=None) -> Any:
        """
        Puts a value with a given key to cache, and returns the previous value
        for that key, or null value if there was not such key.

        :param key: key for the cache entry. Can be of any supported type,
        :param value: value for the key,
        :param key_hint: (optional) Ignite data type, for which the given key
         should be converted,
        :param value_hint: (optional) Ignite data type, for which the given
         value should be converted.
        :return: old value or None.
        """
        if key_hint is None:
            key_hint = AnyDataObject.map_python_type(key)

        conn = await self.get_best_node(key, key_hint)
        result = await cache_get_and_put_async(conn, self._cache_id, key, value, key_hint, value_hint)

        result.value = await self.client.unwrap_binary(result.value)
        return result

    @status_to_exception(CacheError)
    async def get_and_put_if_absent(self, key, value, key_hint=None, value_hint=None):
        """
        Puts a value with a given key to cache only if the key does not
        already exist.

        :param key: key for the cache entry. Can be of any supported type,
        :param value: value for the key,
        :param key_hint: (optional) Ignite data type, for which the given key
         should be converted,
        :param value_hint: (optional) Ignite data type, for which the given
         value should be converted,
        :return: old value or None.
        """
        if key_hint is None:
            key_hint = AnyDataObject.map_python_type(key)

        conn = await self.get_best_node(key, key_hint)
        result = await cache_get_and_put_if_absent_async(conn, self._cache_id, key, value, key_hint, value_hint)
        result.value = await self.client.unwrap_binary(result.value)
        return result

    @status_to_exception(CacheError)
    async def put_if_absent(self, key, value, key_hint=None, value_hint=None):
        """
        Puts a value with a given key to cache only if the key does not
        already exist.

        :param key: key for the cache entry. Can be of any supported type,
        :param value: value for the key,
        :param key_hint: (optional) Ignite data type, for which the given key
         should be converted,
        :param value_hint: (optional) Ignite data type, for which the given
         value should be converted.
        """
        if key_hint is None:
            key_hint = AnyDataObject.map_python_type(key)

        conn = await self.get_best_node(key, key_hint)
        return await cache_put_if_absent_async(conn, self._cache_id, key, value, key_hint, value_hint)

    @status_to_exception(CacheError)
    async def get_and_remove(self, key, key_hint=None) -> Any:
        """
        Removes the cache entry with specified key, returning the value.

        :param key: key for the cache entry. Can be of any supported type,
        :param key_hint: (optional) Ignite data type, for which the given key
         should be converted,
        :return: old value or None.
        """
        if key_hint is None:
            key_hint = AnyDataObject.map_python_type(key)

        conn = await self.get_best_node(key, key_hint)
        result = await cache_get_and_remove_async(conn, self._cache_id, key, key_hint)
        result.value = await self.client.unwrap_binary(result.value)
        return result

    @status_to_exception(CacheError)
    async def get_and_replace(self, key, value, key_hint=None, value_hint=None) -> Any:
        """
        Puts a value with a given key to cache, returning previous value
        for that key, if and only if there is a value currently mapped
        for that key.

        :param key: key for the cache entry. Can be of any supported type,
        :param value: value for the key,
        :param key_hint: (optional) Ignite data type, for which the given key
         should be converted,
        :param value_hint: (optional) Ignite data type, for which the given
         value should be converted.
        :return: old value or None.
        """
        if key_hint is None:
            key_hint = AnyDataObject.map_python_type(key)

        conn = await self.get_best_node(key, key_hint)
        result = await cache_get_and_replace_async(conn, self._cache_id, key, value, key_hint, value_hint)
        result.value = await self.client.unwrap_binary(result.value)
        return result

    @status_to_exception(CacheError)
    async def remove_key(self, key, key_hint=None):
        """
        Clears the cache key without notifying listeners or cache writers.

        :param key: key for the cache entry,
        :param key_hint: (optional) Ignite data type, for which the given key
         should be converted,
        """
        if key_hint is None:
            key_hint = AnyDataObject.map_python_type(key)

        conn = await self.get_best_node(key, key_hint)
        return await cache_remove_key_async(conn, self._cache_id, key, key_hint)

    @status_to_exception(CacheError)
    async def remove_keys(self, keys: list):
        """
        Removes cache entries by given list of keys, notifying listeners
        and cache writers.

        :param keys: list of keys or tuples of (key, key_hint) to remove.
        """
        conn = await self.get_best_node()
        return await cache_remove_keys_async(conn, self._cache_id, keys)

    @status_to_exception(CacheError)
    async def remove_all(self):
        """
        Removes all cache entries, notifying listeners and cache writers.
        """
        conn = await self.get_best_node()
        return await cache_remove_all_async(conn, self._cache_id)

    @status_to_exception(CacheError)
    async def remove_if_equals(self, key, sample, key_hint=None, sample_hint=None):
        """
        Removes an entry with a given key if provided value is equal to
        actual value, notifying listeners and cache writers.

        :param key:  key for the cache entry,
        :param sample: a sample to compare the stored value with,
        :param key_hint: (optional) Ignite data type, for which the given key
         should be converted,
        :param sample_hint: (optional) Ignite data type, for whic
         the given sample should be converted.
        """
        if key_hint is None:
            key_hint = AnyDataObject.map_python_type(key)

        conn = await self.get_best_node(key, key_hint)
        return await cache_remove_if_equals_async(conn, self._cache_id, key, sample, key_hint, sample_hint)

    @status_to_exception(CacheError)
    async def replace_if_equals(self, key, sample, value, key_hint=None, sample_hint=None, value_hint=None) -> Any:
        """
        Puts a value with a given key to cache only if the key already exists
        and value equals provided sample.

        :param key:  key for the cache entry,
        :param sample: a sample to compare the stored value with,
        :param value: new value for the given key,
        :param key_hint: (optional) Ignite data type, for which the given key
         should be converted,
        :param sample_hint: (optional) Ignite data type, for whic
         the given sample should be converted
        :param value_hint: (optional) Ignite data type, for which the given
         value should be converted,
        :return: boolean `True` when key is present, `False` otherwise.
        """
        if key_hint is None:
            key_hint = AnyDataObject.map_python_type(key)

        conn = await self.get_best_node(key, key_hint)
        result = await cache_replace_if_equals_async(conn, self._cache_id, key, sample, value, key_hint, sample_hint,
                                                     value_hint)
        result.value = await self.client.unwrap_binary(result.value)
        return result

    @status_to_exception(CacheError)
    async def get_size(self, peek_modes=None):
        """
        Gets the number of entries in cache.

        :param peek_modes: (optional) limit count to near cache partition
         (PeekModes.NEAR), primary cache (PeekModes.PRIMARY), or backup cache
         (PeekModes.BACKUP). Defaults to primary cache partitions (PeekModes.PRIMARY),
        :return: integer number of cache entries.
        """
        conn = await self.get_best_node()
        return await cache_get_size_async(conn, self._cache_id, peek_modes)

    def scan(self, page_size: int = 1, partitions: int = -1, local: bool = False):
        """
        Returns all key-value pairs from the cache, similar to `get_all`, but
        with internal pagination, which is slower, but safer.

        :param page_size: (optional) page size. Default size is 1 (slowest
         and safest),
        :param partitions: (optional) number of partitions to query
         (negative to query entire cache),
        :param local: (optional) pass True if this query should be executed
         on local node only. Defaults to False,
        :return: async scan query cursor
        """
        return AioScanCursor(self.client, self._cache_id, page_size, partitions, local)
