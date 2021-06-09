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

import ctypes
from io import SEEK_CUR

import attr

from pyignite.api.result import APIResult
from pyignite.connection import Connection, AioConnection
from pyignite.constants import MAX_LONG, RHF_TOPOLOGY_CHANGED
from pyignite.queries.response import Response
from pyignite.stream import AioBinaryStream, BinaryStream, READ_BACKWARD


def query_perform(query_struct, conn, post_process_fun=None, **kwargs):
    async def _async_internal():
        result = await query_struct.perform_async(conn, **kwargs)
        if post_process_fun:
            return post_process_fun(result)
        return result

    def _internal():
        result = query_struct.perform(conn, **kwargs)
        if post_process_fun:
            return post_process_fun(result)
        return result

    if isinstance(conn, AioConnection):
        return _async_internal()
    return _internal()


_QUERY_COUNTER = 0


def _get_query_id():
    global _QUERY_COUNTER
    if _QUERY_COUNTER >= MAX_LONG:
        return 0
    _QUERY_COUNTER += 1
    return _QUERY_COUNTER


@attr.s
class Query:
    op_code = attr.ib(type=int)
    following = attr.ib(type=list, factory=list)
    query_id = attr.ib(type=int)
    response_type = attr.ib(type=type(Response), default=Response)
    _query_c_type = None

    @query_id.default
    def _set_query_id(self):
        return _get_query_id()

    @classmethod
    def build_c_type(cls):
        if cls._query_c_type is None:
            cls._query_c_type = type(
                cls.__name__,
                (ctypes.LittleEndianStructure,),
                {
                    '_pack_': 1,
                    '_fields_': [
                        ('length', ctypes.c_int),
                        ('op_code', ctypes.c_short),
                        ('query_id', ctypes.c_longlong),
                    ],
                },
            )
        return cls._query_c_type

    def from_python(self, stream, values: dict = None):
        init_pos, header = stream.tell(), self._build_header(stream)
        values = values if values else None

        for name, c_type in self.following:
            c_type.from_python(stream, values[name])

        self.__write_header(stream, header, init_pos)

    async def from_python_async(self, stream, values: dict = None):
        init_pos, header = stream.tell(), self._build_header(stream)
        values = values if values else None

        for name, c_type in self.following:
            await c_type.from_python_async(stream, values[name])

        self.__write_header(stream, header, init_pos)

    def _build_header(self, stream):
        global _QUERY_COUNTER
        header_class = self.build_c_type()
        header_len = ctypes.sizeof(header_class)
        stream.seek(header_len, SEEK_CUR)

        header = header_class()
        header.op_code = self.op_code
        header.query_id = self.query_id

        return header

    @staticmethod
    def __write_header(stream, header, init_pos):
        header.length = stream.tell() - init_pos - ctypes.sizeof(ctypes.c_int)
        stream.seek(init_pos)
        stream.write(header)

    def perform(
        self, conn: Connection, query_params: dict = None,
        response_config: list = None, **kwargs,
    ) -> APIResult:
        """
        Perform query and process result.

        :param conn: connection to Ignite server,
        :param query_params: (optional) dict of named query parameters.
         Defaults to no parameters,
        :param response_config: (optional) response configuration − list of
         (name, type_hint) tuples. Defaults to empty return value,
        :return: instance of :class:`~pyignite.api.result.APIResult` with raw
         value (may undergo further processing in API functions).
        """
        with BinaryStream(conn.client) as stream:
            self.from_python(stream, query_params)
            response_data = conn.request(stream.getvalue())

        response_struct = self.response_type(protocol_context=conn.protocol_context,
                                             following=response_config, **kwargs)

        with BinaryStream(conn.client, response_data) as stream:
            response_ctype = response_struct.parse(stream)
            response = stream.read_ctype(response_ctype, direction=READ_BACKWARD)

        result = self.__post_process_response(conn, response_struct, response)

        if result.status == 0:
            result.value = response_struct.to_python(response)
        return result

    async def perform_async(
        self, conn: AioConnection, query_params: dict = None,
        response_config: list = None, **kwargs,
    ) -> APIResult:
        """
        Perform query and process result.

        :param conn: connection to Ignite server,
        :param query_params: (optional) dict of named query parameters.
         Defaults to no parameters,
        :param response_config: (optional) response configuration − list of
         (name, type_hint) tuples. Defaults to empty return value,
        :return: instance of :class:`~pyignite.api.result.APIResult` with raw
         value (may undergo further processing in API functions).
        """
        with AioBinaryStream(conn.client) as stream:
            await self.from_python_async(stream, query_params)
            data = await conn.request(self.query_id, stream.getvalue())

        response_struct = self.response_type(protocol_context=conn.protocol_context,
                                             following=response_config, **kwargs)

        with AioBinaryStream(conn.client, data) as stream:
            response_ctype = await response_struct.parse_async(stream)
            response = stream.read_ctype(response_ctype, direction=READ_BACKWARD)

        result = self.__post_process_response(conn, response_struct, response)

        if result.status == 0:
            result.value = await response_struct.to_python_async(response)
        return result

    @staticmethod
    def __post_process_response(conn, response_struct, response):
        if getattr(response, 'flags', False) & RHF_TOPOLOGY_CHANGED:
            # update latest affinity version
            new_affinity = (response.affinity_version, response.affinity_minor)
            old_affinity = conn.client.affinity_version

            if new_affinity > old_affinity:
                conn.client.affinity_version = new_affinity

        # build result
        return APIResult(response)


class ConfigQuery(Query):
    """
    This is a special query, used for creating caches with configuration.
    """
    _query_c_type = None

    @classmethod
    def build_c_type(cls):
        if cls._query_c_type is None:
            cls._query_c_type = type(
                cls.__name__,
                (ctypes.LittleEndianStructure,),
                {
                    '_pack_': 1,
                    '_fields_': [
                        ('length', ctypes.c_int),
                        ('op_code', ctypes.c_short),
                        ('query_id', ctypes.c_longlong),
                        ('config_length', ctypes.c_int),
                    ],
                },
            )
        return cls._query_c_type

    def _build_header(self, stream):
        header = super()._build_header(stream)
        header.config_length = header.length - ctypes.sizeof(type(header))
        return header
