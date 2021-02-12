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

import attr
import ctypes
from random import randint

from pyignite.api.result import APIResult
from pyignite.connection import Connection
from pyignite.constants import MIN_LONG, MAX_LONG, RHF_TOPOLOGY_CHANGED
from pyignite.queries.response import Response, SQLResponse
from pyignite.stream import BinaryStream, READ_BACKWARD


@attr.s
class Query:
    op_code = attr.ib(type=int)
    following = attr.ib(type=list, factory=list)
    query_id = attr.ib(type=int, default=None)
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
                    ],
                },
            )
        return cls._query_c_type

    def _build_header(self, stream, values: dict):
        header_class = self.build_c_type()
        header_len = ctypes.sizeof(header_class)
        init_pos = stream.tell()
        stream.seek(init_pos + header_len)

        header = header_class()
        header.op_code = self.op_code
        if self.query_id is None:
            header.query_id = randint(MIN_LONG, MAX_LONG)

        for name, c_type in self.following:
            c_type.from_python(stream, values[name])

        header.length = stream.tell() - init_pos - ctypes.sizeof(ctypes.c_int)
        stream.seek(init_pos)

        return header

    def from_python(self, stream, values: dict = None):
        header = self._build_header(stream, values if values else {})
        stream.write(header)

    def perform(
        self, conn: Connection, query_params: dict = None,
        response_config: list = None, sql: bool = False, **kwargs,
    ) -> APIResult:
        """
        Perform query and process result.

        :param conn: connection to Ignite server,
        :param query_params: (optional) dict of named query parameters.
         Defaults to no parameters,
        :param response_config: (optional) response configuration − list of
         (name, type_hint) tuples. Defaults to empty return value,
        :param sql: (optional) use normal (default) or SQL response class,
        :return: instance of :class:`~pyignite.api.result.APIResult` with raw
         value (may undergo further processing in API functions).
        """
        with BinaryStream(conn) as stream:
            self.from_python(stream, query_params)
            conn.send(stream.getbuffer())

        if sql:
            response_struct = SQLResponse(protocol_version=conn.get_protocol_version(),
                                          following=response_config, **kwargs)
        else:
            response_struct = Response(protocol_version=conn.get_protocol_version(),
                                       following=response_config)

        with BinaryStream(conn, conn.recv()) as stream:
            response_ctype = response_struct.parse(stream)
            response = stream.read_ctype(response_ctype, direction=READ_BACKWARD)

        # this test depends on protocol version
        if getattr(response, 'flags', False) & RHF_TOPOLOGY_CHANGED:
            # update latest affinity version
            new_affinity = (response.affinity_version, response.affinity_minor)
            old_affinity = conn.client.affinity_version

            if new_affinity > old_affinity:
                conn.client.affinity_version = new_affinity

        # build result
        result = APIResult(response)
        if result.status == 0:
            result.value = response_struct.to_python(response)
        return result


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

    def _build_header(self, stream, values: dict):
        header = super()._build_header(stream, values)
        header.config_length = header.length - ctypes.sizeof(type(header))
        return header
