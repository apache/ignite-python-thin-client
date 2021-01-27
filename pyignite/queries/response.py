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
from collections import OrderedDict
import ctypes

from pyignite.constants import RHF_TOPOLOGY_CHANGED, RHF_ERROR
from pyignite.connection import Connection
from pyignite.datatypes import AnyDataObject, Bool, Int, Long, String, StringArray, Struct
from pyignite.queries.op_codes import OP_SUCCESS


@attr.s
class Response:
    following = attr.ib(type=list, factory=list)
    protocol_version = attr.ib(type=tuple, factory=tuple)
    _response_header = None

    def __attrs_post_init__(self):
        # replace None with empty list
        self.following = self.following or []

    def build_header(self):
        if self._response_header is None:
            fields = [
                ('length', ctypes.c_int),
                ('query_id', ctypes.c_longlong),
            ]

            if self.protocol_version and self.protocol_version >= (1, 4, 0):
                fields.append(('flags', ctypes.c_short))
            else:
                fields.append(('status_code', ctypes.c_int),)

            self._response_header = type(
                'ResponseHeader',
                (ctypes.LittleEndianStructure,),
                {
                    '_pack_': 1,
                    '_fields_': fields,
                },
            )
        return self._response_header

    def parse(self, conn: Connection):
        header_class = self.build_header()
        buffer = bytearray(conn.recv(ctypes.sizeof(header_class)))
        header = header_class.from_buffer_copy(buffer)
        fields = []

        has_error = False
        if self.protocol_version and self.protocol_version >= (1, 4, 0):
            if header.flags & RHF_TOPOLOGY_CHANGED:
                fields = [
                    ('affinity_version', ctypes.c_longlong),
                    ('affinity_minor', ctypes.c_int),
                ]

            if header.flags & RHF_ERROR:
                fields.append(('status_code', ctypes.c_int))
                has_error = True
        else:
            has_error = header.status_code != OP_SUCCESS

        if fields:
            buffer += conn.recv(
                sum([ctypes.sizeof(c_type) for _, c_type in fields])
            )

        if has_error:
            msg_type, buffer_fragment = String.parse(conn)
            buffer += buffer_fragment
            fields.append(('error_message', msg_type))
        else:
            self._parse_success(conn, buffer, fields)

        return self._create_parse_result(conn, header_class, fields, buffer)

    def _create_parse_result(self, conn: Connection, header_class, fields: list, buffer: bytearray):
        response_class = type(
            'Response',
            (header_class,),
            {
                '_pack_': 1,
                '_fields_': fields,
            }
        )
        return response_class, bytes(buffer)

    def _parse_success(self, conn: Connection, buffer: bytearray, fields: list):
        for name, ignite_type in self.following:
            c_type, buffer_fragment = ignite_type.parse(conn)
            buffer += buffer_fragment
            fields.append((name, c_type))

    def to_python(self, ctype_object, *args, **kwargs):
        result = OrderedDict()

        for name, c_type in self.following:
            result[name] = c_type.to_python(
                getattr(ctype_object, name),
                *args, **kwargs
            )

        return result if result else None


@attr.s
class SQLResponse(Response):
    """
    The response class of SQL functions is special in the way the row-column
    data is counted in it. Basically, Ignite thin client API is following a
    “counter right before the counted objects” rule in most of its parts.
    SQL ops are breaking this rule.
    """
    include_field_names = attr.ib(type=bool, default=False)
    has_cursor = attr.ib(type=bool, default=False)

    def fields_or_field_count(self):
        if self.include_field_names:
            return 'fields', StringArray
        return 'field_count', Int

    def _parse_success(self, conn: Connection, buffer: bytearray, fields: list):
        following = [
            self.fields_or_field_count(),
            ('row_count', Int),
        ]
        if self.has_cursor:
            following.insert(0, ('cursor', Long))
        body_struct = Struct(following)
        body_class, body_buffer = body_struct.parse(conn)
        body = body_class.from_buffer_copy(body_buffer)
        buffer += body_buffer

        if self.include_field_names:
            field_count = body.fields.length
        else:
            field_count = body.field_count

        data_fields = []
        for i in range(body.row_count):
            row_fields = []
            for j in range(field_count):
                field_class, field_buffer = AnyDataObject.parse(conn)
                row_fields.append(('column_{}'.format(j), field_class))
                buffer += field_buffer

            row_class = type(
                'SQLResponseRow',
                (ctypes.LittleEndianStructure,),
                {
                    '_pack_': 1,
                    '_fields_': row_fields,
                }
            )
            data_fields.append(('row_{}'.format(i), row_class))

        data_class = type(
            'SQLResponseData',
            (ctypes.LittleEndianStructure,),
            {
                '_pack_': 1,
                '_fields_': data_fields,
            }
        )
        fields += body_class._fields_ + [
            ('data', data_class),
            ('more', ctypes.c_bool),
        ]

    def _create_parse_result(self, conn: Connection, header_class, fields: list, buffer: bytearray):
        final_class = type(
            'SQLResponse',
            (header_class,),
            {
                '_pack_': 1,
                '_fields_': fields,
            }
        )
        buffer += conn.recv(ctypes.sizeof(final_class) - len(buffer))
        return final_class, bytes(buffer)

    def to_python(self, ctype_object, *args, **kwargs):
        if not hasattr(ctype_object, 'status_code'):
            result = {
                'more': Bool.to_python(
                    ctype_object.more, *args, **kwargs
                ),
                'data': [],
            }
            if hasattr(ctype_object, 'fields'):
                result['fields'] = StringArray.to_python(
                    ctype_object.fields, *args, **kwargs
                )
            else:
                result['field_count'] = Int.to_python(
                    ctype_object.field_count, *args, **kwargs
                )
            if hasattr(ctype_object, 'cursor'):
                result['cursor'] = Long.to_python(
                    ctype_object.cursor, *args, **kwargs
                )
            for row_item in ctype_object.data._fields_:
                row_name = row_item[0]
                row_object = getattr(ctype_object.data, row_name)
                row = []
                for col_item in row_object._fields_:
                    col_name = col_item[0]
                    col_object = getattr(row_object, col_name)
                    row.append(
                        AnyDataObject.to_python(col_object, *args, **kwargs)
                    )
                result['data'].append(row)
            return result
