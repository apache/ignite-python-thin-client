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

"""
Null object.

There can't be null type, because null payload takes exactly 0 bytes.
"""

import ctypes
from io import SEEK_CUR
from typing import Any

from .base import IgniteDataType
from .type_codes import TC_NULL


__all__ = ['Null']

from ..constants import PROTOCOL_BYTE_ORDER


class Null(IgniteDataType):
    default = None
    pythonic = type(None)
    _object_c_type = None

    @staticmethod
    def hashcode(value: Any) -> int:
        # Null object can not be a cache key.
        return 0

    @classmethod
    def build_c_type(cls):
        if cls._object_c_type is None:
            cls._object_c_type = type(
                cls.__name__,
                (ctypes.LittleEndianStructure,),
                {
                    '_pack_': 1,
                    '_fields_': [
                        ('type_code', ctypes.c_byte),
                    ],
                },
            )
        return cls._object_c_type

    @classmethod
    def parse(cls, stream):
        init_pos, offset = stream.tell(), ctypes.sizeof(ctypes.c_byte)
        stream.seek(offset, SEEK_CUR)
        return cls.build_c_type()

    @staticmethod
    def to_python(*args, **kwargs):
        return None

    @staticmethod
    def from_python(stream, *args):
        stream.write(TC_NULL)


class Nullable:
    @classmethod
    def parse_not_null(cls, stream):
        raise NotImplementedError

    @classmethod
    def parse(cls, stream):
        type_len = ctypes.sizeof(ctypes.c_byte)

        if stream.mem_view(offset=type_len) == TC_NULL:
            stream.seek(type_len, SEEK_CUR)
            return Null.build_c_type()

        return cls.parse_not_null(stream)

    @classmethod
    def to_python_not_null(cls, ctypes_object, *args, **kwargs):
        raise NotImplementedError

    @classmethod
    def to_python(cls, ctypes_object, *args, **kwargs):
        if ctypes_object.type_code == int.from_bytes(
                TC_NULL,
                byteorder=PROTOCOL_BYTE_ORDER
        ):
            return None

        return cls.to_python_not_null(ctypes_object, *args, **kwargs)

    @classmethod
    def from_python_not_null(cls, stream, value):
        raise NotImplementedError

    @classmethod
    def from_python(cls, stream, value):
        if value is None:
            Null.from_python(stream)
        else:
            cls.from_python_not_null(stream, value)
