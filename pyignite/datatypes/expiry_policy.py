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
from typing import Union

import attr

from pyignite.constants import PROTOCOL_BYTE_ORDER


def _positive(_, attrib, value):
    if value < 0 and value not in [ExpiryPolicy.UNCHANGED, ExpiryPolicy.ETERNAL]:
        raise ValueError(f"'{attrib.name}' value must not be negative")


def _write_duration(stream, value):
    if isinstance(value, float):
        value = int(value * 1000)

    stream.write(value.to_bytes(8, byteorder=PROTOCOL_BYTE_ORDER, signed=True))


@attr.s
class ExpiryPolicy:
    UNCHANGED = -2
    ETERNAL = -1

    create = attr.ib(kw_only=True, default=UNCHANGED,
                     validator=[attr.validators.instance_of((int, float)), _positive])
    update = attr.ib(kw_only=True, default=UNCHANGED, type=Union[int, float],
                     validator=[attr.validators.instance_of((int, float)), _positive])
    access = attr.ib(kw_only=True, default=UNCHANGED, type=Union[int, float],
                     validator=[attr.validators.instance_of((int, float)), _positive])

    class CType(ctypes.LittleEndianStructure):
        _pack_ = 1
        _fields_ = [
            ('not_null', ctypes.c_byte),
            ('create', ctypes.c_longlong),
            ('update', ctypes.c_longlong),
            ('access', ctypes.c_longlong)
        ]

    @classmethod
    def parse(cls, stream):
        init = stream.tell()
        not_null = int.from_bytes(stream.slice(init, 1), byteorder=PROTOCOL_BYTE_ORDER)
        if not_null:
            stream.seek(ctypes.sizeof(ExpiryPolicy.CType), SEEK_CUR)
            return ExpiryPolicy.CType
        stream.seek(ctypes.sizeof(ctypes.c_byte), SEEK_CUR)
        return ctypes.c_byte

    @classmethod
    async def parse_async(cls, stream):
        return cls.parse(stream)

    @classmethod
    def to_python(cls, ctypes_object):
        if ctypes_object == 0:
            return None

        return ExpiryPolicy(create=ctypes_object.create, update=ctypes_object.update, access=ctypes_object.access)

    @classmethod
    async def to_python_async(cls, ctypes_object):
        return cls.to_python(ctypes_object)

    @classmethod
    def from_python(cls, stream, value):
        if not value:
            stream.write(b'\x00')
            return

        stream.write(b'\x01')
        cls.write_policy(stream, value)

    @classmethod
    async def from_python_async(cls, stream, value):
        return cls.from_python(stream, value)

    @classmethod
    def write_policy(cls, stream, value):
        _write_duration(stream, value.create)
        _write_duration(stream, value.update)
        _write_duration(stream, value.access)
