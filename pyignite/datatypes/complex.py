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

from collections import OrderedDict
import ctypes
from io import SEEK_CUR
from typing import Iterable, Dict

from pyignite.constants import *
from pyignite.exceptions import ParseError
from .base import IgniteDataType
from .internal import AnyDataObject, infer_from_python
from .type_codes import *
from .type_ids import *
from .type_names import *
from .null_object import Null, Nullable

__all__ = [
    'Map', 'ObjectArrayObject', 'CollectionObject', 'MapObject',
    'WrappedDataObject', 'BinaryObject',
]

from ..stream import BinaryStream


class ObjectArrayObject(IgniteDataType, Nullable):
    """
    Array of Ignite objects of any consistent type. Its Python representation
    is tuple(type_id, iterable of any type). The only type ID that makes sense
    in Python client is :py:attr:`~OBJECT`, that corresponds directly to
    the root object type in Java type hierarchy (`java.lang.Object`).
    """
    OBJECT = -1

    _type_name = NAME_OBJ_ARR
    _type_id = TYPE_OBJ_ARR
    type_code = TC_OBJECT_ARRAY

    @staticmethod
    def hashcode(value: Iterable) -> int:
        # Arrays are not supported as keys at the moment.
        return 0

    @classmethod
    def build_header(cls):
        return type(
            cls.__name__+'Header',
            (ctypes.LittleEndianStructure,),
            {
                '_pack_': 1,
                '_fields_': [
                    ('type_code', ctypes.c_byte),
                    ('type_id', ctypes.c_int),
                    ('length', ctypes.c_int),
                ],
            }
        )

    @classmethod
    def parse_not_null(cls, stream):
        header_class = cls.build_header()
        header = stream.read_ctype(header_class)
        stream.seek(ctypes.sizeof(header_class), SEEK_CUR)

        fields = []
        for i in range(header.length):
            c_type = AnyDataObject.parse(stream)
            fields.append(('element_{}'.format(i), c_type))

        final_class = type(
            cls.__name__,
            (header_class,),
            {
                '_pack_': 1,
                '_fields_': fields,
            }
        )

        return final_class

    @classmethod
    def to_python_not_null(cls, ctype_object, *args, **kwargs):
        result = []
        for i in range(ctype_object.length):
            result.append(
                AnyDataObject.to_python(
                    getattr(ctype_object, 'element_{}'.format(i)),
                    *args, **kwargs
                )
            )
        return ctype_object.type_id, result

    @classmethod
    def from_python_not_null(cls, stream, value):
        type_or_id, value = value
        header_class = cls.build_header()
        header = header_class()
        header.type_code = int.from_bytes(
            cls.type_code,
            byteorder=PROTOCOL_BYTE_ORDER
        )
        try:
            length = len(value)
        except TypeError:
            value = [value]
            length = 1
        header.length = length
        header.type_id = type_or_id

        stream.write(header)
        for x in value:
            infer_from_python(stream, x)


class WrappedDataObject(IgniteDataType, Nullable):
    """
    One or more binary objects can be wrapped in an array. This allows reading,
    storing, passing and writing objects efficiently without understanding
    their contents, performing simple byte copy.

    Python representation: tuple(payload: bytes, offset: integer). Offset
    points to the root object of the array.
    """
    type_code = TC_ARRAY_WRAPPED_OBJECTS

    @classmethod
    def build_header(cls):
        return type(
            cls.__name__+'Header',
            (ctypes.LittleEndianStructure,),
            {
                '_pack_': 1,
                '_fields_': [
                    ('type_code', ctypes.c_byte),
                    ('length', ctypes.c_int),
                ],
            }
        )

    @classmethod
    def parse_not_null(cls, stream):
        header_class = cls.build_header()
        header = stream.read_ctype(header_class)

        final_class = type(
            cls.__name__,
            (header_class,),
            {
                '_pack_': 1,
                '_fields_': [
                    ('payload', ctypes.c_byte*header.length),
                    ('offset', ctypes.c_int),
                ],
            }
        )

        stream.seek(ctypes.sizeof(final_class), SEEK_CUR)
        return final_class

    @classmethod
    def to_python(cls, ctype_object, *args, **kwargs):
        return bytes(ctype_object.payload), ctype_object.offset

    @classmethod
    def from_python(cls, stream, value):
        raise ParseError('Send unwrapped data.')


class CollectionObject(IgniteDataType, Nullable):
    """
    Similar to object array, but contains platform-agnostic deserialization
    type hint instead of type ID.

    Represented as tuple(hint, iterable of any type) in Python. Hints are:

    * :py:attr:`~pyignite.datatypes.complex.CollectionObject.USER_SET` −
      a set of unique Ignite thin data objects. The exact Java type of a set
      is undefined,
    * :py:attr:`~pyignite.datatypes.complex.CollectionObject.USER_COL` −
      a collection of Ignite thin data objects. The exact Java type
      of a collection is undefined,
    * :py:attr:`~pyignite.datatypes.complex.CollectionObject.ARR_LIST` −
      represents the `java.util.ArrayList` type,
    * :py:attr:`~pyignite.datatypes.complex.CollectionObject.LINKED_LIST` −
      represents the `java.util.LinkedList` type,
    * :py:attr:`~pyignite.datatypes.complex.CollectionObject.HASH_SET`−
      represents the `java.util.HashSet` type,
    * :py:attr:`~pyignite.datatypes.complex.CollectionObject.LINKED_HASH_SET` −
      represents the `java.util.LinkedHashSet` type,
    * :py:attr:`~pyignite.datatypes.complex.CollectionObject.SINGLETON_LIST` −
      represents the return type of the `java.util.Collection.singletonList`
      method.

    It is safe to say that `USER_SET` (`set` in Python) and `USER_COL` (`list`)
    can cover all the imaginable use cases from Python perspective.
    """
    USER_SET = -1
    USER_COL = 0
    ARR_LIST = 1
    LINKED_LIST = 2
    HASH_SET = 3
    LINKED_HASH_SET = 4
    SINGLETON_LIST = 5

    _type_name = NAME_COL
    _type_id = TYPE_COL
    type_code = TC_COLLECTION
    pythonic = list
    default = []

    @staticmethod
    def hashcode(value: Iterable) -> int:
        # Collections are not supported as keys at the moment.
        return 0

    @classmethod
    def build_header(cls):
        return type(
            cls.__name__+'Header',
            (ctypes.LittleEndianStructure,),
            {
                '_pack_': 1,
                '_fields_': [
                    ('type_code', ctypes.c_byte),
                    ('length', ctypes.c_int),
                    ('type', ctypes.c_byte),
                ],
            }
        )

    @classmethod
    def parse_not_null(cls, stream):
        header_class = cls.build_header()
        header = stream.read_ctype(header_class)
        stream.seek(ctypes.sizeof(header_class), SEEK_CUR)

        fields = []
        for i in range(header.length):
            c_type = AnyDataObject.parse(stream)
            fields.append(('element_{}'.format(i), c_type))

        final_class = type(
            cls.__name__,
            (header_class,),
            {
                '_pack_': 1,
                '_fields_': fields,
            }
        )
        return final_class

    @classmethod
    def to_python(cls, ctype_object, *args, **kwargs):
        result = []
        length = getattr(ctype_object, "length", None)
        if length is None:
            return None
        for i in range(length):
            result.append(
                AnyDataObject.to_python(
                    getattr(ctype_object, 'element_{}'.format(i)),
                    *args, **kwargs
                )
            )
        return ctype_object.type, result

    @classmethod
    def from_python_not_null(cls, stream, value):
        type_or_id, value = value
        header_class = cls.build_header()
        header = header_class()
        header.type_code = int.from_bytes(
            cls.type_code,
            byteorder=PROTOCOL_BYTE_ORDER
        )
        try:
            length = len(value)
        except TypeError:
            value = [value]
            length = 1
        header.length = length
        header.type = type_or_id

        stream.write(header)
        for x in value:
            infer_from_python(stream, x)


class Map(IgniteDataType, Nullable):
    """
    Dictionary type, payload-only.

    Ignite does not track the order of key-value pairs in its caches, hence
    the ordinary Python dict type, not the collections.OrderedDict.
    """
    _type_name = NAME_MAP
    _type_id = TYPE_MAP
    HASH_MAP = 1
    LINKED_HASH_MAP = 2

    @staticmethod
    def hashcode(value: Dict) -> int:
        # Maps are not supported as keys at the moment.
        return 0

    @classmethod
    def build_header(cls):
        return type(
            cls.__name__+'Header',
            (ctypes.LittleEndianStructure,),
            {
                '_pack_': 1,
                '_fields_': [
                    ('length', ctypes.c_int),
                ],
            }
        )

    @classmethod
    def parse_not_null(cls, stream):
        header_class = cls.build_header()
        header = stream.read_ctype(header_class)
        stream.seek(ctypes.sizeof(header_class), SEEK_CUR)

        fields = []
        for i in range(header.length << 1):
            c_type = AnyDataObject.parse(stream)
            fields.append(('element_{}'.format(i), c_type))

        final_class = type(
            cls.__name__,
            (header_class,),
            {
                '_pack_': 1,
                '_fields_': fields,
            }
        )
        return final_class

    @classmethod
    def to_python(cls, ctype_object, *args, **kwargs):
        map_type = getattr(ctype_object, 'type', cls.HASH_MAP)
        result = OrderedDict() if map_type == cls.LINKED_HASH_MAP else {}

        for i in range(0, ctype_object.length << 1, 2):
            k = AnyDataObject.to_python(
                    getattr(ctype_object, 'element_{}'.format(i)),
                    *args, **kwargs
                )
            v = AnyDataObject.to_python(
                    getattr(ctype_object, 'element_{}'.format(i + 1)),
                    *args, **kwargs
                )
            result[k] = v
        return result

    @classmethod
    def from_python(cls, stream, value, type_id=None):
        header_class = cls.build_header()
        header = header_class()
        length = len(value)
        header.length = length
        if hasattr(header, 'type_code'):
            header.type_code = int.from_bytes(
                cls.type_code,
                byteorder=PROTOCOL_BYTE_ORDER
            )
        if hasattr(header, 'type'):
            header.type = type_id

        stream.write(header)
        for k, v in value.items():
            infer_from_python(stream, k)
            infer_from_python(stream, v)


class MapObject(Map):
    """
    This is a dictionary type.

    Represented as tuple(type_id, value).

    Type ID can be a :py:attr:`~HASH_MAP` (corresponds to an ordinary `dict`
    in Python) or a :py:attr:`~LINKED_HASH_MAP` (`collections.OrderedDict`).
    """
    _type_name = NAME_MAP
    _type_id = TYPE_MAP
    type_code = TC_MAP
    pythonic = dict
    default = {}

    @classmethod
    def build_header(cls):
        return type(
            cls.__name__+'Header',
            (ctypes.LittleEndianStructure,),
            {
                '_pack_': 1,
                '_fields_': [
                    ('type_code', ctypes.c_byte),
                    ('length', ctypes.c_int),
                    ('type', ctypes.c_byte),
                ],
            }
        )

    @classmethod
    def to_python(cls, ctype_object, *args, **kwargs):
        obj_type = getattr(ctype_object, "type", None)
        if obj_type is None:
            return None
        return obj_type, super().to_python(
            ctype_object, *args, **kwargs
        )

    @classmethod
    def from_python(cls, stream, value):
        if value is None:
            Null.from_python(stream)
            return

        type_id, value = value
        super().from_python(stream, value, type_id)


class BinaryObject(IgniteDataType, Nullable):
    _type_id = TYPE_BINARY_OBJ
    type_code = TC_COMPLEX_OBJECT

    USER_TYPE = 0x0001
    HAS_SCHEMA = 0x0002
    HAS_RAW_DATA = 0x0004
    OFFSET_ONE_BYTE = 0x0008
    OFFSET_TWO_BYTES = 0x0010
    COMPACT_FOOTER = 0x0020

    @staticmethod
    def hashcode(value: object, client: None) -> int:
        # binary objects's hashcode implementation is special in the sense
        # that you need to fully serialize the object to calculate
        # its hashcode
        if not value._hashcode and client :

            with BinaryStream(client.random_node) as stream:
                value._from_python(stream, save_to_buf=True)

        return value._hashcode

    @classmethod
    def build_header(cls):
        return type(
            cls.__name__,
            (ctypes.LittleEndianStructure,),
            {
                '_pack_': 1,
                '_fields_': [
                    ('type_code', ctypes.c_byte),
                    ('version', ctypes.c_byte),
                    ('flags', ctypes.c_short),
                    ('type_id', ctypes.c_int),
                    ('hash_code', ctypes.c_int),
                    ('length', ctypes.c_int),
                    ('schema_id', ctypes.c_int),
                    ('schema_offset', ctypes.c_int),
                ],
            }
        )

    @classmethod
    def offset_c_type(cls, flags: int):
        if flags & cls.OFFSET_ONE_BYTE:
            return ctypes.c_ubyte
        if flags & cls.OFFSET_TWO_BYTES:
            return ctypes.c_uint16
        return ctypes.c_uint

    @classmethod
    def schema_type(cls, flags: int):
        if flags & cls.COMPACT_FOOTER:
            return cls.offset_c_type(flags)
        return type(
            'SchemaElement',
            (ctypes.LittleEndianStructure,),
            {
                '_pack_': 1,
                '_fields_': [
                    ('field_id', ctypes.c_int),
                    ('offset', cls.offset_c_type(flags)),
                ],
            },
        )

    @classmethod
    def parse_not_null(cls, stream):
        from pyignite.datatypes import Struct

        header_class = cls.build_header()
        header = stream.read_ctype(header_class)
        stream.seek(ctypes.sizeof(header_class), SEEK_CUR)

        # ignore full schema, always retrieve fields' types and order
        # from complex types registry
        data_class = stream.get_dataclass(header)
        fields = data_class.schema.items()
        object_fields_struct = Struct(fields)
        object_fields = object_fields_struct.parse(stream)
        final_class_fields = [('object_fields', object_fields)]

        if header.flags & cls.HAS_SCHEMA:
            schema = cls.schema_type(header.flags) * len(fields)
            stream.seek(ctypes.sizeof(schema), SEEK_CUR)
            final_class_fields.append(('schema', schema))

        final_class = type(
            cls.__name__,
            (header_class,),
            {
                '_pack_': 1,
                '_fields_': final_class_fields,
            }
        )
        # register schema encoding approach
        stream.compact_footer = bool(header.flags & cls.COMPACT_FOOTER)
        return final_class

    @classmethod
    def to_python(cls, ctype_object, client: 'Client' = None, *args, **kwargs):
        type_id = getattr(ctype_object, "type_id", None)
        if type_id is None:
            return None

        if not client:
            raise ParseError(
                'Can not query binary type {}'.format(type_id)
            )

        data_class = client.query_binary_type(
            type_id,
            ctype_object.schema_id
        )
        result = data_class()

        result.version = ctype_object.version
        for field_name, field_type in data_class.schema.items():
            setattr(
                result, field_name, field_type.to_python(
                    getattr(ctype_object.object_fields, field_name),
                    client, *args, **kwargs
                )
            )
        return result

    @classmethod
    def from_python_not_null(cls, stream, value):
        if getattr(value, '_buffer', None):
            stream.write(value._buffer)
        else:
            stream.register_binary_type(value.__class__)
            value._from_python(stream)
