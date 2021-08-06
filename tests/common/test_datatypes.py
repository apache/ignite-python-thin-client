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
from datetime import datetime, timedelta
import decimal
import pytest
import uuid

from pyignite import GenericObjectMeta
from pyignite.datatypes import (
    ByteObject, IntObject, FloatObject, CharObject, ShortObject, BoolObject, ByteArrayObject, IntArrayObject,
    ShortArrayObject, FloatArrayObject, BoolArrayObject, CharArrayObject, TimestampObject, String, BinaryEnumObject,
    TimestampArrayObject, BinaryEnumArrayObject, ObjectArrayObject, CollectionObject, MapObject
)
from pyignite.utils import unsigned


class Value(
    metaclass=GenericObjectMeta,
    schema={
        'id': IntObject,
        'name': String,
    }
):
    pass


put_get_data_params = [
    # integers
    (42, None),
    (42, ByteObject),
    (42, ShortObject),
    (42, IntObject),

    # floats
    (3.1415, None),  # True for Double but not Float
    (3.5, FloatObject),

    # char is never autodetected
    ('ы', CharObject),
    ('カ', CharObject),

    # bool
    (True, None),
    (False, None),
    (True, BoolObject),
    (False, BoolObject),

    # arrays of integers
    ([1, 2, 3, 5], None),
    (b'buzz', None),
    (b'buzz', ByteArrayObject),
    (bytearray([7, 8, 8, 11]), None),
    (bytearray([7, 8, 8, 11]), ByteArrayObject),
    ([1, 2, 3, 5], ShortArrayObject),
    ([1, 2, 3, 5], IntArrayObject),

    # arrays of floats
    ([2.2, 4.4, 6.6], None),
    ([2.5, 6.5], FloatArrayObject),

    # array of char
    (['ы', 'カ'], CharArrayObject),

    # array of bool
    ([True, False, True], None),
    ([True, False], BoolArrayObject),
    ([False, True], BoolArrayObject),
    ([True, False, True, False], BoolArrayObject),

    # string
    ('Little Mary had a lamb', None),
    ('This is a test', String),

    # decimals
    (decimal.Decimal('2.5'), None),
    (decimal.Decimal('-1.3'), None),

    # uuid
    (uuid.uuid4(), None),

    # date
    (datetime(year=1998, month=4, day=6, hour=18, minute=30), None),

    # no autodetection for timestamp either
    (
        (datetime(year=1998, month=4, day=6, hour=18, minute=30), 1000),
        TimestampObject
    ),

    # time
    (timedelta(days=4, hours=4, minutes=24), None),

    # enum is useless in Python, except for interoperability with Java.
    # Also no autodetection
    ((5, 6), BinaryEnumObject),

    # arrays of standard types
    (['String 1', 'String 2'], None),
    (['Some of us are empty', None, 'But not the others'], None),

    ([decimal.Decimal('2.71828'), decimal.Decimal('100')], None),
    ([decimal.Decimal('2.1'), None, decimal.Decimal('3.1415')], None),

    ([uuid.uuid4(), uuid.uuid4()], None),
    (
        [
            datetime(year=2010, month=1, day=1),
            datetime(year=2010, month=12, day=31),
        ],
        None,
    ),
    ([timedelta(minutes=30), timedelta(hours=2)], None),
    (
        [
            (datetime(year=2010, month=1, day=1), 1000),
            (datetime(year=2010, month=12, day=31), 200),
        ],
        TimestampArrayObject
    ),
    ((-1, [(6001, 1), (6002, 2), (6003, 3)]), BinaryEnumArrayObject),

    # object array
    ((ObjectArrayObject.OBJECT, [1, 2, decimal.Decimal('3'), bytearray(b'\x10\x20')]), ObjectArrayObject),
    ((ObjectArrayObject.OBJECT, [Value(id=i, name=f'val_{i}') for i in range(10)]), ObjectArrayObject),

    # collection
    ((CollectionObject.LINKED_LIST, [1, 2, 3]), None),

    # map
    ((MapObject.HASH_MAP, {'key': 4, 5: 6.0}), None),
    ((MapObject.LINKED_HASH_MAP, OrderedDict([('key', 4), (5, 6.0)])), None),
]


@pytest.mark.parametrize(
    'value, value_hint',
    put_get_data_params
)
def test_put_get_data(cache, value, value_hint):
    cache.put('my_key', value, value_hint=value_hint)
    assert cache.get('my_key') == value


@pytest.mark.parametrize(
    'value, value_hint',
    put_get_data_params
)
@pytest.mark.asyncio
async def test_put_get_data_async(async_cache, value, value_hint):
    await async_cache.put('my_key', value, value_hint=value_hint)
    assert await async_cache.get('my_key') == value


nested_array_objects_params = [
    [
        (ObjectArrayObject.OBJECT, [
            ((ObjectArrayObject.OBJECT, [
                'test', 1, Value(1, 'test'),
                ((ObjectArrayObject.OBJECT, ['test', 1, Value(1, 'test')]), ObjectArrayObject)
            ]), ObjectArrayObject)
        ]),
        (ObjectArrayObject.OBJECT, [
            (ObjectArrayObject.OBJECT, ['test', 1, Value(1, 'test'),
                                        (ObjectArrayObject.OBJECT, ['test', 1, Value(1, 'test')])])
        ])
    ],
]


@pytest.mark.parametrize(
    'hinted_value, value',
    nested_array_objects_params
)
def test_put_get_nested_array_objects(cache, hinted_value, value):
    cache.put('my_key', hinted_value, value_hint=ObjectArrayObject)
    assert cache.get('my_key') == value


@pytest.mark.parametrize(
    'hinted_value, value',
    nested_array_objects_params
)
@pytest.mark.asyncio
async def test_put_get_nested_array_objects_async(async_cache, hinted_value, value):
    await async_cache.put('my_key', hinted_value, value_hint=ObjectArrayObject)
    assert await async_cache.get('my_key') == value


bytearray_params = [
    ([1, 2, 3, 5], ByteArrayObject),
    ((7, 8, 13, 18), ByteArrayObject),
    ((-128, -1, 0, 1, 127, 255), ByteArrayObject),
    (b'\x01\x03\x10', None),
    (bytearray(b'\x01\x30'), None)
]


@pytest.mark.parametrize(
    'value,type_hint',
    bytearray_params
)
def test_bytearray_from_different_input(cache, value, type_hint):
    """
    ByteArrayObject's pythonic type is `bytearray`, but it should also accept
    lists or tuples as a content.
    """
    cache.put('my_key', value, value_hint=type_hint)
    __check_bytearray_from_different_input(cache.get('my_key'), value)


@pytest.mark.parametrize(
    'value,type_hint',
    bytearray_params
)
@pytest.mark.asyncio
async def test_bytearray_from_different_input_async(async_cache, value, type_hint):
    """
    ByteArrayObject's pythonic type is `bytearray`, but it should also accept
    lists or tuples as a content.
    """
    await async_cache.put('my_key', value, value_hint=ByteArrayObject)
    __check_bytearray_from_different_input(await async_cache.get('my_key'), value)


def __check_bytearray_from_different_input(result, value):
    if isinstance(value, (bytes, bytearray)):
        assert isinstance(result, bytes)
        assert value == result
    else:
        assert result == bytearray([unsigned(ch, ctypes.c_ubyte) for ch in value])


uuid_params = [
    'd57babad-7bc1-4c82-9f9c-e72841b92a85',
    '5946c0c0-2b76-479d-8694-a2e64a3968da',
    'a521723d-ad5d-46a6-94ad-300f850ef704',
]

uuid_table_create_sql = "CREATE TABLE test_uuid_repr (id INTEGER PRIMARY KEY, uuid_field UUID)"
uuid_table_drop_sql = "DROP TABLE test_uuid_repr IF EXISTS"
uuid_table_insert_sql = "INSERT INTO test_uuid_repr(id, uuid_field) VALUES (?, ?)"
uuid_table_query_sql = "SELECT * FROM test_uuid_repr WHERE uuid_field=?"


@pytest.fixture()
async def uuid_table(client):
    client.sql(uuid_table_drop_sql)
    client.sql(uuid_table_create_sql)
    yield None
    client.sql(uuid_table_drop_sql)


@pytest.fixture()
async def uuid_table_async(async_client):
    await async_client.sql(uuid_table_drop_sql)
    await async_client.sql(uuid_table_create_sql)
    yield None
    await async_client.sql(uuid_table_drop_sql)


@pytest.mark.parametrize(
    'uuid_string',
    uuid_params
)
def test_uuid_representation(client, uuid_string, uuid_table):
    """ Test if textual UUID representation is correct. """
    uuid_value = uuid.UUID(uuid_string)

    # use uuid.UUID class to insert data
    client.sql(uuid_table_insert_sql, query_args=[1, uuid_value])
    # use hex string to retrieve data
    with client.sql(uuid_table_query_sql, query_args=[str(uuid_value)]) as cursor:
        result = list(cursor)

        # if a line was retrieved, our test was successful
        assert len(result) == 1
        assert result[0][1] == uuid_value


@pytest.mark.parametrize(
    'uuid_string',
    uuid_params
)
@pytest.mark.asyncio
async def test_uuid_representation_async(async_client, uuid_string, uuid_table_async):
    """ Test if textual UUID representation is correct. """
    uuid_value = uuid.UUID(uuid_string)

    # use uuid.UUID class to insert data
    await async_client.sql(uuid_table_insert_sql, query_args=[1, uuid_value])
    # use hex string to retrieve data
    async with async_client.sql(uuid_table_query_sql, query_args=[str(uuid_value)]) as cursor:
        result = [row async for row in cursor]

        # if a line was retrieved, our test was successful
        assert len(result) == 1
        assert result[0][1] == uuid_value
