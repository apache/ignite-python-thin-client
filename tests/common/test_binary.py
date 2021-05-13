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
import re
from collections import OrderedDict
from decimal import Decimal

import pytest

from pyignite import GenericObjectMeta
from pyignite.aio_cache import AioCache
from pyignite.datatypes import (
    BinaryObject, BoolObject, IntObject, DecimalObject, LongObject, String, ByteObject, ShortObject, FloatObject,
    DoubleObject, CharObject, UUIDObject, DateObject, TimestampObject, TimeObject, EnumObject, BinaryEnumObject,
    ByteArrayObject, ShortArrayObject, IntArrayObject, LongArrayObject, FloatArrayObject, DoubleArrayObject,
    CharArrayObject, BoolArrayObject, UUIDArrayObject, DateArrayObject, TimestampArrayObject, TimeArrayObject,
    EnumArrayObject, StringArrayObject, DecimalArrayObject, ObjectArrayObject, CollectionObject, MapObject)
from pyignite.datatypes.prop_codes import PROP_NAME, PROP_SQL_SCHEMA, PROP_QUERY_ENTITIES

insert_data = [
    [1, True, 'asdf', 42, Decimal('2.4')],
    [2, False, 'zxcvb', 43, Decimal('2.5')],
    [3, True, 'qwerty', 44, Decimal('2.6')],
]

page_size = 100

scheme_name = 'PUBLIC'

table_sql_name = 'AllDataType'
table_cache_name = 'SQL_{}_{}'.format(
    scheme_name,
    table_sql_name.upper(),
)

create_query = '''
CREATE TABLE {} (
  test_pk INTEGER(11) PRIMARY KEY,
  test_bool BOOLEAN,
  test_str VARCHAR(24),
  test_int INTEGER(11),
  test_decimal DECIMAL(11, 5),
)
'''.format(table_sql_name)

insert_query = '''
INSERT INTO {} (
  test_pk, test_bool, test_str, test_int, test_decimal,
) VALUES (?, ?, ?, ?, ?)'''.format(table_sql_name)

select_query = '''SELECT * FROM {}'''.format(table_sql_name)

drop_query = 'DROP TABLE {} IF EXISTS'.format(table_sql_name)


@pytest.fixture
def table_cache_read(client):
    client.sql(drop_query)
    client.sql(create_query)

    for line in insert_data:
        client.sql(insert_query, query_args=line)

    cache = client.get_cache(table_cache_name)
    yield cache
    client.sql(drop_query)


@pytest.fixture
async def table_cache_read_async(async_client):
    await async_client.sql(drop_query)
    await async_client.sql(create_query)

    for line in insert_data:
        await async_client.sql(insert_query, query_args=line)

    cache = await async_client.get_cache(table_cache_name)
    yield cache
    await async_client.sql(drop_query)


def test_sql_read_as_binary(table_cache_read):
    with table_cache_read.scan() as cursor:
        # convert Binary object fields' values to a tuple
        # to compare it with the initial data
        for key, value in cursor:
            assert key in {x[0] for x in insert_data}
            assert (value.TEST_BOOL, value.TEST_STR, value.TEST_INT, value.TEST_DECIMAL) \
                   in {tuple(x[1:]) for x in insert_data}


@pytest.mark.asyncio
async def test_sql_read_as_binary_async(table_cache_read_async):
    async with table_cache_read_async.scan() as cursor:
        # convert Binary object fields' values to a tuple
        # to compare it with the initial data
        async for key, value in cursor:
            assert key in {x[0] for x in insert_data}
            assert (value.TEST_BOOL, value.TEST_STR, value.TEST_INT, value.TEST_DECIMAL) \
                   in {tuple(x[1:]) for x in insert_data}


class AllDataType(
    metaclass=GenericObjectMeta,
    type_name=table_cache_name,
    schema=OrderedDict([
        ('TEST_BOOL', BoolObject),
        ('TEST_STR', String),
        ('TEST_INT', IntObject),
        ('TEST_DECIMAL', DecimalObject),
    ]),
):
    pass


@pytest.fixture
def table_cache_write_settings():
    return {
        PROP_NAME: table_cache_name,
        PROP_SQL_SCHEMA: scheme_name,
        PROP_QUERY_ENTITIES: [
            {
                'table_name': table_sql_name.upper(),
                'key_field_name': 'TEST_PK',
                'key_type_name': 'java.lang.Integer',
                'field_name_aliases': [],
                'query_fields': [
                    {
                        'name': 'TEST_PK',
                        'type_name': 'java.lang.Integer',
                        'is_notnull_constraint_field': True,
                    },
                    {
                        'name': 'TEST_BOOL',
                        'type_name': 'java.lang.Boolean',
                    },
                    {
                        'name': 'TEST_STR',
                        'type_name': 'java.lang.String',
                    },
                    {
                        'name': 'TEST_INT',
                        'type_name': 'java.lang.Integer',
                    },
                    {
                        'name': 'TEST_DECIMAL',
                        'type_name': 'java.math.BigDecimal',
                        'default_value': Decimal('0.00'),
                        'precision': 11,
                        'scale': 2,
                    },
                ],
                'query_indexes': [],
                'value_type_name': table_cache_name,
                'value_field_name': None,
            },
        ],
    }


@pytest.fixture
def table_cache_write(client, table_cache_write_settings):
    cache = client.get_or_create_cache(table_cache_write_settings)
    assert cache.settings, 'SQL table cache settings are empty'

    for row in insert_data:
        value = AllDataType()
        (
            value.TEST_BOOL,
            value.TEST_STR,
            value.TEST_INT,
            value.TEST_DECIMAL,
        ) = row[1:]
        cache.put(row[0], value, key_hint=IntObject)

    data = cache.scan()
    assert len(list(data)) == len(insert_data), 'Not all data was read as key-value'

    yield cache
    cache.destroy()


@pytest.fixture
async def async_table_cache_write(async_client, table_cache_write_settings):
    cache = await async_client.get_or_create_cache(table_cache_write_settings)
    assert await cache.settings(), 'SQL table cache settings are empty'

    for row in insert_data:
        value = AllDataType()
        (
            value.TEST_BOOL,
            value.TEST_STR,
            value.TEST_INT,
            value.TEST_DECIMAL,
        ) = row[1:]
        await cache.put(row[0], value, key_hint=IntObject)

    async with cache.scan() as cursor:
        data = [a async for a in cursor]
        assert len(data) == len(insert_data), 'Not all data was read as key-value'

    yield cache
    await cache.destroy()


def test_sql_write_as_binary(client, table_cache_write):
    # read rows as SQL
    data = client.sql(select_query, include_field_names=True)

    header_row = next(data)
    for field_name in AllDataType.schema.keys():
        assert field_name in header_row, 'Not all field names in header row'

    data = list(data)
    assert len(data) == len(insert_data), 'Not all data was read as SQL rows'


@pytest.mark.asyncio
async def test_sql_write_as_binary_async(async_client, async_table_cache_write):
    # read rows as SQL
    async with async_client.sql(select_query, include_field_names=True) as cursor:
        header_row = await cursor.__anext__()
        for field_name in AllDataType.schema.keys():
            assert field_name in header_row, 'Not all field names in header row'

        data = [v async for v in cursor]
        assert len(data) == len(insert_data), 'Not all data was read as SQL rows'


def test_nested_binary_objects(cache):
    __check_nested_binary_objects(cache)


@pytest.mark.asyncio
async def test_nested_binary_objects_async(async_cache):
    await __check_nested_binary_objects(async_cache)


def __check_nested_binary_objects(cache):
    class InnerType(
        metaclass=GenericObjectMeta,
        schema=OrderedDict([
            ('inner_int', LongObject),
            ('inner_str', String),
        ]),
    ):
        pass

    class OuterType(
        metaclass=GenericObjectMeta,
        schema=OrderedDict([
            ('outer_int', LongObject),
            ('nested_binary', BinaryObject),
            ('outer_str', String),
        ]),
    ):
        pass

    def prepare_obj():
        inner = InnerType(inner_int=42, inner_str='This is a test string')

        return OuterType(
            outer_int=43,
            nested_binary=inner,
            outer_str='This is another test string'
        )

    def check_obj(result):
        assert result.outer_int == 43
        assert result.outer_str == 'This is another test string'
        assert result.nested_binary.inner_int == 42
        assert result.nested_binary.inner_str == 'This is a test string'

    async def inner_async():
        await cache.put(1, prepare_obj())
        check_obj(await cache.get(1))

    def inner():
        cache.put(1, prepare_obj())
        check_obj(cache.get(1))

    return inner_async() if isinstance(cache, AioCache) else inner()


def test_add_schema_to_binary_object(cache):
    __check_add_schema_to_binary_object(cache)


@pytest.mark.asyncio
async def test_add_schema_to_binary_object_async(async_cache):
    await __check_add_schema_to_binary_object(async_cache)


def __check_add_schema_to_binary_object(cache):
    class MyBinaryType(
        metaclass=GenericObjectMeta,
        schema=OrderedDict([
            ('test_str', String),
            ('test_int', LongObject),
            ('test_bool', BoolObject),
        ]),
    ):
        pass

    def prepare_bo_v1():
        return MyBinaryType(test_str='Test string', test_int=42, test_bool=True)

    def check_bo_v1(result):
        assert result.test_str == 'Test string'
        assert result.test_int == 42
        assert result.test_bool is True

    def prepare_bo_v2():
        modified_schema = MyBinaryType.schema.copy()
        modified_schema['test_decimal'] = DecimalObject
        del modified_schema['test_bool']

        class MyBinaryTypeV2(
            metaclass=GenericObjectMeta,
            type_name='MyBinaryType',
            schema=modified_schema,
        ):
            pass

        assert MyBinaryType.type_id == MyBinaryTypeV2.type_id
        assert MyBinaryType.schema_id != MyBinaryTypeV2.schema_id

        return MyBinaryTypeV2(test_str='Another test', test_int=43, test_decimal=Decimal('2.34'))

    def check_bo_v2(result):
        assert result.test_str == 'Another test'
        assert result.test_int == 43
        assert result.test_decimal == Decimal('2.34')
        assert not hasattr(result, 'test_bool')

    async def inner_async():
        await cache.put(1, prepare_bo_v1())
        check_bo_v1(await cache.get(1))
        await cache.put(2, prepare_bo_v2())
        check_bo_v2(await cache.get(2))

    def inner():
        cache.put(1, prepare_bo_v1())
        check_bo_v1(cache.get(1))
        cache.put(2, prepare_bo_v2())
        check_bo_v2(cache.get(2))

    return inner_async() if isinstance(cache, AioCache) else inner()


def test_complex_object_names(cache):
    """
    Test the ability to work with Complex types, which names contains symbols
    not suitable for use in Python identifiers.
    """
    __check_complex_object_names(cache)


@pytest.mark.asyncio
async def test_complex_object_names_async(async_cache):
    await __check_complex_object_names(async_cache)


def __check_complex_object_names(cache):
    type_name = 'Non.Pythonic#type-name$'
    key = 'key'
    data = 'test'

    class NonPythonicallyNamedType(
        metaclass=GenericObjectMeta,
        type_name=type_name,
        schema=OrderedDict([
            ('field', String),
        ])
    ):
        pass

    def check(obj):
        assert obj.type_name == type_name, 'Complex type name mismatch'
        assert obj.field == data, 'Complex object data failure'

    async def inner_async():
        await cache.put(key, NonPythonicallyNamedType(field=data))
        check(await cache.get(key))

    def inner():
        cache.put(key, NonPythonicallyNamedType(field=data))
        check(cache.get(key))

    return inner_async() if isinstance(cache, AioCache) else inner()


class Internal(
    metaclass=GenericObjectMeta, type_name='Internal',
    schema=OrderedDict([
        ('id', IntObject),
        ('str', String)
    ])
):
    pass


class NestedObject(
    metaclass=GenericObjectMeta, type_name='NestedObject',
    schema=OrderedDict([
        ('id', IntObject),
        ('str', String),
        ('internal', BinaryObject)
    ])
):
    pass


@pytest.fixture
def complex_objects():
    fixtures = []

    obj_ascii = NestedObject()
    obj_ascii.id = 1
    obj_ascii.str = 'test_string'

    obj_ascii.internal = Internal()
    obj_ascii.internal.id = 2
    obj_ascii.internal.str = 'lorem ipsum'

    fixtures.append((obj_ascii, -1314567146))

    obj_utf8 = NestedObject()
    obj_utf8.id = 1
    obj_utf8.str = 'юникод'

    obj_utf8.internal = Internal()
    obj_utf8.internal.id = 2
    obj_utf8.internal.str = 'ユニコード'

    fixtures.append((obj_utf8, -1945378474))

    yield fixtures


def test_complex_object_hash(client, complex_objects):
    for obj, hash in complex_objects:
        assert hash == BinaryObject.hashcode(obj, client=client)


@pytest.mark.asyncio
async def test_complex_object_hash_async(async_client, complex_objects):
    for obj, hash in complex_objects:
        assert hash == await BinaryObject.hashcode_async(obj, client=async_client)


def camel_to_snake(name):
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', name).lower()


fields = {camel_to_snake(type_.__name__): type_ for type_ in [
    ByteObject, ShortObject, IntObject, LongObject, FloatObject, DoubleObject, CharObject, BoolObject, UUIDObject,
    DateObject, TimestampObject, TimeObject, EnumObject, BinaryEnumObject, ByteArrayObject, ShortArrayObject,
    IntArrayObject, LongArrayObject, FloatArrayObject, DoubleArrayObject, CharArrayObject, BoolArrayObject,
    UUIDArrayObject, DateArrayObject, TimestampArrayObject, TimeArrayObject, EnumArrayObject, String,
    StringArrayObject, DecimalObject, DecimalArrayObject, ObjectArrayObject, CollectionObject, MapObject,
    BinaryObject]}


class AllTypesObject(metaclass=GenericObjectMeta, type_name='AllTypesObject', schema=fields):
    pass


@pytest.fixture
def null_fields_object():
    res = AllTypesObject()

    for field in fields.keys():
        setattr(res, field, None)

    yield res


def test_complex_object_null_fields(cache, null_fields_object):
    """
    Test that Python client can correctly write and read binary object that
    contains null fields.
    """
    cache.put(1, null_fields_object)
    assert cache.get(1) == null_fields_object, 'Objects mismatch'


@pytest.mark.asyncio
async def test_complex_object_null_fields_async(async_cache, null_fields_object):
    """
    Test that Python client can correctly write and read binary object that
    contains null fields.
    """
    await async_cache.put(1, null_fields_object)
    assert await async_cache.get(1) == null_fields_object, 'Objects mismatch'


def test_object_with_collections_of_binary_objects(cache):
    __check_object_with_collections_of_binary_objects(cache)


@pytest.mark.asyncio
async def test_object_with_collections_of_binary_objects_async(async_cache):
    await __check_object_with_collections_of_binary_objects(async_cache)


def __check_object_with_collections_of_binary_objects(cache):
    class Container(
        metaclass=GenericObjectMeta,
        schema={
            'id': IntObject,
            'collection': CollectionObject,
            'array': ObjectArrayObject,
            'map': MapObject
        }
    ):
        pass

    class Value(
        metaclass=GenericObjectMeta,
        schema={
            'id': IntObject,
            'name': String
        }
    ):
        pass

    def fixtures():
        map_obj = (MapObject.HASH_MAP, {i: Value(i, f'val_{i}') for i in range(10)})
        col_obj = (CollectionObject.ARR_LIST, [Value(i, f'val_{i}') for i in range(10)])
        arr_obj = (ObjectArrayObject.OBJECT, [Value(i, f'val_{i}') for i in range(10)])
        return [
            Container(1, map=map_obj, collection=col_obj, array=arr_obj),
            Container(2),  # Check if collections are not set
        ]

    async def inner_async():
        for i, val in enumerate(fixtures()):
            await cache.put(i, val)
            assert await cache.get(i) == val

    def inner():
        for i, val in enumerate(fixtures()):
            cache.put(i, val)
            assert cache.get(i) == val

    return inner_async() if isinstance(cache, AioCache) else inner()
