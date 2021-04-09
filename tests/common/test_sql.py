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

import pytest

from pyignite import AioClient
from pyignite.aio_cache import AioCache
from pyignite.datatypes.cache_config import CacheMode
from pyignite.datatypes.prop_codes import PROP_NAME, PROP_SQL_SCHEMA, PROP_QUERY_ENTITIES, PROP_CACHE_MODE
from pyignite.exceptions import SQLError
from pyignite.utils import entity_id

student_table_data = [
    ('John', 'Doe', 5),
    ('Jane', 'Roe', 4),
    ('Joe', 'Bloggs', 4),
    ('Richard', 'Public', 3),
    ('Negidius', 'Numerius', 3),
]

student_table_select_query = 'SELECT id, first_name, last_name, grade FROM Student ORDER BY ID ASC'


@pytest.fixture
def student_table_fixture(client):
    yield from __create_student_table_fixture(client)


@pytest.fixture
async def async_student_table_fixture(async_client):
    async for _ in __create_student_table_fixture(async_client):
        yield


def __create_student_table_fixture(client):
    create_query = '''CREATE TABLE Student (
        id INT(11) PRIMARY KEY,
        first_name CHAR(24),
        last_name CHAR(32),
        grade INT(11))'''

    insert_query = '''INSERT INTO Student(id, first_name, last_name, grade)
    VALUES (?, ?, ?, ?)'''

    drop_query = 'DROP TABLE Student IF EXISTS'

    def inner():
        client.sql(drop_query)
        client.sql(create_query)

        for i, data_line in enumerate(student_table_data):
            fname, lname, grade = data_line
            client.sql(insert_query, query_args=[i, fname, lname, grade])

        yield None
        client.sql(drop_query)

    async def inner_async():
        await client.sql(drop_query)
        await client.sql(create_query)

        for i, data_line in enumerate(student_table_data):
            fname, lname, grade = data_line
            await client.sql(insert_query, query_args=[i, fname, lname, grade])

        yield None
        await client.sql(drop_query)

    return inner_async() if isinstance(client, AioClient) else inner()


@pytest.mark.parametrize('page_size', range(1, 6, 2))
def test_sql(client, student_table_fixture, page_size):
    cache = client.get_cache('SQL_PUBLIC_STUDENT')
    cache_config = cache.settings

    binary_type_name = cache_config[PROP_QUERY_ENTITIES][0]['value_type_name']

    with cache.select_row('ORDER BY ID ASC', page_size=4) as cursor:
        for i, row in enumerate(cursor):
            k, v = row
            assert k == i

            assert (v.FIRST_NAME, v.LAST_NAME, v.GRADE) == student_table_data[i]
            assert v.type_id == entity_id(binary_type_name)


@pytest.mark.parametrize('page_size', range(1, 6, 2))
def test_sql_fields(client, student_table_fixture, page_size):
    with client.sql(student_table_select_query, page_size=page_size, include_field_names=True) as cursor:
        for i, row in enumerate(cursor):
            if i > 0:
                assert tuple(row) == (i - 1,) + student_table_data[i - 1]
            else:
                assert row == ['ID', 'FIRST_NAME', 'LAST_NAME', 'GRADE']


@pytest.mark.asyncio
@pytest.mark.parametrize('page_size', range(1, 6, 2))
async def test_sql_fields_async(async_client, async_student_table_fixture, page_size):
    async with async_client.sql(student_table_select_query, page_size=page_size, include_field_names=True) as cursor:
        i = 0
        async for row in cursor:
            if i > 0:
                assert tuple(row) == (i - 1,) + student_table_data[i - 1]
            else:
                assert row == ['ID', 'FIRST_NAME', 'LAST_NAME', 'GRADE']
            i += 1

    cursor = await async_client.sql(student_table_select_query, page_size=page_size, include_field_names=True)
    try:
        i = 0
        async for row in cursor:
            if i > 0:
                assert tuple(row) == (i - 1,) + student_table_data[i - 1]
            else:
                assert row == ['ID', 'FIRST_NAME', 'LAST_NAME', 'GRADE']
            i += 1
    finally:
        await cursor.close()


multipage_fields = ["id", "abc", "ghi", "def", "jkl", "prs", "mno", "tuw", "zyz", "abc1", "def1", "jkl1", "prs1"]


@pytest.fixture
def long_multipage_table_fixture(client):
    yield from __long_multipage_table_fixture(client)


@pytest.fixture
async def async_long_multipage_table_fixture(async_client):
    async for _ in __long_multipage_table_fixture(async_client):
        yield


def __long_multipage_table_fixture(client):
    drop_query = 'DROP TABLE LongMultipageQuery IF EXISTS'

    create_query = "CREATE TABLE LongMultiPageQuery (%s, %s)" % (
        multipage_fields[0] + " INT(11) PRIMARY KEY", ",".join(map(lambda f: f + " INT(11)", multipage_fields[1:])))

    insert_query = "INSERT INTO LongMultipageQuery (%s) VALUES (%s)" % (
        ",".join(multipage_fields), ",".join("?" * len(multipage_fields)))

    def query_args(_id):
        return [_id] + list(i * _id for i in range(1, len(multipage_fields)))

    def inner():
        client.sql(drop_query)
        client.sql(create_query)

        for i in range(1, 21):
            client.sql(insert_query, query_args=query_args(i))
        yield None

        client.sql(drop_query)

    async def inner_async():
        await client.sql(drop_query)
        await client.sql(create_query)

        for i in range(1, 21):
            await client.sql(insert_query, query_args=query_args(i))
        yield None

        await client.sql(drop_query)

    return inner_async() if isinstance(client, AioClient) else inner()


def test_long_multipage_query(client, long_multipage_table_fixture):
    """
    The test creates a table with 13 columns (id and 12 enumerated columns)
    and 20 records with id in range from 1 to 20. Values of enumerated columns
    are = column number * id.

    The goal is to ensure that all the values are selected in a right order.
    """

    with client.sql('SELECT * FROM LongMultipageQuery', page_size=1) as cursor:
        for page in cursor:
            assert len(page) == len(multipage_fields)
            for field_number, value in enumerate(page[1:], start=1):
                assert value == field_number * page[0]


@pytest.mark.asyncio
async def test_long_multipage_query_async(async_client, async_long_multipage_table_fixture):
    async with async_client.sql('SELECT * FROM LongMultipageQuery', page_size=1) as cursor:
        async for page in cursor:
            assert len(page) == len(multipage_fields)
            for field_number, value in enumerate(page[1:], start=1):
                assert value == field_number * page[0]


def test_sql_not_create_cache_with_schema(client):
    with pytest.raises(SQLError, match=r".*Cache does not exist.*"):
        client.sql(schema=None, cache='NOT_EXISTING', query_str='select * from NotExisting')


@pytest.mark.asyncio
async def test_sql_not_create_cache_with_schema_async(async_client):
    with pytest.raises(SQLError, match=r".*Cache does not exist.*"):
        await async_client.sql(schema=None, cache='NOT_EXISTING_ASYNC', query_str='select * from NotExistingAsync')


def test_sql_not_create_cache_with_cache(client):
    with pytest.raises(SQLError, match=r".*Failed to set schema.*"):
        client.sql(schema='NOT_EXISTING', query_str='select * from NotExisting')


@pytest.mark.asyncio
async def test_sql_not_create_cache_with_cache_async(async_client):
    with pytest.raises(SQLError, match=r".*Failed to set schema.*"):
        await async_client.sql(schema='NOT_EXISTING_ASYNC', query_str='select * from NotExistingAsync')


@pytest.fixture
def indexed_cache_settings():
    cache_name = 'indexed_cache'
    schema_name = f'{cache_name}_schema'.upper()
    table_name = f'{cache_name}_table'.upper()

    yield {
        PROP_NAME: cache_name,
        PROP_SQL_SCHEMA: schema_name,
        PROP_CACHE_MODE: CacheMode.PARTITIONED,
        PROP_QUERY_ENTITIES: [
            {
                'table_name': table_name,
                'key_field_name': 'KEY',
                'value_field_name': 'VALUE',
                'key_type_name': 'java.lang.Long',
                'value_type_name': 'java.lang.String',
                'query_indexes': [],
                'field_name_aliases': [],
                'query_fields': [
                    {
                        'name': 'KEY',
                        'type_name': 'java.lang.Long',
                        'is_key_field': True,
                        'is_notnull_constraint_field': True,
                    },
                    {
                        'name': 'VALUE',
                        'type_name': 'java.lang.String',
                    },
                ],
            },
        ],
    }


@pytest.fixture
def indexed_cache_fixture(client, indexed_cache_settings):
    cache_name = indexed_cache_settings[PROP_NAME]
    schema_name = indexed_cache_settings[PROP_SQL_SCHEMA]
    table_name = indexed_cache_settings[PROP_QUERY_ENTITIES][0]['table_name']

    cache = client.create_cache(indexed_cache_settings)

    yield cache, cache_name, schema_name, table_name
    cache.destroy()


@pytest.fixture
async def async_indexed_cache_fixture(async_client, indexed_cache_settings):
    cache_name = indexed_cache_settings[PROP_NAME]
    schema_name = indexed_cache_settings[PROP_SQL_SCHEMA]
    table_name = indexed_cache_settings[PROP_QUERY_ENTITIES][0]['table_name']

    cache = await async_client.create_cache(indexed_cache_settings)

    yield cache, cache_name, schema_name, table_name
    await cache.destroy()


def test_query_with_cache(client, indexed_cache_fixture):
    return __check_query_with_cache(client, indexed_cache_fixture)


@pytest.mark.asyncio
async def test_query_with_cache_async(async_client, async_indexed_cache_fixture):
    return await __check_query_with_cache(async_client, async_indexed_cache_fixture)


def __check_query_with_cache(client, cache_fixture):
    test_key, test_value = 42, 'Lorem ipsum'
    cache, cache_name, schema_name, table_name = cache_fixture
    query = f'select value from {table_name}'

    args_to_check = [
        ('schema', schema_name),
        ('cache', cache),
        ('cache', cache_name),
        ('cache', cache.cache_id)
    ]

    def inner():
        cache.put(test_key, test_value)
        for param, value in args_to_check:
            with client.sql(query, **{param: value}) as cursor:
                received = next(cursor)[0]
                assert test_value == received

    async def async_inner():
        await cache.put(test_key, test_value)
        for param, value in args_to_check:
            async with client.sql(query, **{param: value}) as cursor:
                row = await cursor.__anext__()
                received = row[0]
                assert test_value == received

    return async_inner() if isinstance(cache, AioCache) else inner()


VARBIN_CREATE_QUERY = 'CREATE TABLE VarbinTable(id int primary key, varbin VARBINARY)'
VARBIN_DROP_QUERY = 'DROP TABLE VarbinTable'
VARBIN_MERGE_QUERY = 'MERGE INTO VarbinTable(id, varbin) VALUES (?, ?)'
VARBIN_SELECT_QUERY = 'SELECT * FROM VarbinTable'

VARBIN_TEST_PARAMS = [
    bytearray('Test message', 'UTF-8'),
    bytes('Test message', 'UTF-8')
]


@pytest.fixture
def varbin_table(client):
    client.sql(VARBIN_CREATE_QUERY)
    yield None
    client.sql(VARBIN_DROP_QUERY)


@pytest.mark.parametrize(
    'value', VARBIN_TEST_PARAMS
)
def test_sql_cache_varbinary_handling(client, varbin_table, value):
    client.sql(VARBIN_MERGE_QUERY, query_args=(1, value))
    with client.sql(VARBIN_SELECT_QUERY) as cursor:
        for row in cursor:
            assert isinstance(row[1], bytes)
            assert row[1] == value
            break


@pytest.fixture
async def varbin_table_async(async_client):
    await async_client.sql(VARBIN_CREATE_QUERY)
    yield None
    await async_client.sql(VARBIN_DROP_QUERY)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'value', VARBIN_TEST_PARAMS
)
async def test_sql_cache_varbinary_handling_async(async_client, varbin_table_async, value):
    await async_client.sql(VARBIN_MERGE_QUERY, query_args=(1, value))
    async with async_client.sql(VARBIN_SELECT_QUERY) as cursor:
        async for row in cursor:
            assert isinstance(row[1], bytes)
            assert row[1] == value
            break


@pytest.fixture
def varbin_cache_settings():
    cache_name = 'varbin_cache'
    table_name = f'{cache_name}_table'.upper()

    yield {
        PROP_NAME: cache_name,
        PROP_SQL_SCHEMA: 'PUBLIC',
        PROP_CACHE_MODE: CacheMode.PARTITIONED,
        PROP_QUERY_ENTITIES: [
            {
                'table_name': table_name,
                'key_field_name': 'ID',
                'value_field_name': 'VALUE',
                'key_type_name': 'java.lang.Long',
                'value_type_name': 'byte[]',
                'query_indexes': [],
                'field_name_aliases': [],
                'query_fields': [
                    {
                        'name': 'ID',
                        'type_name': 'java.lang.Long',
                        'is_key_field': True,
                        'is_notnull_constraint_field': True,
                    },
                    {
                        'name': 'VALUE',
                        'type_name': 'byte[]',
                    },
                ],
            },
        ],
    }


VARBIN_CACHE_TABLE_NAME = 'varbin_cache_table'.upper()
VARBIN_CACHE_SELECT_QUERY = f'SELECT * FROM {VARBIN_CACHE_TABLE_NAME}'


@pytest.fixture
def varbin_cache(client, varbin_cache_settings):
    cache = client.get_or_create_cache(varbin_cache_settings)
    yield cache
    cache.destroy()


@pytest.mark.parametrize(
    'value', VARBIN_TEST_PARAMS
)
def test_cache_varbinary_handling(client, varbin_cache, value):
    varbin_cache.put(1, value)
    with client.sql(VARBIN_CACHE_SELECT_QUERY) as cursor:
        for row in cursor:
            assert isinstance(row[1], bytes)
            assert row[1] == value
            break


@pytest.fixture
async def varbin_cache_async(async_client, varbin_cache_settings):
    cache = await async_client.get_or_create_cache(varbin_cache_settings)
    yield cache
    await cache.destroy()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'value', VARBIN_TEST_PARAMS
)
async def test_cache_varbinary_handling_async(async_client, varbin_cache_async, value):
    await varbin_cache_async.put(1, value)
    async with async_client.sql(VARBIN_CACHE_SELECT_QUERY) as cursor:
        async for row in cursor:
            assert isinstance(row[1], bytes)
            assert row[1] == value
            break
