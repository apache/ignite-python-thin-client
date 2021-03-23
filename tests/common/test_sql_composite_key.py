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
from enum import Enum

import pytest

from pyignite import GenericObjectMeta, AioClient
from pyignite.datatypes import IntObject, String


class StudentKey(
    metaclass=GenericObjectMeta,
    type_name='test.model.StudentKey',
    schema=OrderedDict([
        ('ID', IntObject),
        ('DEPT', String)
    ])
):
    pass


class Student(
    metaclass=GenericObjectMeta,
    type_name='test.model.Student',
    schema=OrderedDict([
        ('NAME', String),
    ])
):
    pass


create_query = '''CREATE TABLE StudentTable (
    id INT(11),
    dept VARCHAR,
    name CHAR(24),
    PRIMARY KEY (id, dept))
    WITH "CACHE_NAME=StudentCache, KEY_TYPE=test.model.StudentKey, VALUE_TYPE=test.model.Student"'''

insert_query = '''INSERT INTO StudentTable (id, dept, name) VALUES (?, ?, ?)'''

select_query = 'SELECT id, dept, name FROM StudentTable'

select_kv_query = 'SELECT _key, _val FROM StudentTable'

drop_query = 'DROP TABLE StudentTable IF EXISTS'


@pytest.fixture
def student_table_fixture(client):
    yield from __create_student_table_fixture(client)


@pytest.fixture
async def async_student_table_fixture(async_client):
    async for _ in __create_student_table_fixture(async_client):
        yield


def __create_student_table_fixture(client):
    def inner():
        client.sql(drop_query)
        client.sql(create_query)
        yield None
        client.sql(drop_query)

    async def inner_async():
        await client.sql(drop_query)
        await client.sql(create_query)
        yield None
        await client.sql(drop_query)

    return inner_async() if isinstance(client, AioClient) else inner()


class InsertMode(Enum):
    SQL = 1
    CACHE = 2


@pytest.mark.parametrize('insert_mode', [InsertMode.SQL, InsertMode.CACHE])
def test_sql_composite_key(client, insert_mode, student_table_fixture):
    __perform_test(client, insert_mode)


@pytest.mark.asyncio
@pytest.mark.parametrize('insert_mode', [InsertMode.SQL, InsertMode.CACHE])
async def test_sql_composite_key_async(async_client, insert_mode, async_student_table_fixture):
    await __perform_test(async_client, insert_mode)


def __perform_test(client, insert=InsertMode.SQL):
    student_key = StudentKey(2, 'Business')
    student_val = Student('Abe')

    def validate_query_result(key, val, query_result):
        """
        Compare query result with expected key and value.
        """
        assert len(query_result) == 2
        sql_row = dict(zip(query_result[0], query_result[1]))

        assert sql_row['ID'] == key.ID
        assert sql_row['DEPT'] == key.DEPT
        assert sql_row['NAME'] == val.NAME

    def validate_kv_query_result(key, val, query_result):
        """
        Compare query result with expected key and value.
        """
        assert len(query_result) == 2
        sql_row = dict(zip(query_result[0], query_result[1]))

        sql_key, sql_val = sql_row['_KEY'], sql_row['_VAL']
        assert sql_key.ID == key.ID
        assert sql_key.DEPT == key.DEPT
        assert sql_val.NAME == val.NAME

    def inner():
        if insert == InsertMode.SQL:
            result = client.sql(insert_query, query_args=[student_key.ID, student_key.DEPT, student_val.NAME])
            assert next(result)[0] == 1
        else:
            studentCache = client.get_cache('StudentCache')
            studentCache.put(student_key, student_val)
            val = studentCache.get(student_key)
            assert val is not None
            assert val.NAME == student_val.NAME

        query_result = list(client.sql(select_query, include_field_names=True))
        validate_query_result(student_key, student_val, query_result)

        query_result = list(client.sql(select_kv_query, include_field_names=True))
        validate_kv_query_result(student_key, student_val, query_result)

    async def inner_async():
        if insert == InsertMode.SQL:
            result = await client.sql(insert_query, query_args=[student_key.ID, student_key.DEPT, student_val.NAME])
            assert (await result.__anext__())[0] == 1
        else:
            studentCache = await client.get_cache('StudentCache')
            await studentCache.put(student_key, student_val)
            val = await studentCache.get(student_key)
            assert val is not None
            assert val.NAME == student_val.NAME

        async with client.sql(select_query, include_field_names=True) as cursor:
            query_result = [r async for r in cursor]
            validate_query_result(student_key, student_val, query_result)

        async with client.sql(select_kv_query, include_field_names=True) as cursor:
            query_result = [r async for r in cursor]
            validate_kv_query_result(student_key, student_val, query_result)

    return inner_async() if isinstance(client, AioClient) else inner()
