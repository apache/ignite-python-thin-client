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

from pyignite.api import (
    sql_fields, sql_fields_cursor_get_page,
    sql, sql_cursor_get_page,
    cache_get_configuration,
)
from pyignite.datatypes.cache_config import CacheMode
from pyignite.datatypes.prop_codes import *
from pyignite.exceptions import SQLError
from pyignite.utils import entity_id
from pyignite.binary import unwrap_binary

initial_data = [
        ('John', 'Doe', 5),
        ('Jane', 'Roe', 4),
        ('Joe', 'Bloggs', 4),
        ('Richard', 'Public', 3),
        ('Negidius', 'Numerius', 3),
    ]

create_query = '''CREATE TABLE Student (
    id INT(11) PRIMARY KEY,
    first_name CHAR(24),
    last_name CHAR(32),
    grade INT(11))'''

insert_query = '''INSERT INTO Student(id, first_name, last_name, grade)
VALUES (?, ?, ?, ?)'''

select_query = 'SELECT id, first_name, last_name, grade FROM Student'

drop_query = 'DROP TABLE Student IF EXISTS'

page_size = 4


def test_sql(client):

    conn = client.random_node

    # cleanup
    client.sql(drop_query)

    result = sql_fields(
        conn,
        0,
        create_query,
        page_size,
        schema='PUBLIC',
        include_field_names=True
    )
    assert result.status == 0, result.message

    for i, data_line in enumerate(initial_data, start=1):
        fname, lname, grade = data_line
        result = sql_fields(
            conn,
            0,
            insert_query,
            page_size,
            schema='PUBLIC',
            query_args=[i, fname, lname, grade],
            include_field_names=True
        )
        assert result.status == 0, result.message

    result = cache_get_configuration(conn, 'SQL_PUBLIC_STUDENT')
    assert result.status == 0, result.message

    binary_type_name = result.value[PROP_QUERY_ENTITIES][0]['value_type_name']
    result = sql(
        conn,
        'SQL_PUBLIC_STUDENT',
        binary_type_name,
        'TRUE',
        page_size
    )
    assert result.status == 0, result.message
    assert len(result.value['data']) == page_size
    assert result.value['more'] is True

    for wrapped_object in result.value['data'].values():
        data = unwrap_binary(client, wrapped_object)
        assert data.type_id == entity_id(binary_type_name)

    cursor = result.value['cursor']

    while result.value['more']:
        result = sql_cursor_get_page(conn, cursor)
        assert result.status == 0, result.message

        for wrapped_object in result.value['data'].values():
            data = unwrap_binary(client, wrapped_object)
            assert data.type_id == entity_id(binary_type_name)

    # repeat cleanup
    result = sql_fields(conn, 0, drop_query, page_size, schema='PUBLIC')
    assert result.status == 0


def test_sql_fields(client):

    conn = client.random_node

    # cleanup
    client.sql(drop_query)

    result = sql_fields(
        conn,
        0,
        create_query,
        page_size,
        schema='PUBLIC',
        include_field_names=True
    )
    assert result.status == 0, result.message

    for i, data_line in enumerate(initial_data, start=1):
        fname, lname, grade = data_line
        result = sql_fields(
            conn,
            0,
            insert_query,
            page_size,
            schema='PUBLIC',
            query_args=[i, fname, lname, grade],
            include_field_names=True
        )
        assert result.status == 0, result.message

    result = sql_fields(
        conn,
        0,
        select_query,
        page_size,
        schema='PUBLIC',
        include_field_names=True
    )
    assert result.status == 0
    assert len(result.value['data']) == page_size
    assert result.value['more'] is True

    cursor = result.value['cursor']

    result = sql_fields_cursor_get_page(conn, cursor, field_count=4)
    assert result.status == 0
    assert len(result.value['data']) == len(initial_data) - page_size
    assert result.value['more'] is False

    # repeat cleanup
    result = sql_fields(conn, 0, drop_query, page_size, schema='PUBLIC')
    assert result.status == 0


def test_long_multipage_query(client):
    """
    The test creates a table with 13 columns (id and 12 enumerated columns)
    and 20 records with id in range from 1 to 20. Values of enumerated columns
    are = column number * id.

    The goal is to ensure that all the values are selected in a right order.
    """

    fields = ["id", "abc", "ghi", "def", "jkl", "prs", "mno", "tuw", "zyz", "abc1", "def1", "jkl1", "prs1"]

    client.sql('DROP TABLE LongMultipageQuery IF EXISTS')

    client.sql("CREATE TABLE LongMultiPageQuery (%s, %s)" % \
               (fields[0] + " INT(11) PRIMARY KEY", ",".join(map(lambda f: f + " INT(11)", fields[1:]))))

    for id in range(1, 21):
        client.sql(
            "INSERT INTO LongMultipageQuery (%s) VALUES (%s)" % (",".join(fields), ",".join("?" * len(fields))),
            query_args=[id] + list(i * id for i in range(1, len(fields))))

    result = client.sql('SELECT * FROM LongMultipageQuery', page_size=1)
    for page in result:
        assert len(page) == len(fields)
        for field_number, value in enumerate(page[1:], start=1):
            assert value == field_number * page[0]

    client.sql(drop_query)


def test_sql_not_create_cache_with_schema(client):
    with pytest.raises(SQLError, match=r".*Cache does not exist.*"):
        client.sql(schema=None, cache='NOT_EXISTING', query_str='select * from NotExisting')


def test_sql_not_create_cache_with_cache(client):
    with pytest.raises(SQLError, match=r".*Failed to set schema.*"):
        client.sql(schema='NOT_EXISTING', query_str='select * from NotExisting')


def test_query_with_cache(client):
    test_key = 42
    test_value = 'Lorem ipsum'

    cache_name = test_query_with_cache.__name__.upper()
    schema_name = f'{cache_name}_schema'.upper()
    table_name = f'{cache_name}_table'.upper()

    cache = client.create_cache({
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
    })

    qry = f'select value from {table_name}'

    cache.put(test_key, test_value)

    page = client.sql(qry, schema=schema_name)
    received = next(page)[0]

    assert test_value == received

    page = client.sql(qry, cache=cache.name)
    received = next(page)[0]

    assert test_value == received

    page = client.sql(qry, cache=cache.cache_id)
    received = next(page)[0]

    assert test_value == received
