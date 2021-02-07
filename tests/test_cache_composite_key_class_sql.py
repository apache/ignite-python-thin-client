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

from pyignite import GenericObjectMeta
from pyignite.datatypes import (
    IntObject, String
)


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

select_query = 'SELECT _KEY, id, dept, name FROM StudentTable'

drop_query = 'DROP TABLE StudentTable IF EXISTS'


def test_cache_get_with_composite_key_finds_sql_value(client):
    """
    Should query a record with composite key and calculate 
    internal hashcode correctly.
    """
    
    client.sql(drop_query)

    # Create table.
    result = client.sql(create_query)
    assert next(result)[0] == 0

    student_key = StudentKey(1, 'Acct')
    student_val = Student('John')

    # Put new Strudent with StudentKey.
    result = client.sql(insert_query, query_args=[student_key.ID, student_key.DEPT, student_val.NAME])
    assert next(result)[0] == 1

    # Cache get finds the same value.
    studentCache = client.get_cache('StudentCache')
    val = studentCache.get(student_key)
    assert val is not None
    assert val.NAME == student_val.NAME

    query_result = list(client.sql(select_query, include_field_names=True))
    
    validate_query_result(student_key, student_val, query_result)


def test_python_sql_finds_inserted_value_with_composite_key(client):
    """
    Insert a record with a composite key and query it with SELECT SQL.
    """
    
    client.sql(drop_query)
   
    # Create table.
    result = client.sql(create_query)
    assert next(result)[0] == 0

    student_key = StudentKey(2, 'Business')
    student_val = Student('Abe')

    # Put new value using cache.
    studentCache = client.get_cache('StudentCache')
    studentCache.put(student_key, student_val)

    # Find the value using SQL.
    query_result = list(client.sql(select_query, include_field_names=True))

    validate_query_result(student_key, student_val, query_result)


def validate_query_result(student_key, student_val, query_result):
    """
    Compare query result with expected key and value.
    """
    assert len(query_result) == 2
    sql_row = dict(zip(query_result[0], query_result[1]))

    assert sql_row['ID'] == student_key.ID
    assert sql_row['DEPT'] == student_key.DEPT
    assert sql_row['NAME'] == student_val.NAME
