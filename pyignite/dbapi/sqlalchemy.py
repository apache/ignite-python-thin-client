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

from sqlalchemy import types
from sqlalchemy.engine import default
from sqlalchemy.sql import compiler

from .. import dbapi
# from . dbclient import DBClient
from .. datatypes.type_names import *

RESERVED_SCHEMAS = ['IGNITE', 'SYS']


# type_map = {
#     "char": types.String,
#     "varchar": types.String,
#     "float": types.Float,
#     "decimal": types.Float,
#     "real": types.Float,
#     "double": types.Float,
#     "boolean": types.Boolean,
#     "tinyint": types.BigInteger,
#     "smallint": types.BigInteger,
#     "integer": types.BigInteger,
#     "bigint": types.BigInteger,
#     "timestamp": types.TIMESTAMP,
#     "date": types.DATE,
#     "other": types.BLOB,
# }

type_map = {
	NAME_BYTE: types.BigInteger,
	NAME_SHORT: types.BigInteger,
	NAME_INT: types.BigInteger,
	NAME_LONG: types.BigInteger,
	NAME_FLOAT: types.Float,
	NAME_DOUBLE: types.Float,
	NAME_CHAR: types.String,
	NAME_BOOLEAN: types.Boolean,
	NAME_STRING: types.String,
	# NAME_UUID = 'java.util.UUID'
	# NAME_DATE = 'java.util.Date'
	# NAME_BYTE_ARR = 'class [B'
	# NAME_SHORT_ARR = 'class [S'
	# NAME_INT_ARR = 'class [I'
	# NAME_LONG_ARR = 'class [J'
	# NAME_FLOAT_ARR = 'class [F'
	# NAME_DOUBLE_ARR = 'class [D'
	# NAME_CHAR_ARR = 'class [C'
	# NAME_BOOLEAN_ARR = 'class [Z'
	# NAME_STRING_ARR = 'class [Ljava.lang.String;'
	# NAME_UUID_ARR = 'class [Ljava.util.UUID;'
	# NAME_DATE_ARR = 'class [Ljava.util.Date;'
	# NAME_OBJ_ARR = 'class [Ljava.lang.Object;'
	# NAME_COL = 'java.util.Collection'
	# NAME_MAP = 'java.util.Map'
	# NAME_DECIMAL = 'java.math.BigDecimal'
	# NAME_DECIMAL_ARR = 'class [Ljava.math.BigDecimal;'
	# NAME_TIMESTAMP = 'java.sql.Timestamp'
	# NAME_TIMESTAMP_ARR = 'class [Ljava.sql.Timestamp;'
	# NAME_TIME = 'java.sql.Time'
	# NAME_TIME_ARR = 'class [Ljava.sql.Time;'
}


class UniversalSet(object):
    def __contains__(self, item):
        return True


class IgniteIdentifierPreparer(compiler.IdentifierPreparer):
    reserved_words = UniversalSet()


class IgniteCompiler(compiler.SQLCompiler):
    pass

class IgniteDDLCompiler(compiler.DDLCompiler):
    def visit_foreign_key_constraint(self, constraint, **kw):
        return None

class IgniteTypeCompiler(compiler.GenericTypeCompiler):
    def visit_REAL(self, type_, **kwargs):
        return "DOUBLE"

    def visit_NUMERIC(self, type_, **kwargs):
        return "LONG"

    visit_DECIMAL = visit_NUMERIC
    visit_INTEGER = visit_NUMERIC
    visit_SMALLINT = visit_NUMERIC
    visit_BIGINT = visit_NUMERIC
    visit_BOOLEAN = visit_NUMERIC
    visit_TIMESTAMP = visit_NUMERIC
    visit_DATE = visit_NUMERIC

    def visit_CHAR(self, type_, **kwargs):
        return "VARCHAR"

    visit_NCHAR = visit_CHAR
    visit_VARCHAR = visit_CHAR
    visit_NVARCHAR = visit_CHAR
    visit_TEXT = visit_CHAR

    def visit_DATETIME(self, type_, **kwargs):
        return "LONG"

    def visit_TIME(self, type_, **kwargs):
        return "LONG"

    def visit_BLOB(self, type_, **kwargs):
        return "COMPLEX"

    visit_CLOB = visit_BLOB
    visit_NCLOB = visit_BLOB
    visit_VARBINARY = visit_BLOB
    visit_BINARY = visit_BLOB


class IgniteDialect(default.DefaultDialect):

    name = "ignite"
    scheme = "thin"
#     driver = "rest"
    user = None
    password = None
    preparer = IgniteIdentifierPreparer
    statement_compiler = IgniteCompiler
    type_compiler = IgniteTypeCompiler
    ddl_compiler = IgniteDDLCompiler
    supports_alter = False
    supports_views = False
    postfetch_lastrowid = False
    supports_pk_autoincrement = False
    supports_default_values = False
    supports_empty_insert = False
    supports_unicode_statements = True
    supports_unicode_binds = True
    returns_unicode_strings = True
    description_encoding = None
    supports_native_boolean = True

    def __init__(self, context=None, *args, **kwargs):
        super(IgniteDialect, self).__init__(*args, **kwargs)
        self.context = context or {}

    @classmethod
    def dbapi(cls):
        return dbapi

    def create_connect_args(self, url):
        kwargs = {
            "host": url.host,
            "port": url.port or 10800,
            "user": url.username or None,
            "password": url.password or None,
            "path": url.database,
            "scheme": self.scheme,
            "context": self.context,
            "header": url.query.get("header") == "true",
        }
        return ([], kwargs)

    def get_schema_names(self, connection, **kwargs):
        # Each Ignite datasource appears as a table in the "SYS" schema.
        result = connection.execute(
            "SELECT SCHEMA_NAME FROM SYS.SCHEMAS"
        )

        return [
            row.SCHEMA_NAME for row in result if row.SCHEMA_NAME not in RESERVED_SCHEMAS
        ]

    def has_table(self, connection, table_name, schema=None):
        query = """
            SELECT COUNT(*) > 0 AS exists_
              FROM SYS.TABLES
             WHERE TABLE_NAME = '{table_name}'
        """.format(
            table_name=table_name
        )

        result = connection.execute(query)
        return result.fetchone().EXISTS_

    def get_table_names(self, connection, schema=None, **kwargs):
        query = "SELECT TABLE_NAME FROM SYS.TABLES"
        if schema:
            query = "{query} WHERE TABLE_SCHEMA = '{schema}'".format(
                query=query, schema=schema
            )

        result = connection.execute(query)
        return [row.TABLE_NAME for row in result]

    def get_view_names(self, connection, schema=None, **kwargs):
        return []

    def get_table_options(self, connection, table_name, schema=None, **kwargs):
        return {}

    def get_columns(self, connection, table_name, schema=None, **kwargs):
        query = """
            SELECT COLUMN_NAME,
                   TYPE,
                   NULLABLE,
                   DEFAULT_VALUE
              FROM SYS.TABLE_COLUMNS
             WHERE TABLE_NAME = '{table_name}'
        """.format(
            table_name=table_name
        )
        if schema:
            query = "{query} AND SCHEMA_NAME = '{schema}'".format(
                query=query, schema=schema
            )

        result = connection.execute(query)

        return [
            {
                "name": row.COLUMN_NAME,
                "type": type_map[row.DATA_TYPE.lower()],
                "nullable": get_is_nullable(row.IS_NULLABLE),
                "default": get_default(row.COLUMN_DEFAULT),
            }
            for row in result
        ]

    def get_pk_constraint(self, connection, table_name, schema=None, **kwargs):
        return {"constrained_columns": [], "name": None}

    def get_foreign_keys(self, connection, table_name, schema=None, **kwargs):
        return []

    def get_check_constraints(self, connection, table_name, schema=None, **kwargs):
        return []

    def get_table_comment(self, connection, table_name, schema=None, **kwargs):
        return {"text": ""}

    def get_indexes(self, connection, table_name, schema=None, **kwargs):
        return []

    def get_unique_constraints(self, connection, table_name, schema=None, **kwargs):
        return []

    def get_view_definition(self, connection, view_name, schema=None, **kwargs):
        pass

    def do_rollback(self, dbapi_connection):
        pass

    def _check_unicode_returns(self, connection, additional_tests=None):
        return True

    def _check_unicode_description(self, connection):
        return True

def get_is_nullable(Ignite_is_nullable):
    # this should be 'YES' or 'NO'; we default to no
    return Ignite_is_nullable.lower() == "yes"


def get_default(Ignite_column_default):
    # currently unused, returns ''
    return str(Ignite_column_default) if Ignite_column_default != "" else None