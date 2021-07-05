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

from .dbclient import DBClient
from .errors import *
from .. import constants
from urllib.parse import urlparse, parse_qs

apiLevel = '2.0'
threadsafety = 2
paramstyle = 'qmark'

def connect(dsn=None,
            user=None, password=None,
            host=constants.IGNITE_DEFAULT_HOST, port=constants.IGNITE_DEFAULT_PORT,
            **kwargs):
    """
    Create a new database connection.

    The connection can be specified via DSN:

        ``conn = connect("ignite://localhost/test?param1=value1&...")``

    or using database and credentials arguments:

        ``conn = connect(database="test", user="default", password="default",
        host="localhost", **kwargs)``

    The basic connection parameters are:

    - *host*: host with running Ignite server.
    - *port*: port Ignite server is bound to.
    - *database*: database connect to.
    - *user*: database user.
    - *password*: user's password.

    See defaults in :data:`~pyignite.connection.Connection`
    constructor.

    DSN or host is required.

    Any other keyword parameter will be passed to the underlying Connection
    class.

    :return: a new connection.
    """
    
    if dsn is not None:
        parsed_dsn = _parse_dsn(dsn)
        host = parsed_dsn['host']
        port = parsed_dsn['port']

    client = DBClient()
    client.connect(host, port)

    return client

def _parse_dsn(dsn):
    url_components = urlparse(dsn)
    host = url_components.hostname
    port = url_components.port or 10800
    if url_components.path is not None:
        schema = url_components.path.replace('/', '')
    else:
        schema = 'PUBLIC'
    schema = url_components.path
    return { 'host':host, 'port':port, 'schema':schema }

__all__ = [
    'connect', 'apiLevel', 'threadsafety', 'paramstyle',
    'Warning', 'Error', 'DataError', 'DatabaseError', 'ProgrammingError',
    'IntegrityError', 'InterfaceError', 'InternalError', 'NotSupportedError',
    'OperationalError', 'IgniteDialect'
]
