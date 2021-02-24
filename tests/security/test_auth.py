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

from pyignite import Client
from pyignite.exceptions import AuthenticationError
from tests.util import start_ignite_gen, clear_ignite_work_dir


DEFAULT_IGNITE_USERNAME = 'ignite'
DEFAULT_IGNITE_PASSWORD = 'ignite'


@pytest.fixture(scope='module', autouse=True)
def server(cleanup):
    yield from start_ignite_gen(use_ssl=True, use_auth=True)


@pytest.fixture(scope='module', autouse=True)
def cleanup():
    clear_ignite_work_dir()
    yield None
    clear_ignite_work_dir()


def test_auth_success(ssl_params):
    client = Client(username=DEFAULT_IGNITE_USERNAME, password=DEFAULT_IGNITE_PASSWORD, **ssl_params)

    client.connect("127.0.0.1", 10801)

    assert all(node.alive for node in client._nodes)


@pytest.mark.parametrize(
    'username, password',
    [
        [DEFAULT_IGNITE_USERNAME, None],
        ['invalid_user', 'invalid_password'],
        [None, None]
    ]
)
def test_connection_error_with_incorrect_config(username, password, ssl_params):
    if not username or not password:
        ssl_params['use_ssl'] = True

    with pytest.raises(AuthenticationError):
        client = Client(username=username, password=password, **ssl_params)

        client.connect("127.0.0.1", 10801)
