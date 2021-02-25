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

from pyignite.exceptions import ReconnectError
from tests.util import start_ignite_gen, get_client, get_or_create_cache


@pytest.fixture(scope='module', autouse=True)
def server():
    yield from start_ignite_gen(use_ssl=True, use_auth=False)


def test_connect_ssl_keystore_with_password(ssl_params_with_password):
    __test_connect_ssl(**ssl_params_with_password)


def test_connect_ssl(ssl_params):
    __test_connect_ssl(**ssl_params)

def __test_connect_ssl(**kwargs):
    kwargs['use_ssl'] = True

    with get_client(**kwargs) as client:
        client.connect("127.0.0.1", 10801)

        with get_or_create_cache(client, 'test-cache') as cache:
            cache.put(1, 1)

            assert cache.get(1) == 1


@pytest.mark.parametrize(
    'invalid_ssl_params',
    [
        {'use_ssl': False},
        {'use_ssl': True},
        {'use_ssl': True, 'ssl_keyfile': 'invalid.pem', 'ssl_certfile': 'invalid.pem'}
    ]
)
def test_connection_error_with_incorrect_config(invalid_ssl_params):
    with pytest.raises(ReconnectError):
        with get_client(**invalid_ssl_params) as client:
            client.connect([("127.0.0.1", 10801)])
