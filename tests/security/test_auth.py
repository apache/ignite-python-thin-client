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

from pyignite.exceptions import AuthenticationError
from tests.util import start_ignite_gen, clear_ignite_work_dir, get_client, get_client_async

DEFAULT_IGNITE_USERNAME = 'ignite'
DEFAULT_IGNITE_PASSWORD = 'ignite'


@pytest.fixture(params=['with-ssl', 'without-ssl'])
def with_ssl(request):
    return request.param == 'with-ssl'


@pytest.fixture(autouse=True)
def server(with_ssl, cleanup):
    yield from start_ignite_gen(use_ssl=with_ssl, use_auth=True)


@pytest.fixture(scope='module', autouse=True)
def cleanup():
    clear_ignite_work_dir()
    yield None
    clear_ignite_work_dir()


def test_auth_success(with_ssl, ssl_params):
    ssl_params['use_ssl'] = with_ssl

    with get_client(username=DEFAULT_IGNITE_USERNAME, password=DEFAULT_IGNITE_PASSWORD, **ssl_params) as client:
        client.connect("127.0.0.1", 10801)

        assert all(node.alive for node in client._nodes)


@pytest.mark.asyncio
async def test_auth_success_async(with_ssl, ssl_params):
    ssl_params['use_ssl'] = with_ssl

    async with get_client_async(username=DEFAULT_IGNITE_USERNAME, password=DEFAULT_IGNITE_PASSWORD,
                                **ssl_params) as client:
        await client.connect("127.0.0.1", 10801)

        assert all(node.alive for node in client._nodes)


auth_failed_params = [
    [DEFAULT_IGNITE_USERNAME, None],
    ['invalid_user', 'invalid_password'],
    [None, None]
]


@pytest.mark.parametrize(
    'username, password',
    auth_failed_params
)
def test_auth_failed(username, password, with_ssl, ssl_params):
    ssl_params['use_ssl'] = with_ssl

    with pytest.raises(AuthenticationError):
        with get_client(username=username, password=password, **ssl_params) as client:
            client.connect("127.0.0.1", 10801)


@pytest.mark.parametrize(
    'username, password',
    auth_failed_params
)
@pytest.mark.asyncio
async def test_auth_failed_async(username, password, with_ssl, ssl_params):
    ssl_params['use_ssl'] = with_ssl

    with pytest.raises(AuthenticationError):
        async with get_client_async(username=username, password=password, **ssl_params) as client:
            await client.connect("127.0.0.1", 10801)
