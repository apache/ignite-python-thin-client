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
import logging
import re

import pytest

from pyignite import Client, AioClient, monitoring
from pyignite.exceptions import ReconnectError
from tests.security.conftest import AccumulatingConnectionListener
from tests.util import start_ignite_gen, get_or_create_cache, get_or_create_cache_async


@pytest.fixture(scope='module', autouse=True)
def server():
    yield from start_ignite_gen(use_ssl=True, use_auth=False)


def test_connect_ssl_keystore_with_password(ssl_params_with_password):
    __test_connect_ssl(**ssl_params_with_password)


def test_connect_ssl(ssl_params):
    __test_connect_ssl(**ssl_params)


@pytest.mark.asyncio
async def test_connect_ssl_keystore_with_password_async(ssl_params_with_password):
    await __test_connect_ssl(is_async=True, **ssl_params_with_password)


@pytest.mark.asyncio
async def test_connect_ssl_async(ssl_params):
    await __test_connect_ssl(is_async=True, **ssl_params)


def __test_connect_ssl(is_async=False, **kwargs):
    kwargs['use_ssl'] = True

    def inner():
        client = Client(**kwargs)
        with client.connect("127.0.0.1", 10801):
            with get_or_create_cache(client, 'test-cache') as cache:
                cache.put(1, 1)

                assert cache.get(1) == 1

    async def inner_async():
        client = AioClient(**kwargs)
        async with client.connect("127.0.0.1", 10801):
            async with get_or_create_cache_async(client, 'test-cache') as cache:
                await cache.put(1, 1)

                assert (await cache.get(1)) == 1

    return inner_async() if is_async else inner()


invalid_params = [
    {'use_ssl': False},
    {'use_ssl': True},
    {'use_ssl': True, 'ssl_keyfile': 'invalid.pem', 'ssl_certfile': 'invalid.pem'}
]


@pytest.mark.parametrize('invalid_ssl_params', invalid_params)
def test_connection_error_with_incorrect_config(invalid_ssl_params, caplog):
    listener = AccumulatingConnectionListener()
    with pytest.raises(ReconnectError):
        client = Client(event_listeners=[listener], **invalid_ssl_params)
        with client.connect([("127.0.0.1", 10801)]):
            pass

    __assert_handshake_failed_log(caplog)
    __assert_handshake_failed_listener(listener)


@pytest.mark.parametrize('invalid_ssl_params', invalid_params)
@pytest.mark.asyncio
async def test_connection_error_with_incorrect_config_async(invalid_ssl_params, caplog):
    listener = AccumulatingConnectionListener()
    with pytest.raises(ReconnectError):
        client = AioClient(event_listeners=[listener], **invalid_ssl_params)
        async with client.connect([("127.0.0.1", 10801)]):
            pass

    __assert_handshake_failed_log(caplog)
    __assert_handshake_failed_listener(listener)


def __assert_handshake_failed_log(caplog):
    pattern = r'Failed to perform handshake, connection to node\(address=127.0.0.1,\s+port=10801.*failed:'
    assert any(re.match(pattern, r.message) and r.levelname == logging.getLevelName(logging.ERROR)
               for r in caplog.records)


def __assert_handshake_failed_listener(listener):
    found = False
    for ev in listener.events:
        if isinstance(ev, monitoring.HandshakeFailedEvent):
            found = True
            assert ev.host == '127.0.0.1'
            assert ev.port == 10801
            assert ev.error_msg
    assert found
