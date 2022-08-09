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

from pyignite import Client, AioClient
from pyignite.monitoring import (
    HandshakeStartEvent, HandshakeSuccessEvent, ConnectionClosedEvent, AuthenticationFailedEvent
)
from pyignite.exceptions import AuthenticationError
from tests.security.conftest import AccumulatingConnectionListener
from tests.util import start_ignite_gen, clear_ignite_work_dir

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


def check_auth_success(ssl_params, caplog):
    listener = AccumulatingConnectionListener()
    client = Client(username=DEFAULT_IGNITE_USERNAME, password=DEFAULT_IGNITE_PASSWORD,
                    event_listeners=[listener], **ssl_params)
    with caplog.at_level(logger='pyignite', level=logging.DEBUG):
        with client.connect("127.0.0.1", 10801):
            assert all(node.alive for node in client._nodes)
            conn = client._nodes[0]

        __assert_successful_connect_log(conn, caplog)
        __assert_successful_connect_events(conn, listener)


def test_auth_success_no_explicit_ssl(with_ssl, ssl_params, caplog):
    if with_ssl:
        ssl_params['use_ssl'] = with_ssl
    check_auth_success(ssl_params, caplog)


def test_auth_success(with_ssl, ssl_params, caplog):
    ssl_params['use_ssl'] = with_ssl
    check_auth_success(ssl_params, caplog)


async def check_auth_success_async(ssl_params, caplog):
    listener = AccumulatingConnectionListener()
    client = AioClient(username=DEFAULT_IGNITE_USERNAME, password=DEFAULT_IGNITE_PASSWORD,
                       event_listeners=[listener], **ssl_params)
    with caplog.at_level(logger='pyignite', level=logging.DEBUG):
        async with client.connect("127.0.0.1", 10801):
            assert all(node.alive for node in client._nodes)
            conn = client._nodes[0]

        __assert_successful_connect_log(conn, caplog)
        __assert_successful_connect_events(conn, listener)


@pytest.mark.asyncio
async def test_auth_success_no_explicit_ssl_async(with_ssl, ssl_params, caplog):
    if with_ssl:
        ssl_params['use_ssl'] = with_ssl
    await check_auth_success_async(ssl_params, caplog)


@pytest.mark.asyncio
async def test_auth_success_async(with_ssl, ssl_params, caplog):
    ssl_params['use_ssl'] = with_ssl
    await check_auth_success_async(ssl_params, caplog)


def __assert_successful_connect_log(conn, caplog):
    assert any(re.match(rf'Connecting to node\(address={conn.host},\s+port={conn.port}', r.message)
               for r in caplog.records)
    assert any(re.match(rf'Connected to node\(address={conn.host},\s+port={conn.port}', r.message)
               for r in caplog.records)
    assert any(re.match(rf'Connection closed to node\(address={conn.host},\s+port={conn.port}', r.message)
               for r in caplog.records)


def __assert_successful_connect_events(conn, listener):
    event_classes = (HandshakeStartEvent, HandshakeSuccessEvent, ConnectionClosedEvent)

    for cls in event_classes:
        any(isinstance(ev, cls) for ev in listener.events)

    for ev in listener.events:
        if isinstance(ev, event_classes):
            assert ev.host == conn.host
            assert ev.port == conn.port
            if isinstance(ev, (HandshakeSuccessEvent, ConnectionClosedEvent)):
                if ev.node_uuid:
                    assert ev.node_uuid == str(conn.uuid)
                if isinstance(ev, HandshakeSuccessEvent):
                    assert ev.protocol_context


auth_failed_params = [
    [DEFAULT_IGNITE_USERNAME, None],
    ['invalid_user', 'invalid_password'],
    [None, None]
]


@pytest.mark.parametrize(
    'username, password',
    auth_failed_params
)
def test_auth_failed(username, password, with_ssl, ssl_params, caplog):
    ssl_params['use_ssl'] = with_ssl
    listener = AccumulatingConnectionListener()
    with pytest.raises(AuthenticationError):
        client = Client(username=username, password=password,
                        event_listeners=[listener], **ssl_params)
        with client.connect("127.0.0.1", 10801):
            pass

    __assert_auth_failed_log(caplog)
    __assert_auth_failed_listener(listener)


@pytest.mark.parametrize(
    'username, password',
    auth_failed_params
)
@pytest.mark.asyncio
async def test_auth_failed_async(username, password, with_ssl, ssl_params, caplog):
    ssl_params['use_ssl'] = with_ssl
    listener = AccumulatingConnectionListener()
    with pytest.raises(AuthenticationError):
        client = AioClient(username=username, password=password,
                           event_listeners=[listener], **ssl_params)
        async with client.connect("127.0.0.1", 10801):
            pass

    __assert_auth_failed_log(caplog)
    __assert_auth_failed_listener(listener)


def __assert_auth_failed_log(caplog):
    pattern = r'Authentication failed while connecting to node\(address=127.0.0.1,\s+port=10801'
    assert any(re.match(pattern, r.message) and r.levelname == logging.getLevelName(logging.ERROR)
               for r in caplog.records)


def __assert_auth_failed_listener(listener):
    found = False
    for ev in listener.events:
        if isinstance(ev, AuthenticationFailedEvent):
            found = True
            assert ev.host == '127.0.0.1'
            assert ev.port == 10801
            assert ev.protocol_context
            assert 'AuthenticationError' in ev.error_msg
    assert found
