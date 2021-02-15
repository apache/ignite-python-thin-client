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

import argparse
from distutils.util import strtobool
import ssl

import pytest

from pyignite import Client
from pyignite.constants import *
from pyignite.api import cache_create, cache_destroy
from tests.util import _start_ignite, start_ignite_gen


class BoolParser(argparse.Action):

    def __call__(self, parser, namespace, values, option_string=None):
        values = True if values is None else bool(strtobool(values))
        setattr(namespace, self.dest, values)


class CertReqsParser(argparse.Action):
    conv_map = {
        'NONE': ssl.CERT_NONE,
        'OPTIONAL': ssl.CERT_OPTIONAL,
        'REQUIRED': ssl.CERT_REQUIRED,
    }

    def __call__(self, parser, namespace, values, option_string=None):
        value = values.upper()
        if value in self.conv_map:
            setattr(namespace, self.dest, self.conv_map[value])
        else:
            raise ValueError(
                'Undefined argument: --ssl-cert-reqs={}'.format(value)
            )


class SSLVersionParser(argparse.Action):
    conv_map = {
        'TLSV1_1': ssl.PROTOCOL_TLSv1_1,
        'TLSV1_2': ssl.PROTOCOL_TLSv1_2,
    }

    def __call__(self, parser, namespace, values, option_string=None):
        value = values.upper()
        if value in self.conv_map:
            setattr(namespace, self.dest, self.conv_map[value])
        else:
            raise ValueError(
                'Undefined argument: --ssl-version={}'.format(value)
            )


@pytest.fixture(scope='session', autouse=True)
def server1(request):
    yield from start_ignite_server_gen(1, request)


@pytest.fixture(scope='session', autouse=True)
def server2(request):
    yield from start_ignite_server_gen(2, request)


@pytest.fixture(scope='session', autouse=True)
def server3(request):
    yield from start_ignite_server_gen(3, request)


@pytest.fixture(scope='module')
def start_ignite_server(use_ssl):
    def start(idx=1):
        return _start_ignite(idx, use_ssl=use_ssl)

    return start


def start_ignite_server_gen(idx, request):
    use_ssl = request.config.getoption("--use-ssl")
    yield from start_ignite_gen(idx, use_ssl)


@pytest.fixture(scope='module')
def client(
    node, timeout, partition_aware, use_ssl, ssl_keyfile, ssl_keyfile_password,
    ssl_certfile, ssl_ca_certfile, ssl_cert_reqs, ssl_ciphers, ssl_version,
    username, password,
):
    yield from client0(node, timeout, partition_aware, use_ssl, ssl_keyfile, ssl_keyfile_password, ssl_certfile,
                       ssl_ca_certfile, ssl_cert_reqs, ssl_ciphers, ssl_version, username, password)


@pytest.fixture(scope='module')
def client_partition_aware(
        node, timeout, use_ssl, ssl_keyfile, ssl_keyfile_password, ssl_certfile,
        ssl_ca_certfile, ssl_cert_reqs, ssl_ciphers, ssl_version, username,
        password
):
    yield from client0(node, timeout, True, use_ssl, ssl_keyfile, ssl_keyfile_password, ssl_certfile, ssl_ca_certfile,
                       ssl_cert_reqs, ssl_ciphers, ssl_version, username, password)


@pytest.fixture(scope='module')
def client_partition_aware_single_server(
        node, timeout, use_ssl, ssl_keyfile, ssl_keyfile_password, ssl_certfile,
        ssl_ca_certfile, ssl_cert_reqs, ssl_ciphers, ssl_version, username,
        password
):
    node = node[:1]
    yield from client0(node, timeout, True, use_ssl, ssl_keyfile, ssl_keyfile_password, ssl_certfile, ssl_ca_certfile,
                      ssl_cert_reqs, ssl_ciphers, ssl_version, username, password)


@pytest.fixture
def cache(client):
    cache_name = 'my_bucket'
    conn = client.random_node

    cache_create(conn, cache_name)
    yield cache_name
    cache_destroy(conn, cache_name)


@pytest.fixture(scope='module')
def start_client(use_ssl, ssl_keyfile, ssl_keyfile_password, ssl_certfile, ssl_ca_certfile, ssl_cert_reqs, ssl_ciphers,
                 ssl_version,username, password):
    def start(**kwargs):
        cli_kw = kwargs.copy()
        cli_kw.update({
            'use_ssl': use_ssl,
            'ssl_keyfile': ssl_keyfile,
            'ssl_keyfile_password': ssl_keyfile_password,
            'ssl_certfile': ssl_certfile,
            'ssl_ca_certfile': ssl_ca_certfile,
            'ssl_cert_reqs': ssl_cert_reqs,
            'ssl_ciphers': ssl_ciphers,
            'ssl_version': ssl_version,
            'username': username,
            'password': password
        })
        return Client(**cli_kw)

    return start


def client0(
    node, timeout, partition_aware, use_ssl, ssl_keyfile, ssl_keyfile_password,
    ssl_certfile, ssl_ca_certfile, ssl_cert_reqs, ssl_ciphers, ssl_version,
    username, password,
):
    client = Client(
        timeout=timeout,
        partition_aware=partition_aware,
        use_ssl=use_ssl,
        ssl_keyfile=ssl_keyfile,
        ssl_keyfile_password=ssl_keyfile_password,
        ssl_certfile=ssl_certfile,
        ssl_ca_certfile=ssl_ca_certfile,
        ssl_cert_reqs=ssl_cert_reqs,
        ssl_ciphers=ssl_ciphers,
        ssl_version=ssl_version,
        username=username,
        password=password,
    )
    nodes = []
    for n in node:
        host, port = n.split(':')
        port = int(port)
        nodes.append((host, port))
    client.connect(nodes)
    yield client
    client.close()


@pytest.fixture
def examples(request):
    return request.config.getoption("--examples")


@pytest.fixture(autouse=True)
def run_examples(request, examples):
    if request.node.get_closest_marker('examples'):
        if not examples:
            pytest.skip('skipped examples: --examples is not passed')


@pytest.fixture(autouse=True)
def skip_if_no_cext(request):
    skip = False
    try:
        from pyignite import _cutils
    except ImportError:
        skip = True

    if skip:
        pytest.skip('skipped c extensions test, c extension is not available.')


def pytest_addoption(parser):
    parser.addoption(
        '--node',
        action='append',
        default=None,
        help=(
            'Ignite binary protocol test server connection string '
            '(default: "localhost:10801")'
        )
    )
    parser.addoption(
        '--timeout',
        action='store',
        type=float,
        default=2.0,
        help=(
            'Timeout (in seconds) for each socket operation. Can accept '
            'integer or float value. Default is None'
        )
    )
    parser.addoption(
        '--partition-aware',
        action=BoolParser,
        nargs='?',
        default=False,
        help='Turn on the best effort affinity feature'
    )
    parser.addoption(
        '--use-ssl',
        action=BoolParser,
        nargs='?',
        default=False,
        help='Use SSL encryption'
    )
    parser.addoption(
        '--ssl-keyfile',
        action='store',
        default=None,
        type=str,
        help='a path to SSL key file to identify local party'
    )
    parser.addoption(
        '--ssl-keyfile-password',
        action='store',
        default=None,
        type=str,
        help='password for SSL key file'
    )
    parser.addoption(
        '--ssl-certfile',
        action='store',
        default=None,
        type=str,
        help='a path to ssl certificate file to identify local party'
    )
    parser.addoption(
        '--ssl-ca-certfile',
        action='store',
        default=None,
        type=str,
        help='a path to a trusted certificate or a certificate chain'
    )
    parser.addoption(
        '--ssl-cert-reqs',
        action=CertReqsParser,
        default=ssl.CERT_NONE,
        help=(
            'determines how the remote side certificate is treated: '
            'NONE (ignore, default), '
            'OPTIONAL (validate, if provided) or '
            'REQUIRED (valid remote certificate is required)'
        )
    )
    parser.addoption(
        '--ssl-ciphers',
        action='store',
        default=SSL_DEFAULT_CIPHERS,
        type=str,
        help='ciphers to use'
    )
    parser.addoption(
        '--ssl-version',
        action=SSLVersionParser,
        default=SSL_DEFAULT_VERSION,
        help='SSL version: TLSV1_1 or TLSV1_2'
    )
    parser.addoption(
        '--username',
        action='store',
        type=str,
        help='user name'
    )
    parser.addoption(
        '--password',
        action='store',
        type=str,
        help='password'
    )
    parser.addoption(
        '--examples',
        action='store_true',
        help='check if examples can be run',
    )


def pytest_generate_tests(metafunc):
    session_parameters = {
        'node': ['{host}:{port}'.format(host='127.0.0.1', port=10801),
                 '{host}:{port}'.format(host='127.0.0.1', port=10802),
                 '{host}:{port}'.format(host='127.0.0.1', port=10803)],
        'timeout': None,
        'partition_aware': False,
        'use_ssl': False,
        'ssl_keyfile': None,
        'ssl_keyfile_password': None,
        'ssl_certfile': None,
        'ssl_ca_certfile': None,
        'ssl_cert_reqs': ssl.CERT_NONE,
        'ssl_ciphers': SSL_DEFAULT_CIPHERS,
        'ssl_version': SSL_DEFAULT_VERSION,
        'username': None,
        'password': None,
    }

    for param_name in session_parameters:
        if param_name in metafunc.fixturenames:
            param = metafunc.config.getoption(param_name)
            # TODO: This does not work for bool
            if param is None:
                param = session_parameters[param_name]
            if param_name == 'node' or type(param) is not list:
                param = [param]
            metafunc.parametrize(param_name, param, scope='session')


def pytest_configure(config):
    config.addinivalue_line(
        "markers", "examples: mark test to run only if --examples are set\n"
                   "skip_if_no_cext: mark test to run only if c extension is available"
    )
