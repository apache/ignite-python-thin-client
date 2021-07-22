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
import asyncio
import logging
import sys

import pytest

logger = logging.getLogger('pyignite')
logger.setLevel(logging.DEBUG)


@pytest.fixture(autouse=True)
def run_examples(request):
    run_examples = request.config.getoption("--examples")
    if request.node.get_closest_marker('examples'):
        if not run_examples:
            pytest.skip('skipped examples: --examples is not passed')


@pytest.fixture(autouse=True)
def skip_if_no_cext(request):
    skip = False
    try:
        from pyignite import _cutils  # noqa: F401
    except ImportError:
        if request.config.getoption('--force-cext'):
            pytest.fail("C extension failed to build, fail test because of --force-cext is set.")
            return
        skip = True

    if skip and request.node.get_closest_marker('skip_if_no_cext'):
        pytest.skip('skipped c extensions test, c extension is not available.')


@pytest.fixture(scope='session')
def event_loop():
    """Create an instance of the default event loop for each test case."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


def pytest_addoption(parser):
    parser.addoption(
        '--examples',
        action='store_true',
        help='check if examples can be run',
    )
    parser.addoption(
        '--force-cext',
        action='store_true',
        help='check if examples can be run',
    )


def pytest_configure(config):
    marker_docs = [
        "skip_if_no_cext: mark test to run only if c extension is available",
        "skip_if_no_expiry_policy: mark test to run only if expiry policy is supported by server",
        "examples: mark test to run only if --examples are set"
    ]

    for marker_doc in marker_docs:
        config.addinivalue_line("markers", marker_doc)
