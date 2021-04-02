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

from pyignite import Client, AioClient
from tests.util import start_ignite


@pytest.fixture(scope='module')
def start_ignite_server():
    def start(idx=1, debug=False, use_ssl=False, use_auth=False):
        return start_ignite(
            idx=idx,
            debug=debug,
            use_ssl=use_ssl,
            use_auth=use_auth
        )

    return start


@pytest.fixture(scope='module')
def start_client():
    def start(**kwargs):
        return Client(**kwargs)

    return start


@pytest.fixture(scope='module')
def start_async_client():
    def start(**kwargs):
        return AioClient(**kwargs)

    return start
