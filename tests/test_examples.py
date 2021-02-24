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

import glob
import subprocess

import pytest

from tests.util import start_ignite_gen

SKIP_LIST = [
    'failover.py',  # it hangs by design
]


@pytest.fixture(scope='module', autouse=True)
def server():
    yield from start_ignite_gen(1)


@pytest.mark.examples
def test_examples():
    for script in glob.glob1('../examples', '*.py'):
        if script not in SKIP_LIST:
            proc = subprocess.run([
                'python',
                '../examples/{}'.format(script),
            ])

            assert proc.returncode == 0
