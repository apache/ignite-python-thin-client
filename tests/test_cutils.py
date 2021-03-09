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

import random
from collections import OrderedDict

import pytest

import pyignite.utils as _putils
from pyignite.datatypes import IntObject

try:
    from pyignite import _cutils

    _cutils_hashcode = _cutils.hashcode
    _cutils_schema_id = _cutils.schema_id
except ImportError:
    _cutils_hashcode = lambda x: None  # noqa: E731
    _cutils_schema_id = lambda x: None  # noqa: E731
    pass


@pytest.mark.skip_if_no_cext
def test_bytes_hashcode():
    assert _cutils_hashcode(None) == 0
    assert _cutils_hashcode(b'') == 1
    assert _cutils_hashcode(bytearray()) == 1
    assert _cutils_hashcode(memoryview(b'')) == 1

    for i in range(1000):
        rnd_bytes = bytearray([random.randint(0, 255) for _ in range(random.randint(1, 1024))])

        fallback_val = _putils.__hashcode_fallback(rnd_bytes)
        assert _cutils_hashcode(rnd_bytes) == fallback_val
        assert _cutils_hashcode(bytes(rnd_bytes)) == fallback_val
        assert _cutils_hashcode(memoryview(rnd_bytes)) == fallback_val


@pytest.mark.skip_if_no_cext
@pytest.mark.parametrize(
    'value',
    [
        '皮膚の色、',
        'Произвольный символ',
        'Random string',
        '',
    ]
)
def test_string_hashcode(value):
    assert _cutils_hashcode(value) == _putils.__hashcode_fallback(value), f'failed on {value}'


@pytest.mark.skip_if_no_cext
def test_random_string_hashcode():
    assert _cutils_hashcode(None) == 0
    assert _cutils_hashcode('') == 0

    for i in range(1000):
        rnd_str = get_random_unicode(random.randint(1, 128))
        assert _cutils_hashcode(rnd_str) == _putils.__hashcode_fallback(rnd_str), f'failed on {rnd_str}'


@pytest.mark.skip_if_no_cext
def test_schema_id():
    rnd_id = random.randint(-100, 100)
    assert _cutils_schema_id(rnd_id) == rnd_id
    assert _cutils_schema_id(None) == 0
    assert _cutils_schema_id({}) == 0

    for i in range(1000):
        schema = OrderedDict({get_random_field_name(20): IntObject for _ in range(20)})
        assert _cutils_schema_id(schema) == _putils.__schema_id_fallback(schema), f'failed on {schema}'


@pytest.mark.skip_if_no_cext
@pytest.mark.parametrize(
    'func,args,kwargs,err_cls',
    [
        [_cutils_hashcode, [123], {}, ValueError],
        [_cutils_hashcode, [{'test': 'test'}], {}, ValueError],
        [_cutils_hashcode, [], {}, TypeError],
        [_cutils_hashcode, [123, 123], {}, TypeError],
        [_cutils_hashcode, [], {'input': 'test'}, TypeError],
        [_cutils_schema_id, ['test'], {}, ValueError],
        [_cutils_schema_id, [], {}, TypeError],
        [_cutils_schema_id, [], {}, TypeError],
        [_cutils_schema_id, [123, 123], {}, TypeError],
        [_cutils_schema_id, [], {'input': 'test'}, TypeError],
    ]
)
def test_handling_errors(func, args, kwargs, err_cls):
    with pytest.raises(err_cls):
        func(*args, **kwargs)


def get_random_field_name(length):
    first = get_random_unicode(length // 2, latin=True)
    second = get_random_unicode(length - length // 2, latin=True)

    first = first.upper() if random.randint(0, 1) else first.lower()
    second = second.upper() if random.randint(0, 1) else second.lower()

    return first + '_' + second


def get_random_unicode(length, latin=False):
    include_ranges = [
        (0x0041, 0x005A),  # Latin high
        (0x0061, 0x007A),  # Latin lower
        (0x0410, 0x042F),  # Russian high
        (0x0430, 0x044F),  # Russian lower
        (0x05D0, 0x05EA)   # Hebrew
    ]

    alphabet = []

    if latin:
        include_ranges = include_ranges[0:2]

    for current_range in include_ranges:
        for code_point in range(current_range[0], current_range[1] + 1):
            alphabet.append(chr(code_point))

    return ''.join(random.choice(alphabet) for _ in range(length))
