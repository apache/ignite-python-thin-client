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


from enum import IntEnum

from pyignite.constants import PROTOCOL_BYTE_ORDER


class BitmaskFeature(IntEnum):
    CLUSTER_API = 2


def feature_flags_as_bytes(features: [int]) -> bytearray:
    """
    Convert feature flags array to bytearray bitmask.

    :param features: Features list,
    :return: Bitmask as bytearray.
    """
    value = 0
    for feature in features:
        value |= (1 << feature)

    bytes_num = max(features) / 8 + 1

    return bytearray(value.to_bytes(bytes_num, byteorder=PROTOCOL_BYTE_ORDER))


def all_supported_features() -> [int]:
    """
    Get all supported features.

    :return: List of supported features.
    """
    return [f.value for f in BitmaskFeature]


def all_supported_feature_flags_as_bytes() -> bytearray:
    """
    Get all supported features as bytearray bitmask.

    :return: Bitmask as bytearray.
    """
    return feature_flags_as_bytes(all_supported_features())
