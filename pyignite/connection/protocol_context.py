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

from typing import Tuple

from pyignite.connection.bitmask_feature import BitmaskFeature


class ProtocolContext:
    """
    Protocol context. Provides ability to easily check supported supported
    protocol features.
    """

    def __init__(self, version: Tuple[int, int, int], features: [int] = None):
        self.version = version
        self.features = features

    def is_partition_awareness_supported(self) -> bool:
        """
        Check whether partition awareness supported by the current protocol.
        """
        return self.version >= (1, 4, 0)

    def is_status_flags_supported(self) -> bool:
        """
        Check whether status flags supported by the current protocol.
        """
        return self.version >= (1, 4, 0)

    def is_feature_flags_supported(self) -> bool:
        """
        Check whether feature flags supported by the current protocol.
        """
        return self.version >= (1, 7, 0)

    def if_cluster_api_supported(self) -> bool:
        """
        Check whether cluster API supported by the current protocol.
        """
        return BitmaskFeature.CLUSTER_API in self.features
