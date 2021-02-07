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

"""
This module contains classes, used internally by `pyignite` for parsing and
creating binary data.
"""

from .complex import *
from .internal import *
from .null_object import *
from .primitive import *
from .primitive_arrays import *
from .primitive_objects import *
from .standard import *
from ..stream import BinaryStream, READ_BACKWARD


def unwrap_binary(client: 'Client', wrapped: tuple) -> object:
    """
    Unwrap wrapped BinaryObject and convert it to Python data.

    :param client: connection to Ignite cluster,
    :param wrapped: `WrappedDataObject` value,
    :return: dict representing wrapped BinaryObject.
    """
    from pyignite.datatypes.complex import BinaryObject

    blob, offset = wrapped
    with BinaryStream(blob, client.random_node) as stream:
        data_class = BinaryObject.parse(stream)
        result = BinaryObject.to_python(stream.read_ctype(data_class, direction=READ_BACKWARD), client)

    return result
