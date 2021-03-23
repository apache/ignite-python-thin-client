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

from typing import Optional, Tuple

from pyignite.datatypes import Byte, Int, Short, String, UUIDObject
from pyignite.datatypes.internal import Struct
from pyignite.stream import READ_BACKWARD

OP_HANDSHAKE = 1


class HandshakeRequest:
    """ Handshake request. """
    handshake_struct = None
    username = None
    password = None
    protocol_version = None

    def __init__(
        self, protocol_version: Tuple[int, int, int],
        username: Optional[str] = None, password: Optional[str] = None
    ):
        fields = [
            ('length', Int),
            ('op_code', Byte),
            ('version_major', Short),
            ('version_minor', Short),
            ('version_patch', Short),
            ('client_code', Byte),
        ]
        self.protocol_version = protocol_version
        if username and password:
            self.username = username
            self.password = password
            fields.extend([
                ('username', String),
                ('password', String),
            ])
        self.handshake_struct = Struct(fields)

    def from_python(self, stream):
        self.handshake_struct.from_python(stream, self.__create_handshake_data())

    async def from_python_async(self, stream):
        await self.handshake_struct.from_python_async(stream, self.__create_handshake_data())

    def __create_handshake_data(self):
        handshake_data = {
            'length': 8,
            'op_code': OP_HANDSHAKE,
            'version_major': self.protocol_version[0],
            'version_minor': self.protocol_version[1],
            'version_patch': self.protocol_version[2],
            'client_code': 2,  # fixed value defined by protocol
        }
        if self.username and self.password:
            handshake_data.update({
                'username': self.username,
                'password': self.password,
            })
            handshake_data['length'] += sum([
                10,  # each `String` header takes 5 bytes
                len(self.username),
                len(self.password),
            ])
        return handshake_data


class HandshakeResponse(dict):
    """
    Handshake response.
    """
    __response_start = Struct([
        ('length', Int),
        ('op_code', Byte),
    ])

    def __init__(self, data):
        super().__init__()
        self.update(data)

    def __getattr__(self, item):
        return self.get(item)

    @classmethod
    def parse(cls, stream, protocol_version):
        start_class = cls.__response_start.parse(stream)
        start = stream.read_ctype(start_class, direction=READ_BACKWARD)
        data = cls.__response_start.to_python(start)

        response_end = cls.__create_response_end(data, protocol_version)
        if response_end:
            end_class = response_end.parse(stream)
            end = stream.read_ctype(end_class, direction=READ_BACKWARD)
            data.update(response_end.to_python(end))

        return cls(data)

    @classmethod
    async def parse_async(cls, stream, protocol_version):
        start_class = cls.__response_start.parse(stream)
        start = stream.read_ctype(start_class, direction=READ_BACKWARD)
        data = await cls.__response_start.to_python_async(start)

        response_end = cls.__create_response_end(data, protocol_version)
        if response_end:
            end_class = await response_end.parse_async(stream)
            end = stream.read_ctype(end_class, direction=READ_BACKWARD)
            data.update(await response_end.to_python_async(end))

        return cls(data)

    @classmethod
    def __create_response_end(cls, start_data, protocol_version):
        response_end = None
        if start_data['op_code'] == 0:
            response_end = Struct([
                ('version_major', Short),
                ('version_minor', Short),
                ('version_patch', Short),
                ('message', String),
                ('client_status', Int)
            ])
        elif protocol_version >= (1, 4, 0):
            response_end = Struct([
                ('node_uuid', UUIDObject),
            ])
        return response_end
