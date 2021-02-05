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

from io import BytesIO

import pyignite.utils as ignite_utils


class BinaryStream:
    def __init__(self, stream, conn):
        self.stream = BytesIO(stream) if stream else BytesIO()
        self.conn = conn

    @property
    def compact_footer(self) -> bool:
        return self.conn.client.compact_footer

    @compact_footer.setter
    def compact_footer(self, value: bool):
        self.conn.client.compact_footer = value

    def read(self, size):
        buf = bytearray(size)
        self.stream.readinto(buf)
        return buf

    def write(self, buf):
        return self.stream.write(buf)

    def tell(self):
        return self.stream.tell()

    def seek(self, *args, **kwargs):
        return self.stream.seek(*args, **kwargs)

    def getvalue(self):
        return self.stream.getvalue()

    def mem_view(self, start, offset):
        return self.stream.getbuffer()[start:start+offset]

    def hashcode(self, start, bytes_len):
        return ignite_utils.hashcode(self.stream.getbuffer()[start:start+bytes_len])

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.stream.close()

    def get_dataclass(self, header):
        # get field names from outer space
        result = self.conn.client.query_binary_type(
            header.type_id,
            header.schema_id
        )
        if not result:
            raise RuntimeError('Binary type is not registered')
        return result

    def register_binary_type(self, *args, **kwargs):
        return self.conn.client.register_binary_type(*args, **kwargs)
