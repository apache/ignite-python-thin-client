#
# Copyright 2021 GridGain Systems, Inc. and Contributors.
#
# Licensed under the GridGain Community Edition License (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from ..client import Client
from .dbcursor import DBCursor

class DBClient (Client):


    def close(self):
        """
        """
        # TODO: close ope cursors
        super.close()
    
    def commit(self):
        """
        Ignite doesn't have SQL transactions
        """
        pass
    
    def rollback(self):
        """
        Ignite doesn't have SQL transactions
        """
        pass
    
    def cursor(self):
        """
        Cursors work slightly differently in Ignite versus DBAPI, so
        we map from one to the other
        """
        return DBCursor(self)
        