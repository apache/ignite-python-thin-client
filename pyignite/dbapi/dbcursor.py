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

from ..cursors import SqlCursor

class DBCursor(object):

    def __init__(self, connection):
        self.connection = connection
        self.cursor = None
        self.rowcount = -1
    
    @property
    def description(self):
#         columns =  self._columns
#         types =  [ bool ]

        return [
            [name, None, None, None, None, None, True]
            for name in self._columns
#             for name, type_code in zip(columns, types)
        ]

    def close(self):
        """
        Close the cursor now. The cursor will be unusable from this point
        forward; an :data:`~pyignite.dbapi.Error` (or subclass)
        exception will be raised if any operation is attempted with the
        cursor.
        """
#         self.connection.disconnect()
#         self._state = self._states.CURSOR_CLOSED

#         try:
#             # cursor can be already closed
#             self.connection.cursors.remove(self)
#         except ValueError:
#             pass

    def execute(self, operation, parameters=None):
        """
        Prepare and execute a database operation (query or command).

        :param operation: query or command to execute.
        :param parameters: sequence or mapping that will be bound to
                           variables in the operation.
        :return: None
        """
        self.cursor = self.connection.sql(operation, query_args=parameters, include_field_names=True)
        self._columns = next(self.cursor)

    def executemany(self, operation, seq_of_parameters):
        """
        Prepare a database operation (query or command) and then execute it
        against all parameter sequences found in the sequence
        `seq_of_parameters`.

        :param operation: query or command to execute.
        :param seq_of_parameters: sequences or mappings for execution.
        :return: None
        """
        pass

    def fetchone(self):
        """
        Fetch the next row of a query result set, returning a single sequence,
        or None when no more data is available.

        :return: the next row of a query result set or None.
        """
        if self.cursor is not None:
            return next(self.cursor)
        else:
            return None

    def fetchmany(self, size=None):
        """
        Fetch the next set of rows of a query result, returning a sequence of
        sequences (e.g. a list of tuples). An empty sequence is returned when
        no more rows are available.

        :param size: amount of rows to return.
        :return: list of fetched rows or empty list.
        """
        self._check_query_started()

        if size is None:
            size = self.arraysize

        if self._stream_results:
            if size == -1:
                return list(self._rows)
            else:
                return list(islice(self._rows, size))

        if size < 0:
            rv = self._rows
            self._rows = []
        else:
            rv = self._rows[:size]
            self._rows = self._rows[size:]

        return rv

    def fetchall(self):
        """
        Fetch all (remaining) rows of a query result, returning them as a
        sequence of sequences (e.g. a list of tuples).

        :return: list of fetched rows.
        """
        if self.cursor != None:
            rows = []
            for row in self.cursor:
              rows.append(row)
        else:
            return None # error?

        return rows

    def setinputsizes(self, sizes):
        # Do nothing.
        pass

    def setoutputsize(self, size, column=None):
        # Do nothing.
        pass
