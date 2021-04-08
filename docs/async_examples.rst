..  Licensed to the Apache Software Foundation (ASF) under one or more
    contributor license agreements.  See the NOTICE file distributed with
    this work for additional information regarding copyright ownership.
    The ASF licenses this file to You under the Apache License, Version 2.0
    (the "License"); you may not use this file except in compliance with
    the License.  You may obtain a copy of the License at

..      http://www.apache.org/licenses/LICENSE-2.0

..  Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.

.. _async_examples_of_usage:

============================
Asynchronous client examples
============================
File: `async_key_value.py`_.

Basic usage
-----------
Asynchronous client and cache (:py:class:`~pyignite.aio_client.AioClient` and :py:class:`~pyignite.aio_cache.AioCache`)
has mostly the same API as synchronous ones (:py:class:`~pyignite.client.Client` and :py:class:`~pyignite.cache.Cache`).
But there is some peculiarities.

Basic key-value
===============
Firstly, import dependencies.

.. literalinclude:: ../examples/async_key_value.py
  :language: python
  :lines: 18

Let's connect to cluster and perform key-value queries.

.. literalinclude:: ../examples/async_key_value.py
  :language: python
  :dedent: 4
  :lines: 23-38

Scan
====
The :py:meth:`~pyignite.aio_cache.AioСache.scan` method returns :py:class:`~pyignite.cursors.AioScanCursor`,
that yields the resulting rows.

.. literalinclude:: ../examples/async_key_value.py
  :language: python
  :dedent: 4
  :lines: 39-50


File: `async_sql.py`_.

SQL
---

First let us establish a connection.

.. literalinclude:: ../examples/async_sql.py
  :language: python
  :dedent: 4
  :lines: 197-198

Then create tables. Begin with `Country` table, than proceed with related
tables `City` and `CountryLanguage`.

.. literalinclude:: ../examples/async_sql.py
  :language: python
  :lines: 25-42, 51-59, 67-74

.. literalinclude:: ../examples/async_sql.py
  :language: python
  :dedent: 4
  :lines: 199-205

Create indexes.

.. literalinclude:: ../examples/async_sql.py
  :language: python
  :lines: 60-62, 75-77

.. literalinclude:: ../examples/async_sql.py
  :language: python
  :dedent: 8
  :lines: 207-209

Fill tables with data.

.. literalinclude:: ../examples/async_sql.py
  :language: python
  :lines: 43-50, 63-66, 78-81

.. literalinclude:: ../examples/async_sql.py
  :language: python
  :dedent: 8
  :lines: 212-223

Now let us answer some questions.

What are the 10 largest cities in our data sample (population-wise)?
====================================================================

.. literalinclude:: ../examples/async_sql.py
  :language: python
  :dedent: 8
  :lines: 225-243

The :py:meth:`~pyignite.aio_client.AioClient.sql` method returns :py:class:`~pyignite.cursors.AioSqlFieldsCursor`,
that yields the resulting rows.

What are the 10 most populated cities throughout the 3 chosen countries?
========================================================================

If you set the `include_field_names` argument to `True`, the
:py:meth:`~pyignite.client.Client.sql` method will generate a list of
column names as a first yield. Unfortunately, there is no async equivalent of `next` but
you can await :py:meth:`__anext__()`
of :py:class:`~pyignite.cursors.AioSqlFieldsCursor`

.. literalinclude:: ../examples/async_sql.py
  :language: python
  :dedent: 8
  :lines: 246-271

Display all the information about a given city
==============================================

.. literalinclude:: ../examples/async_sql.py
  :language: python
  :dedent: 8
  :lines: 273-288

Finally, delete the tables used in this example with the following queries:

.. literalinclude:: ../examples/async_sql.py
  :language: python
  :lines: 83

.. literalinclude:: ../examples/async_sql.py
  :language: python
  :dedent: 8
  :lines: 290-297




.. _async_key_value.py: https://github.com/apache/ignite-python-thin-client/blob/master/examples/async_key_value.py
.. _async_sql.py: https://github.com/apache/ignite-python-thin-client/blob/master/examples/async_sql.py