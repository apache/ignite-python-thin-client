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
  :dedent: 8
  :lines: 39-50

ExpiryPolicy
============
File: `expiry_policy.py`_.

You can enable expiry policy (TTL) by two approaches.

Firstly, expiry policy can be set for entire cache by setting :py:attr:`~pyignite.datatypes.prop_codes.PROP_EXPIRY_POLICY`
in cache settings dictionary on creation.

.. literalinclude:: ../examples/expiry_policy.py
  :language: python
  :dedent: 12
  :lines: 72-75

.. literalinclude:: ../examples/expiry_policy.py
  :language: python
  :dedent: 12
  :lines: 81-89

Secondly, expiry policy can be set for all cache operations, which are done under decorator. To create it use
:py:meth:`~pyignite.cache.BaseCache.with_expire_policy`

.. literalinclude:: ../examples/expiry_policy.py
  :language: python
  :dedent: 12
  :lines: 96-105

Transactions
------------
File: `transactions.py`_.

Client transactions are supported for caches with
:py:attr:`~pyignite.datatypes.cache_config.CacheAtomicityMode.TRANSACTIONAL` mode.

Let's create transactional cache:

.. literalinclude:: ../examples/transactions.py
  :language: python
  :dedent: 8
  :lines: 29-32

Let's start a transaction and commit it:

.. literalinclude:: ../examples/transactions.py
  :language: python
  :dedent: 8
  :lines: 35-39

Let's check that the transaction was committed successfully:

.. literalinclude:: ../examples/transactions.py
  :language: python
  :dedent: 8
  :lines: 41-42

Let's check that raising exception inside `async with` block leads to transaction's rollback

.. literalinclude:: ../examples/transactions.py
  :language: python
  :dedent: 8
  :lines: 45-55

Let's check that timed out transaction is successfully rolled back

.. literalinclude:: ../examples/transactions.py
  :language: python
  :dedent: 8
  :lines: 58-68

See more info about transaction's parameters in a documentation of :py:meth:`~pyignite.aio_client.AioClient.tx_start`

SQL
---
File: `async_sql.py`_.

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



.. _expiry_policy.py: https://github.com/apache/ignite-python-thin-client/blob/master/examples/expiry_policy.py
.. _async_key_value.py: https://github.com/apache/ignite-python-thin-client/blob/master/examples/async_key_value.py
.. _async_sql.py: https://github.com/apache/ignite-python-thin-client/blob/master/examples/async_sql.py
.. _transactions.py: https://github.com/apache/ignite-python-thin-client/blob/master/examples/transactions.py