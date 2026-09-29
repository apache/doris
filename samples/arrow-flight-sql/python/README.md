<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# How to use:

	1. pip install adbc_driver_manager
       pip install adbc_driver_flightsql
    2. Modify my_uri, my_db_kwargs, sql in test.py
    3. python test.py

# What can this demo do:

	This is a python demo for doris arrow flight sql, you can use this to test various connection
    methods for sending queries to the doris arrow flight server, help you understand how to use arrow flight sql
    and test performance.

# Performance test

    Section 6.1 of https://github.com/apache/doris/issues/25514 is the performance test
    results of the doris arrow flight sql using python.

# Notes

     For more details, refer to [Python Usage] in the document https://doris.apache.org/zh-CN/docs/dev/db-connect/arrow-flight-sql-connect


## MAP values with NULL keys

Doris permits NULL map keys, but the Arrow MAP type does not. On the same Flight SQL
connection, enable the lossless list representation before executing such queries:

```sql
SET arrow_flight_sql_map_as_list = true;
SELECT map(CAST(NULL AS STRING), 100) AS m;
```

The result is an Arrow `List<Struct<key, value>>` containing
`[{"key": null, "value": 100}]`. The setting applies recursively to all MAP types,
including maps inside arrays, structs, and map values. It preserves NULL maps,
empty maps, NULL keys, and NULL values. The schema is fixed before any batch is read;
even maps without NULL keys use the list representation while the setting is enabled.
Flight SQL table-schema metadata uses the same setting.

The default is `false`, preserving the existing Arrow MAP schema for clients that
expect it. MySQL results and external table writers are unaffected. For a single
expression, `map_entries(m)` is also available without changing the session setting.
Use matching FE and BE versions that support the setting before enabling it.

Run the MAP compatibility integration tests against a test cluster by setting
`DORIS_FLIGHT_SQL_URI`, and optionally `DORIS_USER` and `DORIS_PASSWORD`, then running
`python test_map_null_keys.py`. The tests use read-only queries and session settings;
they do not create tables.
