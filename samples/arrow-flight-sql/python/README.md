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


# Native VARIANT results

On branch-4.1 builds with native VARIANT support, enable it on the same Flight SQL
connection that executes the query:

```sql
SET enable_arrow_flight_sql_native_variant = true;
SELECT variant_column FROM example_table;
```

The default is `false`, which retains the existing UTF8 representation. When enabled,
VARIANT fields (including nested fields) use the `arrow.parquet.variant` extension
with `struct<metadata: binary not null, value: binary not null>` storage. SQL NULL is
a null struct. V2 Variant null is a non-null struct containing the encoded null value.
V2 values retain their binary representation. Supported legacy scalar roots use typed
encoding; legacy documents use JSON conversion with the existing null/missing semantics.
Decimal256 scalar roots are rejected because the wire format has no Decimal256 primitive.

ADBC can transport this schema and its binary values. A client without a registered
Variant extension exposes the struct with `ARROW:extension:name` field metadata.
Receiving native VARIANT does not automatically decode it to Python dictionaries or
pandas objects; use a Parquet Variant decoder, or keep the default string mode.

To check ADBC query and partition reads against a running cluster:

```bash
pip install adbc_driver_flightsql pyarrow
export DORIS_FLIGHT_URI='grpc://localhost:8815'
export DORIS_USER='root'
# Set DORIS_PASSWORD in the environment if authentication requires it.
python test_variant.py
```
