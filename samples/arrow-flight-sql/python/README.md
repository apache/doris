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

## Logical type metadata

Some Doris types share an Arrow storage type with ordinary strings or integers.
The `doris_type` field metadata identifies `LARGEINT`, `IPV4`, `IPV6`, `JSON`, and
`VARIANT`, including fields nested in arrays, maps, and structs. Consumers should
inspect each nested Arrow field instead of inferring a type from its value.

LARGEINT retains its Arrow string encoding, including the full signed 128-bit
range. PyArrow does not automatically convert custom metadata into Python types;
a client can use `doris_type=LARGEINT` to safely convert that field's non-NULL
values with `int(value)`, while leaving ordinary STRING fields unchanged.

During rolling upgrades, queries planned by older FEs retain the legacy metadata
layout. Upgraded FEs advertise complete logical type metadata in the FlightInfo
schema even when some BEs are older. An older BE's DoGet stream may still omit
these markers; use the FlightInfo schema as the logical type reference until the
upgrade finishes. No session setting is required.

Run `python test_nested_type_metadata.py` with `DORIS_FLIGHT_SQL_URI` and optional
`DORIS_USER` / `DORIS_PASSWORD` to verify the metadata and values against a cluster.
The tests execute only read-only queries.

# Notes

     For more details, refer to [Python Usage] in the document https://doris.apache.org/zh-CN/docs/dev/db-connect/arrow-flight-sql-connect


# Native Variant V2 results

On branch-4.1 builds with this feature and Variant V2 enabled on FE and BE, Arrow
Flight SQL / ADBC returns Variant V2 as native binary values automatically:

```sql
SELECT parse_to_variant('{"key":42}') AS v;
-- Request text explicitly when the client needs JSON strings.
SELECT CAST(variant_column AS STRING) FROM example_table;
```

Variant V2 fields, including nested fields, use the `arrow.parquet.variant` extension
with `struct<metadata: binary not null, value: binary not null>` storage. SQL NULL is
a null struct. Variant null is a non-null struct containing the encoded null value.
V2 values retain their physical scalar types and decimal scales. Each Arrow row carries
only the dictionary keys it uses, rather than copying keys from unrelated rows.

Legacy Variant is unsupported, including nested legacy fields, SQL NULL and empty
query results. Use an explicit SQL cast to STRING for text output. `parse_to_variant`
follows the configured Variant representation; it does not convert legacy storage to
V2. Native encoding accepts up to 128 nested levels.

Every registered BE must advertise native Variant support. During a rolling upgrade,
Variant queries and Variant schema discovery fail explicitly until that requirement
is met; there is no automatic UTF8 fallback. Non-Variant queries remain available.
Heartbeat discovery cannot make an in-place downgrade atomic with query planning;
drain active Variant queries before downgrading a BE.

ADBC can transport this schema and its binary values. A client without a registered
Variant extension exposes the struct with `ARROW:extension:name` field metadata.
Receiving native VARIANT does not automatically decode it to Python dictionaries or
pandas objects; use a Parquet Variant decoder, or explicitly cast the result to STRING.

To check ADBC query and partition reads against a running Variant V2 cluster:

```bash
pip install adbc_driver_flightsql pyarrow
export DORIS_FLIGHT_URI='grpc://localhost:8815'
export DORIS_USER='root'
# Set DORIS_PASSWORD in the environment if authentication requires it.
python test_variant.py
```
