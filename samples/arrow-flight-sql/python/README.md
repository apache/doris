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


# Native VARIANT results

On branch-4.1 builds with native VARIANT support, enable it on the same Flight SQL
connection that executes the query:

```sql
SET enable_arrow_flight_sql_native_variant = true;
SELECT variant_column FROM example_table;
```

The default is `false`, which retains the existing UTF8 representation. When enabled
and every registered BE has advertised native Variant support in its heartbeat,
VARIANT fields (including nested fields) use the `arrow.parquet.variant` extension
with `struct<metadata: binary not null, value: binary not null>` storage. SQL NULL is
a null struct. V2 Variant null is a non-null struct containing the encoded null value.
V2 values retain their physical scalar types and decimal scales. Each Arrow row carries
only the dictionary keys it uses, rather than copying keys from unrelated rows.
Legacy roots use recursive typed encoding,
including MAP, STRUCT, ARRAY, VARBINARY, TIMEV2 and nested VARIANT values. VARBINARY
retains its original bytes. MAP keys become object field names; NULL keys are rejected
because they cannot be distinguished from a literal `"null"` object key. TIMEV2 values
in `[00:00:00, 24:00:00)` retain their microseconds as a native Variant time value;
negative and longer durations are rejected because Parquet TIME is a time of day.
Use `enable_arrow_flight_sql_native_variant=false` to read these unsupported values.
Legacy document fields also use typed encoding, preserving DECIMAL precision and
DATE identity across dense paths, sparse paths and document snapshots. Existing
null/missing semantics are retained. In particular,
a legacy null root remains an empty object, including in a scalar-only batch; outer
SQL NULL remains a null struct.
Decimal256 scalar roots are rejected because the wire format has no Decimal256 primitive.
Native encoding currently accepts at most 128 nested levels. Deeper legacy documents
remain readable with `enable_arrow_flight_sql_native_variant=false`.
During a rolling upgrade, missing support on any registered BE keeps both query results
and GetTables metadata in UTF8 mode, including when an older BE may proxy a result.
Capability follows the last successful heartbeat during tolerated heartbeat failures.
When heartbeat failures mark a BE dead, its capability is cleared on every FE until a
successful heartbeat advertises support again. A successful heartbeat from an older BE
also clears the capability. These changes affect newly planned queries; outstanding
Flight tickets are not migrated across BE replacement. Heartbeat discovery cannot make
an in-place downgrade atomic with query planning. Drain active queries and stop new
native-mode queries before downgrading a BE.

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
