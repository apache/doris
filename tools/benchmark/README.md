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

# Streaming benchmark workflows

The SSB, TPCH, and TPCDS entry points create a new Doris database, generate rows
while Doris scans a Trino catalog, import with `INSERT INTO ... SELECT ...`,
collect statistics synchronously, and execute each existing query three times.
No intermediate data files or separate Trino server are needed.

| Entry point | Scale factors | Tables | Query files |
| --- | --- | --- | --- |
| `../ssb-tools/bin/run-ssb.sh` | 1, 100, 1000 | Five SSB tables and `lineorder_flat` | 13 SSB + 13 SSB Flat |
| `../tpch-tools/bin/run-tpch.sh` | 1, 100, 1000, 10000 | Eight TPCH tables, `revenue0` view, and `lineitem_flat` | 22 |
| `../tpcds-tools/bin/run-tpcds.sh` | 1, 100, 1000, 10000 | 24 TPCDS tables and three sales flat tables | 103, including the second statements of 14, 23, 24, and 39 |

TPCH and TPCDS retain their existing normalized schemas and query SQL. Their
wide tables are additional prepared datasets; the measured query suites still
read the normalized tables. SSB measures both its original and flat-table queries.

## Wide tables

Preparation automatically builds the following tables after loading the base data:

- TPCH `lineitem_flat`: one row per line item, joined to orders, part, partsupp,
  customer, supplier, and both customer/supplier nation and region. Nation/region
  columns use `c_n_`/`c_r_` and `s_n_`/`s_r_` prefixes to distinguish the roles.
- TPCDS `store_sales_flat`, `catalog_sales_flat`, and `web_sales_flat`: one row per
  sale, joined to sold date/time, item, customer, customer demographics, household
  demographics, customer address, and promotion. Store sales also join store;
  catalog sales join call center, catalog page, ship mode, and warehouse; web
  sales join web site, web page, ship mode, and warehouse. Catalog/web customer
  attributes refer to the **bill-to** keys. Other date/customer roles and returns
  remain in the base tables and are not joined into these wide tables.

All these joins are LEFT JOINs on dimension keys, including the composite
partsupp key, so NULL foreign keys retain their fact rows. The runner verifies
that every wide table has exactly as many rows as its fact table and writes
`flat-row-counts.tsv`; a mismatch aborts the run. Base-table statistics are
collected in full before the joins. Wide-table statistics use synchronous
100,000-row sampling before query timing: repeatedly scanning every denormalized
column in full can dominate preparation even at SF1. Sampling affects optimizer
statistics only; imports and row-count checks still cover every row. For an
exhaustive wide-table analysis, run `ANALYZE TABLE <table> WITH FULL WITH SYNC`
after preparation.
Wide tables use one replica and automatic bucket counts. They require extra disk
space; the original query suite does not measure wide-table query performance.

## Install the generators once

For SSB, follow [the SSB plugin build instructions](../ssb-tools/README.md).
For TPCH/TPCDS, download the complete upstream plugin distributions with `curl`,
`unzip`, and `sha256sum` installed:

```bash
./download-tpc-plugins.sh
cp -r target/trino-tpch-435 target/trino-tpcds-435 /path/to/fe/plugins/trino_plugins/
cp -r target/trino-tpch-435 target/trino-tpcds-435 /path/to/be/plugins/trino_plugins/
```

Install both directories on **every FE and BE**, then restart those nodes. The
script pins Trino 435 to match Doris's Trino SPI and verifies each archive's
SHA-256 before extracting it. Keep all bundled runtime JARs in each directory;
copying only the connector JAR is insufficient. Downloads happen only during
this operator installation step, never from catalog properties.

These plugins use Trino's Java TPCH/TPCDS generators. They are distinct from the
native generator versions used by the older file-based scripts; their outputs
are not asserted to be byte-for-byte identical to those tools. TPCH explicitly
uses standard prefixed column names and DECIMAL monetary values. Each import
selects target column names explicitly, so different source and destination
column orders do not reorder the data. The TPCDS import also maps Trino 435's `p_response_targe` spelling to the existing Doris
`promotion.p_response_target` column.

## Run

Set each suite's `conf/doris-cluster.conf`, or pass a separate connection file:

```bash
../tpch-tools/bin/run-tpch.sh -s 1 -d tpch_sf1 -c /path/to/cluster.conf
../tpcds-tools/bin/run-tpcds.sh -s 1 -d tpcds_sf1 -c /path/to/cluster.conf
../ssb-tools/bin/run-ssb.sh -s 1 -d ssb_sf1 -c /path/to/cluster.conf
```

TPCH/TPCDS generation defaults to `max(10, SCALE)` splits per table:

| Scale factor | Default generator splits |
| --- | --- |
| 1 | 10 |
| 100 | 100 |
| 1000 | 1000 |
| 10000 | 10000 |

Use `--splits COUNT` to override this count during preparation, for example:

```bash
../tpch-tools/bin/run-tpch.sh -s 1000 -d tpch_sf1000 --splits 256
```

`COUNT` must be an integer from 1 to 2147483647. The selected count is recorded
in `prepare.log`; the number of tasks actually executing at once depends on
cluster resources. `--queries-only` does not recreate or change the generator
catalog. SSB does not accept `--splits`: it retains the original generator's ten
lineorder partitions because changing the partition count changes its random
data streams. SSB dimension tables use one split each.

Preparation requires a **new database**. An existing database is rejected before
any tables are changed, preventing duplicate appends after a partial load.
Imports use strict mode and zero filtered rows. Each INSERT must return a
`status` field equal to `VISIBLE`; errors, missing status, or any other status
(including `COMMITTED`) abort preparation. Failed runs preserve tables and logs
for inspection. Do not rerun preparation into that database.

To repeat measurements against completed data, use `--queries-only`. For TPCDS,
pass the same scale factor as the imported data, because query constants differ
by scale:

```bash
../tpcds-tools/bin/run-tpcds.sh -s 100 -d tpcds_sf100 --queries-only
```

SSB also accepts `--mode ssb|flat|both` (default `both`). This option is rejected
by TPCH and TPCDS. Existing file-based entry points remain available.

## Results and timing

Each run creates a fresh `<suite>-tools/results/<timestamp>-<pid>/` directory.
Use `--result-dir /path/to/new-directory` to select another location; existing
result directories are rejected rather than overwritten.

- `result.csv`: suite, query, first-run milliseconds, two repeated-run times, and
  the minimum repeated-run time.
- `<suite>/q*.cold.out`, `q*.hot1.out`, `q*.hot2.out`: query results, with matching
  `.err` files for diagnostics. TPCDS variants have distinct names such as `q14_1`.
- `prepare.log`: DDL, import, and statistics diagnostics.
- `flat-row-counts.tsv`: validated TPCH/TPCDS wide-table row counts.
- `environment.txt`: Doris version, session variables, and table status.

Timing includes mysql client startup, connection, execution, and result transfer.
The `cold_ms` column means the **first execution**; no storage or OS caches are
flushed. SQL result caching and BE query result caching are disabled in every
measured session. A query failure stops immediately and never produces a
successful timing row for that query. These are engineering measurements, not
certified TPC benchmark results.

## Orchestration tests

From the repository root:

```bash
python3 -m unittest discover -s tools/benchmark/tests -v
```

The tests use a fake mysql client to exercise complete suite selection, split
queries, explicit column projection, scale-specific SQL, cache settings, result
isolation, wide-table preparation, row-count checks, and failure paths. They do not replace a real Doris import/query run.
