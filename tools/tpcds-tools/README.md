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

# Run TPCDS with one command

Install the Trino TPCDS generator on every FE and BE using the
[shared installation instructions](../benchmark/README.md#install-the-generators-once),
then configure `conf/doris-cluster.conf` and run:

```bash
./bin/run-tpcds.sh -s 1 -d tpcds_sf1
```

This streams generated rows directly into Doris, creates the existing benchmark
tables, builds `store_sales_flat`, `catalog_sales_flat`, and `web_sales_flat`,
collects statistics, and records three executions per query. It supports
SF1, SF100, SF1000, and SF10000. Preparation requires a new database.
Use `--queries-only` to measure existing data again, with the same scale factor.

See [shared workflow options, result files, and timing semantics](../benchmark/README.md).
TPCDS timings use the existing normalized-table queries. The additional wide tables
are prepared and checked for matching fact-row counts; their joins and column roles
are documented in the shared README. Install the plugin before running the command.

# Original file-based workflow

## Usage

These scripts are used to make tpc-ds test.
follow the steps below:

### 1. build tpc-ds dsdgen dsqgen tool.

    ./bin/build-tpcds-tools.sh

    If the build failed in dbgen tools' compilation, update your GCC version or change all "TPC-DS_Tools_v3.2.0new.zip" in build-tpcds-dbgen.sh to "TPC-DS_Tools_v3.2.0.zip"

### 2. generate tpc-ds data. use -h for more infomations.

    ./bin/gen-tpcds-data.sh -s 1

### 3. create tpc-ds tables. modify `conf/doris-cluster.conf` to specify doris info, then run script below.

    ./bin/create-tpcds-tables.sh -s 1

### 4. load tpc-ds data. use -h for help.

    ./bin/load-tpcds-data.sh

### 5. run tpc-ds queries.

    ./bin/run-tpcds-queries.sh -s 1
