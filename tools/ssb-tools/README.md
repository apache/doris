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

# Run SSB with one command

The Trino SSB connector generates rows while Doris reads them. It uses the same
`ssb-dbgen` distribution as the existing tools, with stdout connected to the scan
through a bounded pipe. There are no intermediate `.tbl` files, Stream Load
requests, or separate Trino servers.

## Install the generator plugin once

Build on Linux with the same CPU architecture as the Doris nodes. The build needs
JDK 17, Maven, GCC, Make, curl, patch, and sha256sum:

```bash
./trino-ssb/build.sh
```

Copy the resulting **entire directory** `trino-ssb/target/ssb` into the configured
Trino plugin directory on **every FE and BE**. For current Doris installations:

```bash
cp -r trino-ssb/target/ssb /path/to/fe/plugins/trino_plugins/
cp -r trino-ssb/target/ssb /path/to/be/plugins/trino_plugins/
```

Start or restart these nodes after installing the plugin. It consists of a JAR,
the native `dbgen` executable, and its distribution file. The executable must
remain executable. No paths or executable commands are accepted as catalog properties.

## Create, import, and benchmark

Set the Doris connection in `conf/doris-cluster.conf`, then run:

```bash
./bin/run-ssb.sh -s 1 -d ssb_sf1
```

This creates a new database and its `ssb_gen_ssb_sf1` catalog, creates the five
SSB tables, and imports them using `INSERT INTO ... SELECT ...` from the catalog's
`sf1` schema. It then builds `lineorder_flat`, collects statistics synchronously,
and runs all 13 SSB queries and all 13 SSB Flat queries three times each.
The supported scale factors are 1, 100, and 1000. Use `--mode ssb` or `--mode flat`
to select one benchmark; the default is `both`.

Each run writes a new `results/<timestamp>-<pid>/` directory with:

- `result.csv`: first-run, two repeated-run, and best repeated-run times in milliseconds.
- `ssb/` and `ssb-flat/`: each query's output and errors.
- `prepare.log` and `environment.txt`: import diagnostics and Doris settings.

Timing includes the mysql client connection and result transfer, as in the old
scripts. The `cold` column means the first execution; the script does not flush
data caches. SQL result caching and BE query caching are disabled for every measured
session. A query or import error stops the run with a nonzero exit status.

Preparation requires a **new database**, so rerunning a failed or completed load
cannot append duplicate data. Tables and the catalog are preserved for inspection.
To rerun only the queries against completed data:

```bash
./bin/run-ssb.sh -d ssb_sf1 --queries-only
```

Use `-c /path/to/cluster.conf` for a separate connection configuration, and
`--result-dir /path/to/new-directory` for a custom output directory. Existing
result directories are never overwritten.

## Generator consistency and tests

The connector keeps the old tools' default of ten `lineorder` generator partitions.
Each dimension uses one generator. Changing the original dbgen partition count
changes some random streams, so partition count is intentionally fixed here.
The date generator runs in UTC on every BE. The native build enables dbgen's
existing 64-bit code path for large scale factors and avoids an out-of-bounds
usage-counter update for its SSB-only random streams. Neither change replaces
the data-generation algorithm.

The existing benchmark SQL is preserved. In particular, SSB Q1.3 filters the
generator's `d_weeknuminyear`, whereas SSB Flat Q1.3 uses `weekofyear()`; these week
definitions differ, so their aggregate results are not expected to match.

The plugin build runs its Java tests, including full SF1 checksums of all five tables,
handle serialization, projected columns, large keys, producer failures, and early
cursor closure. Run script orchestration tests with:

```bash
python3 -m unittest discover -s tests -v
```

# Original file-based workflow

    These scripts are used to make ssb and ssb flat test.
    The ssb flat data comes from ssb tables by way of 'INSERT INTO ... SELECT ...'.

## ssb test, follow the steps below:
### 1. build ssb dbgen tool.
    ./bin/build-ssb-dbgen.sh
### 2. generate ssb data. use -h for more infomations.
    ./bin/gen-ssb-data.sh -s 1
### 3. create ssb tables. modify `conf/doris-cluster.conf` to specify Doris cluster info, then run script below.
    ./bin/create-ssb-tables.sh -s 1
### 4. load ssb data. use -h for help.
    ./bin/load-ssb-data.sh
### 5. run ssb queries.
    ./bin/run-ssb-queries.sh

## ssb flat test, follow the steps below:
### 1. prepare ssb data, which means ssb test steps 1 to 4 have been done.
### 2. run ssb flat queries.
    ./bin/run-ssb-flat-queries.sh
