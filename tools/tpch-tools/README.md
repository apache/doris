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

## Usage

These scripts are used to make tpc-h test.
follow the steps below:

### 1. build tpc-h dbgen tool.

    ./bin/build-tpch-dbgen.sh

    If the build failed in dbgen tools' compilation, update your GCC version or change download link used by wget in build-tpch-dbgen.sh from ".../TPC-H_Tools_v3.0.0new.zip" to ".../TPC-H_Tools_v3.0.0.zip"

### 2. generate tpc-h data. use -h for more infomations.

    ./bin/gen-tpch-data.sh -s 1

### 3. create tpc-h tables. modify `conf/doris-cluster.conf` to specify doris info, then run script below.

    ./bin/create-tpch-tables.sh -s 1

### 4. load tpc-h data. use -h for help.

    ./bin/load-tpch-data.sh

### 5. run tpc-h queries.

    ./bin/run-tpch-queries.sh -s 1

    NOTICE: At present, Doris's query optimizer and statistical information functions are not complete, so we rewrite some queries in TPC-H to adapt to Doris' execution framework, but it does not affect the correctness of the results. The rewritten SQL is marked with "Modified" in the corresponding .sql file.

    A new query optimizer will be released in subsequent releases.

    Currently, differnt scales use the same suite of query sqls.
