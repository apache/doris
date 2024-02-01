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

These scripts are used to make tpc-ds test.
follow the steps below:

### 1. build tpc-ds dsdgen dsqgen tool.

    export PATH=/usr/bin/:$PATH
    ./bin/build-tpcds-tools.sh

### 2. generate tpc-ds data. use -h for more infomations.

    ./bin/gen-tpcds-data.sh -s 1

### 3. generate tpc-ds queries. use -h for more infomations.

    ./bin/gen-tpcds-queries.sh -s 1

### 4. create tpc-ds tables. modify `conf/doris-cluster.conf` to specify doris info, then run script below.

    ./bin/create-tpcds-tables.sh -s 1

### 5. load tpc-ds data. use -h for help.

    ./bin/load-tpcds-data.sh

### 6. run tpc-ds queries.

    ./bin/run-tpcds-queries.sh -s 1
