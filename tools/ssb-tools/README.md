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

# Usage

    These scripts are used to make ssb and ssb flat test.
    The ssb flat data comes from ssb tables by way of 'INSERT INTO ... SELECT ...',
    which means ssb test steps 1 to 4 should have been done before loading ssb flat data.

## ssb test, follow the steps below:
### 1. build ssb dbgen tool.
    ./build-ssb-dbgen.sh
### 2. generate ssb data. use -h for more infomations.
    ./gen-ssb-data.sh -s 1
### 3. create ssb tables. modify `doris-cluster.conf` to specify doris info, then run script below.
    ./create-ssb-tables.sh
### 4. load ssb data. use -h for help.
    ./load-ssb-dimension-data.sh
    ./load-ssb-fact-data.sh
### 5. run ssb queries.
    ./run-ssb-queries.sh

## ssb flat test, follow the steps below:
### 1. prepare ssb data, which means ssb test steps 1 to 4 have been done.
### 2. create ssb flat table in the same database of ssb tables.
    ./create-ssb-flat-table.sh
### 3. load ssb flat data.
    ./load-ssb-flat-data.sh
### 4. run ssb flat queries.
    ./run-ssb-flat-queries.sh
