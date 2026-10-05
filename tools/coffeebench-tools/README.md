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

These scripts are used to make coffee-bench test.
follow the steps below:

### 1. create tables. modify `conf/doris-cluster.conf` to specify doris info, then run script below.

    ./bin/create-tables.sh

### 2. load coffee-bench data. use -s for scale. "500m/1b/5b"

    ./bin/load-data.sh -s 500m

    **Note: The data loading process uses S3 data located in the US East (us-east-1) region.**
    If you cannot access this region due to network restrictions or other reasons, you can:
    1. Use AWS CLI to copy the data to an S3 bucket in your preferred region
    2. Download the data locally and use alternative methods to import it into Doris

### 3. run queries.

    ./bin/run-queries.sh

    NOTICE: At present, we rewrite some queries in coffee bench to adapt to Doris' execution framework, but it does not affect the correctness of the results.
