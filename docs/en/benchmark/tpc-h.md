---
{
    "title": "TPC-H Benchmark Test",
    "language": "en"
}
---

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

# TPC-H Benchmark

[TPC-H](https://www.tpc.org/tpch/) is a test set developed by the Transaction Processing Performance Council (TPC) to simulate decision support applications. The TPC-H query contains 8 data tables, and the data volume can be set from 1GB to 3TB. Contains 22 complex SQL queries, most of which include several table joins, subqueries, and group-by aggregations.

The TPC-H benchmark test includes 22 queries (Q1~Q22), and its main evaluation index is the response time of each query, that is, the time from submitting the query to returning the result. The measurement unit of the TPC-H benchmark test is performed per hour Number of queries.

## Test Process

The test scripts are in the directory [tpch-tools](https://github.com/apache/incubator-doris/tree/master/tools/tpch-tools)

### 1. Compile the TPC-H generation tool

````
./build-tpch-dbgen.sh
````

### 2. Generate data

````
./gen-tpch-data.sh -s 1
````

### 3. Create a table in the doris cluster

```shell
#Doris-cluster.conf cluster configuration needs to be modified before execution
./create-tpch-tables.sh
````

### 4. Import data

````
./load-tpch-data.sh
````

### 5. Execute the query

````
./run-tpch-queries.sh
````

## Testing Report

The following test report is based on the Doris1.0 test and is for reference only.

1. Hardware environment

   - 1 FE + 3 BE
   - CPU: 16 core CPU
   - Memory: 64GB
   - Hard disk: SSD 1T
   - Network card: 10 Gigabit network card

2. Dataset

   The TPC-H test set scale is 100, and the generated raw data file is about 107G.

3. Test results

   | Unit (ms) | doris 1.0 (ms) |
   | --------- | -------------- |
   | q1        | 4215           |
   | q2        | 13633          |
   | q3        | 9677           |
   | q4        | 7087           |
   | q5        | 4290           |
   | q6        | 1045           |
   | q7        | 2147           |
   | q8        | 3073           |
   | q9        | 33064          |
   | q10       | 5733           |
   | q11       | 2598           |
   | q12       | 4998           |
   | q13       | 10798          |
   | q14       | 11786          |
   | q15       | 2038           |
   | q16       | 3313           |
   | q17       | 20340          |
   | q18       | 23277          |
   | q19       | 1645           |
   | q20       | 5738           |
   | q21       | 18520          |
   | q22       | 1041           |
