---
{
    "title": "TPC-H Benchmark",
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

# TPC-H benchmark

TPC-H is a Decision Support Benchmark consisting of a set of business-oriented ad hoc queries and concurrent data modifications. The data that queries and populates the database has broad industry relevance. This benchmark demonstrates a decision support system that examines large amounts of data, executes highly complex queries, and answers critical business questions. The performance metric reported by TPC-H is called the TPC-H Hourly Compound Query Performance Metric (QphH@Size) and reflects multiple aspects of the system's ability to process queries. These aspects include the database size chosen when executing the query, the query processing power when the query is submitted by a single stream, and the query throughput when the query is submitted by multiple concurrent users.

This document mainly introduces the performance of Doris on the TPC-H test set.

> Note 1: Standard test sets including TPC-H are usually far from actual business scenarios, and some tests will perform parameter tuning for the test set. Therefore, the test results of the standard test set can only reflect the performance of the database in specific scenarios. Users are advised to conduct further testing with actual business data.
>
> Note 2: The operations covered in this document are tested on CentOS 7.x.

On 22 queries on the TPC-H standard test dataset, we tested the upcoming Doris 1.1 version and Doris 0.15.0 RC04 version side by side, and the overall performance improved by 3-4 times. In individual scenarios, it can achieve a ten-fold improvement.

![image-20220614114351241](/images/image-20220614114351241.png)

## 1. Hardware Environment

| Hardware           | Configuration Instructions                                   |
| -------- | ------------------------------------ |
| number of machines | 4 Alibaba Cloud hosts (1 FE, 3 BE) |
| CPU      | Intel Xeon(Cascade Lake) Platinum 8269CY  16C  (2.5 GHz/3.2 GHz) |
| Memory | 64G                                  |
| Network | 5Gbps                              |
| Disk   | ESSD cloud hard disk  |

## 2. Software Environment

- Doris deploys 3BE 1FE;
- Kernel version: Linux version 5.4.0-96-generic (buildd@lgw01-amd64-051)
- OS version: CentOS 7.8
- Doris software version: Apache Doris 1.1, Apache Doris 0.15.0 RC04
- JDK: openjdk version "11.0.14" 2022-01-18

## 3. Test Data Volume

The entire test simulation generates 100G of data and is imported into Doris 0.15.0 RC04 and Doris 1.1 versions for testing. The following is the relevant description of the table and the amount of data.

| TPC-H Table Name | Rows        | data size  | remark |
| :--------------- | :---------- | ---------- | :----- |
| REGION           | 5           | 400KB      |        |
| NATION           | 25          | 7.714 KB   |        |
| SUPPLIER         | 100 million | 85.528 MB  |        |
| PART             | 20 million  | 752.330 MB |        |
| PARTSUPP         | 80 million  | 4.375 GB   |        |
| CUSTOMER         | 15 million  | 1.317 GB   |        |
| ORDERS           | 1.5 billion | 6.301 GB   |        |
| LINEITEM         | 6 billion   | 20.882 GB  |        |

## 4. Test SQL

TPCH 22 test query statements ： [TPCH-Query-SQL](https://github.com/apache/doris/tree/master/tools/tpch-tools/queries)

Notice:

The following four parameters in the above SQL do not exist in 0.15.0 RC04. When executed in 0.15.0 RC04, remove them:

```
1. enable_vectorized_engine=true,
2. batch_size=4096,
3. disable_join_reorder=false
4. enable_projection=true
```

## 5. Test Result

Here we use the upcoming Doris-1.1 version and Doris-0.15.0 RC04 version for comparative testing. The test results are as follows:

| Query     | Doris-1.1(s) | 0.15.0 RC04(s) |
| --------- | ------------ | -------------- |
| Q1        | 3.75         | 28.63          |
| Q2        | 4.22         | 7.88           |
| Q3        | 2.64         | 9.39           |
| Q4        | 1.5          | 9.3            |
| Q5        | 2.15         | 4.11           |
| Q6        | 0.19         | 0.43           |
| Q7        | 1.04         | 1.61           |
| Q8        | 1.75         | 50.35          |
| Q9        | 7.94         | 16.34          |
| Q10       | 1.41         | 5.21           |
| Q11       | 0.35         | 1.72           |
| Q12       | 0.57         | 5.39           |
| Q13       | 8.15         | 20.88          |
| Q14       | 0.3          |                |
| Q15       | 0.66         | 1.86           |
| Q16       | 0.79         | 1.32           |
| Q17       | 1.51         | 26.67          |
| Q18       | 3.364        | 11.77          |
| Q19       | 0.829        | 1.71           |
| Q20       | 2.77         | 5.2            |
| Q21       | 4.47         | 10.34          |
| Q22       | 0.9          | 3.22           |
| **total** | **51.253**   | **223.33**     |

- **Result description**
  - The data set corresponding to the test results is scale 100, about 600 million.
  - The test environment is configured to be commonly used by users, including 4 cloud servers, 16-core 64G SSD, and 1 FE and 3 BE deployment.
  - Use common user configuration tests to reduce user selection and evaluation costs, but will not consume so many hardware resources during the entire test process.
  - The test results are averaged over 3 executions. And the data has been fully compacted (if the data is tested immediately after the data is imported, the query delay may be higher than the test result, and the speed of compaction is being continuously optimized, and will be significantly reduced in the future).
  - 0.15 RC04 Q14 execution failed in TPC-H test, unable to complete query.

## 6. Environmental Preparation

Please refer to the [official document](../install/install-deploy.md) to install and deploy Doris to obtain a normal running Doris cluster (at least 1 FE 1 BE, 1 FE 3 BE is recommended).

## 7. Data Preparation

### 7.1 Download and install the TPC-H data generation tool

Execute the following script to download and compile the [tpch-tools](https://github.com/apache/doris/tree/master/tools/tpch-tools) tool.

```shell
sh bin/build-tpch-dbgen.sh
```

After successful installation, the `dbgen` binary will be generated in the `TPC-H_Tools_v3.0.0/` directory.

### 7.2 Generate TPC-H test set

Execute the following script to generate the TPC-H dataset:

```shell
sh bin/gen-tpch-data.sh
```

> Note 1: View script help via `sh bin/gen-tpch-data.sh -h`.
>
> Note 2: The data will be generated in the `bin/tpch-data/` directory with the suffix `.tbl`. The total file size is about 100GB. The generation time may vary from a few minutes to an hour.
>
> Note 3: The standard test data set of 100G is generated by default

### 7.3 Create Table

#### 7.3.1 Prepare the `doris-cluster.conf` file

Before calling the import script, you need to write the FE's ip port and other information in the `conf/doris-cluster.conf` file.

File location and `load-tpch-data.sh` level.

The contents of the file include FE's ip, HTTP port, user name, password and the DB name of the data to be imported:

```shell
# Any of FE host
export FE_HOST='127.0.0.1'
# http_port in fe.conf
export FE_HTTP_PORT=8030
# query_port in fe.conf
export FE_QUERY_PORT=9030
# Doris username
export USER='root'
# Doris password
export PASSWORD=''
# The database where TPC-H tables located
export DB='tpch'
```

#### 7.3.2 Execute the following script to generate and create the TPC-H table

```shell
sh create-tpch-tables.sh
```
Or copy the table creation statement in [create-tpch-tables.sql](https://github.com/apache/doris/blob/master/tools/tpch-tools/ddl/create-tpch-tables.sql), Execute in Doris.


### 7.4 导入数据

通过下面的命令执行数据导入：

```shell
sh bin/load-tpch-data.sh
```

### 7.5 Check Imported Data

Execute the following SQL statement to check the imported data volume is consistent with the above data volume.

```sql
select count(*)  from  lineitem;
select count(*)  from  orders;
select count(*)  from  partsupp;
select count(*)  from  part;
select count(*)  from  customer;
select count(*)  from  supplier;
select count(*)  from  nation;
select count(*)  from  region;
select count(*)  from  revenue0;
```

### 7.6 Query Test

Execute the above test SQL or execute the following command

```
sh bin/run-tpch-queries.sh
```

>Notice:
>
>1. At present, the query optimizer and statistics functions of Doris are not perfect, so we rewrite some queries in TPC-H to adapt to the execution framework of Doris, but it does not affect the correctness of the results
>
>2. Doris' new query optimizer will be released in subsequent versions
