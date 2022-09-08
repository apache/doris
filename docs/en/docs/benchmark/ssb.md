---
{
    "title": "Star-Schema-Benchmark",
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

# Star Schema Benchmark

[Star Schema Benchmark(SSB)](https://www.cs.umb.edu/~poneil/StarSchemaB.PDF) is a performance test set in a lightweight data warehouse scenario. Based on [TPC-H](http://www.tpc.org/tpch/), SSB provides a simplified version of the star schema dataset, which is mainly used to test the performance of multi-table association queries under the star schema. . In addition, the industry usually flattens SSB as a wide table model (hereinafter referred to as: SSB flat) to test the performance of the query engine, refer to [Clickhouse](https://clickhouse.com/docs/zh/getting-started /example-datasets/star-schema).

This document mainly introduces the performance of Doris on the SSB test set.

> Note 1: The standard test set including SSB is usually far from the actual business scenario, and some tests will perform parameter tuning for the test set. Therefore, the test results of the standard test set can only reflect the performance of the database in specific scenarios. Users are advised to conduct further testing with actual business data.
>
> Note 2: The operations involved in this document are all performed in the Ubuntu Server 20.04 environment, and CentOS 7 can also be tested.

On 13 queries on the SSB standard test dataset, we tested the upcoming Doris 1.1 version and Doris 0.15.0 RC04 version peer-to-peer, and the overall performance improved by 2-3 times.

![ssb_v11_v015_compare](/images/ssb_v11_v015_compare.png)

## 1. Hardware Environment

| Number of machines | 4 Tencent Cloud hosts (1 FE, 3 BE)        |
| ------------------ | ----------------------------------------- |
| CPU                | AMD EPYC™ Milan (2.55GHz/3.5GHz) 16 cores |
| Memory             | 64G                                       |
| Network Bandwidth  | 7Gbps                                     |
| Disk               | High-performance cloud disk               |

## 2. Software Environment

- Doris deploys 3BE 1FE;
- Kernel version: Linux version 5.4.0-96-generic (buildd@lgw01-amd64-051)
- OS version: Ubuntu Server 20.04 LTS 64 bit
- Doris software version: Apache Doris 1.1, Apache Doris 0.15.0 RC04
- JDK: openjdk version "11.0.14" 2022-01-18

## 3. Test data volume

| SSB table name | number of rows | remarks                          |
| :------------- | :------------- | :------------------------------- |
| lineorder      | 600,037,902    | Commodity order list             |
| customer       | 3,000,000      | Customer Information Sheet       |
| part           | 1,400,000      | Parts Information Sheet          |
| supplier       | 200,000        | Supplier Information Sheet       |
| date           | 2,556          | Date table                       |
| lineorder_flat | 600,037,902    | Wide table after data flattening |

## 4. Test Results

Here we use the upcoming Doris-1.1 version and Doris-0.15.0 RC04 version for comparative testing. The test results are as follows:

| Query | Doris-1.1(ms) | Doris-0.15.0 RC04(ms) |
| ----- | ------------- | --------------------- |
| Q1.1  | 90            | 250                   |
| Q1.2  | 10            | 30                    |
| Q1.3  | 70            | 120                   |
| Q2.1  | 360           | 900                   |
| Q2.2  | 340           | 1020                  |
| Q2.3  | 260           | 770                   |
| Q3.1  | 550           | 1710                  |
| Q3.2  | 290           | 670                   |
| Q3.3  | 240           | 550                   |
| Q3.4  | 20            | 30                    |
| Q4.1  | 480           | 1250                  |
| Q4.2  | 240           | 400                   |
| Q4.3  | 200           | 330                   |

**Interpretation of results**

- The data set corresponding to the test results is scale 100, about 600 million.
- The test environment is configured to be commonly used by users, including 4 cloud servers, 16-core 64G SSD, and 1 FE and 3 BE deployment.
- Use common user configuration tests to reduce user selection and evaluation costs, but will not consume so many hardware resources during the entire test process.
- The test results are averaged over 3 executions. And the data has been fully compacted (if the data is tested immediately after the data is imported, the query delay may be higher than the test result, and the speed of compaction is being continuously optimized and will be significantly reduced in the future).

## 5. Environment Preparation

Please refer to the [official document](../install/install-deploy.md) to install and deploy Doris to obtain a normal running Doris cluster (at least 1 FE 1 BE, 1 FE 3 BE is recommended).

You can modify BE's configuration file be.conf to add the following configuration items and restart BE for better query performance.

```shell
enable_storage_vectorization=true
enable_low_cardinality_optimize=true
```

The scripts covered in the following documents are stored in `tools/ssb-tools/` in the Doris codebase.

> **Notice:**
>
> The above two parameters do not have these two parameters in version 0.15.0 RC04 and do not need to be configured.

## 6. Data Preparation

### 6.1 Download and install the SSB data generation tool.

Execute the following script to download and compile the [ssb-dbgen](https://github.com/electrum/ssb-dbgen.git) tool.

```shell
bash bin/build-ssb-dbgen.sh
````

After successful installation, the `dbgen` binary will be generated in the `bin/ssb-dbgen/` directory.

### 6.2 Generate SSB test set

Execute the following script to generate the SSB dataset:

```shell
bash bin/gen-ssb-data.sh
````

> Note 1: See script help with `bash gen-ssb-data.sh -h`.The default scale factor is 100 (referred to as sf100 for short). By default, it takes 6 minutes to generate 10 data files, namely `bash bin/gen-ssb-data.sh -s 100 -c 10`.
>
> Note 2: The data will be generated in the directory `bin/ssb-data/` with the suffix`. tbl`. The total file size is about 60GB. The generation time may vary from several minutes to one hour, and the information of the generated files will be listed after the generation is completed.
>
> Note 3: `-s 100` indicates that the test set scale factor is 100, `-c 10` indicates that 10 concurrent threads generate data for the lineorder table. The `-c` parameter also determines the number of files in the final lineorder table. The larger the parameter, the larger the number of files and the smaller each file. Use the default parameters to test sf100, and `-s1000 -c100` to test sf1000.

With the `-s 100` parameter, the resulting dataset size is:

| Table     | Rows             | Size | File Number |
| --------- | ---------------- | ---- | ----------- |
| lineorder | 6亿（600037902） | 60GB | 10         |
| customer  | 300万（3000000） | 277M | 1           |
| part      | 140万（1400000） | 116M | 1           |
| supplier  | 20万（200000）   | 17M  | 1           |
| dates     | 2556            | 228K | 1           |

### 6.3 Create table

#### 6.3.1 Prepare the `conf/doris-cluster.conf` file.

Before calling the import script, you need to write the FE's ip port and other information in the `conf/doris-cluster.conf` file.

The contents of the file include FE's ip, HTTP port, user name, password and the DB name of the data to be imported:

```shell
export FE_HOST="127.0.0.1"
export FE_HTTP_PORT="8030"
export FE_QUERY_PORT="9030"
export USER="root"
export PASSWORD=""
export DB="ssb"
````

#### 6.3.2 Execute the following script to generate and create the SSB table:

```shell
bash bin/create-ssb-tables.sh
````

Or copy the build table in [create-ssb-tables.sql](https://github.com/apache/incubator-doris/tree/master/tools/ssb-tools/ddl/create-ssb-tables.sql) Statement, executed in Doris.
copy [create-ssb-flat-table.sql](https://github.com/apache/incubator-doris/tree/master/tools/ssb-tools/ddl/create-ssb-flat-table.sql) The table building statement in , executed in Doris.

Below is the `lineorder_flat` table building statement. The "lineorder_flat" table is created in the above `bin/create-ssb-flat-table.sh` script with the default number of buckets (48 buckets). You can delete this table and adjust the number of buckets according to your cluster size node configuration, so as to obtain a better test effect.

```sql
CREATE TABLE `lineorder_flat` (
  `LO_ORDERDATE` date NOT NULL COMMENT "",
  `LO_ORDERKEY` int(11) NOT NULL COMMENT "",
  `LO_LINENUMBER` tinyint(4) NOT NULL COMMENT "",
  `LO_CUSTKEY` int(11) NOT NULL COMMENT "",
  `LO_PARTKEY` int(11) NOT NULL COMMENT "",
  `LO_SUPPKEY` int(11) NOT NULL COMMENT "",
  `LO_ORDERPRIORITY` varchar(100) NOT NULL COMMENT "",
  `LO_SHIPPRIORITY` tinyint(4) NOT NULL COMMENT "",
  `LO_QUANTITY` tinyint(4) NOT NULL COMMENT "",
  `LO_EXTENDEDPRICE` int(11) NOT NULL COMMENT "",
  `LO_ORDTOTALPRICE` int(11) NOT NULL COMMENT "",
  `LO_DISCOUNT` tinyint(4) NOT NULL COMMENT "",
  `LO_REVENUE` int(11) NOT NULL COMMENT "",
  `LO_SUPPLYCOST` int(11) NOT NULL COMMENT "",
  `LO_TAX` tinyint(4) NOT NULL COMMENT "",
  `LO_COMMITDATE` date NOT NULL COMMENT "",
  `LO_SHIPMODE` varchar(100) NOT NULL COMMENT "",
  `C_NAME` varchar(100) NOT NULL COMMENT "",
  `C_ADDRESS` varchar(100) NOT NULL COMMENT "",
  `C_CITY` varchar(100) NOT NULL COMMENT "",
  `C_NATION` varchar(100) NOT NULL COMMENT "",
  `C_REGION` varchar(100) NOT NULL COMMENT "",
  `C_PHONE` varchar(100) NOT NULL COMMENT "",
  `C_MKTSEGMENT` varchar(100) NOT NULL COMMENT "",
  `S_NAME` varchar(100) NOT NULL COMMENT "",
  `S_ADDRESS` varchar(100) NOT NULL COMMENT "",
  `S_CITY` varchar(100) NOT NULL COMMENT "",
  `S_NATION` varchar(100) NOT NULL COMMENT "",
  `S_REGION` varchar(100) NOT NULL COMMENT "",
  `S_PHONE` varchar(100) NOT NULL COMMENT "",
  `P_NAME` varchar(100) NOT NULL COMMENT "",
  `P_MFGR` varchar(100) NOT NULL COMMENT "",
  `P_CATEGORY` varchar(100) NOT NULL COMMENT "",
  `P_BRAND` varchar(100) NOT NULL COMMENT "",
  `P_COLOR` varchar(100) NOT NULL COMMENT "",
  `P_TYPE` varchar(100) NOT NULL COMMENT "",
  `P_SIZE` tinyint(4) NOT NULL COMMENT "",
  `P_CONTAINER` varchar(100) NOT NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`LO_ORDERDATE`, `LO_ORDERKEY`)
COMMENT "OLAP"
PARTITION BY RANGE(`LO_ORDERDATE`)
(PARTITION p1 VALUES [('0000-01-01'), ('1993-01-01')),
PARTITION p2 VALUES [('1993-01-01'), ('1994-01-01')),
PARTITION p3 VALUES [('1994-01-01'), ('1995-01-01')),
PARTITION p4 VALUES [('1995-01-01'), ('1996-01-01')),
PARTITION p5 VALUES [('1996-01-01'), ('1997-01-01')),
PARTITION p6 VALUES [('1997-01-01'), ('1998-01-01')),
PARTITION p7 VALUES [('1998-01-01'), ('1999-01-01')))
DISTRIBUTED BY HASH(`LO_ORDERKEY`) BUCKETS 48
PROPERTIES (
"replication_num" = "1",
"colocate_with" = "groupxx1",
"in_memory" = "false",
"storage_format" = "DEFAULT"
);
```

### 6.4 Import data

The following script will connects Doirs to import according to the parameters in ` conf/Doris-cluster.conf`, including imports four dimension tables (customer, part, supplier and date) which has a small amount of data in single thread, simultaneously imports one fact table (lineorder), and imports a wide table (lineorder_flat) by' INSERT INTO ... SELECT ...'.

```shell
bash bin/load-ssb-data.sh
````

> Note 1: Check the script help through `bash bin/load-ssb-data.sh-h`, and by default, it will start 5 threads to import lineorder concurrently, that is `-c 5`. If more threads are started, the import speed can be accelerated, but additional memory overhead will be added.
>
> Note 2: For faster import speed, you can restart BE after adding `flush_thread_num_per_store=5` in be.conf. This configuration indicates the number of disk write threads for each data directory, and the default is 2. Larger data can improve write data throughput, but may increase IO Util. (Reference value: 1 mechanical disk, when the default is 2, the IO Util during the import process is about 12%, and when it is set to 5, the IO Util is about 26%. If it is an SSD disk, it is almost 0) .
>
> Note 3: It cost about 389s in loading customer, part, supplier, date and lineorder, and 740s in inserting into lineorder_flat.
### 6.5 Check imported data

```sql
select count(*) from part;
select count(*) from customer;
select count(*) from supplier;
select count(*) from dates;
select count(*) from lineorder;
select count(*) from lineorder_flat;
```

The amount of data should be the same as the number of rows that generate the data.

| Table          | Rows             | Origin Size | Compacted Size(1 Replica) |
| -------------- | ---------------- | ----------- | ------------------------- |
| lineorder_flat | 6亿（600037902） |             | 59.709 GB                 |
| lineorder      | 6亿（600037902） | 60 GB       | 14.514 GB                 |
| customer       | 300万（3000000） | 277 MB      | 138.247 MB                |
| part           | 140万（1400000） | 116 MB      | 12.759 MB                 |
| supplier       | 20万（200000）   | 17 MB       | 9.143 MB                  |
| dates          | 2556            | 228 KB      | 34.276 KB                 |

### 6.6 Query test

#### 6.6.1 Test script

The following script connects Doris according to the parameters in ` conf/Doris-cluster.conf`, and prints out the rows of each table before executing the query.

```shell
bash bin/run-ssb-flat-queries.sh
```

#### 6.6.2 Test SQL

```sql
--Q1.1
SELECT SUM(LO_EXTENDEDPRICE * LO_DISCOUNT) AS revenue
FROM lineorder_flat
WHERE  LO_ORDERDATE >= 19930101  AND LO_ORDERDATE <= 19931231 AND LO_DISCOUNT BETWEEN 1 AND 3  AND LO_QUANTITY < 25;
--Q1.2
SELECT SUM(LO_EXTENDEDPRICE * LO_DISCOUNT) AS revenue
FROM lineorder_flat
WHERE LO_ORDERDATE >= 19940101 AND LO_ORDERDATE <= 19940131  AND LO_DISCOUNT BETWEEN 4 AND 6 AND LO_QUANTITY BETWEEN 26 AND 35;

--Q1.3
SELECT SUM(LO_EXTENDEDPRICE * LO_DISCOUNT) AS revenue
FROM lineorder_flat
WHERE  weekofyear(LO_ORDERDATE) = 6 AND LO_ORDERDATE >= 19940101  AND LO_ORDERDATE <= 19941231 AND LO_DISCOUNT BETWEEN 5 AND 7  AND LO_QUANTITY BETWEEN 26 AND 35;

--Q2.1
SELECT SUM(LO_REVENUE), (LO_ORDERDATE DIV 10000) AS YEAR, P_BRAND
FROM lineorder_flat WHERE P_CATEGORY = 'MFGR#12' AND S_REGION = 'AMERICA'
GROUP BY YEAR, P_BRAND
ORDER BY YEAR, P_BRAND;

--Q2.2
SELECT  SUM(LO_REVENUE), (LO_ORDERDATE DIV 10000) AS YEAR, P_BRAND
FROM lineorder_flat
WHERE P_BRAND >= 'MFGR#2221' AND P_BRAND <= 'MFGR#2228'  AND S_REGION = 'ASIA'
GROUP BY YEAR, P_BRAND
ORDER BY YEAR, P_BRAND;

--Q2.3
SELECT SUM(LO_REVENUE), (LO_ORDERDATE DIV 10000) AS YEAR, P_BRAND
FROM lineorder_flat
WHERE P_BRAND = 'MFGR#2239' AND S_REGION = 'EUROPE'
GROUP BY YEAR, P_BRAND
ORDER BY YEAR, P_BRAND;

--Q3.1
SELECT C_NATION, S_NATION, (LO_ORDERDATE DIV 10000) AS YEAR, SUM(LO_REVENUE) AS revenue
FROM lineorder_flat
WHERE C_REGION = 'ASIA' AND S_REGION = 'ASIA' AND LO_ORDERDATE >= 19920101  AND LO_ORDERDATE <= 19971231
GROUP BY C_NATION, S_NATION, YEAR
ORDER BY YEAR ASC, revenue DESC;

--Q3.2
SELECT C_CITY, S_CITY, (LO_ORDERDATE DIV 10000) AS YEAR, SUM(LO_REVENUE) AS revenue
FROM lineorder_flat
WHERE C_NATION = 'UNITED STATES' AND S_NATION = 'UNITED STATES' AND LO_ORDERDATE >= 19920101 AND LO_ORDERDATE <= 19971231
GROUP BY C_CITY, S_CITY, YEAR
ORDER BY YEAR ASC, revenue DESC;

--Q3.3
SELECT C_CITY, S_CITY, (LO_ORDERDATE DIV 10000) AS YEAR, SUM(LO_REVENUE) AS revenue
FROM lineorder_flat
WHERE C_CITY IN ('UNITED KI1', 'UNITED KI5') AND S_CITY IN ('UNITED KI1', 'UNITED KI5') AND LO_ORDERDATE >= 19920101 AND LO_ORDERDATE <= 19971231
GROUP BY C_CITY, S_CITY, YEAR
ORDER BY YEAR ASC, revenue DESC;

--Q3.4
SELECT C_CITY, S_CITY, (LO_ORDERDATE DIV 10000) AS YEAR, SUM(LO_REVENUE) AS revenue
FROM lineorder_flat
WHERE C_CITY IN ('UNITED KI1', 'UNITED KI5') AND S_CITY IN ('UNITED KI1', 'UNITED KI5') AND LO_ORDERDATE >= 19971201  AND LO_ORDERDATE <= 19971231
GROUP BY C_CITY, S_CITY, YEAR
ORDER BY YEAR ASC, revenue DESC;

--Q4.1
SELECT (LO_ORDERDATE DIV 10000) AS YEAR, C_NATION, SUM(LO_REVENUE - LO_SUPPLYCOST) AS profit
FROM lineorder_flat
WHERE C_REGION = 'AMERICA' AND S_REGION = 'AMERICA' AND P_MFGR IN ('MFGR#1', 'MFGR#2')
GROUP BY YEAR, C_NATION
ORDER BY YEAR ASC, C_NATION ASC;

--Q4.2
SELECT (LO_ORDERDATE DIV 10000) AS YEAR,S_NATION, P_CATEGORY, SUM(LO_REVENUE - LO_SUPPLYCOST) AS profit
FROM lineorder_flat
WHERE C_REGION = 'AMERICA' AND S_REGION = 'AMERICA' AND LO_ORDERDATE >= 19970101 AND LO_ORDERDATE <= 19981231 AND P_MFGR IN ('MFGR#1', 'MFGR#2')
GROUP BY YEAR, S_NATION, P_CATEGORY
ORDER BY YEAR ASC, S_NATION ASC, P_CATEGORY ASC;

--Q4.3
SELECT (LO_ORDERDATE DIV 10000) AS YEAR, S_CITY, P_BRAND, SUM(LO_REVENUE - LO_SUPPLYCOST) AS profit
FROM lineorder_flat
WHERE S_NATION = 'UNITED STATES' AND LO_ORDERDATE >= 19970101 AND LO_ORDERDATE <= 19981231 AND P_CATEGORY = 'MFGR#14'
GROUP BY YEAR, S_CITY, P_BRAND
ORDER BY YEAR ASC, S_CITY ASC, P_BRAND ASC;
```

