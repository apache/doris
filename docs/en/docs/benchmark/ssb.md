---
{
    "title": "Star Schema Benchmark",
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

[Star Schema Benchmark(SSB)](https://www.cs.umb.edu/~poneil/StarSchemaB.PDF) is a lightweight performance test set in the data warehouse scenario. SSB provides a simplified star schema data based on [TPC-H](http://www.tpc.org/tpch/), which is mainly used to test the performance of multi-table JOIN query under star schema.  In addition, the industry usually flattens SSB into a wide table model (Referred as: SSB flat) to test the performance of the query engine, refer to [Clickhouse](https://clickhouse.com/docs/zh/getting-started).

This document mainly introduces the performance of Doris on the SSB 100G test set.

> Note 1: The standard test set including SSB usually has a large gap with the actual business scenario, and some tests will perform parameter tuning for the test set. Therefore, the test results of the standard test set can only reflect the performance of the database in a specific scenario. It is recommended that users use actual business data for further testing.
>
> Note 2: The operations involved in this document are all performed in the Ubuntu Server 20.04 environment, and CentOS 7 as well.
>
> Note 3: Doris starting from version 1.2.2, the page cache is turned off by default to reduce memory usage, which has a certain impact on performance. For performance testing, enable the page cache by adding disable_storage_page_cache=false to be.conf.

With 13 queries on the SSB standard test data set, we conducted a comparison test based on Apache Doris 1.2.0-rc01, Apache Doris 1.1.3 and Apache Doris 0.15.0 RC04 versions.

On the SSB flat wide table, the overall performance of Apache Doris 1.2.0-rc01 has been improved by nearly 4 times compared with Apache Doris 1.1.3, and nearly 10 times compared with Apache Doris 0.15.0 RC04.

On the SQL test with standard SSB, the overall performance of Apache Doris 1.2.0-rc01 has been improved by nearly 2 times compared with Apache Doris 1.1.3, and nearly 31 times compared with Apache Doris 0.15.0 RC04.

## 1. Hardware Environment

| Number of machines | 4 Tencent Cloud Hosts (1 FE, 3 BEs)        |
| ------------------ | ----------------------------------------- |
| CPU                | AMD EPYC™ Milan (2.55GHz/3.5GHz) 16 Cores |
| Memory             | 64G                                       |
| Network Bandwidth  | 7Gbps                                     |
| Disk               | High-performance Cloud Disk               |

## 2. Software Environment

- Doris deployed 3BEs and 1FE;
- Kernel version: Linux version 5.4.0-96-generic (buildd@lgw01-amd64-051)
- OS version: Ubuntu Server 20.04 LTS 64-bit
- Doris software versions: Apache Doris 1.2.0-rc01, Apache Doris 1.1.3 and Apache Doris 0.15.0 RC04
- JDK: openjdk version "11.0.14" 2022-01-18

## 3. Test Data Volume

| SSB Table Name | Rows | Annotation                          |
| :------------- | :------------- | :------------------------------- |
| lineorder      | 600,037,902    | Commodity Order Details             |
| customer       | 3,000,000      | Customer Information        |
| part           | 1,400,000      | Parts Information          |
| supplier       | 200,000        | Supplier Information        |
| dates          | 2,556          | Date                        |
| lineorder_flat | 600,037,902    | Wide Table after Data Flattening |

## 4. Test Results

We use Apache Doris 1.2.0-rc01, Apache Doris 1.1.3 and Apache Doris 0.15.0 RC04 for comparative testing. The test results are as follows:

| Query | Apache Doris 1.2.0-rc01(ms) | Apache Doris 1.1.3 (ms) |  Doris 0.15.0 RC04 (ms) |
| ----- | ------------- | ------------- | ----------------- |
| Q1.1  | 20            | 90            | 250               |
| Q1.2  | 10            | 10            | 30                |
| Q1.3  | 30            | 70            | 120               |
| Q2.1  | 90            | 360           | 900               |
| Q2.2  | 90            | 340           | 1,020              |
| Q2.3  | 60            | 260           | 770               |
| Q3.1  | 160           | 550           | 1,710              |
| Q3.2  | 80            | 290           | 670               |
| Q3.3  | 90            | 240           | 550               |
| Q3.4  | 20            | 20            | 30                |
| Q4.1  | 140           | 480           | 1,250              |
| Q4.2  | 50            | 240           | 400               |
| Q4.3  | 30            | 200           | 330               |
| Total  | 880           | 3,150          | 8,030              |

![ssb_v11_v015_compare](/images/ssb_flat.png)

**Interpretation of Results**

- The data set corresponding to the test results is scale 100, about 600 million.
- The test environment is configured as the user's common configuration, with 4 cloud servers, 16-core 64G SSD, and 1 FE, 3 BEs deployment.
- We select the user's common configuration test to reduce the cost of user selection and evaluation, but the entire test process will not consume so many hardware resources.


## 5. Standard SSB Test Results

Here we use Apache Doris 1.2.0-rc01, Apache Doris 1.1.3 and Apache Doris 0.15.0 RC04 for comparative testing. In the test, we use Query Time（ms） as the main performance indicator. The test results are as follows:

| Query | Apache Doris 1.2.0-rc01 (ms) | Apache Doris 1.1.3 (ms) | Doris 0.15.0 RC04 (ms) |
| ----- | ------- | ---------------------- | ------------------------------- |
| Q1.1  | 40      | 18                    | 350                           |
| Q1.2  | 30      | 100                    | 80                             |
| Q1.3  | 20      | 70                     | 80                            |
| Q2.1  | 350     | 940                  | 20,680                     |
| Q2.2  | 320     | 750                  | 18,250                    |
| Q2.3  | 300     | 720                  | 14,760                   |
| Q3.1  | 650     | 2,150                 | 22,190                   |
| Q3.2  | 260     | 510                 | 8,360                          |
| Q3.3  | 220     | 450                  | 6,200                        |
| Q3.4  | 60      | 70                   | 160                            |
| Q4.1  | 840     | 1,480                   | 24,320                      |
| Q4.2  | 460     | 560                 | 6,310                          |
| Q4.3  | 610     | 660                  | 10,170                    |
| Total  | 4,160    | 8,478                | 131,910 |

![ssb_12_11_015](/images/ssb.png)

**Interpretation of Results**

- The data set corresponding to the test results is scale 100, about 600 million.
- The test environment is configured as the user's common configuration, with 4 cloud servers, 16-core 64G SSD, and 1 FE 3 BEs deployment.
- We select the user's common configuration test to reduce the cost of user selection and evaluation, but the entire test process will not consume so many hardware resources.

## 6. Environment Preparation

Please first refer to the [official documentation](. /install/install-deploy.md) to install and deploy Apache Doris first to obtain a Doris cluster which is working well(including at least 1 FE 1 BE, 1 FE 3 BEs is recommended).

The scripts mentioned in the following documents are stored in the Apache Doris codebase: [ssb-tools](https://github.com/apache/doris/tree/master/tools/ssb-tools)

## 7. Data Preparation

### 7.1 Download and Install the SSB Data Generation Tool.

Execute the following script to download and compile the [ssb-dbgen](https://github.com/electrum/ssb-dbgen.git) tool.

```shell
sh build-ssb-dbgen.sh
````

After successful installation, the `dbgen` binary will be generated under the `ssb-dbgen/` directory.

### 7.2 Generate SSB Test Set

Execute the following script to generate the SSB dataset:

```shell
sh gen-ssb-data.sh -s 100 -c 100
````

> Note 1: Check the script help via `sh gen-ssb-data.sh -h`.
>
> Note 2: The data will be generated under the `ssb-data/` directory with the suffix `.tbl`. The total file size is about 60GB and may need a few minutes to an hour to generate.
>
> Note 3: `-s 100` indicates that the test set size factor is 100, `-c 100` indicates that 100 concurrent threads generate the data of the lineorder table. The `-c` parameter also determines the number of files in the final lineorder table. The larger the parameter, the larger the number of files and the smaller each file.

With the `-s 100` parameter, the resulting dataset size is:

| Table     | Rows             | Size | File Number |
| --------- | ---------------- | ---- | ----------- |
| lineorder | 600,037,902 | 60GB | 100         |
| customer  | 3,000,000 | 277M | 1           |
| part      | 1,400,000 | 116M | 1           |
| supplier  | 200,000   | 17M  | 1           |
| dates     | 2,556     | 228K | 1           |

### 7.3 Create Table

#### 7.3.1 Prepare the `doris-cluster.conf` File.

Before import the script, you need to write the FE’s ip port and other information in the `doris-cluster.conf` file.

The file is located under `${DORIS_HOME}/tools/ssb-tools/conf/`.

The content of the file includes FE's ip, HTTP port, user name, password and the DB name of the data to be imported:

```shell
export FE_HOST="xxx"
export FE_HTTP_PORT="8030"
export FE_QUERY_PORT="9030"
export USER="root"
export PASSWORD='xxx'
export DB="ssb"
```

#### 7.3.2 Execute the Following Script to Generate and Create the SSB Table:

```shell
sh create-ssb-tables.sh
````

Or copy the table creation statements in [create-ssb-tables.sql](https://github.com/apache/incubator-doris/tree/master/tools/ssb-tools/ddl/create-ssb-tables.sql) and [ create-ssb-flat-table.sql](https://github.com/apache/incubator-doris/tree/master/tools/ssb-tools/ddl/create-ssb-flat-table.sql) and then execute them in the MySQL client.

The following is the `lineorder_flat` table build statement. Create the `lineorder_flat` table in the above `create-ssb-flat-table.sh` script, and perform the default number of buckets (48 buckets). You can delete this table and adjust the number of buckets according to your cluster scale node configuration, so as to obtain a better test result.

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

### 7.4 Import data

We use the following command to complete all data import of SSB test set and SSB FLAT wide table data synthesis and then import into the table.

```shell
 sh bin/load-ssb-data.sh -c 10
```

`-c 5` means start 10 concurrent threads to import (5 by default). In the case of a single BE node, the lineorder data generated by `sh gen-ssb-data.sh -s 100 -c 100` will also generate the data of the ssb-flat table in the end. If more threads are enabled, the import speed can be accelerated. But it will cost extra memory.

> Notes.
>
> 1. To get faster import speed, you can add `flush_thread_num_per_store=10` in be.conf and then restart BE. This configuration indicates the number of disk writing threads for each data directory, 6 by default. Larger data can improve write data throughput, but may increase IO Util. (Reference value: 1 mechanical disk, with 2 by default, the IO Util during the import process is about 12%. When it is set to 5, the IO Util is about 26%. If it is an SSD disk, it is almost 0%) .
>
> 2. The flat table data is imported by 'INSERT INTO ... SELECT ... '.

### 7.5 Checking Imported data


```sql
select count(*) from part;
select count(*) from customer;
select count(*) from supplier;
select count(*) from dates;
select count(*) from lineorder;
select count(*) from lineorder_flat;
```

The amount of data should be consistent with the number of rows of generated data.

| Table          | Rows             | Origin Size | Compacted Size(1 Replica) |
| -------------- | ---------------- | ----------- | ------------------------- |
| lineorder_flat | 600,037,902 |             | 59.709 GB                 |
| lineorder      | 600,037,902 | 60 GB       | 14.514 GB                 |
| customer       | 3,000,000 | 277 MB      | 138.247 MB                |
| part           | 1,400,000 | 116 MB      | 12.759 MB                 |
| supplier       | 200,000   | 17 MB       | 9.143 MB                  |
| dates          | 2,556     | 228 KB      | 34.276 KB                 |

### 7.6 Query Test

- SSB-Flat Query Statement: [ ssb-flat-queries](https://github.com/apache/doris/tree/master/tools/ssb-tools/ssb-flat-queries)
- Standard SSB Queries: [ ssb-queries](https://github.com/apache/doris/tree/master/tools/ssb-tools/ssb-queries)

#### 7.6.1 SSB FLAT Test for SQL

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

#### 7.6.2 SSB Standard Test for SQL

```SQL
--Q1.1
SELECT SUM(lo_extendedprice * lo_discount) AS REVENUE
FROM lineorder, dates
WHERE
    lo_orderdate = d_datekey
    AND d_year = 1993
    AND lo_discount BETWEEN 1 AND 3
    AND lo_quantity < 25;
--Q1.2
SELECT SUM(lo_extendedprice * lo_discount) AS REVENUE
FROM lineorder, dates
WHERE
    lo_orderdate = d_datekey
    AND d_yearmonth = 'Jan1994'
    AND lo_discount BETWEEN 4 AND 6
    AND lo_quantity BETWEEN 26 AND 35;
    
--Q1.3
SELECT
    SUM(lo_extendedprice * lo_discount) AS REVENUE
FROM lineorder, dates
WHERE
    lo_orderdate = d_datekey
    AND d_weeknuminyear = 6
    AND d_year = 1994
    AND lo_discount BETWEEN 5 AND 7
    AND lo_quantity BETWEEN 26 AND 35;
    
--Q2.1
SELECT SUM(lo_revenue), d_year, p_brand
FROM lineorder, dates, part, supplier
WHERE
    lo_orderdate = d_datekey
    AND lo_partkey = p_partkey
    AND lo_suppkey = s_suppkey
    AND p_category = 'MFGR#12'
    AND s_region = 'AMERICA'
GROUP BY d_year, p_brand
ORDER BY p_brand;

--Q2.2
SELECT SUM(lo_revenue), d_year, p_brand
FROM lineorder, dates, part, supplier
WHERE
    lo_orderdate = d_datekey
    AND lo_partkey = p_partkey
    AND lo_suppkey = s_suppkey
    AND p_brand BETWEEN 'MFGR#2221' AND 'MFGR#2228'
    AND s_region = 'ASIA'
GROUP BY d_year, p_brand
ORDER BY d_year, p_brand;

--Q2.3
SELECT SUM(lo_revenue), d_year, p_brand
FROM lineorder, dates, part, supplier
WHERE
    lo_orderdate = d_datekey
    AND lo_partkey = p_partkey
    AND lo_suppkey = s_suppkey
    AND p_brand = 'MFGR#2239'
    AND s_region = 'EUROPE'
GROUP BY d_year, p_brand
ORDER BY d_year, p_brand;

--Q3.1
SELECT
    c_nation,
    s_nation,
    d_year,
    SUM(lo_revenue) AS REVENUE
FROM customer, lineorder, supplier, dates
WHERE
    lo_custkey = c_custkey
    AND lo_suppkey = s_suppkey
    AND lo_orderdate = d_datekey
    AND c_region = 'ASIA'
    AND s_region = 'ASIA'
    AND d_year >= 1992
    AND d_year <= 1997
GROUP BY c_nation, s_nation, d_year
ORDER BY d_year ASC, REVENUE DESC;

--Q3.2
SELECT
    c_city,
    s_city,
    d_year,
    SUM(lo_revenue) AS REVENUE
FROM customer, lineorder, supplier, dates
WHERE
    lo_custkey = c_custkey
    AND lo_suppkey = s_suppkey
    AND lo_orderdate = d_datekey
    AND c_nation = 'UNITED STATES'
    AND s_nation = 'UNITED STATES'
    AND d_year >= 1992
    AND d_year <= 1997
GROUP BY c_city, s_city, d_year
ORDER BY d_year ASC, REVENUE DESC;

--Q3.3
SELECT
    c_city,
    s_city,
    d_year,
    SUM(lo_revenue) AS REVENUE
FROM customer, lineorder, supplier, dates
WHERE
    lo_custkey = c_custkey
    AND lo_suppkey = s_suppkey
    AND lo_orderdate = d_datekey
    AND (
        c_city = 'UNITED KI1'
        OR c_city = 'UNITED KI5'
    )
    AND (
        s_city = 'UNITED KI1'
        OR s_city = 'UNITED KI5'
    )
    AND d_year >= 1992
    AND d_year <= 1997
GROUP BY c_city, s_city, d_year
ORDER BY d_year ASC, REVENUE DESC;

--Q3.4
SELECT
    c_city,
    s_city,
    d_year,
    SUM(lo_revenue) AS REVENUE
FROM customer, lineorder, supplier, dates
WHERE
    lo_custkey = c_custkey
    AND lo_suppkey = s_suppkey
    AND lo_orderdate = d_datekey
    AND (
        c_city = 'UNITED KI1'
        OR c_city = 'UNITED KI5'
    )
    AND (
        s_city = 'UNITED KI1'
        OR s_city = 'UNITED KI5'
    )
    AND d_yearmonth = 'Dec1997'
GROUP BY c_city, s_city, d_year
ORDER BY d_year ASC, REVENUE DESC;

--Q4.1
SELECT /*+SET_VAR(parallel_fragment_exec_instance_num=4, enable_vectorized_engine=true, batch_size=4096, enable_cost_based_join_reorder=true, enable_projection=true) */
    d_year,
    c_nation,
    SUM(lo_revenue - lo_supplycost) AS PROFIT
FROM dates, customer, supplier, part, lineorder
WHERE
    lo_custkey = c_custkey
    AND lo_suppkey = s_suppkey
    AND lo_partkey = p_partkey
    AND lo_orderdate = d_datekey
    AND c_region = 'AMERICA'
    AND s_region = 'AMERICA'
    AND (
        p_mfgr = 'MFGR#1'
        OR p_mfgr = 'MFGR#2'
    )
GROUP BY d_year, c_nation
ORDER BY d_year, c_nation;

--Q4.2
SELECT /*+SET_VAR(parallel_fragment_exec_instance_num=2, enable_vectorized_engine=true, batch_size=4096, enable_cost_based_join_reorder=true, enable_projection=true) */  
    d_year,
    s_nation,
    p_category,
    SUM(lo_revenue - lo_supplycost) AS PROFIT
FROM dates, customer, supplier, part, lineorder
WHERE
    lo_custkey = c_custkey
    AND lo_suppkey = s_suppkey
    AND lo_partkey = p_partkey
    AND lo_orderdate = d_datekey
    AND c_region = 'AMERICA'
    AND s_region = 'AMERICA'
    AND (
        d_year = 1997
        OR d_year = 1998
    )
    AND (
        p_mfgr = 'MFGR#1'
        OR p_mfgr = 'MFGR#2'
    )
GROUP BY d_year, s_nation, p_category
ORDER BY d_year, s_nation, p_category;

--Q4.3
SELECT /*+SET_VAR(parallel_fragment_exec_instance_num=2, enable_vectorized_engine=true, batch_size=4096, enable_cost_based_join_reorder=true, enable_projection=true) */
    d_year,
    s_city,
    p_brand,
    SUM(lo_revenue - lo_supplycost) AS PROFIT
FROM dates, customer, supplier, part, lineorder
WHERE
    lo_custkey = c_custkey
    AND lo_suppkey = s_suppkey
    AND lo_partkey = p_partkey
    AND lo_orderdate = d_datekey
    AND s_nation = 'UNITED STATES'
    AND (
        d_year = 1997
        OR d_year = 1998
    )
    AND p_category = 'MFGR#14'
GROUP BY d_year, s_city, p_brand
ORDER BY d_year, s_city, p_brand;
```
