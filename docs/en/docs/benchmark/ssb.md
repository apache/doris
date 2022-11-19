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

This document mainly introduces the performance of Doris on the SSB 100G test set.

> Note 1: The standard test set including SSB is usually far from the actual business scenario, and some tests will perform parameter tuning for the test set. Therefore, the test results of the standard test set can only reflect the performance of the database in specific scenarios. Users are advised to conduct further testing with actual business data.
>
> Note 2: The operations involved in this document are all performed in the Ubuntu Server 20.04 environment, and CentOS 7 can also be tested.

We conducted pairwise testing on 13 queries on the SSB standard test dataset based on Apache Doris 1.2.0-rc01, Apache Doris 1.1.3 and Apache Doris 0.15.0 RC04 versions.

The overall performance improvement on SSB FlAT wide tables was nearly 4x on Apache Doris 1.2.0-rc01 compared to Apache Doris 1.1.3, and nearly 10x on Apache Doris 0.15.0 RC04.

![ssb_v11_v015_compare](/images/ssb_v11_v015_compare.png)

On the standard SSB test SQL, Apache Doris 1.2.0-rc01 delivers an overall performance improvement of nearly 2X over Apache Doris 1.1.3 and nearly 31X over Apache Doris 0.15.0 RC04.

![ssb_12_11_015](/images/ssb_12_11_015.png)

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
- Doris software version:  Apache Doris 1.2.0-rc01, Apache Doris 1.1.3 , Apache Doris 0.15.0 RC04
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

Here we use Apache Doris 1.2.0-rc01, Apache Doris 1.1.3 and Apache Doris 0.15.0 RC04 versions for comparative testing, with the following results.

| Query | Apache Doris 1.2.0-rc01(ms) | Apache Doris 1.1.3 (ms) |  Doris 0.15.0 RC04 (ms) |
| ----- | ------------- | ------------- | ----------------- |
| Q1.1  | 20            | 90            | 250               |
| Q1.2  | 10            | 10            | 30                |
| Q1.3  | 30            | 70            | 120               |
| Q2.1  | 90            | 360           | 900               |
| Q2.2  | 90            | 340           | 1020              |
| Q2.3  | 60            | 260           | 770               |
| Q3.1  | 160           | 550           | 1710              |
| Q3.2  | 80            | 290           | 670               |
| Q3.3  | 90            | 240           | 550               |
| Q3.4  | 20            | 20            | 30                |
| Q4.1  | 140           | 480           | 1250              |
| Q4.2  | 50            | 240           | 400               |
| Q4.3  | 30            | 200           | 330               |
| Total  | 880           | 3150          | 8030              |

**Interpretation of results**

- The data set corresponding to the test results is scale 100, about 600 million.
- The test environment is configured to be commonly used by users, including 4 cloud servers, 16-core 64G SSD, and 1 FE and 3 BE deployment.
- Use common user configuration tests to reduce user selection and evaluation costs, but will not consume so many hardware resources during the entire test process.


## 5. Standard SSB test results

Here we use Apache Doris 1.2.0-rc01, Apache Doris 1.1.3 and Apache Doris 0.15.0 RC04 versions for comparative testing, with the following results.

| Query | Apache Doris 1.2.0-rc01 (ms) | Apache Doris 1.1.3 (ms) | Doris 0.15.0 RC04 (ms) |
| ----- | ------- | ---------------------- | ------------------------------- |
| Q1.1  | 40      | 18                    | 350                           |
| Q1.2  | 30      | 100                    | 80                             |
| Q1.3  | 20      | 70                     | 80                            |
| Q2.1  | 350     | 940                  | 20680                     |
| Q2.2  | 320     | 750                  | 18250                    |
| Q2.3  | 300     | 720                  | 14760                   |
| Q3.1  | 650     | 2150                 | 22190                   |
| Q3.2  | 260     | 510                 | 8360                          |
| Q3.3  | 220     | 450                  | 6200                        |
| Q3.4  | 60      | 70                   | 160                            |
| Q4.1  | 840     | 1480                   | 24320                      |
| Q4.2  | 460     | 560                 | 6310                          |
| Q4.3  | 610     | 660                  | 10170                    |
| Total  | 4160    | 8478                | 131910 |

**Interpretation of results**

- The data set corresponding to the test results is scale 100, about 600 million.
- The test environment is configured to be commonly used by users, including 4 cloud servers, 16-core 64G SSD, and 1 FE and 3 BE deployment.
- Use common user configuration tests to reduce user selection and evaluation costs, but will not consume so many hardware resources during the entire test process.

## 6. Environment Preparation

Please first refer to the [official documentation](. /install/install-deploy.md) for Apache Doris installation and deployment to get a working Doris cluster (at least 1 FE 1 BE, 1 FE 3 BE recommended).

The scripts covered in the following documentation are stored in the Apache Doris codebase: [ssb-tools](https://github.com/apache/doris/tree/master/tools/ssb-tools)

## 7. Data Preparation

### 7.1 Download and install the SSB data generation tool.

Execute the following script to download and compile the [ssb-dbgen](https://github.com/electrum/ssb-dbgen.git) tool.

```shell
sh build-ssb-dbgen.sh
````

After successful installation, the `dbgen` binary will be generated in the `ssb-dbgen/` directory.

### 7.2 Generate SSB test set

Execute the following script to generate the SSB dataset:

```shell
sh gen-ssb-data.sh -s 100 -c 100
````

> Note 1: See script help with `sh gen-ssb-data.sh -h`.
>
> Note 2: The data will be generated in the `ssb-data/` directory with the suffix `.tbl`. The total file size is about 60GB. The generation time may vary from a few minutes to an hour.
>
> Note 3: `-s 100` indicates that the test set size factor is 100, `-c 100` indicates that 100 concurrent threads generate data for the lineorder table. The `-c` parameter also determines the number of files in the final lineorder table. The larger the parameter, the larger the number of files and the smaller each file.

With the `-s 100` parameter, the resulting dataset size is:

| Table     | Rows             | Size | File Number |
| --------- | ---------------- | ---- | ----------- |
| lineorder | 6亿（600037902） | 60GB | 100         |
| customer  | 300万（3000000） | 277M | 1           |
| part      | 140万（1400000） | 116M | 1           |
| supplier  | 20万（200000）   | 17M  | 1           |
| date      | 2556             | 228K | 1           |

### 7.3 Create table

#### 7.3.1 Prepare the `doris-cluster.conf` file.

Before calling the import script, you need to write the FE's ip port and other information in the `doris-cluster.conf` file.

File location and `load-ssb-dimension-data.sh` level.

The contents of the file include FE's ip, HTTP port, user name, password and the DB name of the data to be imported:

```shell
export FE_HOST="xxx"
export FE_HTTP_PORT="8030"
export FE_QUERY_PORT="9030"
export USER="root"
export PASSWORD='xxx'
export DB="ssb"
```

#### 7.3.2 Execute the following script to generate and create the SSB table:

```shell
sh create-ssb-tables.sh
````

Or copy [create-ssb-tables.sql](https://github.com/apache/incubator-doris/tree/master/tools/ssb-tools/ddl/create-ssb-tables.sql) and [ create-ssb-flat-table.sql](https://github.com/apache/incubator-doris/tree/master/tools/ssb-tools/ddl/create-ssb-flat-table.sql) of the The create table statements are executed in the MySQL client.

The following is the `lineorder_flat` table build statement. The `lineorder_flat` table is created in the `create-ssb-flat-table.sh` script above with the default number of buckets (48 buckets). You can delete this table and tune this bucketing number according to your cluster size node configuration to get better one test results.

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

We use the following command to complete the import of all data from SSB test set and SSB FLAT wide table data synthesis and import into the table.

```shell
 sh bin/load-ssb-data.sh -c 10
```

`-c 5` means start 10 concurrent threads for import (default is 5). In the single BE node case, the lineorder data generated by `sh gen-ssb-data.sh -s 100 -c 100` will also generate the data of the ssb-flat table at the end, if more threads are started, it can speed up the import, but it will add extra memory overhead.

> Notes.
>
> 1. This configuration indicates the number of write threads per data directory, and the default is 2. Larger data can improve write data throughput, but may increase IO Util. (Reference value: 1 mechanical disk, at default is 2, the IO Util during import is about 12%, and when set to 5, the IO Util is about 26%. (In case of SSD disks, it is almost 0).
> 
> 2. flat table data using 'INSERT INTO ... SELECT ... ' method to import.

### 7.5 Check imported data


```sql
select count(*) from part;
select count(*) from customer;
select count(*) from supplier;
select count(*) from date;
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
| date           | 2556             | 228 KB      | 34.276 KB                 |

### 7.6 Query test

#### 7.6.1 SSB FLAT Test SQL

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

#### 7.6.2 SSB Standard Test SQL

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
