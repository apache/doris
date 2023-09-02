---
{
    "title": "Star Schema Benchmark",
    "language": "zh-CN"
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

[Star Schema Benchmark(SSB)](https://www.cs.umb.edu/~poneil/StarSchemaB.PDF) 是一个轻量级的数仓场景下的性能测试集。SSB 基于 [TPC-H](http://www.tpc.org/tpch/) 提供了一个简化版的星型模型数据集，主要用于测试在星型模型下，多表关联查询的性能表现。另外，业界内通常也会将 SSB 打平为宽表模型（以下简称：SSB flat），来测试查询引擎的性能，参考[Clickhouse](https://clickhouse.com/docs/zh/getting-started/example-datasets/star-schema)。

本文档主要介绍Apache Doris 在 SSB 100G 测试集上的性能表现。

> 注 1：包括 SSB 在内的标准测试集通常和实际业务场景差距较大，并且部分测试会针对测试集进行参数调优。所以标准测试集的测试结果仅能反映数据库在特定场景下的性能表现。建议用户使用实际业务数据进行进一步的测试。
>
> 注 2：本文档涉及的操作都在 Ubuntu Server 20.04 环境进行，CentOS 7 也可测试。
> 
> 注 3: Doris 从 1.2.2 版本开始，为了减少内存占用，默认关闭了 Page Cache，会对性能有一定影响，所以在进行性能测试时请在 be.conf 添加 disable_storage_page_cache=false 来打开 Page Cache。

在 SSB 标准测试数据集上的 13 个查询上，我们基于 Apache Doris 1.2.0-rc01， Apache Doris 1.1.3 及 Apache Doris 0.15.0 RC04 版本进行了对别测试。

在 SSB FlAT 宽表上， Apache Doris 1.2.0-rc01上相对 Apache Doris 1.1.3 整体性能提升了将近4倍，相对于 Apache Doris 0.15.0 RC04 ,性能提升了将近10倍 。

![ssb_v11_v015_compare](/images/ssb_flat.png)

在标准的 SSB 测试SQL上， Apache Doris 1.2.0-rc01 上相对 Apache Doris 1.1.3 整体性能提升了将近2倍，相对于 Apache Doris 0.15.0 RC04 ,性能提升了将近 31 倍 。

![ssb_12_11_015](/images/ssb.png)

## 1. 硬件环境

| 机器数量 | 4 台腾讯云主机（1个FE，3个BE）       |
| -------- | ------------------------------------ |
| CPU      | AMD EPYC™ Milan(2.55GHz/3.5GHz) 16核 |
| 内存     | 64G                                  |
| 网络带宽  | 7Gbps                               |
| 磁盘     | 高性能云硬盘                         |

## 2. 软件环境

- Doris 部署 3BE 1FE；
- 内核版本：Linux version 5.4.0-96-generic (buildd@lgw01-amd64-051)
- 操作系统版本：Ubuntu Server 20.04 LTS 64位
- Doris 软件版本： Apache Doris 1.2.0-rc01、Apache Doris 1.1.3 及 Apache Doris 0.15.0 RC04
- JDK：openjdk version "11.0.14" 2022-01-18

## 3. 测试数据量

| SSB表名        | 行数       | 备注             |
| :------------- | :--------- | :--------------- |
| lineorder      | 600,037,902 | 商品订单明细表表 |
| customer       | 3,000,000    | 客户信息表       |
| part           | 1,400,000    | 零件信息表       |
| supplier       | 200,000     | 供应商信息表     |
| dates          | 2,556       | 日期表           |
| lineorder_flat | 600,037,902 | 数据展平后的宽表 |

## 4. SSB 宽表测试结果

这里我们使用 Apache Doris 1.2.0-rc01、 Apache Doris 1.1.3 及 Apache Doris 0.15.0 RC04 版本进行对比测试，测试结果如下：


| Query | Apache Doris 1.2.0-rc01(ms) | Apache Doris 1.1.3(ms) | Apache Doris 0.15.0 RC04(ms) |
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
| 合计  | 880           | 3150          | 8030              |

**结果说明**

- 测试结果对应的数据集为 scale 100, 约 6 亿条。
- 测试环境配置为用户常用配置，云服务器 4 台，16 核 64G SSD，1 FE 3 BE 部署。
- 选用用户常见配置测试以降低用户选型评估成本，但整个测试过程中不会消耗如此多的硬件资源。

## 5. 标准 SSB 测试结果

这里我们使用 Apache Doris 1.2.0-rc01、Apache Doris 1.1.3 及 Apache Doris 0.15.0 RC04 版本进行对比测试，测试结果如下：

| Query | Apache Doris 1.2.0-rc01(ms) | Apache Doris 1.1.3 (ms) |  Apache Doris 0.15.0 RC04(ms) |
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
| 合计  | 4160    | 8478                | 131910 |

**结果说明**

- 测试结果对应的数据集为scale 100, 约6亿条。
- 测试环境配置为用户常用配置，云服务器4台，16核 64G SSD，1 FE 3 BE 部署。
- 选用用户常见配置测试以降低用户选型评估成本，但整个测试过程中不会消耗如此多的硬件资源。


## 6. 环境准备

请先参照 [官方文档](../install/standard-deployment.md) 进行 Apache Doris 的安装部署，以获得一个正常运行中的 Doris 集群（至少包含 1 FE 1 BE，推荐 1 FE 3 BE）。

以下文档中涉及的脚本都存放在 Apache Doris 代码库：[ssb-tools](https://github.com/apache/doris/tree/master/tools/ssb-tools)

## 7. 数据准备

### 7.1 下载安装 SSB 数据生成工具。

执行以下脚本下载并编译 [ssb-dbgen](https://github.com/electrum/ssb-dbgen.git) 工具。

```shell
sh build-ssb-dbgen.sh
```

安装成功后，将在 `ssb-dbgen/` 目录下生成 `dbgen` 二进制文件。

### 7.2 生成 SSB 测试集

执行以下脚本生成 SSB 数据集：

```shell
sh gen-ssb-data.sh -s 100 -c 100
```

> 注1：通过 `sh gen-ssb-data.sh -h` 查看脚本帮助。
>
> 注2：数据会以 `.tbl` 为后缀生成在  `ssb-data/` 目录下。文件总大小约60GB。生成时间可能在数分钟到1小时不等。
>
> 注3：`-s 100` 表示测试集大小系数为 100，`-c 100` 表示并发100个线程生成 lineorder 表的数据。`-c` 参数也决定了最终 lineorder 表的文件数量。参数越大，文件数越多，每个文件越小。

在 `-s 100` 参数下，生成的数据集大小为：

| Table     | Rows             | Size | File Number |
| --------- | ---------------- | ---- | ----------- |
| lineorder | 6亿（600037902） | 60GB | 100         |
| customer  | 300万（3000000） | 277M | 1           |
| part      | 140万（1400000） | 116M | 1           |
| supplier  | 20万（200000）   | 17M  | 1           |
| dates     | 2556            | 228K | 1           |

### 7.3 建表

#### 7.3.1 准备 `doris-cluster.conf` 文件。

在调用导入脚本前，需要将 FE 的 ip 端口等信息写在 `doris-cluster.conf` 文件中。

文件位置在 `${DORIS_HOME}/tools/ssb-tools/conf/` 目录下 。

文件内容包括 FE 的 ip，HTTP 端口，用户名，密码以及待导入数据的 DB 名称：

```shell
export FE_HOST="xxx"
export FE_HTTP_PORT="8030"
export FE_QUERY_PORT="9030"
export USER="root"
export PASSWORD='xxx'
export DB="ssb"
```

#### 7.3.2 执行以下脚本生成创建 SSB 表：

```shell
sh create-ssb-tables.sh
```
或者复制 [create-ssb-tables.sql](https://github.com/apache/incubator-doris/tree/master/tools/ssb-tools/ddl/create-ssb-tables.sql)  和 [create-ssb-flat-table.sql](https://github.com/apache/incubator-doris/tree/master/tools/ssb-tools/ddl/create-ssb-flat-table.sql)  中的建表语句，在 MySQL 客户端中执行。

下面是 `lineorder_flat` 表建表语句。在上面的 `create-ssb-flat-table.sh`  脚本中创建 `lineorder_flat` 表，并进行了默认分桶数（48个桶)。您可以删除该表，根据您的集群规模节点配置对这个分桶数进行调整，这样可以获取到更好的一个测试效果。

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

### 7.4 导入数据

我们使用以下命令完成 SSB 测试集所有数据导入及 SSB FLAT 宽表数据合成并导入到表里。


```shell
sh bin/load-ssb-data.sh -c 10
```

`-c 5` 表示启动 10 个并发线程导入（默认为 5）。在单 BE 节点情况下，由 `sh gen-ssb-data.sh -s 100 -c 100` 生成的 lineorder 数据，同时会在最后生成ssb-flat表的数据，如果开启更多线程，可以加快导入速度，但会增加额外的内存开销。

> 注：
>
> 1. 为获得更快的导入速度，你可以在 be.conf 中添加 `flush_thread_num_per_store=10` 后重启BE。该配置表示每个数据目录的写盘线程数，默认为6。较大的数据可以提升写数据吞吐，但可能会增加 IO Util。（参考值：1块机械磁盘，在默认为2的情况下，导入过程中的 IO Util 约为12%，设置为5时，IO Util 约为26%。如果是 SSD 盘，则几乎为 0）。
>
> 2. flat 表数据采用 'INSERT INTO ... SELECT ... ' 的方式导入。


### 7.5 检查导入数据

```sql
select count(*) from part;
select count(*) from customer;
select count(*) from supplier;
select count(*) from dates;
select count(*) from lineorder;
select count(*) from lineorder_flat;
```

数据量应和生成数据的行数一致。

| Table          | Rows             | Origin Size | Compacted Size(1 Replica) |
| -------------- | ---------------- | ----------- | ------------------------- |
| lineorder_flat | 6亿（600037902） |             | 59.709 GB                 |
| lineorder      | 6亿（600037902） | 60 GB       | 14.514 GB                 |
| customer       | 300万（3000000） | 277 MB      | 138.247 MB                |
| part           | 140万（1400000） | 116 MB      | 12.759 MB                 |
| supplier       | 20万（200000）   | 17 MB       | 9.143 MB                  |
| dates          | 2556            | 228 KB      | 34.276 KB                 |

### 7.6 查询测试

SSB-FlAT 查询语句 ：[ssb-flat-queries](https://github.com/apache/doris/tree/master/tools/ssb-tools/ssb-flat-queries)


标准 SSB 查询语句 ：[ssb-queries](https://github.com/apache/doris/tree/master/tools/ssb-tools/ssb-queries)

#### 7.6.1 SSB FLAT 测试 SQL


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



#### **7.6.2 SSB 标准测试 SQL**

```sql
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
