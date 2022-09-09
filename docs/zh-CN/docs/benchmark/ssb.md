---
{
    "title": "Star-Schema-Benchmark 测试",
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

[Star Schema Benchmark(SSB)](https://www.cs.umb.edu/~poneil/StarSchemaB.PDF) 是一个轻量级的数仓场景下的性能测试集。SSB基于 [TPC-H](http://www.tpc.org/tpch/) 提供了一个简化版的星型模型数据集，主要用于测试在星型模型下，多表关联查询的性能表现。另外，业界内通常也会将SSB打平为宽表模型（以下简称：SSB flat），来测试查询引擎的性能，参考[Clickhouse](https://clickhouse.com/docs/zh/getting-started/example-datasets/star-schema)。

本文档主要介绍 Doris 在 SSB 测试集上的性能表现。

> 注1：包括 SSB 在内的标准测试集通常和实际业务场景差距较大，并且部分测试会针对测试集进行参数调优。所以标准测试集的测试结果仅能反映数据库在特定场景下的性能表现。建议用户使用实际业务数据进行进一步的测试。
>
> 注2：本文档涉及的操作都在 Ubuntu Server 20.04 环境进行，CentOS 7 也可测试。

在 SSB 标准测试数据集上的 13 个查询上，我们对即将发布的 Doris 1.1 版本和 Doris 0.15.0 RC04 版本进行了对别测试，整体性能提升了 2-3 倍。

![ssb_v11_v015_compare](/images/ssb_v11_v015_compare.png)

## 1. 硬件环境

| 机器数量 | 4 台腾讯云主机（1个FE，3个BE）       |
| -------- | ------------------------------------ |
| CPU      | AMD EPYC™ Milan(2.55GHz/3.5GHz) 16核 |
| 内存     | 64G                                  |
| 网络带宽  | 7Gbps                               |
| 磁盘     | 高性能云硬盘                         |

## 2. 软件环境

- Doris部署 3BE 1FE；
- 内核版本：Linux version 5.4.0-96-generic (buildd@lgw01-amd64-051)
- 操作系统版本：Ubuntu Server 20.04 LTS 64位
- Doris 软件版本：Apache Doris 1.1 、Apache Doris 0.15.0 RC04
- JDK：openjdk version "11.0.14" 2022-01-18

## 3. 测试数据量

| SSB表名        | 行数       | 备注             |
| :------------- | :--------- | :--------------- |
| lineorder      | 600,037,902 | 商品订单明细表表  |
| customer       | 3,000,000   | 客户信息表       |
| part           | 1,400,000   | 零件信息表       |
| supplier       | 200,000     | 供应商信息表     |
| date           | 2,556       | 日期表          |
| lineorder_flat | 600,037,902 | 数据展平后的宽表  |

## 4. 测试结果

这里我们使用即将发布的 Doris-1.1版本和 Doris-0.15.0 RC04 版本进行对比测试，测试结果如下：

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

**结果说明**

- 测试结果对应的数据集为scale 100, 约6亿条。
- 测试环境配置为用户常用配置，云服务器4台，16核 64G SSD，1 FE 3 BE 部署。
- 选用用户常见配置测试以降低用户选型评估成本，但整个测试过程中不会消耗如此多的硬件资源。
- 测试结果为3次执行取平均值。并且数据经过充分的 compaction（如果在刚导入数据后立刻测试，则查询延迟可能高于本测试结果，compaction的速度正在持续优化中，未来会显著降低）。

## 5. 环境准备

请先参照 [官方文档](../install/install-deploy.md) 进行 Doris 的安装部署，以获得一个正常运行中的 Doris 集群（至少包含 1 FE 1 BE，推荐 1 FE 3 BE）。

可修改 BE 的配置文件 be.conf 添加以下配置项，重启BE，以获得更好的查询性能。

```shell
enable_storage_vectorization=true
enable_low_cardinality_optimize=true
```

以下文档中涉及的脚本都存放在 Doris 代码库的 `tools/ssb-tools/` 下。

> **注意：**
>
> 上面这两个参数在 0.15.0 RC04 版本里没有这两个参数，不需要配置。

## 6. 数据准备

### 6.1 下载安装 SSB 数据生成工具。

执行以下脚本下载并编译 [ssb-dbgen](https://github.com/electrum/ssb-dbgen.git) 工具。

```shell
bash bin/build-ssb-dbgen.sh
```

安装成功后，将在 `bin/ssb-dbgen/` 目录下生成 `dbgen` 二进制文件。

### 6.2 生成 SSB 测试集

执行以下脚本生成 SSB 数据集：

```shell
bash bin/gen-ssb-data.sh
```

> 注1：通过 `bash bin/gen-ssb-data.sh -h` 查看脚本帮助，默认 scale factor 为 100（简称sf100），默认生成 10 个数据文件，即 `bash bin/gen-ssb-data.sh -s 100 -c 10`，耗时数分钟。
>
> 注2：数据会以 `.tbl` 为后缀生成在  `bin/ssb-data/` 目录下。文件总大小约60GB。生成时间可能在数分钟到1小时不等，生成完成后会列出生成文件的信息。
>
> 注3：`-s 100` 表示测试集大小系数为 100，`-c 10` 表示并发10个线程生成 lineorder 表的数据。`-c` 参数也决定了最终 lineorder 表的文件数量。参数越大，文件数越多，每个文件越小。测试sf100用默认参数即可，测试sf1000用 `-s 1000 -c 100` 。

在 `-s 100` 参数下，生成的数据集大小为：

| Table     | Rows             | Size | File Number |
| --------- | ---------------- | ---- | ----------- |
| lineorder | 6亿（600037902） | 60GB | 10          |
| customer  | 300万（3000000） | 277M | 1           |
| part      | 140万（1400000） | 116M | 1           |
| supplier  | 20万（200000）   | 17M  | 1           |
| dates     | 2556            | 228K | 1           |

### 6.3 建表

#### 6.3.1 准备 `conf/doris-cluster.conf` 文件。

在调用导入脚本前，需要将 FE 的 ip 端口等信息写在 `conf/doris-cluster.conf` 文件中。

文件内容包括 FE 的 ip，HTTP 端口，用户名，密码（默认为空）以及待导入数据的 DB 名称：

```shell
export FE_HOST="127.0.0.1"
export FE_HTTP_PORT="8030"
export FE_QUERY_PORT="9030"
export USER="root"
export PASSWORD=""
export DB="ssb"
```

#### 6.3.2 执行以下脚本生成创建 SSB 表：

```shell
bash bin/create-ssb-tables.sh
```
或者复制 [create-ssb-tables.sql](https://github.com/apache/incubator-doris/tree/master/tools/ssb-tools/ddl/create-ssb-tables.sql) 中的建表语句，在 Doris 中执行。
复制 [create-ssb-flat-table.sql](https://github.com/apache/incubator-doris/tree/master/tools/ssb-tools/ddl/create-ssb-flat-table.sql) 中的建表语句，在 Doris 中执行。

下面是 `lineorder_flat` 表建表语句。在上面的 `bin/create-ssb-table.sh`  脚本中创建"lineorder_flat"表，并进行了默认分桶数（48个桶)。您可以删除该表，根据您的集群规模节点配置对这个分桶数进行调整，这样可以获取到更好的一个测试效果。

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




### 6.4 导入数据

下面的脚本根据 `conf/doris-cluster.conf` 中的参数连接Doirs进行导入，单线程导入数据量较小的 4 张维度表（customer, part, supplier and date），并发导入 1 张事实表（lineorder），以及采用 'INSERT INTO ... SELECT ... ' 的方式导入宽表（lineorder_flat）。

```shell
bash bin/load-ssb-data.sh
```

> 注1：通过 `bash bin/load-ssb-data.sh -h` 查看脚本帮助, 默认 5 线程并发导入 lineorder，即 `-c 5` 。如果开启更多线程，可以加快导入速度，但会增加额外的内存开销。
>
> 注2：为获得更快的导入速度，你可以在 be.conf 中添加 `flush_thread_num_per_store=5` 后重启BE。该配置表示每个数据目录的写盘线程数，默认为2。较大的数据可以提升写数据吞吐，但可能会增加 IO Util。（参考值：1块机械磁盘，在默认为2的情况下，导入过程中的 IO Util 约为12%，设置为5时，IO Util 约为26%。如果是 SSD 盘，则几乎为 0）。
>
> 注3：导入customer, part, supplier, date 及 lineorder 表耗时389s，打平到 lineorder_flat 耗时740s.

### 6.5 检查导入数据

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

### 6.6 查询测试

#### 6.6.1 测试脚本

下面脚本根据 `conf/doris-cluster.conf` 中的参数连接Doris，执行查询前会先打印出各表的数据行数。

```shell
bash bin/run-ssb-flat-queries.sh
```

#### 6.6.2 测试SQL

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

