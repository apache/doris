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

本文档主要介绍 Doris 在 SSB 及 SSB flat 测试集上的性能表现。

> 注1：包括 SSB 在内的标准测试集通常和实际业务场景差距较大，并且部分测试会针对测试集进行参数调优。所以标准测试集的测试结果仅能反映数据库在特定场景下的性能表现。建议用户使用实际业务数据进行进一步的测试。
> 
> 注2：本文档涉及的操作都在 Ubuntu Server 20.04 环境进行，CentOS 7 也可测试。

## 测试结果

1. SSB flat 测试结果

    |Query | Doris-1.1(ms) | Doris-1.0(ms) | Doris-0.15(ms) |  |
    |---|---|---|---|---|
    | q1.1 | 73 | 162 | 192 |  |
    | q1.2 | 11 | 90 | 90 |  |
    | q1.3 | 65 | 139 | 157 |  |
    | q2.1 | 360 | 798 | 943 |  |
    | q2.2 | 338 | 940 | 1014 |  |
    | q2.3 | 254 | 699 | 813 |  |
    | q3.1 | 495 | 956 | 1146 |  |
    | q3.2 | 287 | 693 | 696 |  |
    | q3.3 | 232 | 546 | 562 |  |
    | q3.4 | 16 | 116 | 130 |  |
    | q4.1 | 479 | 1120 | 1255 |  |
    | q4.2 | 235 | 363 | 386 |  |
    | q4.3 | 200 | 275 | 304 |  |

2. SSB 测试结果

    |Query | Doris-1.1(ms) | Doris-1.0(ms) | Doris-0.15(ms) |  |
    |---|---|---|---|---|
    | q1.1 | 90 | 184 | 216 |  |
    | q1.2 | 69 | 114 | 116 |  |
    | q1.3 | 64 | 107 | 98 |  |
    | q2.1 | 638 | 1055 | 6932 |  |
    | q2.2 | 559 | 922 | 5892 |  |
    | q2.3 | 528 | 874 | 5116 |  |
    | q3.1 | 1136 | 2012 | 7243 |  |
    | q3.2 | 572 | 652 | 4646 |  |
    | q3.3 | 492 | 529 | 3386 |  |
    | q3.4 | 117 | 134 | 175 |  |
    | q4.1 | 1410 | 2914 | 8121 |  |
    | q4.2 | 787 | 2112 | 2218 |  |
    | q4.3 | 976 | 3725 | 3029 |  |

3. 结果说明

    - 测试结果对应的数据集为scale 100, 约6亿条。
    - 测试环境配置为用户常用配置，云服务器4台，16核 64G SSD，1 FE 3 BE 部署。
    - 选用用户常见配置测试以降低用户选型评估成本，但整个测试过程中不会消耗如此多的硬件资源。
    - 测试结果为3次执行取平均值。并且数据经过充分的 compaction（如果在刚导入数据后立刻测试，则查询延迟可能高于本测试结果，compaction的速度正在持续优化中，未来会显著降低）。

## 环境准备

请先参照 [官方文档](../install/install-deploy.md) 进行 Doris 的安装部署，以获得一个正常运行中的 Doris 集群（至少包含 1 FE 1 BE，推荐 1 FE 3 BE）。

可修改 BE 的配置文件 be.conf 添加以下配置项，以获得更好的查询性能。

```
enable_storage_vectorization=true
enable_low_cardinality_optimize=true
```

以下文档中涉及的脚本都存放在 Doris 代码库的 `tools/ssb-tools/` 下。

## 数据准备

### 1. 下载安装 SSB 数据生成工具。

执行以下脚本下载并编译 [ssb-dbgen](https://github.com/electrum/ssb-dbgen.git) 工具。

```
sh build-ssb-dbgen.sh
```

安装成功后，将在 `ssb-dbgen/` 目录下生成 `dbgen` 二进制文件。

### 2. 生成 SSB 测试集

执行以下脚本生成 SSB 数据集：

```
sh gen-ssb-data.sh -s 100 -c 100
```

> 注1：通过 `sh gen-ssb-data.sh -h` 查看脚本帮助。
> 
> 注2：数据会以 `.tbl` 为后缀生成在  `ssb-data/` 目录下。文件总大小约60GB。生成时间可能在数分钟到1小时不等。
> 
> 注3：`-s 100` 表示测试集大小系数为 100，`-c 100` 表示并发100个线程生成 lineorder 表的数据。`-c` 参数也决定了最终 lineorder 表的文件数量。参数越大，文件数越多，每个文件越小。

在 `-s 100` 参数下，生成的数据集大小为：

|Table |Rows |Size | File Number |
|---|---|---|---|
|lineorder| 6亿（600037902） | 60GB | 100|
|customer|300万（3000000） |277M |1|
|part|140万（1400000） | 116M|1|
|supplier|20万（200000） |17M |1|
|date| 2556|228K |1|

### 3. 建表

0. 准备 'doris-cluster.conf' 文件。
    
    在调用导入脚本前，需要将 FE 的 ip 端口等信息写在 `doris-cluster.conf` 文件中。
    
    文件位置和 `load-ssb-dimension-data.sh` 平级。
    
    文件内容包括 FE 的 ip，HTTP 端口，用户名，密码以及待导入数据的 DB 名称：
    
    ```
    export FE_HOST="xxx"
    export FE_HTTP_PORT="8030"
    export FE_QUERY_PORT="9030"
    export USER="root"
    export PASSWORD='xxx'
    export DB="ssb"
    ```

1. 执行以下脚本生成创建 SSB 表：

    ```
    sh create-ssb-tables.sh
    ```
    或者复制 [create-ssb-tables.sql](https://github.com/apache/incubator-doris/tree/master/tools/ssb-tools/ddl/create-ssb-tables.sql) 中的建表语句，在 Doris 中执行。

2. 执行以下脚本生成创建 SSB flat 表：

    ```
    sh create-ssb-flat-table.sh
    ```
    或者复制 [create-ssb-flat-table.sql](https://github.com/apache/incubator-doris/tree/master/tools/ssb-tools/ddl/create-ssb-flat-table.sql) 中的建表语句，在 Doris 中执行。


### 4. 导入数据

1. 导入 4 张维度表数据（customer, part, supplier and date）
    
    因为这4张维表数据量较小，导入较简单，我们使用以下命令先导入这4表的数据：
    
    `sh load-ssb-dimension-data.sh`
        
2. 导入事实表 lineorder。

    通过以下命令导入 lineorder 表数据：
    
    `sh load-ssb-fact-data.sh -c 5`
    
    `-c 5` 表示启动 5 个并发线程导入（默认为3）。在单 BE 节点情况下，由 `sh gen-ssb-data.sh -s 100 -c 100` 生成的 lineorder 数据，使用 `sh load-ssb-fact-data.sh -c 3` 的导入时间约为 10min。内存开销约为 5-6GB。如果开启更多线程，可以加快导入速度，但会增加额外的内存开销。

    > 注：为获得更快的导入速度，你可以在 be.conf 中添加 `flush_thread_num_per_store=5` 后重启BE。该配置表示每个数据目录的写盘线程数，默认为2。较大的数据可以提升写数据吞吐，但可能会增加 IO Util。（参考值：1块机械磁盘，在默认为2的情况下，导入过程中的 IO Util 约为12%，设置为5时，IO Util 约为26%。如果是 SSD 盘，则几乎为 0）。

3. 导入flat表

    通过以下命令导入 lineorder_flat 表数据：

    `sh load-ssb-flat-data.sh`

    > 注：flat 表数据采用 'INSERT INTO ... SELECT ... ' 的方式导入。

### 5. 检查导入数据

```
select count(*) from part;
select count(*) from customer;
select count(*) from supplier;
select count(*) from date;
select count(*) from lineorder;
select count(*) from lineorder_flat;
```

数据量应和生成数据的行数一致。

|Table |Rows |Origin Size | Compacted Size(1 Replica) |
|---|---|---|---|
|lineorder_flat| 6亿（600037902） |  | 59.709 GB |
|lineorder| 6亿（600037902） | 60 GB | 14.514 GB |
|customer|300万（3000000） |277 MB | 138.247 MB |
|part|140万（1400000） | 116 MB | 12.759 MB |
|supplier|20万（200000） |17 MB | 9.143 MB |
|date| 2556|228 KB | 34.276 KB |
    
### 6. 查询测试

1. 执行以下脚本跑 SSB flat 的查询：

    ```
    sh run-ssb-flat-queries.sh
    ```

    > 注1：可修改脚本中的设置的session变量来查看变化。
    >
    > 注2：SSB flat 测试集共 4 组 13 个 SQL。查询语句在 [ssb-flat-queries/](https://github.com/apache/incubator-doris/tree/master/tools/ssb-tools/ssb-flat-queries) 目录下。

2. 执行以下脚本跑 SSB 的查询：

    ```
    sh run-ssb-queries.sh
    ```

    > 注1：可修改脚本中的设置的session变量来查看变化。
    >
    > 注2：SSB 测试集共 4 组 13 个 SQL。查询语句在 [ssb-queries/](https://github.com/apache/incubator-doris/tree/master/tools/ssb-tools/ssb-queries) 目录下。
