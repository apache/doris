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

[Star Schema Benchmark(SSB)](https://www.cs.umb.edu/~poneil/StarSchemaB.PDF) 是一个轻量级的数仓场景下的性能测试集。SSB基于 [TPC-H](http://www.tpc.org/tpch/) 提供了一个简化版的星型模型数据集，主要用于测试在星型模型下，多表关联查询的性能表现。

本文档主要介绍如何在 Doris 中通过 SSB 进行初步的性能测试。

> 注1：包括 SSB 在内的标准测试集通常和实际业务场景差距较大，并且部分测试会针对测试集进行参数调优。所以标准测试集的测试结果仅能反映数据库在特定场景下的性能表现。建议用户使用实际业务数据进行进一步的测试。
> 
> 注2：本文档涉及的操作都在 CentOS 7 环境进行。

## 环境准备

请先参照 [官方文档](../install/install-deploy.html) 进行 Doris 的安装部署，以获得一个正常运行中的 Doris 集群（至少包含 1 FE，1 BE）。

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

3. 建表

    复制 [create-tables.sql](https://github.com/apache/incubator-doris/tree/master/tools/ssb-tools/create-tables.sql) 中的建表语句，在 Doris 中执行。

4. 导入数据

    0. 准备 'doris-cluster.conf' 文件。
    
        在调用导入脚本前，需要将 FE 的 ip 端口等信息写在 `doris-cluster.conf` 文件中。
        
        文件位置和 `load-dimension-data.sh` 平级。
      
        文件内容包括 FE 的 ip，HTTP 端口，用户名，密码以及待导入数据的 DB 名称：
      
        ```
        export FE_HOST="xxx"
        export FE_HTTP_PORT="8030"
        export USER="root"
        export PASSWORD='xxx'
        export DB="ssb"
        ```

    1. 导入 4 张维度表数据（customer, part, supplier and date）
    
        因为这4张维表数据量较小，导入较简单，我们使用以下命令先导入这4表的数据：
        
        `sh load-dimension-data.sh`
        
    2. 导入事实表 lineorder。

        通过以下命令导入 lineorder 表数据：
        
        `sh load-fact-data.sh -c 5`
        
        `-c 5` 表示启动 5 个并发线程导入（默认为3）。在单 BE 节点情况下，由 `sh gen-ssb-data.sh -s 100 -c 100` 生成的 lineorder 数据，使用 `sh load-fact-data.sh -c 3` 的导入时间约为 10min。内存开销约为 5-6GB。如果开启更多线程，可以加快导入速度，但会增加额外的内存开销。

    > 注：为获得更快的导入速度，你可以在 be.conf 中添加 `flush_thread_num_per_store=5` 后重启BE。该配置表示每个数据目录的写盘线程数，默认为2。较大的数据可以提升写数据吞吐，但可能会增加 IO Util。（参考值：1块机械磁盘，在默认为2的情况下，导入过程中的 IO Util 约为12%，设置为5时，IO Util 约为26%。如果是 SSD 盘，则几乎为 0）。

5. 检查导入数据

    ```
    select count(*) from part;
    select count(*) from customer;
    select count(*) from supplier;
    select count(*) from date;
    select count(*) from lineorder;
    ```
    
    数据量应和生成数据的行数一致。
    
## 查询测试

SSB 测试集共 4 组 14 个 SQL。查询语句在 [queries/](https://github.com/apache/incubator-doris/tree/master/tools/ssb-tools/queries) 目录下。 

## 测试报告

以下测试报告基于 Doris [branch-0.15](https://github.com/apache/incubator-doris/tree/branch-0.15) 分支代码测试，仅供参考。（更新时间：2021年10月25号）

1. 硬件环境

    * 1 FE + 1-3 BE 混部
    * CPU：96core, Intel(R) Xeon(R) Gold 6271C CPU @ 2.60GHz
    * 内存：384GB
    * 硬盘：1块机械硬盘
    * 网卡：万兆网卡

2. 数据集

    |Table |Rows |Origin Size | Compacted Size(1 Replica) |
    |---|---|---|---|
    |lineorder| 6亿（600037902） | 60 GB | 14.846 GB |
    |customer|300万（3000000） |277 MB | 414.741 MB |
    |part|140万（1400000） | 116 MB | 38.277 MB |
    |supplier|20万（200000） |17 MB | 27.428 MB |
    |date| 2556|228 KB | 275.804 KB |

3. 测试结果

    |Query |Time(ms) (1 BE) | Time(ms) (3 BE) | Parallelism | Runtime Filter Mode |
    |---|---|---|---|---|
    | q1.1 | 200 | 140 | 8 | IN |
    | q1.2 | 90 | 80 | 8 | IN |
    | q1.3 | 90 | 80 | 8 | IN |
    | q2.1 | 1100 | 400 |  8 | BLOOM_FILTER |
    | q2.2 | 900 | 330 | 8 | BLOOM_FILTER |
    | q2.3 | 790 | 320 | 8 | BLOOM_FILTER |
    | q3.1 | 3100 | 1280 | 8 | BLOOM_FILTER |
    | q3.2 | 700 | 270 | 8 | BLOOM_FILTER |
    | q3.3 | 540 | 270 | 8 | BLOOM_FILTER |
    | q3.4 | 560 | 240 | 8 | BLOOM_FILTER |
    | q4.1 | 2820 | 1150 | 8 | BLOOM_FILTER |
    | q4.2 | 1430 | 670 | 8 | BLOOM_FILTER |
    | q4.2 | 1750 | 1030 | 8 | BLOOM_FILTER |

    > 注1：“这个测试集和你的生产环境相去甚远，请对他保持怀疑态度！”
    > 
    > 注2：测试结果为多次执行取平均值（Page Cache 会起到一定加速作用）。并且数据经过充分的 compaction （如果在刚导入数据后立刻测试，则查询延迟可能高于本测试结果）
    >
    > 注3：因环境受限，本测试使用的硬件规格较高，但整个测试过程中不会消耗如此多的硬件资源。其中内存消耗在 10GB 以内，CPU使用率在 10% 以内。
    >
    > 注4：Parallelism 表示查询并发度，通过 `set parallel_fragment_exec_instance_num=8` 设置。
    >
    > 注5：Runtime Filter Mode 是 Runtime Filter 的类型，通过 `set runtime_filter_type="BLOOM_FILTER"` 设置。（[Runtime Filter](../advanced/join-optimization/runtime-filter.html) 功能对 SSB 测试集效果显著。因为该测试集中，Join 算子右表的数据可以对左表起到很好的过滤作用。你可以尝试通过 `set runtime_filter_mode=off` 关闭该功能，看看查询延迟的变化。）

