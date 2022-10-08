---
{
    "title": "TPC-H benchmark",
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

# TPC-H benchmark

TPC-H是一个决策支持基准（Decision Support Benchmark），它由一套面向业务的特别查询和并发数据修改组成。查询和填充数据库的数据具有广泛的行业相关性。这个基准测试演示了检查大量数据、执行高度复杂的查询并回答关键业务问题的决策支持系统。TPC-H报告的性能指标称为TPC-H每小时复合查询性能指标(QphH@Size)，反映了系统处理查询能力的多个方面。这些方面包括执行查询时所选择的数据库大小，由单个流提交查询时的查询处理能力，以及由多个并发用户提交查询时的查询吞吐量。

本文档主要介绍 Doris 在 TPC-H 测试集上的性能表现。

> 注1：包括 TPC-H 在内的标准测试集通常和实际业务场景差距较大，并且部分测试会针对测试集进行参数调优。所以标准测试集的测试结果仅能反映数据库在特定场景下的性能表现。建议用户使用实际业务数据进行进一步的测试。
>
> 注2：本文档涉及的操作都在 CentOS 7.x 上进行测试。

在 TPC-H 标准测试数据集上的 22 个查询上，我们对即将发布的 Doris 1.1 版本和 Doris 0.15.0 RC04 版本进行了对别测试，整体性能提升了 3-4 倍。个别场景下达到十几倍的提升。

![image-20220614114351241](/images/image-20220614114351241.png)

## 1. 硬件环境

| 硬件     | 配置说明                                                     |
| -------- | ------------------------------------ |
| 机器数量 | 4 台腾讯云主机（1个FE，3个BE）       |
| CPU      | Intel Xeon(Cascade Lake) Platinum 8269CY  16核  (2.5 GHz/3.2 GHz) |
| 内存     | 64G                                  |
| 网络带宽  | 5Gbps                              |
| 磁盘     | ESSD云硬盘                      |

## 2. 软件环境

- Doris部署 3BE 1FE；
- 内核版本：Linux version 5.4.0-96-generic (buildd@lgw01-amd64-051)
- 操作系统版本：CentOS 7.8
- Doris 软件版本：Apache Doris 1.1 、Apache Doris 0.15.0 RC04
- JDK：openjdk version "11.0.14" 2022-01-18

## 3. 测试数据量

整个测试模拟生成100G的数据分别导入到 Doris 0.15.0 RC04 和 Doris 1.1 版本进行测试，下面是表的相关说明及数据量。

| TPC-H表名 | 行数   | 导入后大小 | 备注         |
| :-------- | :----- | ---------- | :----------- |
| REGION    | 5      | 400KB      | 区域表       |
| NATION    | 25     | 7.714 KB   | 国家表       |
| SUPPLIER  | 100万  | 85.528 MB  | 供应商表     |
| PART      | 2000万 | 752.330 MB | 零部件表     |
| PARTSUPP  | 8000万 | 4.375 GB   | 零部件供应表 |
| CUSTOMER  | 1500万 | 1.317 GB   | 客户表       |
| ORDERS    | 1.5亿  | 6.301 GB   | 订单表       |
| LINEITEM  | 6亿    | 20.882 GB  | 订单明细表   |

## 4. 测试SQL

TPCH 22个测试查询语句 ： [TPCH-Query-SQL](https://github.com/apache/doris/tree/master/tools/tpch-tools/queries)

**注意：**

以上SQL中的以下四个参数在0.15.0 RC04中不存在，在0.15.0 RC04中执行的时候，去掉：

```
1. enable_vectorized_engine=true,
2. batch_size=4096,
3. disable_join_reorder=false
4. enable_projection=true
```



## 5. 测试结果

这里我们使用即将发布的 Doris-1.1版本和 Doris-0.15.0 RC04 版本进行对比测试，测试结果如下：

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

**结果说明**

- 测试结果对应的数据集为scale 100, 约6亿条。
- 测试环境配置为用户常用配置，云服务器4台，16核 64G SSD，1 FE 3 BE 部署。
- 选用用户常见配置测试以降低用户选型评估成本，但整个测试过程中不会消耗如此多的硬件资源。
- 测试结果为3次执行取平均值。并且数据经过充分的 compaction（如果在刚导入数据后立刻测试，则查询延迟可能高于本测试结果，compaction的速度正在持续优化中，未来会显著降低）。
- 0.15 RC04 在 TPC-H 测试中 Q14 执行失败，无法完成查询。

## 6. 环境准备

请先参照 [官方文档](../install/install-deploy.md) 进行 Doris 的安装部署，以获得一个正常运行中的 Doris 集群（至少包含 1 FE 1 BE，推荐 1 FE 3 BE）。

## 7. 数据准备

### 7.1 下载安装 TPC-H 数据生成工具

执行以下脚本下载并编译  [tpch-tools](https://github.com/apache/doris/tree/master/tools/tpch-tools)  工具。

```shell
sh bin/build-tpch-dbgen.sh
```

安装成功后，将在 `TPC-H_Tools_v3.0.0/` 目录下生成 `dbgen` 二进制文件。

### 7.2 生成 TPC-H 测试集

执行以下脚本生成 TPC-H 数据集：

```shell
sh bin/gen-tpch-data.sh
```

> 注1：通过 `sh bin/gen-tpch-data.sh -h` 查看脚本帮助。
>
> 注2：数据会以 `.tbl` 为后缀生成在  `bin/tpch-data/` 目录下。文件总大小约100GB。生成时间可能在数分钟到1小时不等。
>
> 注3：默认生成 100G 的标准测试数据集

### 7.3 建表

#### 7.3.1 准备 `doris-cluster.conf` 文件

在调用导入脚本前，需要将 FE 的 ip 端口等信息写在 `conf/doris-cluster.conf` 文件中。

文件内容包括 FE 的 ip，HTTP 端口，用户名，密码以及待导入数据的 DB 名称：

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

#### 7.3.2 执行以下脚本生成创建 TPC-H 表

```shell
sh bin/create-tpch-tables.sh
```
或者复制 [create-tpch-tables.sql](https://github.com/apache/doris/blob/master/tools/tpch-tools/ddl/create-tpch-tables.sql) 中的建表语句，在 Doris 中执行。


### 7.4 导入数据

通过下面的命令执行数据导入：

```shell
sh bin/load-tpch-data.sh
```

### 7.5 检查导入数据

执行下面的 SQL 语句检查导入的数据量上 上面的数据量是一致。

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

### 7.6 查询测试

执行上面的测试 SQL 或者 执行下面的命令

```
sh bin/run-tpch-queries.sh
```

>注意：
>
>1. 目前Doris的查询优化器和统计信息功能还不完善，所以我们在TPC-H中重写了一些查询以适应Doris的执行框架，但不影响结果的正确性
>
>2. Doris 新的查询优化器将在后续的版本中发布
