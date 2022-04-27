---
{
    "title": "TPC-H Benchmark 测试",
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

# TPC-H Benchmark

[TPC-H](https://www.tpc.org/tpch/) 是美国交易处理效能委员会 TPC（Transaction Processing Performance Council）组织制定的用来模拟决策支持类应用的测试集。TPC-H查询包含8张数据表，数据量可设定从1GB~3TB 不等。包含22条复杂的SQL查询，大多数查询包含若干表Join、子查询和Group-by聚合等。

TPC-H 基准测试包括 22 个查询（Q1~Q22）,其主要评价指标是各个查询的响应时间,即从提交查询到结果返回所需时间.TPC-H 基准测试的度量单位是每小时执行的查询数。

## 测试流程

测试脚本均在目录 [tpch-tools](https://github.com/apache/incubator-doris/tree/master/tools/tpch-tools)

### 1.编译TPC-H生成工具

```
./build-tpch-dbgen.sh
```

### 2.生成数据

```
./gen-tpch-data.sh -s 1
```

### 3.在doris集群中创建表

```shell
#执行前需要修改doris-cluster.conf集群配置
./create-tpch-tables.sh
```

### 4.导入数据

```
./load-tpch-data.sh
```

### 5.执行查询

```
./run-tpch-queries.sh
```

## 测试报告

以下测试报告基于 Doris1.0 测试，仅供参考。

1. 硬件环境

   - 1 FE + 3 BE
   - CPU：16核CPU
   - 内存：64GB
   - 硬盘：SSD 1T
   - 网卡：万兆网卡

2. 数据集

   TPC-H测试集scale 100，生成的原始数据文件大约107G。

3. 测试结果

   | 单位（ms） | doris 1.0（ms） |
   | ---------- | --------------- |
   | q1         | 4215            |
   | q2         | 13633           |
   | q3         | 9677            |
   | q4         | 7087            |
   | q5         | 4290            |
   | q6         | 1045            |
   | q7         | 2147            |
   | q8         | 3073            |
   | q9         | 33064           |
   | q10        | 5733            |
   | q11        | 2598            |
   | q12        | 4998            |
   | q13        | 10798           |
   | q14        | 11786           |
   | q15        | 2038            |
   | q16        | 3313            |
   | q17        | 20340           |
   | q18        | 23277           |
   | q19        | 1645            |
   | q20        | 5738            |
   | q21        | 18520           |
   | q22        | 1041            |

