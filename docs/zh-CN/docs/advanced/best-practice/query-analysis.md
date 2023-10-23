---
{
    "title": "查询分析",
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

# 查询分析

Doris 提供了一个图形化的命令以帮助用户更方便的分析一个具体的查询或导入。本文介绍如何使用该功能。

## 查询计划树

SQL 是一个描述性语言，用户通过一个 SQL 来描述想获取的数据。而一个 SQL 的具体执行方式依赖于数据库的实现。而查询规划器就是用来决定数据库如何具体执行一个 SQL 的。

比如用户指定了一个 Join 算子，则查询规划器需要决定具体的 Join 算法，比如是 Hash Join，还是 Merge Sort Join；是使用 Shuffle 还是 Broadcast；Join 顺序是否需要调整以避免笛卡尔积；以及确定最终的在哪些节点执行等等。

Doris 的查询规划过程是先将一个 SQL 语句转换成一个单机执行计划树。

```text
     ┌────┐
     │Sort│
     └────┘
        │
  ┌───────────┐
  │Aggregation│
  └───────────┘
        │
     ┌────┐
     │Join│
     └────┘
    ┌───┴────┐
┌──────┐ ┌──────┐
│Scan-1│ │Scan-2│
└──────┘ └──────┘
```

之后，查询规划器会根据具体的算子执行方式、数据的具体分布，将单机查询计划转换为分布式查询计划。分布式查询计划是由多个 Fragment 组成的，每个 Fragment 负责查询计划的一部分，各个 Fragment 之间会通过 ExchangeNode 算子进行数据的传输。

```text
        ┌────┐
        │Sort│
        │F1  │
        └────┘
           │
     ┌───────────┐
     │Aggregation│
     │F1         │
     └───────────┘
           │
        ┌────┐
        │Join│
        │F1  │
        └────┘
    ┌──────┴────┐
┌──────┐ ┌────────────┐
│Scan-1│ │ExchangeNode│
│F1    │ │F1          │
└──────┘ └────────────┘
                │
          ┌──────────────┐
          │DataStreamSink│
          │F2            │
          └──────────────┘
                │
            ┌──────┐
            │Scan-2│
            │F2    │
            └──────┘
```

如上图，我们将单机计划分成了两个 Fragment：F1 和 F2。两个 Fragment 之间通过一个 ExchangeNode 节点传输数据。

而一个 Fragment 会进一步的划分为多个 Instance。Instance 是最终具体的执行实例。划分成多个 Instance 有助于充分利用机器资源，提升一个 Fragment 的执行并发度。

## 查看查询计划

可以通过以下三种命令查看一个 SQL 的执行计划。

- `EXPLAIN GRAPH select ...;` 或者 `DESC GRAPH select ...;`：这些命令提供了执行计划的图形表示。它们帮助我们可视化查询执行的流程，包括关联路径和数据访问方法。

- `EXPLAIN select ...;`：这个命令显示指定 SQL 查询的执行计划的文本表示形式。它提供了有关查询优化步骤的信息，例如操作的顺序、执行算法和访问方法等。

- `EXPLAIN VERBOSE select ...;`：与前一个命令类似，这个命令提供了更详细的输出结果。

- `EXPLAIN PARSED PLAN select ...;`：这个命令返回 SQL 查询的解析后的执行计划。它显示了计划树和查询处理中涉及的逻辑操作符的信息。

- `EXPLAIN ANALYZED PLAN select ...;`：这个命令返回 SQL 查询的分析后的执行计划。

- `EXPLAIN REWRITTEN PLAN select ...;`：这个命令在数据库引擎对查询进行任何查询转换或优化后显示了重写后的执行计划。它提供了查询为了提高性能而进行的修改的见解。

- `EXPLAIN OPTIMIZED PLAN select ...;` 这个命令返回了 CBO 中得到的最优计划

- `EXPLAIN SHAPE PLAN select ...;`：这个命令以查询的形状和结构为重点，呈现了简化后的最优执行计划。

其中第一个命令以图形化的方式展示一个查询计划，这个命令可以比较直观的展示查询计划的树形结构，以及 Fragment 的划分情况：

```sql
mysql> explain graph select tbl1.k1, sum(tbl1.k2) from tbl1 join tbl2 on tbl1.k1 = tbl2.k1 group by tbl1.k1 order by tbl1.k1;
+---------------------------------------------------------------------------------------------------------------------------------+
| Explain String                                                                                                                  |
+---------------------------------------------------------------------------------------------------------------------------------+
|                                                                                                                                 |
|              ┌───────────────┐                                                                                                  |
|              │[9: ResultSink]│                                                                                                  |
|              │[Fragment: 4]  │                                                                                                  |
|              │RESULT SINK    │                                                                                                  |
|              └───────────────┘                                                                                                  |
|                      │                                                                                                          |
|           ┌─────────────────────┐                                                                                               |
|           │[9: MERGING-EXCHANGE]│                                                                                               |
|           │[Fragment: 4]        │                                                                                               |
|           └─────────────────────┘                                                                                               |
|                      │                                                                                                          |
|            ┌───────────────────┐                                                                                                |
|            │[9: DataStreamSink]│                                                                                                |
|            │[Fragment: 3]      │                                                                                                |
|            │STREAM DATA SINK   │                                                                                                |
|            │  EXCHANGE ID: 09  │                                                                                                |
|            │  UNPARTITIONED    │                                                                                                |
|            └───────────────────┘                                                                                                |
|                      │                                                                                                          |
|               ┌─────────────┐                                                                                                   |
|               │[4: TOP-N]   │                                                                                                   |
|               │[Fragment: 3]│                                                                                                   |
|               └─────────────┘                                                                                                   |
|                      │                                                                                                          |
|      ┌───────────────────────────────┐                                                                                          |
|      │[8: AGGREGATE (merge finalize)]│                                                                                          |
|      │[Fragment: 3]                  │                                                                                          |
|      └───────────────────────────────┘                                                                                          |
|                      │                                                                                                          |
|               ┌─────────────┐                                                                                                   |
|               │[7: EXCHANGE]│                                                                                                   |
|               │[Fragment: 3]│                                                                                                   |
|               └─────────────┘                                                                                                   |
|                      │                                                                                                          |
|            ┌───────────────────┐                                                                                                |
|            │[7: DataStreamSink]│                                                                                                |
|            │[Fragment: 2]      │                                                                                                |
|            │STREAM DATA SINK   │                                                                                                |
|            │  EXCHANGE ID: 07  │                                                                                                |
|            │  HASH_PARTITIONED │                                                                                                |
|            └───────────────────┘                                                                                                |
|                      │                                                                                                          |
|     ┌─────────────────────────────────┐                                                                                         |
|     │[3: AGGREGATE (update serialize)]│                                                                                         |
|     │[Fragment: 2]                    │                                                                                         |
|     │STREAMING                        │                                                                                         |
|     └─────────────────────────────────┘                                                                                         |
|                      │                                                                                                          |
|     ┌─────────────────────────────────┐                                                                                         |
|     │[2: HASH JOIN]                   │                                                                                         |
|     │[Fragment: 2]                    │                                                                                         |
|     │join op: INNER JOIN (PARTITIONED)│                                                                                         |
|     └─────────────────────────────────┘                                                                                         |
|           ┌──────────┴──────────┐                                                                                               |
|    ┌─────────────┐       ┌─────────────┐                                                                                        |
|    │[5: EXCHANGE]│       │[6: EXCHANGE]│                                                                                        |
|    │[Fragment: 2]│       │[Fragment: 2]│                                                                                        |
|    └─────────────┘       └─────────────┘                                                                                        |
|           │                     │                                                                                               |
| ┌───────────────────┐ ┌───────────────────┐                                                                                     |
| │[5: DataStreamSink]│ │[6: DataStreamSink]│                                                                                     |
| │[Fragment: 0]      │ │[Fragment: 1]      │                                                                                     |
| │STREAM DATA SINK   │ │STREAM DATA SINK   │                                                                                     |
| │  EXCHANGE ID: 05  │ │  EXCHANGE ID: 06  │                                                                                     |
| │  HASH_PARTITIONED │ │  HASH_PARTITIONED │                                                                                     |
| └───────────────────┘ └───────────────────┘                                                                                     |
|           │                     │                                                                                               |
|  ┌─────────────────┐   ┌─────────────────┐                                                                                      |
|  │[0: OlapScanNode]│   │[1: OlapScanNode]│                                                                                      |
|  │[Fragment: 0]    │   │[Fragment: 1]    │                                                                                      |
|  │TABLE: tbl1      │   │TABLE: tbl2      │                                                                                      |
|  └─────────────────┘   └─────────────────┘                                                                                      |
+---------------------------------------------------------------------------------------------------------------------------------+
```

从图中可以看出，查询计划树被分为了5个 Fragment：0、1、2、3、4。如 `OlapScanNode` 节点上的 `[Fragment: 0]` 表示这个节点属于 Fragment 0。每个Fragment之间都通过 DataStreamSink 和 ExchangeNode 进行数据传输。

图形命令仅展示简化后的节点信息，如果需要查看更具体的节点信息，如下推到节点上的过滤条件等，则需要通过第二个命令查看更详细的文字版信息：

```sql
mysql> explain select tbl1.k1, sum(tbl1.k2) from tbl1 join tbl2 on tbl1.k1 = tbl2.k1 group by tbl1.k1 order by tbl1.k1;
+----------------------------------------------------------------------------------+
| Explain String                                                                   |
+----------------------------------------------------------------------------------+
| PLAN FRAGMENT 0                                                                  |
|  OUTPUT EXPRS:<slot 5> <slot 3> `tbl1`.`k1` | <slot 6> <slot 4> sum(`tbl1`.`k2`) |
|   PARTITION: UNPARTITIONED                                                       |
|                                                                                  |
|   RESULT SINK                                                                    |
|                                                                                  |
|   9:MERGING-EXCHANGE                                                             |
|      limit: 65535                                                                |
|                                                                                  |
| PLAN FRAGMENT 1                                                                  |
|  OUTPUT EXPRS:                                                                   |
|   PARTITION: HASH_PARTITIONED: <slot 3> `tbl1`.`k1`                              |
|                                                                                  |
|   STREAM DATA SINK                                                               |
|     EXCHANGE ID: 09                                                              |
|     UNPARTITIONED                                                                |
|                                                                                  |
|   4:TOP-N                                                                        |
|   |  order by: <slot 5> <slot 3> `tbl1`.`k1` ASC                                 |
|   |  offset: 0                                                                   |
|   |  limit: 65535                                                                |
|   |                                                                              |
|   8:AGGREGATE (merge finalize)                                                   |
|   |  output: sum(<slot 4> sum(`tbl1`.`k2`))                                      |
|   |  group by: <slot 3> `tbl1`.`k1`                                              |
|   |  cardinality=-1                                                              |
|   |                                                                              |
|   7:EXCHANGE                                                                     |
|                                                                                  |
| PLAN FRAGMENT 2                                                                  |
|  OUTPUT EXPRS:                                                                   |
|   PARTITION: HASH_PARTITIONED: `tbl1`.`k1`                                       |
|                                                                                  |
|   STREAM DATA SINK                                                               |
|     EXCHANGE ID: 07                                                              |
|     HASH_PARTITIONED: <slot 3> `tbl1`.`k1`                                       |
|                                                                                  |
|   3:AGGREGATE (update serialize)                                                 |
|   |  STREAMING                                                                   |
|   |  output: sum(`tbl1`.`k2`)                                                    |
|   |  group by: `tbl1`.`k1`                                                       |
|   |  cardinality=-1                                                              |
|   |                                                                              |
|   2:HASH JOIN                                                                    |
|   |  join op: INNER JOIN (PARTITIONED)                                           |
|   |  runtime filter: false                                                       |
|   |  hash predicates:                                                            |
|   |  colocate: false, reason: table not in the same group                        |
|   |  equal join conjunct: `tbl1`.`k1` = `tbl2`.`k1`                              |
|   |  cardinality=2                                                               |
|   |                                                                              |
|   |----6:EXCHANGE                                                                |
|   |                                                                              |
|   5:EXCHANGE                                                                     |
|                                                                                  |
| PLAN FRAGMENT 3                                                                  |
|  OUTPUT EXPRS:                                                                   |
|   PARTITION: RANDOM                                                              |
|                                                                                  |
|   STREAM DATA SINK                                                               |
|     EXCHANGE ID: 06                                                              |
|     HASH_PARTITIONED: `tbl2`.`k1`                                                |
|                                                                                  |
|   1:OlapScanNode                                                                 |
|      TABLE: tbl2                                                                 |
|      PREAGGREGATION: ON                                                          |
|      partitions=1/1                                                              |
|      rollup: tbl2                                                                |
|      tabletRatio=3/3                                                             |
|      tabletList=105104776,105104780,105104784                                    |
|      cardinality=1                                                               |
|      avgRowSize=4.0                                                              |
|      numNodes=6                                                                  |
|                                                                                  |
| PLAN FRAGMENT 4                                                                  |
|  OUTPUT EXPRS:                                                                   |
|   PARTITION: RANDOM                                                              |
|                                                                                  |
|   STREAM DATA SINK                                                               |
|     EXCHANGE ID: 05                                                              |
|     HASH_PARTITIONED: `tbl1`.`k1`                                                |
|                                                                                  |
|   0:OlapScanNode                                                                 |
|      TABLE: tbl1                                                                 |
|      PREAGGREGATION: ON                                                          |
|      partitions=1/1                                                              |
|      rollup: tbl1                                                                |
|      tabletRatio=3/3                                                             |
|      tabletList=105104752,105104763,105104767                                    |
|      cardinality=2                                                               |
|      avgRowSize=8.0                                                              |
|      numNodes=6                                                                  |
+----------------------------------------------------------------------------------+
```

第三个命令`EXPLAIN VERBOSE select ...;`相比第二个命令可以查看更详细的执行计划信息。

```sql
mysql> explain verbose select tbl1.k1, sum(tbl1.k2) from tbl1 join tbl2 on tbl1.k1 = tbl2.k1 group by tbl1.k1 order by tbl1.k1;
+---------------------------------------------------------------------------------------------------------------------------------------------------------+
| Explain String                                                                                                                                          |
+---------------------------------------------------------------------------------------------------------------------------------------------------------+
| PLAN FRAGMENT 0                                                                                                                                         |
|   OUTPUT EXPRS:<slot 5> <slot 3> `tbl1`.`k1` | <slot 6> <slot 4> sum(`tbl1`.`k2`)                                                                       |
|   PARTITION: UNPARTITIONED                                                                                                                              |
|                                                                                                                                                         |
|   VRESULT SINK                                                                                                                                          |
|                                                                                                                                                         |
|   6:VMERGING-EXCHANGE                                                                                                                                   |
|      limit: 65535                                                                                                                                       |
|      tuple ids: 3                                                                                                                                       |
|                                                                                                                                                         |
| PLAN FRAGMENT 1                                                                                                                                         |
|                                                                                                                                                         |
|   PARTITION: HASH_PARTITIONED: `default_cluster:test`.`tbl1`.`k2`                                                                                       |
|                                                                                                                                                         |
|   STREAM DATA SINK                                                                                                                                      |
|     EXCHANGE ID: 06                                                                                                                                     |
|     UNPARTITIONED                                                                                                                                       |
|                                                                                                                                                         |
|   4:VTOP-N                                                                                                                                              |
|   |  order by: <slot 5> <slot 3> `tbl1`.`k1` ASC                                                                                                        |
|   |  offset: 0                                                                                                                                          |
|   |  limit: 65535                                                                                                                                       |
|   |  tuple ids: 3                                                                                                                                       |
|   |                                                                                                                                                     |
|   3:VAGGREGATE (update finalize)                                                                                                                        |
|   |  output: sum(<slot 8>)                                                                                                                              |
|   |  group by: <slot 7>                                                                                                                                 |
|   |  cardinality=-1                                                                                                                                     |
|   |  tuple ids: 2                                                                                                                                       |
|   |                                                                                                                                                     |
|   2:VHASH JOIN                                                                                                                                          |
|   |  join op: INNER JOIN(BROADCAST)[Tables are not in the same group]                                                                                   |
|   |  equal join conjunct: CAST(`tbl1`.`k1` AS DATETIME) = `tbl2`.`k1`                                                                                   |
|   |  runtime filters: RF000[in_or_bloom] <- `tbl2`.`k1`                                                                                                 |
|   |  cardinality=0                                                                                                                                      |
|   |  vec output tuple id: 4  |  tuple ids: 0 1                                                                                                          |
|   |                                                                                                                                                     |
|   |----5:VEXCHANGE                                                                                                                                      |
|   |       tuple ids: 1                                                                                                                                  |
|   |                                                                                                                                                     |
|   0:VOlapScanNode                                                                                                                                       |
|      TABLE: tbl1(null), PREAGGREGATION: OFF. Reason: the type of agg on StorageEngine's Key column should only be MAX or MIN.agg expr: sum(`tbl1`.`k2`) |
|      runtime filters: RF000[in_or_bloom] -> CAST(`tbl1`.`k1` AS DATETIME)                                                                               |
|      partitions=0/1, tablets=0/0, tabletList=                                                                                                           |
|      cardinality=0, avgRowSize=20.0, numNodes=1                                                                                                         |
|      tuple ids: 0                                                                                                                                       |
|                                                                                                                                                         |
| PLAN FRAGMENT 2                                                                                                                                         |
|                                                                                                                                                         |
|   PARTITION: HASH_PARTITIONED: `default_cluster:test`.`tbl2`.`k2`                                                                                       |
|                                                                                                                                                         |
|   STREAM DATA SINK                                                                                                                                      |
|     EXCHANGE ID: 05                                                                                                                                     |
|     UNPARTITIONED                                                                                                                                       |
|                                                                                                                                                         |
|   1:VOlapScanNode                                                                                                                                       |
|      TABLE: tbl2(null), PREAGGREGATION: OFF. Reason: null                                                                                               |
|      partitions=0/1, tablets=0/0, tabletList=                                                                                                           |
|      cardinality=0, avgRowSize=16.0, numNodes=1                                                                                                         |
|      tuple ids: 1                                                                                                                                       |
|                                                                                                                                                         |
| Tuples:                                                                                                                                                 |
| TupleDescriptor{id=0, tbl=tbl1, byteSize=32, materialized=true}                                                                                         |
|   SlotDescriptor{id=0, col=k1, type=DATE}                                                                                                               |
|     parent=0                                                                                                                                            |
|     materialized=true                                                                                                                                   |
|     byteSize=16                                                                                                                                         |
|     byteOffset=16                                                                                                                                       |
|     nullIndicatorByte=0                                                                                                                                 |
|     nullIndicatorBit=-1                                                                                                                                 |
|     slotIdx=1                                                                                                                                           |
|                                                                                                                                                         |
|   SlotDescriptor{id=2, col=k2, type=INT}                                                                                                                |
|     parent=0                                                                                                                                            |
|     materialized=true                                                                                                                                   |
|     byteSize=4                                                                                                                                          |
|     byteOffset=0                                                                                                                                        |
|     nullIndicatorByte=0                                                                                                                                 |
|     nullIndicatorBit=-1                                                                                                                                 |
|     slotIdx=0                                                                                                                                           |
|                                                                                                                                                         |
|                                                                                                                                                         |
| TupleDescriptor{id=1, tbl=tbl2, byteSize=16, materialized=true}                                                                                         |
|   SlotDescriptor{id=1, col=k1, type=DATETIME}                                                                                                           |
|     parent=1                                                                                                                                            |
|     materialized=true                                                                                                                                   |
|     byteSize=16                                                                                                                                         |
|     byteOffset=0                                                                                                                                        |
|     nullIndicatorByte=0                                                                                                                                 |
|     nullIndicatorBit=-1                                                                                                                                 |
|     slotIdx=0                                                                                                                                           |
|                                                                                                                                                         |
|                                                                                                                                                         |
| TupleDescriptor{id=2, tbl=null, byteSize=32, materialized=true}                                                                                         |
|   SlotDescriptor{id=3, col=null, type=DATE}                                                                                                             |
|     parent=2                                                                                                                                            |
|     materialized=true                                                                                                                                   |
|     byteSize=16                                                                                                                                         |
|     byteOffset=16                                                                                                                                       |
|     nullIndicatorByte=0                                                                                                                                 |
|     nullIndicatorBit=-1                                                                                                                                 |
|     slotIdx=1                                                                                                                                           |
|                                                                                                                                                         |
|   SlotDescriptor{id=4, col=null, type=BIGINT}                                                                                                           |
|     parent=2                                                                                                                                            |
|     materialized=true                                                                                                                                   |
|     byteSize=8                                                                                                                                          |
|     byteOffset=0                                                                                                                                        |
|     nullIndicatorByte=0                                                                                                                                 |
|     nullIndicatorBit=-1                                                                                                                                 |
|     slotIdx=0                                                                                                                                           |
|                                                                                                                                                         |
|                                                                                                                                                         |
| TupleDescriptor{id=3, tbl=null, byteSize=32, materialized=true}                                                                                         |
|   SlotDescriptor{id=5, col=null, type=DATE}                                                                                                             |
|     parent=3                                                                                                                                            |
|     materialized=true                                                                                                                                   |
|     byteSize=16                                                                                                                                         |
|     byteOffset=16                                                                                                                                       |
|     nullIndicatorByte=0                                                                                                                                 |
|     nullIndicatorBit=-1                                                                                                                                 |
|     slotIdx=1                                                                                                                                           |
|                                                                                                                                                         |
|   SlotDescriptor{id=6, col=null, type=BIGINT}                                                                                                           |
|     parent=3                                                                                                                                            |
|     materialized=true                                                                                                                                   |
|     byteSize=8                                                                                                                                          |
|     byteOffset=0                                                                                                                                        |
|     nullIndicatorByte=0                                                                                                                                 |
|     nullIndicatorBit=-1                                                                                                                                 |
|     slotIdx=0                                                                                                                                           |
|                                                                                                                                                         |
|                                                                                                                                                         |
| TupleDescriptor{id=4, tbl=null, byteSize=48, materialized=true}                                                                                         |
|   SlotDescriptor{id=7, col=k1, type=DATE}                                                                                                               |
|     parent=4                                                                                                                                            |
|     materialized=true                                                                                                                                   |
|     byteSize=16                                                                                                                                         |
|     byteOffset=16                                                                                                                                       |
|     nullIndicatorByte=0                                                                                                                                 |
|     nullIndicatorBit=-1                                                                                                                                 |
|     slotIdx=1                                                                                                                                           |
|                                                                                                                                                         |
|   SlotDescriptor{id=8, col=k2, type=INT}                                                                                                                |
|     parent=4                                                                                                                                            |
|     materialized=true                                                                                                                                   |
|     byteSize=4                                                                                                                                          |
|     byteOffset=0                                                                                                                                        |
|     nullIndicatorByte=0                                                                                                                                 |
|     nullIndicatorBit=-1                                                                                                                                 |
|     slotIdx=0                                                                                                                                           |
|                                                                                                                                                         |
|   SlotDescriptor{id=9, col=k1, type=DATETIME}                                                                                                           |
|     parent=4                                                                                                                                            |
|     materialized=true                                                                                                                                   |
|     byteSize=16                                                                                                                                         |
|     byteOffset=32                                                                                                                                       |
|     nullIndicatorByte=0                                                                                                                                 |
|     nullIndicatorBit=-1                                                                                                                                 |
|     slotIdx=2                                                                                                                                           |
+---------------------------------------------------------------------------------------------------------------------------------------------------------+
160 rows in set (0.00 sec)
```


> 查询计划中显示的信息还在不断规范和完善中，我们将在后续的文章中详细介绍。

## 查看查询 Profile

用户可以通过以下命令打开会话变量 `is_report_success`：

```sql
SET is_report_success=true;
```

然后执行查询，则 Doris 会产生该查询的一个 Profile。Profile 包含了一个查询各个节点的具体执行情况，有助于我们分析查询瓶颈。

执行完查询后，我们可以通过如下命令先获取 Profile 列表：

```sql
mysql> show query profile "/"\G
*************************** 1. row ***************************
   QueryId: c257c52f93e149ee-ace8ac14e8c9fef9
      User: root
 DefaultDb: default_cluster:db1
       SQL: select tbl1.k1, sum(tbl1.k2) from tbl1 join tbl2 on tbl1.k1 = tbl2.k1 group by tbl1.k1 order by tbl1.k1
 QueryType: Query
 StartTime: 2021-04-08 11:30:50
   EndTime: 2021-04-08 11:30:50
 TotalTime: 9ms
QueryState: EOF
```

这个命令会列出当前保存的所有 Profile。每行对应一个查询。我们可以选择我们想看的 Profile 对应的 QueryId，查看具体情况。

查看一个Profile分为3个步骤：

1. 查看整体执行计划树

   这一步主要用于从整体分析执行计划，并查看每个Fragment的执行耗时。

   ```sql
   mysql> show query profile "/c257c52f93e149ee-ace8ac14e8c9fef9"\G
   *************************** 1. row ***************************
   Fragments:
                ┌──────────────────────┐
                │[-1: DataBufferSender]│
                │Fragment: 0           │
                │MaxActiveTime: 6.626ms│
                └──────────────────────┘
                            │
                  ┌──────────────────┐
                  │[9: EXCHANGE_NODE]│
                  │Fragment: 0       │
                  └──────────────────┘
                            │
                ┌──────────────────────┐
                │[9: DataStreamSender] │
                │Fragment: 1           │
                │MaxActiveTime: 5.449ms│
                └──────────────────────┘
                            │
                    ┌──────────────┐
                    │[4: SORT_NODE]│
                    │Fragment: 1   │
                    └──────────────┘
                           ┌┘
                ┌─────────────────────┐
                │[8: AGGREGATION_NODE]│
                │Fragment: 1          │
                └─────────────────────┘
                           └┐
                  ┌──────────────────┐
                  │[7: EXCHANGE_NODE]│
                  │Fragment: 1       │
                  └──────────────────┘
                            │
                ┌──────────────────────┐
                │[7: DataStreamSender] │
                │Fragment: 2           │
                │MaxActiveTime: 3.505ms│
                └──────────────────────┘
                           ┌┘
                ┌─────────────────────┐
                │[3: AGGREGATION_NODE]│
                │Fragment: 2          │
                └─────────────────────┘
                           │
                 ┌───────────────────┐
                 │[2: HASH_JOIN_NODE]│
                 │Fragment: 2        │
                 └───────────────────┘
              ┌────────────┴────────────┐
    ┌──────────────────┐      ┌──────────────────┐
    │[5: EXCHANGE_NODE]│      │[6: EXCHANGE_NODE]│
    │Fragment: 2       │      │Fragment: 2       │
    └──────────────────┘      └──────────────────┘
              │                         │
   ┌─────────────────────┐ ┌────────────────────────┐
   │[5: DataStreamSender]│ │[6: DataStreamSender]   │
   │Fragment: 4          │ │Fragment: 3             │
   │MaxActiveTime: 1.87ms│ │MaxActiveTime: 636.767us│
   └─────────────────────┘ └────────────────────────┘
              │                        ┌┘
    ┌───────────────────┐    ┌───────────────────┐
    │[0: OLAP_SCAN_NODE]│    │[1: OLAP_SCAN_NODE]│
    │Fragment: 4        │    │Fragment: 3        │
    └───────────────────┘    └───────────────────┘
              │                        │
       ┌─────────────┐          ┌─────────────┐
       │[OlapScanner]│          │[OlapScanner]│
       │Fragment: 4  │          │Fragment: 3  │
       └─────────────┘          └─────────────┘
              │                        │
     ┌─────────────────┐      ┌─────────────────┐
     │[SegmentIterator]│      │[SegmentIterator]│
     │Fragment: 4      │      │Fragment: 3      │
     └─────────────────┘      └─────────────────┘
   
   1 row in set (0.02 sec)
   ```

   如上图，每个节点都标注了自己所属的 Fragment，并且在每个 Fragment 的 Sender节点，标注了该 Fragment 的执行耗时。这个耗时，是Fragment下所有 Instance 执行耗时中最长的一个。这个有助于我们从整体角度发现最耗时的 Fragment。

2. 查看具体 Fragment 下的 Instance 列表

   比如我们发现 Fragment 1 耗时最长，则可以继续查看 Fragment 1 的 Instance 列表：
   
   ```sql
   mysql> show query profile "/c257c52f93e149ee-ace8ac14e8c9fef9/1";
   +-----------------------------------+-------------------+------------+
   | Instances                         | Host              | ActiveTime |
   +-----------------------------------+-------------------+------------+
   | c257c52f93e149ee-ace8ac14e8c9ff03 | 10.200.00.01:9060 | 5.449ms    |
   | c257c52f93e149ee-ace8ac14e8c9ff05 | 10.200.00.02:9060 | 5.367ms    |
   | c257c52f93e149ee-ace8ac14e8c9ff04 | 10.200.00.03:9060 | 5.358ms    |
   +-----------------------------------+-------------------+------------+ 
   ```
   
   这里展示了 Fragment 1 上所有的 3 个 Instance 所在的执行节点和耗时。

1. 查看具体 Instance

   我们可以继续查看某一个具体的 Instance 上各个算子的详细 Profile：

   ```sql
   mysql> show query profile "/c257c52f93e149ee-ace8ac14e8c9fef9/1/c257c52f93e149ee-ace8ac14e8c9ff03"\G
   *************************** 1. row ***************************
   Instance:
    ┌───────────────────────────────────────┐
    │[9: DataStreamSender]                  │
    │(Active: 37.222us, non-child: 0.40)    │
    │  - Counters:                          │
    │      - BytesSent: 0.00                │
    │      - IgnoreRows: 0                  │
    │      - OverallThroughput: 0.0 /sec    │
    │      - PeakMemoryUsage: 8.00 KB       │
    │      - SerializeBatchTime: 0ns        │
    │      - UncompressedRowBatchSize: 0.00 │
    └───────────────────────────────────────┘
                        └┐
                         │
       ┌──────────────────────────────────┐
       │[4: SORT_NODE]                    │
       │(Active: 5.421ms, non-child: 0.71)│
       │  - Counters:                     │
       │      - PeakMemoryUsage: 12.00 KB │
       │      - RowsReturned: 0           │
       │      - RowsReturnedRate: 0       │
       └──────────────────────────────────┘
                        ┌┘
                        │
      ┌───────────────────────────────────┐
      │[8: AGGREGATION_NODE]              │
      │(Active: 5.355ms, non-child: 10.68)│
      │  - Counters:                      │
      │      - BuildTime: 3.701us         │
      │      - GetResultsTime: 0ns        │
      │      - HTResize: 0                │
      │      - HTResizeTime: 1.211us      │
      │      - HashBuckets: 0             │
      │      - HashCollisions: 0          │
      │      - HashFailedProbe: 0         │
      │      - HashFilledBuckets: 0       │
      │      - HashProbe: 0               │
      │      - HashTravelLength: 0        │
      │      - LargestPartitionPercent: 0 │
      │      - MaxPartitionLevel: 0       │
      │      - NumRepartitions: 0         │
      │      - PartitionsCreated: 16      │
      │      - PeakMemoryUsage: 34.02 MB  │
      │      - RowsProcessed: 0           │
      │      - RowsRepartitioned: 0       │
      │      - RowsReturned: 0            │
      │      - RowsReturnedRate: 0        │
      │      - SpilledPartitions: 0       │
      └───────────────────────────────────┘
                        └┐
                         │
   ┌──────────────────────────────────────────┐
   │[7: EXCHANGE_NODE]                        │
   │(Active: 4.360ms, non-child: 46.84)       │
   │  - Counters:                             │
   │      - BytesReceived: 0.00               │
   │      - ConvertRowBatchTime: 387ns        │
   │      - DataArrivalWaitTime: 4.357ms      │
   │      - DeserializeRowBatchTimer: 0ns     │
   │      - FirstBatchArrivalWaitTime: 4.356ms│
   │      - PeakMemoryUsage: 0.00             │
   │      - RowsReturned: 0                   │
   │      - RowsReturnedRate: 0               │
   │      - SendersBlockedTotalTimer(*): 0ns  │
   └──────────────────────────────────────────┘
   ```

   上图展示了 Fragment 1 中，Instance c257c52f93e149ee-ace8ac14e8c9ff03 的各个算子的具体 Profile。

通过以上3个步骤，我们可以逐步排查一个SQL的性能瓶颈。
