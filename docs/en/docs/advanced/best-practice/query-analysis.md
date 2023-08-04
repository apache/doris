---
{
    "title": "Query Analysis",
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


# Query Analysis

Doris provides a graphical command to help users analyze a specific query or import more easily. This article describes how to use this feature.

## Query Plan Tree

SQL is a descriptive language, and users describe the data they want to get through a SQL. The specific execution mode of a SQL depends on the implementation of the database. The query planner is used to determine how the database executes a SQL.

For example, if the user specifies a Join operator, the query planner needs to decide the specific Join algorithm, such as Hash Join or Merge Sort Join; whether to use Shuffle or Broadcast; whether the Join order needs to be adjusted to avoid Cartesian product; on which nodes to execute and so on.

Doris' query planning process is to first convert an SQL statement into a single-machine execution plan tree.

````text
     ┌────┐
     │Sort│
     └────┘
        │
  ┌──────────────┐
  │Aggregation│
  └──────────────┘
        │
     ┌────┐
     │Join│
     └────┘
    ┌────┴────┐
┌──────┐ ┌──────┐
│Scan-1│ │Scan-2│
└──────┘ └──────┘
````

After that, the query planner will convert the single-machine query plan into a distributed query plan according to the specific operator execution mode and the specific distribution of data. The distributed query plan is composed of multiple fragments, each fragment is responsible for a part of the query plan, and the data is transmitted between the fragments through the ExchangeNode operator.

````text
        ┌────┐
        │Sort│
        │F1 │
        └────┘
           │
     ┌──────────────┐
     │Aggregation│
     │F1 │
     └──────────────┘
           │
        ┌────┐
        │Join│
        │F1 │
        └────┘
    ┌──────┴────┐
┌──────┐ ┌────────────┐
│Scan-1│ │ExchangeNode│
│F1 │ │F1 │
└──────┘ └────────────┘
                │
          ┌────────────────┐
          │DataStreamDink│
          │F2 │
          └────────────────┘
                │
            ┌──────┐
            │Scan-2│
            │F2 │
            └──────┘
````

As shown above, we divided the stand-alone plan into two Fragments: F1 and F2. Data is transmitted between two Fragments through an ExchangeNode.

And a Fragment will be further divided into multiple Instances. Instance is the final concrete execution instance. Dividing into multiple Instances helps to make full use of machine resources and improve the execution concurrency of a Fragment.

## View Query Plan

You can view the execution plan of a SQL through the following three commands.

- `EXPLAIN GRAPH select ...;` OR `DESC GRAPH select ...;` These commands provide a graphical representation of the execution plan. 
- `EXPLAIN select ...;` This command displays a textual representation of the execution plan for the specified SQL query. 
- `EXPLAIN VERBOSE select ...;` Similar to the previous command, this command provides a more detailed output.
- `EXPLAIN PARSED PLAN select ...;` This command returns the parsed execution plan of the SQL query. It displays the plan trees and information about the logical operators involved in query processing.
- `EXPLAIN ANALYZED PLAN select ...;` This command returns the analyzed execution plan for the SQL query.
- `EXPLAIN REWRITTEN PLAN select ...;` This command shows the rewritten execution plan after applying any query transformations or optimizations performed by the database engine.
- `EXPLAIN OPTIMIZED PLAN select ...;` This command shows the best execution plan after CBO.
- `EXPLAIN SHAPE PLAN select ...;` This command presents the simplified execution plan with a focus on how the query is shaped and structured.

The first command displays a query plan graphically. This command can more intuitively display the tree structure of the query plan and the division of Fragments:

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

As can be seen from the figure, the query plan tree is divided into 5 fragments: 0, 1, 2, 3, and 4. For example, `[Fragment: 0]` on the `OlapScanNode` node indicates that this node belongs to Fragment 0. Data is transferred between each Fragment through DataStreamSink and ExchangeNode.

The graphics command only displays the simplified node information. If you need to view more specific node information, such as the filter conditions pushed to the node as follows, you need to view the more detailed text version information through the second command:

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

The third command `explain verbose select ...;` gives you more details than the second command.

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

> The information displayed in the query plan is still being standardized and improved, and we will introduce it in detail in subsequent articles.

## View Query Profile

The user can open the session variable `is_report_success` with the following command:

```sql
SET is_report_success=true;
````

Then execute the query, and Doris will generate a Profile of the query. Profile contains the specific execution of a query for each node, which helps us analyze query bottlenecks.

After executing the query, we can first get the Profile list with the following command:

```sql
mysql> show query profile "/"\G
**************************** 1. row ******************** ******
   QueryId: c257c52f93e149ee-ace8ac14e8c9fef9
      User: root
 DefaultDb: default_cluster:db1
       SQL: select tbl1.k1, sum(tbl1.k2) from tbl1 join tbl2 on tbl1.k1 = tbl2.k1 group by tbl1.k1 order by tbl1.k1
 QueryType: Query
 StartTime: 2021-04-08 11:30:50
   EndTime: 2021-04-08 11:30:50
 TotalTime: 9ms
QueryState: EOF
````

This command will list all currently saved profiles. Each row corresponds to a query. We can select the QueryId corresponding to the Profile we want to see to see the specific situation.

Viewing a Profile is divided into 3 steps:

1. View the overall execution plan tree

   This step is mainly used to analyze the execution plan as a whole and view the execution time of each Fragment.
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
   As shown in the figure above, each node is marked with the Fragment to which it belongs, and at the Sender node of each Fragment, the execution time of the Fragment is marked. This time-consuming is the longest of all Instance execution times under Fragment. This helps us find the most time-consuming Fragment from an overall perspective.

2. View the Instance list under the specific Fragment

   For example, if we find that Fragment 1 takes the longest time, we can continue to view the Instance list of Fragment 1:
   
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
This shows the execution nodes and time consumption of all three Instances on Fragment 1.

1. View the specific Instance

   We can continue to view the detailed profile of each operator on a specific Instance:

   ```sql
   mysql> show query profile "/c257c52f93e149ee-ace8ac14e8c9fef9/1/c257c52f93e149ee-ace8ac14e8c9ff03"\G
   **************************** 1. row ******************** ******
   Instance:
    ┌────────────────────────────────────────────┐
    │[9: DataStreamSender] │
    │(Active: 37.222us, non-child: 0.40) │
    │ - Counters: │
    │ - BytesSent: 0.00 │
    │ - IgnoreRows: 0 │
    │ - OverallThroughput: 0.0 /sec │
    │ - PeakMemoryUsage: 8.00 KB │
    │ - SerializeBatchTime: 0ns │
    │ - UncompressedRowBatchSize: 0.00 │
    └──────────────────────────────────────────┘
                        └┐
                         │
       ┌──────────────────────────────────────┐
       │[4: SORT_NODE] │
       │(Active: 5.421ms, non-child: 0.71)│
       │ - Counters: │
       │ - PeakMemoryUsage: 12.00 KB │
       │ - RowsReturned: 0 │
       │ - RowsReturnedRate: 0 │
       └──────────────────────────────────────┘
                        ┌┘
                        │
      ┌──────────────────────────────────────┐
      │[8: AGGREGATION_NODE] │
      │(Active: 5.355ms, non-child: 10.68)│
      │ - Counters: │
      │ - BuildTime: 3.701us │
      │ - GetResultsTime: 0ns │
      │ - HTResize: 0 │
      │ - HTResizeTime: 1.211us │
      │ - HashBuckets: 0 │
      │ - HashCollisions: 0 │
      │ - HashFailedProbe: 0 │
      │ - HashFilledBuckets: 0 │
      │ - HashProbe: 0 │
      │ - HashTravelLength: 0 │
      │ - LargestPartitionPercent: 0 │
      │ - MaxPartitionLevel: 0 │
      │ - NumRepartitions: 0 │
      │ - PartitionsCreated: 16 │
      │ - PeakMemoryUsage: 34.02 MB │
      │ - RowsProcessed: 0 │
      │ - RowsRepartitioned: 0 │
      │ - RowsReturned: 0 │
      │ - RowsReturnedRate: 0 │
      │ - SpilledPartitions: 0 │
      └──────────────────────────────────────┘
                        └┐
                         │
   ┌────────────────────────────────────────────────────┐
   │[7: EXCHANGE_NODE] │
   │(Active: 4.360ms, non-child: 46.84) │
   │ - Counters: │
   │ - BytesReceived: 0.00 │
   │ - ConvertRowBatchTime: 387ns │
   │ - DataArrivalWaitTime: 4.357ms │
   │ - DeserializeRowBatchTimer: 0ns │
   │ - FirstBatchArrivalWaitTime: 4.356ms│
   │ - PeakMemoryUsage: 0.00 │
   │ - RowsReturned: 0 │
   │ - RowsReturnedRate: 0 │
   │ - SendersBlockedTotalTimer(*): 0ns │
   └────────────────────────────────────────────────────┘
   ````

   The above figure shows the specific profiles of each operator of Instance c257c52f93e149ee-ace8ac14e8c9ff03 in Fragment 1.

Through the above three steps, we can gradually check the performance bottleneck of a SQL.