---
{
    "title": "导入分析",
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

# 导入分析

Doris 提供了一个图形化的命令以帮助用户更方便的分析一个具体的导入。本文介绍如何使用该功能。

> 该功能目前仅针对 Broker Load 的分析。

## 导入计划树

如果你对 Doris 的查询计划树还不太了解，请先阅读之前的文章 [DORIS/最佳实践/查询分析](./query-analysis.md)。

一个 [Broker Load](../../data-operate/import/import-way/broker-load-manual) 请求的执行过程，也是基于 Doris 的查询框架的。一个Broker Load 作业会根据导入请求中 DATA INFILE 子句的个数将作业拆分成多个子任务。每个子任务可以视为是一个独立的导入执行计划。一个导入计划的组成只会有一个 Fragment，其组成如下：

```sql
┌─────────────┐
│OlapTableSink│
└─────────────┘
        │
┌──────────────┐
│BrokerScanNode│
└──────────────┘
```

BrokerScanNode 主要负责去读源数据并发送给 OlapTableSink，而 OlapTableSink 负责将数据按照分区分桶规则发送到对应的节点，由对应的节点负责实际的数据写入。

而这个导入执行计划的 Fragment 会根据导入源文件的数量、BE节点的数量等参数，划分成一个或多个 Instance。每个 Instance 负责一部分数据导入。

多个子任务的执行计划是并发执行的，而一个执行计划的多个 Instance 也是并行执行。

## 查看导入 Profile

用户可以通过以下命令打开会话变量 `is_report_success`：

```sql
SET is_report_success=true;
```

然后提交一个 Broker Load 导入请求，并等到导入执行完成。Doris 会产生该导入的一个 Profile。Profile 包含了一个导入各个子任务、Instance 的执行详情，有助于我们分析导入瓶颈。

> 目前不支持查看未执行成功的导入作业的 Profile。

我们可以通过如下命令先获取 Profile 列表：

```sql
mysql> show load profile "/"\G
*************************** 1. row ***************************
                 JobId: 20010
               QueryId: 980014623046410a-af5d36f23381017f
                  User: root
             DefaultDb: default_cluster:test
                   SQL: LOAD LABEL xxx
             QueryType: Load
             StartTime: 2023-03-07 19:48:24
               EndTime: 2023-03-07 19:50:45
             TotalTime: 2m21s
            QueryState: N/A
               TraceId:
          AnalysisTime: NULL
              PlanTime: NULL
          ScheduleTime: NULL
       FetchResultTime: NULL
       WriteResultTime: NULL
WaitAndFetchResultTime: NULL
*************************** 2. row ***************************
                 JobId: N/A
               QueryId: 7cc2d0282a7a4391-8dd75030185134d8
                  User: root
             DefaultDb: default_cluster:test
                   SQL: insert into xxx
             QueryType: Load
             StartTime: 2023-03-07 19:49:15
               EndTime: 2023-03-07 19:49:15
             TotalTime: 102ms
            QueryState: OK
               TraceId:
          AnalysisTime: 825.277us
              PlanTime: 4.126ms
          ScheduleTime: N/A
       FetchResultTime: 0ns
       WriteResultTime: 0ns
WaitAndFetchResultTime: N/A
```

这个命令会列出当前保存的所有导入 Profile。每行对应一个导入。其中 QueryId 列为导入作业的 ID。这个 ID 也可以通过 SHOW LOAD 语句查看拿到。我们可以选择我们想看的 Profile 对应的 QueryId，查看具体情况。

**查看一个Profile分为3个步骤：**

1. 查看子任务总览

   通过以下命令查看有导入作业的子任务概况：

   

   ```sql
   mysql> show load profile "/980014623046410a-af5d36f23381017f";
   +-----------------------------------+------------+
   | TaskId                            | ActiveTime |
   +-----------------------------------+------------+
   | 980014623046410a-af5d36f23381017f | 3m14s      |
   +-----------------------------------+------------+
   ```

   如上图，表示 `980014623046410a-af5d36f23381017f` 这个导入作业总共有一个子任务，其中 ActiveTime 表示这个子任务中耗时最长的 Instance 的执行时间。

2. 查看指定子任务的 Instance 概况

   当我们发现一个子任务耗时较长时，可以进一步查看该子任务的各个 Instance 的执行耗时：

   

   ```sql
   mysql> show load profile "/980014623046410a-af5d36f23381017f/980014623046410a-af5d36f23381017f";
   +-----------------------------------+------------------+------------+
   | Instances                         | Host             | ActiveTime |
   +-----------------------------------+------------------+------------+
   | 980014623046410a-88e260f0c43031f2 | 10.81.85.89:9067 | 3m7s       |
   | 980014623046410a-88e260f0c43031f3 | 10.81.85.89:9067 | 3m6s       |
   | 980014623046410a-88e260f0c43031f4 | 10.81.85.89:9067 | 3m10s      |
   | 980014623046410a-88e260f0c43031f5 | 10.81.85.89:9067 | 3m14s      |
   +-----------------------------------+------------------+------------+
   ```

   这里展示了 980014623046410a-af5d36f23381017f 这个子任务的四个 Instance 耗时，并且还展示了 Instance 所在的执行节点。

3. 查看具体 Instance

   我们可以继续查看某一个具体的 Instance 上各个算子的详细 Profile：

   

   ```sql
   mysql> show load profile "/980014623046410a-af5d36f23381017f/980014623046410a-af5d36f23381017f/980014623046410a-88e260f0c43031f5"\G
   *************************** 1. row ***************************
   Instance:
         ┌-----------------------------------------┐
         │[-1: OlapTableSink]                      │
         │(Active: 2m17s, non-child: 70.91)        │
         │  - Counters:                            │
         │      - CloseWaitTime: 1m53s             │
         │      - ConvertBatchTime: 0ns            │
         │      - MaxAddBatchExecTime: 1m46s       │
         │      - NonBlockingSendTime: 3m11s       │
         │      - NumberBatchAdded: 782            │
         │      - NumberNodeChannels: 1            │
         │      - OpenTime: 743.822us              │
         │      - RowsFiltered: 0                  │
         │      - RowsRead: 1.599729M (1599729)    │
         │      - RowsReturned: 1.599729M (1599729)│
         │      - SendDataTime: 11s761ms           │
         │      - TotalAddBatchExecTime: 1m46s     │
         │      - ValidateDataTime: 9s802ms        │
         └-----------------------------------------┘
                              │
   ┌-----------------------------------------------------┐
   │[0: BROKER_SCAN_NODE]                                │
   │(Active: 56s537ms, non-child: 29.06)                 │
   │  - Counters:                                        │
   │      - BytesDecompressed: 0.00                      │
   │      - BytesRead: 5.77 GB                           │
   │      - DecompressTime: 0ns                          │
   │      - FileReadTime: 34s263ms                       │
   │      - MaterializeTupleTime(*): 45s54ms             │
   │      - NumDiskAccess: 0                             │
   │      - PeakMemoryUsage: 33.03 MB                    │
   │      - RowsRead: 1.599729M (1599729)                │
   │      - RowsReturned: 1.599729M (1599729)            │
   │      - RowsReturnedRate: 28.295K /sec               │
   │      - TotalRawReadTime(*): 1m20s                   │
   │      - TotalReadThroughput: 30.39858627319336 MB/sec│
   │      - WaitScannerTime: 56s528ms                    │
   └-----------------------------------------------------┘
   ```

   上图展示了子任务 980014623046410a-af5d36f23381017f 中，Instance 980014623046410a-88e260f0c43031f5 的各个算子的具体 Profile。

通过以上3个步骤，我们可以逐步排查一个导入任务的执行瓶颈。
