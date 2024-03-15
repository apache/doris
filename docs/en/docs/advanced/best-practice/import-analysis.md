---
{
    "title": "Import Analysis",
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


# Import Analysis

Doris provides a graphical command to help users analyze a specific import more easily. This article describes how to use this feature.

> This function is currently only for Broker Load analysis.

## Import Plan Tree

If you don't know much about Doris' query plan tree, please read the previous article [DORIS/best practices/query analysis](./query-analysis.md).

The execution process of a [Broker Load](../../data-operate/import/import-way/broker-load-manual) request is also based on Doris' query framework. A Broker Load job will be split into multiple subtasks based on the number of DATA INFILE clauses in the import request. Each subtask can be regarded as an independent import execution plan. An import plan consists of only one Fragment, which is composed as follows:

```sql
┌────────────────┐
│OlapTableSink│
└────────────────┘
        │
┌────────────────┐
│BrokerScanNode│
└────────────────┘
````

BrokerScanNode is mainly responsible for reading the source data and sending it to OlapTableSink, and OlapTableSink is responsible for sending data to the corresponding node according to the partition and bucketing rules, and the corresponding node is responsible for the actual data writing.

The Fragment of the import execution plan will be divided into one or more Instances according to the number of imported source files, the number of BE nodes and other parameters. Each Instance is responsible for part of the data import.

The execution plans of multiple subtasks are executed concurrently, and multiple instances of an execution plan are also executed in parallel.

## View import Profile

The user can open the session variable `is_report_success` with the following command:

```sql
SET is_report_success=true;
````

Then submit a Broker Load import request and wait until the import execution completes. Doris will generate a Profile for this import. Profile contains the execution details of importing each subtask and Instance, which helps us analyze import bottlenecks.

> Viewing profiles of unsuccessful import jobs is currently not supported.

We can get the Profile list first with the following command:

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
````

This command will list all currently saved import profiles. Each line corresponds to one import. where the QueryId column is the ID of the import job. This ID can also be viewed through the SHOW LOAD statement. We can select the QueryId corresponding to the Profile we want to see to see the specific situation.

**Viewing a Profile is divided into 3 steps:**

1. View the subtask overview

   View an overview of subtasks with imported jobs by running the following command:



   ```sql
   mysql> show load profile "/980014623046410a-af5d36f23381017f";
   +-----------------------------------+------------+
   | TaskId                            | ActiveTime |
   +-----------------------------------+------------+
   | 980014623046410a-af5d36f23381017f | 3m14s      |
   +-----------------------------------+------------+
   ````

As shown in the figure above, it means that the import job `980014623046410a-af5d36f23381017f` has a total of one subtask, in which ActiveTime indicates the execution time of the longest instance in this subtask.

2. View the Instance overview of the specified subtask

   When we find that a subtask takes a long time, we can further check the execution time of each instance of the subtask:



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
   ````

This shows the time-consuming of four instances of the subtask 980014623046410a-af5d36f23381017f, and also shows the execution node where the instance is located.

3. View the specific Instance

   We can continue to view the detailed profile of each operator on a specific Instance:



   ```sql
   mysql> show load profile "/980014623046410a-af5d36f23381017f/980014623046410a-af5d36f23381017f/980014623046410a-88e260f0c43031f5"\G
   **************************** 1. row ******************** ******
   Instance:
         ┌-----------------------------------------┐
         │[-1: OlapTableSink] │
         │(Active: 2m17s, non-child: 70.91) │
         │ - Counters: │
         │ - CloseWaitTime: 1m53s │
         │ - ConvertBatchTime: 0ns │
         │ - MaxAddBatchExecTime: 1m46s │
         │ - NonBlockingSendTime: 3m11s │
         │ - NumberBatchAdded: 782 │
         │ - NumberNodeChannels: 1 │
         │ - OpenTime: 743.822us │
         │ - RowsFiltered: 0 │
         │ - RowsRead: 1.599729M (1599729) │
         │ - RowsReturned: 1.599729M (1599729)│
         │ - SendDataTime: 11s761ms │
         │ - TotalAddBatchExecTime: 1m46s │
         │ - ValidateDataTime: 9s802ms │
         └-----------------------------------------┘
                              │
   ┌------------------------------------------------- ----┐
   │[0: BROKER_SCAN_NODE] │
   │(Active: 56s537ms, non-child: 29.06) │
   │ - Counters: │
   │ - BytesDecompressed: 0.00 │
   │ - BytesRead: 5.77 GB │
   │ - DecompressTime: 0ns │
   │ - FileReadTime: 34s263ms │
   │ - MaterializeTupleTime(*): 45s54ms │
   │ - NumDiskAccess: 0 │
   │ - PeakMemoryUsage: 33.03 MB │
   │ - RowsRead: 1.599729M (1599729) │
   │ - RowsReturned: 1.599729M (1599729) │
   │ - RowsReturnedRate: 28.295K /sec │
   │ - TotalRawReadTime(*): 1m20s │
   │ - TotalReadThroughput: 30.39858627319336 MB/sec│
   │ - WaitScannerTime: 56s528ms │
   └------------------------------------------------- ----┘
   ````

The figure above shows the specific profiles of each operator of Instance 980014623046410a-af5d36f23381017f in subtask 980014623046410a-88e260f0c43031f5.

Through the above three steps, we can gradually check the execution bottleneck of an import task.
