---
{
    "title": "SHOW-LOAD-PROFILE",
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

## SHOW-LOAD-PROFILE

### Name

SHOW LOAD PROFILE

### Description

This statement is used to view the Profile information of the import operation. This function requires the user to open the Profile settings. The versions before 0.15 perform the following settings:

```sql
SET is_report_success=true;
````

Versions 0.15 and later perform the following settings:

```sql
SET [GLOBAL] enable_profile=true;
````

grammar:

```sql
show load profile "/";

show load profile "/[queryId]"
````

This command will list all currently saved import profiles. Each line corresponds to one import. where the QueryId column is the ID of the import job. This ID can also be viewed through the SHOW LOAD statement. We can select the QueryId corresponding to the Profile we want to see to see the specific situation

### Example

1. List all Load Profiles

   ```sql
   mysql> show load profile "/";
   +---------+------+-----------+------+-----------+---------------------+---------------------+-----------+------------+
   | QueryId | User | DefaultDb | SQL  | QueryType | StartTime           | EndTime             | TotalTime | QueryState |
   +---------+------+-----------+------+-----------+---------------------+---------------------+-----------+------------+
   | 10441   | N/A  | N/A       | N/A  | Load      | 2021-04-10 22:15:37 | 2021-04-10 22:18:54 | 3m17s     | N/A        |
   +---------+------+-----------+------+-----------+---------------------+---------------------+-----------+------------+
   2 rows in set (0.00 sec)
   ````

2. View an overview of the subtasks with imported jobs:

   ```sql
   mysql> show load profile "/10441";
   +-----------------------------------+------------+
   | TaskId                            | ActiveTime |
   +-----------------------------------+------------+
   | 980014623046410a-88e260f0c43031f1 | 3m14s      |
   +-----------------------------------+------------+
   ````
   
3. View the Instance overview of the specified subtask

   ```sql
   mysql> show load profile "/10441/980014623046410a-88e260f0c43031f1";
   +-----------------------------------+------------------+------------+
   | Instances                         | Host             | ActiveTime |
   +-----------------------------------+------------------+------------+
   | 980014623046410a-88e260f0c43031f2 | 10.81.85.89:9067 | 3m7s       |
   | 980014623046410a-88e260f0c43031f3 | 10.81.85.89:9067 | 3m6s       |
   | 980014623046410a-88e260f0c43031f4 | 10.81.85.89:9067 | 3m10s      |
   | 980014623046410a-88e260f0c43031f5 | 10.81.85.89:9067 | 3m14s      |
   +-----------------------------------+------------------+------------+
   ````

4. Continue to view the detailed Profile of each operator on a specific Instance

   ```sql
   mysql> show load profile "/10441/980014623046410a-88e260f0c43031f1/980014623046410a-88e260f0c43031f5"\G
   
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
   
   │      - RowsReturnedRate: 28.295K sec               │
   
   │      - TotalRawReadTime(*): 1m20s                   │
   
   │      - TotalReadThroughput: 30.39858627319336 MB/sec│
   
   │      - WaitScannerTime: 56s528ms                    │
   
   └-----------------------------------------------------┘
   ````

### Keywords

    SHOW, LOAD, PROFILE

### Best Practice

