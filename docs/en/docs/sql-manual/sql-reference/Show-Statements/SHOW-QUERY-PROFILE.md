---
{
    "title": "SHOW-QUERY-PROFILE",
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

## SHOW-QUERY-PROFILE

### Name

SHOW QUERY PROFILE

### Description

This statement is used to view the tree profile information of the query operation,this function requires the user to open profile settings.
Before versions 0.15, perform the following settings:

```sql
SET is_report_success=true;
```

For versions 0.15 and later, perform the following settings:

```sql
SET [GLOBAL] enable_profile=true;
```

grammar:

```sql
show query profile "/";
```
This command will list the profiles of all currently saved query operations.

```sql
show query profile "/queryId"\G
show query profile "/queryId/fragment_id/instance_id"\G
```
Get the tree profile information of the specified query ID,Return to profile simple tree.Specify fragment_ ID and instance_ ID returns the corresponding detailed profile tree.


### Example

1. List all query Profile

   ```sql
   mysql> show query profile "/";
   +-----------------------------------+------+-------------------------+--------------------+-----------+---------------------+---------------------+-----------+------------+
   | QueryId                           | User | DefaultDb               | SQL                | QueryType | StartTime           | EndTime             | TotalTime | QueryState |
   +-----------------------------------+------+-------------------------+--------------------+-----------+---------------------+---------------------+-----------+------------+
   | 327167e0db4749a9-adce3b3d770b2bb1 | root | default_cluster:test_db | select * from test | Query     | 2022-08-09 10:50:09 | 2022-08-09 10:50:09 | 19ms      | EOF        |
   +-----------------------------------+------+-------------------------+--------------------+-----------+---------------------+---------------------+-----------+------------+
   1 row in set (0.00 sec)
   ```

2. List the query profile of the specified queryid

   ```sql
   mysql> show query profile "/327167e0db4749a9-adce3b3d770b2bb1"\G
   *************************** 1. row ***************************
   Fragments: ┌────────────────────────┐
   │[-1: VDataBufferSender] │
   │Fragment: 0             │
   │MaxActiveTime: 783.263us│
   └────────────────────────┘
               ┌┘
               │
     ┌───────────────────┐
     │[1: VEXCHANGE_NODE]│
     │Fragment: 0        │
     └───────────────────┘
               └┐
                │
   ┌────────────────────────┐
   │[1: VDataStreamSender]  │
   │Fragment: 1             │
   │MaxActiveTime: 847.612us│
   └────────────────────────┘
                │
                │
     ┌────────────────────┐
     │[0: VOLAP_SCAN_NODE]│
     │Fragment: 1         │
     └────────────────────┘
               ┌┘
               │
        ┌─────────────┐
        │[OlapScanner]│
        │Fragment: 1  │
        └─────────────┘
               │
               │
      ┌─────────────────┐
      │[SegmentIterator]│
      │Fragment: 1      │
      └─────────────────┘
   1 row in set (0.00 sec)
   ```
3. Lists the instance profile of the specified fragment:

   ```sql
   mysql> show query profile "/327167e0db4749a9-adce3b3d770b2bb1/1/"\G
   *************************** 1. row ***************************
    Instances: 327167e0db4749a9-adce3b3d770b2bb2
         Host: 172.26.0.1:9111
   ActiveTime: 847.612us
   1 row in set (0.01 sec)
   ```

4. Continue to view the detailed profile of each operator on a specific instance:

   ```sql
   mysql> show query profile "/327167e0db4749a9-adce3b3d770b2bb1/1/327167e0db4749a9-adce3b3d770b2bb2"\G
   *************************** 1. row ***************************
   Instance: ┌───────────────────────────────────────┐
   │[1: VDataStreamSender]                 │
   │(Active: 36.944us, non-child: 0.20)    │
   │  - Counters:                          │
   │      - BytesSent: 0.00                │
   │      - IgnoreRows: 0                  │
   │      - LocalBytesSent: 20.00 B        │
   │      - OverallThroughput: 0.0 /sec    │
   │      - PeakMemoryUsage: 0.00          │
   │      - SerializeBatchTime: 0ns        │
   │      - UncompressedRowBatchSize: 0.00 │
   └───────────────────────────────────────┘
                       │
                       │
   ┌───────────────────────────────────────┐
   │[0: VOLAP_SCAN_NODE]                   │
   │(Active: 563.241us, non-child: 3.00)   │
   │  - Counters:                          │
   │      - BatchQueueWaitTime: 444.714us  │
   │      - BytesRead: 37.00 B             │
   │      - NumDiskAccess: 1               │
   │      - NumScanners: 2                 │
   │      - PeakMemoryUsage: 320.00 KB     │
   │      - RowsRead: 4                    │
   │      - RowsReturned: 4                │
   │      - RowsReturnedRate: 7.101K /sec  │
   │      - ScannerBatchWaitTime: 206.40us │
   │      - ScannerSchedCount : 2          │
   │      - ScannerWorkerWaitTime: 34.640us│
   │      - TabletCount : 2                │
   │      - TotalReadThroughput: 0.0 /sec  │
   └───────────────────────────────────────┘
                       │
                       │
      ┌─────────────────────────────────┐
      │[OlapScanner]                    │
      │(Active: 0ns, non-child: 0.00)   │
      │  - Counters:                    │
      │      - BlockConvertTime: 0ns    │
      │      - BlockFetchTime: 183.741us│
      │      - ReaderInitTime: 180.741us│
      │      - RowsDelFiltered: 0       │
      │      - RowsPushedCondFiltered: 0│
      │      - ScanCpuTime: 388.576us   │
      │      - ScanTime: 0ns            │
      │      - ShowHintsTime_V1: 0ns    │
      └─────────────────────────────────┘
                       │
                       │
    ┌─────────────────────────────────────┐
    │[SegmentIterator]                    │
    │(Active: 0ns, non-child: 0.00)       │
    │  - Counters:                        │
    │      - BitmapIndexFilterTimer: 124ns│
    │      - BlockLoadTime: 179.202us     │
    │      - BlockSeekCount: 5            │
    │      - BlockSeekTime: 18.792us      │
    │      - BlocksLoad: 4                │
    │      - CachedPagesNum: 2            │
    │      - CompressedBytesRead: 0.00    │
    │      - DecompressorTimer: 0ns       │
    │      - IOTimer: 0ns                 │
    │      - IndexLoadTime_V1: 0ns        │
    │      - NumSegmentFiltered: 0        │
    │      - NumSegmentTotal: 2           │
    │      - RawRowsRead: 4               │
    │      - RowsBitmapIndexFiltered: 0   │
    │      - RowsBloomFilterFiltered: 0   │
    │      - RowsConditionsFiltered: 0    │
    │      - RowsKeyRangeFiltered: 0      │
    │      - RowsStatsFiltered: 0         │
    │      - RowsVectorPredFiltered: 0    │
    │      - TotalPagesNum: 2             │
    │      - UncompressedBytesRead: 0.00  │
    │      - VectorPredEvalTime: 0ns      │
    └─────────────────────────────────────┘
 
   1 row in set (0.01 sec)
   ```


### Keywords

    SHOW, QUERY, PROFILE

### Best Practice


