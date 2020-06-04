---
{
    "title": "Statistics of query execution",
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

# Statistics of query execution

This document focuses on introducing the **RuningProfle** which recorded runtime status of Doris in query execution. Using these statistical information, we can understand the execution of frgment to become a expert of Doris's **debugging and tuning**.

## Noun Interpretation

* **FE**: Frontend, frontend node of Doris. Responsible for metadata management and request access.

* **BE**: Backend, backend node of Doris. Responsible for query execution and data storage.

* **Fragment**: FE will convert the execution of specific SQL statements into corresponding fragments and distribute them to BE for execution. BE will execute corresponding fragments and gather the result of RunningProfile to send back FE.

## Basic concepts

FE splits the query plan into fragments and distributes them to BE for task execution. BE records the statistics of **Running State** when executing fragment. BE print the outputs statistics of fragment execution into the log. FE can also collect these statistics recorded by each fragment and print the results on FE's web page.
## Specific operation

Turn on the report switch on FE through MySQL command

```
mysql> set is_report_success=true; 
```

After executing the corresponding SQL statement, we can see the report information of the corresponding SQL statement on the FE web page like the picture below.
![image.png](https://upload-images.jianshu.io/upload_images/8552201-f5308be377dc4d90.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

The latest  **100 statements** executed will be listed here. We can view detailed statistics of RunningProfile.
```
Query:
  Summary:
    Query ID: 9664061c57e84404-85ae111b8ba7e83a
    Start Time: 2020-05-02 10:34:57
    End Time: 2020-05-02 10:35:08
    Total: 10s323ms
    Query Type: Query
    Query State: EOF
    Doris Version: trunk
    User: root
    Default Db: default_cluster:test
    Sql Statement: select max(Bid_Price) from quotes group by Symbol
```
Here is a detailed list of  ```query ID, execution time, execution statement``` and other summary information. The next step is to print the details of each fragment collected from be.
 ```
    Fragment 0:
      Instance 9664061c57e84404-85ae111b8ba7e83d (host=TNetworkAddress(hostname:10.144.192.47, port:9060)):(Active: 10s270ms, % non-child: 0.14%)
         - MemoryLimit: 2.00 GB
         - BytesReceived: 168.08 KB
         - PeakUsedReservation: 0.00 
         - SendersBlockedTimer: 0ns
         - DeserializeRowBatchTimer: 501.975us
         - PeakMemoryUsage: 577.04 KB
         - RowsProduced: 8.322K (8322)
        EXCHANGE_NODE (id=4):(Active: 10s256ms, % non-child: 99.35%)
           - ConvertRowBatchTime: 180.171us
           - PeakMemoryUsage: 0.00 
           - RowsReturned: 8.322K (8322)
           - MemoryUsed: 0.00 
           - RowsReturnedRate: 811
```
The fragment ID is listed here; ``` hostname ``` show the be node executing the fragment; ```active: 10s270ms```show the total execution time of the node;  ```non child: 0.14%``` show the execution time of the node self except the execution time of the subchild node. Subsequently, the statistics of the child nodes will be printed in turn. **here you can distinguish the parent-child relationship by intent**.

## Profile statistic analysis

There are many statistical information collected at BE.  so we list the corresponding meanings of profile are below:

#### Fragment
   - AverageThreadTokens: Number of threads used to execute fragment, excluding the usage of thread pool
   - PeakReservation: Peak memory used by buffer pool
   - MemoryLimit: Memory limit at query
   - PeakMemoryUsage: Peak memory usage
   - RowsProduced: Number of rows that process
   - BytesReceived: Size of bytes received by network
   - DeserializeRowBatchTimer: Time consuming to receive data deserialization

#### DataStreamSender
 - BytesSent: Total bytes data sent
 - IgnoreRows: Rows filtered
 - OverallThroughput: Total throughput = BytesSent / Time
 - SerializeBatchTime: Sending data serialization time
 - UncompressedRowBatchSize: Size of rowbatch before sending data compression

#### SORT_NODE
  - InMemorySortTime: In memory sort time
  - InitialRunsCreated: Number of initialize sort run
  - MergeGetNext: Time cost of MergeSort from multiple sort_run to get the next batch (only show spilled disk)
  - MergeGetNextBatch: Time cost MergeSort one sort_run to get the next batch (only show spilled disk)
  - SortDataSize: Total sorted data
  - TotalMergesPerformed: Number of external sort merges

#### AGGREGATION_NODE：
  - PartitionsCreated: Number of partition split by aggregate
  - GetResultsTime: Time to get aggregate results from each partition
  - HTResizeTime:  Time spent in resizing hashtable
  - HTResize:  Number of times hashtable resizes
  - HashBuckets: Number of buckets in hashtable
  - HashBucketsWithDuplicate:  Number of buckets with duplicatenode in hashtable
  - HashCollisions:  Number of hash conflicts generated 
  - HashDuplicateNodes:  Number of duplicate nodes with the same buckets in hashtable
  - HashFailedProbe:  Number of failed probe operations
  - HashFilledBuckets:  Number of buckets filled data
  - HashProbe:  Number of hashtable probe
  - HashTravelLength:  The number of steps moved when hashtable queries

#### OLAP_SCAN_NODE:
 - BytesRead: Total data
 - TotalReadThroughput：Throughput = BytesRead / Time
 - TabletCount: Number of scanned tablets
 - RowsPushedCondFiltered：Number of filters pushed down
 - RawRowsRead: Number of rows read
 - RowsReturned: Number of rows returned by the node
 - RowsReturnedRate: Rate of rows returned
 - PeakMemoryUsage: Peak memory usage of the node

#### Buffer pool:
 - AllocTime: Memory allocation time
 - CumulativeAllocationBytes: Cumulative amount of memory allocated
 - CumulativeAllocations: Cumulative number of memory allocations
 - PeakReservation: Peak of reservation
 - PeakUnpinnedBytes: Amount of memory data of unpin
 - PeakUsedReservation: Peak usage of reservation
 - ReservationLimit: Limit of reservation of bufferpool
