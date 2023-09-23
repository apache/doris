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

This document focuses on introducing the **Running Profile** which recorded runtime status of Doris in query execution. Using these statistical information, we can understand the execution of frgment to become a expert of Doris's **debugging and tuning**.

You can also refer to following statements to view profile in command line:

- [SHOW QUERY PROFILE](../sql-manual/sql-reference/Show-Statements/SHOW-QUERY-PROFILE.md)
- [SHOW LOAD PROFILE](../sql-manual/sql-reference/Show-Statements/SHOW-LOAD-PROFILE.md)

## Noun Interpretation

* **FE**: Frontend, frontend node of Doris. Responsible for metadata management and request access.

* **BE**: Backend, backend node of Doris. Responsible for query execution and data storage.

* **Fragment**: FE will convert the execution of specific SQL statements into corresponding fragments and distribute them to BE for execution. BE will execute corresponding fragments and gather the result of RunningProfile to send back FE.

## Basic concepts

FE splits the query plan into fragments and distributes them to BE for task execution. BE records the statistics of **Running State** when executing fragment. BE print the outputs statistics of fragment execution into the log. FE can also collect these statistics recorded by each fragment and print the results on FE's web page.

## Specific operation

Turn on the report switch on FE through MySQL command

```
mysql> set enable_profile=true; 
```

After executing the corresponding SQL statement(`is_report_success` in old versions), we can see the report information of the corresponding SQL statement on the FE web page like the picture below.
![image.png](/images/running_profile.png)

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
      Instance 9664061c57e84404-85ae111b8ba7e83d (host=TNetworkAddress(hostname:192.168.0.1, port:9060)):(Active: 10s270ms, % non-child: 0.14%)
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
The fragment ID is listed here; ``` hostname ``` show the be node executing the fragment; ```active: 10s270ms```show the total execution time of the node;  ```non child: 0.14%``` means the execution time of the execution node itself (not including the execution time of child nodes) as a percentage of the total time. 

`PeakMemoryUsage` indicates the peak memory usage of `EXCHANGE_NODE`; `RowsReturned` indicates the number of rows returned by `EXCHANGE_NODE`; `RowsReturnedRate`=`RowsReturned`/`ActiveTime`; the meaning of these three statistics in other `NODE` the same.

Subsequently, the statistics of the child nodes will be printed in turn. **here you can distinguish the parent-child relationship by intent**.

## Profile statistic analysis

There are many statistical information collected at BE.  so we list the corresponding meanings of profile are below:

#### `Fragment`
   - AverageThreadTokens: Number of threads used to execute fragment, excluding the usage of thread pool
   - PeakReservation: Peak memory used by buffer pool
   - MemoryLimit: Memory limit at query
   - PeakMemoryUsage: Peak memory usage of instance
   - RowsProduced: Number of rows that process

#### `BlockMgr`
  - BlocksCreated: Number of Block be created by BlockMgr
  - BlocksRecycled: Number of Block be recycled by BlockMgr
  - BytesWritten: How many bytes be writen to spill to disk
  - MaxBlockSize: Max size of one Block
  - TotalReadBlockTime: Total time read block from disk

#### `DataStreamSender`
 - BytesSent: Total bytes data sent
 - IgnoreRows: Rows filtered
 - LocalBytesSent: The amount bytes of local node send to it's self during Exchange
 - OverallThroughput: Total throughput = BytesSent / Time
 - SerializeBatchTime: Sending data serialization time
 - UncompressedRowBatchSize: Size of rowbatch before sending data compression

#### `ODBC_TABLE_SINK`
   - NumSentRows: Total number of rows written to ODBC table
   - TupleConvertTime: Time consuming of sending data serialization to insert statement
   - ResultSendTime: Time consuming of writing through ODBC driver

#### `EXCHANGE_NODE`
  - BytesReceived: Size of bytes received by network
  - DataArrivalWaitTime: Total waiting time of sender to push data 
  - MergeGetNext: When there is a sort in the lower level node, exchange node will perform a unified merge sort and output an ordered result. This indicator records the total time consumption of merge sorting, including the time consumption of MergeGetNextBatch.
  - MergeGetNextBatch：It takes time for merge node to get data. If it is single-layer merge sort, the object to get data is network queue. For multi-level merge sorting, the data object is child merger.
  - ChildMergeGetNext: When there are too many senders in the lower layer to send data, single thread merge will become a performance bottleneck. Doris will start multiple child merge threads to do merge sort in parallel. The sorting time of child merge is recorded, which is the cumulative value of multiple threads.
  - ChildMergeGetNextBatch: It takes time for child merge to get data，If the time consumption is too large, the bottleneck may be the lower level data sending node.
  - FirstBatchArrivalWaitTime: The time waiting for the first batch come from sender
  - DeserializeRowBatchTimer: Time consuming to receive data deserialization
  - SendersBlockedTotalTimer(*): When the DataStreamRecv's queue buffer is full, wait time of sender
  - ConvertRowBatchTime: Time taken to transfer received data to RowBatch
  - RowsReturned: Number of receiving rows
  - RowsReturnedRate: Rate of rows received

#### `SORT_NODE`
  - InMemorySortTime: In memory sort time
  - InitialRunsCreated: Number of initialize sort run
  - MergeGetNext: Time cost of MergeSort from multiple sort_run to get the next batch (only show spilled disk)
  - MergeGetNextBatch: Time cost MergeSort one sort_run to get the next batch (only show spilled disk)
  - SortDataSize: Total sorted data
  - TotalMergesPerformed: Number of external sort merges

#### `AGGREGATION_NODE`
  - PartitionsCreated: Number of partition split by aggregate
  - GetResultsTime: Time to get aggregate results from each partition
  - HTResizeTime: Time spent in resizing hashtable
  - HTResize: Number of times hashtable resizes
  - HashBuckets: Number of buckets in hashtable
  - HashBucketsWithDuplicate: Number of buckets with duplicatenode in hashtable
  - HashCollisions: Number of hash conflicts generated 
  - HashDuplicateNodes: Number of duplicate nodes with the same buckets in hashtable
  - HashFailedProbe: Number of failed probe operations
  - HashFilledBuckets: Number of buckets filled data
  - HashProbe: Number of hashtable probe
  - HashTravelLength: The number of steps moved when hashtable queries

#### `HASH_JOIN_NODE`
  - ExecOption: The way to construct a HashTable for the right child (synchronous or asynchronous), the right child in Join may be a table or a subquery, the same is true for the left child
  - BuildBuckets: The number of Buckets in HashTable
  - BuildRows: the number of rows of HashTable
  - BuildTime: Time-consuming to construct HashTable
  - LoadFactor: Load factor of HashTable (ie the number of non-empty buckets)
  - ProbeRows: Traverse the number of rows of the left child for Hash Probe
  - ProbeTime: Time consuming to traverse the left child for Hash Probe, excluding the time consuming to call GetNext on the left child RowBatch
  - PushDownComputeTime: The calculation time of the predicate pushdown condition
  - PushDownTime: The total time consumed by the predicate push down. When Join, the right child who meets the requirements is converted to the left child's in query

#### `CROSS_JOIN_NODE`
  - ExecOption: The way to construct RowBatchList for the right child (synchronous or asynchronous)
  - BuildRows: The number of rows of RowBatchList (ie the number of rows of the right child)
  - BuildTime: Time-consuming to construct RowBatchList
  - LeftChildRows: the number of rows of the left child
  - LeftChildTime: The time it takes to traverse the left child and find the Cartesian product with the right child, not including the time it takes to call GetNext on the left child RowBatch

#### `UNION_NODE`
  - MaterializeExprsEvaluateTime: When the field types at both ends of the Union are inconsistent, the time spent to evaluates type conversion exprs and materializes the results 

#### `ANALYTIC_EVAL_NODE`
  - EvaluationTime: Analysis function (window function) calculation total time
  - GetNewBlockTime: It takes time to apply for a new block during initialization. Block saves the cache line window or the entire partition for analysis function calculation
  - PinTime: the time it takes to apply for a new block later or reread the block written to the disk back to the memory
  - UnpinTime: the time it takes to flush the data of the block to the disk when the memory pressure of the block that is not in use or the current operator is high

#### `OLAP_SCAN_NODE`

The `OLAP_SCAN_NODE` is responsible for specific data scanning tasks. One `OLAP_SCAN_NODE` will generate one or more `OlapScanner`. Each Scanner thread is responsible for scanning part of the data.

Some or all of the predicate conditions in the query will be pushed to `OLAP_SCAN_NODE`. Some of these predicate conditions will continue to be pushed down to the storage engine in order to use the storage engine's index for data filtering. The other part will be kept in `OLAP_SCAN_NODE` to filter the data returned from the storage engine.

The profile of the `OLAP_SCAN_NODE` node is usually used to analyze the efficiency of data scanning. It is divided into three layers: `OLAP_SCAN_NODE`, `OlapScanner`, and `SegmentIterator` according to the calling relationship.

The profile of a typical `OLAP_SCAN_NODE` is as follows. Some indicators will have different meanings depending on the storage format (V1 or V2).

```
OLAP_SCAN_NODE (id=0):(Active: 1.2ms,% non-child: 0.00%)
  - BytesRead: 265.00 B                 # The amount of data read from the data file. Assuming that 10 32-bit integers are read, the amount of data is 10 * 4B = 40 Bytes. This data only represents the fully expanded size of the data in memory, and does not represent the actual IO size.
  - NumDiskAccess: 1                    # The number of disks involved in this ScanNode node.
  - NumScanners: 20                     # The number of Scanners generated by this ScanNode.
  - PeakMemoryUsage: 0.00               # Peak memory usage during query, not used yet
  - RowsRead: 7                         # The number of rows returned from the storage engine to the Scanner, excluding the number of rows filtered by the Scanner.
  - RowsReturned: 7                     # The number of rows returned from ScanNode to the upper node.
  - RowsReturnedRate: 6.979K /sec       # RowsReturned/ActiveTime
  - TabletCount: 20                     # The number of Tablets involved in this ScanNode.
  - TotalReadThroughput: 74.70 KB/sec   # BytesRead divided by the total time spent in this node (from Open to Close). For IO bounded queries, this should be very close to the total throughput of all the disks
  - ScannerBatchWaitTime: 426.886us     # To count the time the transfer thread waits for the scaner thread to return rowbatch. In pipeline, this value is always 0.
  - ScannerWorkerWaitTime: 17.745us     # To count the time that the scanner thread waits for the available worker threads in the thread pool.
  OlapScanner:
    - BlockConvertTime: 8.941us         # The time it takes to convert a vectorized Block into a RowBlock with a row structure. The vectorized Block is VectorizedRowBatch in V1 and RowBlockV2 in V2.
    - BlockFetchTime: 468.974us         # Rowset Reader gets the time of the Block.
    - ReaderInitTime: 5.475ms           # The time when OlapScanner initializes Reader. V1 includes the time to form MergeHeap. V2 includes the time to generate various Iterators and read the first group of blocks.
    - RowsDelFiltered: 0                # Including the number of rows filtered out according to the Delete information in the Tablet, and the number of rows filtered for marked deleted rows under the unique key model.
    - RowsPushedCondFiltered: 0         # Filter conditions based on the predicates passed down, such as the conditions passed from BuildTable to ProbeTable in Join calculation. This value is not accurate, because if the filtering effect is poor, it will no longer be filtered.
    - ScanTime: 39.24us                 # The time returned from ScanNode to the upper node.
    - ShowHintsTime_V1: 0ns             # V2 has no meaning. Read part of the data in V1 to perform ScanRange segmentation.
    SegmentIterator:
      - BitmapIndexFilterTimer: 779ns   # Use bitmap index to filter data time-consuming.
      - BlockLoadTime: 415.925us        # SegmentReader(V1) or SegmentIterator(V2) gets the time of the block.
      - BlockSeekCount: 12              # The number of block seeks when reading Segment.
      - BlockSeekTime: 222.556us        # It takes time to block seek when reading Segment.
      - BlocksLoad: 6                   # read the number of blocks
      - CachedPagesNum: 30              # In V2 only, when PageCache is enabled, the number of Pages that hit the Cache.
      - CompressedBytesRead: 0.00       # In V1, the size of the data read from the file before decompression. In V2, the pre-compressed size of the read page that did not hit the PageCache.
      - DecompressorTimer: 0ns          # Data decompression takes time.
      - IOTimer: 0ns                    # IO time for actually reading data from the operating system.
      - IndexLoadTime_V1: 0ns           # Only in V1, it takes time to read Index Stream.
      - NumSegmentFiltered: 0           # When generating Segment Iterator, the number of Segments that are completely filtered out through column statistics and query conditions.
      - NumSegmentTotal: 6              # Query the number of all segments involved.
      - RawRowsRead: 7                  # The number of raw rows read in the storage engine. See below for details.
      - RowsBitmapIndexFiltered: 0      # Only in V2, the number of rows filtered by the Bitmap index.
      - RowsBloomFilterFiltered: 0      # Only in V2, the number of rows filtered by BloomFilter index.
      - RowsKeyRangeFiltered: 0         # In V2 only, the number of rows filtered out by SortkeyIndex index.
      - RowsStatsFiltered: 0            # In V2, the number of rows filtered by the ZoneMap index, including the deletion condition. V1 also contains the number of rows filtered by BloomFilter.
      - RowsConditionsFiltered: 0       # Only in V2, the number of rows filtered by various column indexes.
      - RowsVectorPredFiltered: 0       # The number of rows filtered by the vectorized condition filtering operation.
      - TotalPagesNum: 30               # Only in V2, the total number of pages read.
      - UncompressedBytesRead: 0.00     # V1 is the decompressed size of the read data file (if the file does not need to be decompressed, the file size is directly counted). In V2, only the decompressed size of the Page that missed PageCache is counted (if the Page does not need to be decompressed, the Page size is directly counted)
      - VectorPredEvalTime: 0ns         # Time-consuming of vectorized condition filtering operation.
```

The predicate push down and index usage can be inferred from the related indicators of the number of data rows in the profile. The following only describes the profile in the reading process of segment V2 format data. In segment V1 format, the meaning of these indicators is slightly different.

  - When reading a segment V2, if the query has key_ranges (the query range composed of prefix keys), first filter the data through the SortkeyIndex index, and the number of filtered rows is recorded in `RowsKeyRangeFiltered`.
  - After that, use the Bitmap index to perform precise filtering on the columns containing the bitmap index in the query condition, and the number of filtered rows is recorded in `RowsBitmapIndexFiltered`.
  - After that, according to the equivalent (eq, in, is) condition in the query condition, use the BloomFilter index to filter the data and record it in `RowsBloomFilterFiltered`. The value of `RowsBloomFilterFiltered` is the difference between the total number of rows of the Segment (not the number of rows filtered by the Bitmap index) and the number of remaining rows after BloomFilter, so the data filtered by BloomFilter may overlap with the data filtered by Bitmap.
  - After that, use the ZoneMap index to filter the data according to the query conditions and delete conditions and record it in `RowsStatsFiltered`.
  - `RowsConditionsFiltered` is the number of rows filtered by various indexes, including the values ​​of `RowsBloomFilterFiltered` and `RowsStatsFiltered`.
  - So far, the Init phase is completed, and the number of rows filtered by the condition to be deleted in the Next phase is recorded in `RowsDelFiltered`. Therefore, the number of rows actually filtered by the delete condition are recorded in `RowsStatsFiltered` and `RowsDelFiltered` respectively.
  - `RawRowsRead` is the final number of rows to be read after the above filtering.
  - `RowsRead` is the number of rows finally returned to Scanner. `RowsRead` is usually smaller than `RawRowsRead`, because returning from the storage engine to the Scanner may go through a data aggregation. If the difference between `RawRowsRead` and `RowsRead` is large, it means that a large number of rows are aggregated, and aggregation may be time-consuming.
  - `RowsReturned` is the number of rows finally returned by ScanNode to the upper node. `RowsReturned` is usually smaller than `RowsRead`. Because there will be some predicate conditions on the Scanner that are not pushed down to the storage engine, filtering will be performed once. If the difference between `RowsRead` and `RowsReturned` is large, it means that many rows are filtered in the Scanner. This shows that many highly selective predicate conditions are not pushed to the storage engine. The filtering efficiency in Scanner is worse than that in storage engine.

Through the above indicators, you can roughly analyze the number of rows processed by the storage engine and the size of the final filtered result row. Through the `Rows***Filtered` group of indicators, it is also possible to analyze whether the query conditions are pushed down to the storage engine, and the filtering effects of different indexes. In addition, a simple analysis can be made through the following aspects.
    
  - Many indicators under `OlapScanner`, such as `IOTimer`, `BlockFetchTime`, etc., are the accumulation of all Scanner thread indicators, so the value may be relatively large. And because the Scanner thread reads data asynchronously, these cumulative indicators can only reflect the cumulative working time of the Scanner, and do not directly represent the time consumption of the ScanNode. The time-consuming ratio of ScanNode in the entire query plan is the value recorded in the `Active` field. Sometimes it appears that `IOTimer` has tens of seconds, but `Active` is actually only a few seconds. This situation is usually due to:
    - `IOTimer` is the accumulated time of multiple Scanners, and there are more Scanners.
    - The upper node is time-consuming. For example, the upper node takes 100 seconds, while the lower ScanNode only takes 10 seconds. The field reflected in `Active` may be only a few milliseconds. Because while the upper layer is processing data, ScanNode has performed data scanning asynchronously and prepared the data. When the upper node obtains data from ScanNode, it can obtain the prepared data, so the Active time is very short.
  - `NumScanners` represents the number of Tasks submitted by the Scanner to the thread pool. It is scheduled by the thread pool in `RuntimeState`. The two parameters `doris_scanner_thread_pool_thread_num` and `doris_scanner_thread_pool_queue_size` control the size of the thread pool and the queue length respectively. Too many or too few threads will affect query efficiency. At the same time, some summary indicators can be divided by the number of threads to roughly estimate the time consumption of each thread.
  - `TabletCount` indicates the number of tablets to be scanned. Too many may mean a lot of random read and data merge operations.
  - `UncompressedBytesRead` indirectly reflects the amount of data read. If the value is large, it means that there may be a lot of IO operations.
  - `CachedPagesNum` and `TotalPagesNum` can check the hitting status of PageCache. The higher the hit rate, the less time-consuming IO and decompression operations.  

#### `Buffer pool`
 - AllocTime: Memory allocation time
 - CumulativeAllocationBytes: Cumulative amount of memory allocated
 - CumulativeAllocations: Cumulative number of memory allocations
 - PeakReservation: Peak of reservation
 - PeakUnpinnedBytes: Amount of memory data of unpin
 - PeakUsedReservation: Peak usage of reservation
 - ReservationLimit: Limit of reservation of bufferpool

