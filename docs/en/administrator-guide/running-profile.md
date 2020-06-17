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

#### `Fragment`
   - AverageThreadTokens: Number of threads used to execute fragment, excluding the usage of thread pool
   - PeakReservation: Peak memory used by buffer pool
   - MemoryLimit: Memory limit at query
   - PeakMemoryUsage: Peak memory usage
   - RowsProduced: Number of rows that process
   - BytesReceived: Size of bytes received by network
   - DeserializeRowBatchTimer: Time consuming to receive data deserialization

#### `DataStreamSender`
 - BytesSent: Total bytes data sent
 - IgnoreRows: Rows filtered
 - OverallThroughput: Total throughput = BytesSent / Time
 - SerializeBatchTime: Sending data serialization time
 - UncompressedRowBatchSize: Size of rowbatch before sending data compression

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

#### `OLAP_SCAN_NODE`

The `OLAP_SCAN_NODE` is responsible for specific data scanning tasks. One `OLAP_SCAN_NODE` will generate one or more `OlapScanner` threads. Each Scanner thread is responsible for scanning part of the data.

Some or all of the predicate conditions in the query will be pushed to `OLAP_SCAN_NODE`. Some of these predicate conditions will continue to be pushed down to the storage engine in order to use the storage engine's index for data filtering. The other part will be kept in `OLAP_SCAN_NODE` to filter the data returned from the storage engine.

The profile of a typical `OLAP_SCAN_NODE` is as follows. Some indicators will have different meanings depending on the storage format (V1 or V2).

```
OLAP_SCAN_NODE (id=0): (Active: 4.050ms, non-child: 35.68%)
   -BitmapIndexFilterTimer: 0.000ns # Time consuming to filter data using bitmap index.
   -BlockConvertTime: 7.433ms   # Time consuming to convert a vectorized block into a row structure RowBlock. Vectorized Block is VectorizedRowBatch in V1, and RowBlockV2 in V2.
   -BlockFetchTime: 36.934ms    # Rowset Reader time to get Block.
   -BlockLoadTime: 23.368ms # time of SegmentReader(V1) or SegmentIterator(V2) to get the block time.
   -BlockSeekCount: 0   # The number of block seek times when reading segments.
   -BlockSeekTime: 3.062ms  # Time consuming for block seek when reading segments.
   -BlocksLoad: 221 # number of blocks read
   -BytesRead: 6.59 MB  # The amount of data read from the data file. Assuming that 10 32-bit integers are read, the amount of data is 10 * 4B = 40 Bytes. This data only represents the fully expanded size of the data in memory, and does not represent the actual IO size.
   -CachedPagesNum: 0   # In V2 only, when PageCache is enabled, the number of pages that hit Cache.
   -CompressedBytesRead: 1.36 MB    # V1, the size of the data read from the file before decompression. In V2, the uncompressed size of Pages that did not hit PageCache.
   -DecompressorTimer: 4.194ms  # Data decompression takes time.
   -IOTimer: 1.404ms    # IO time to actually read data from the operating system.
   -IndexLoadTime: 1.521ms  # In V1 only, it takes time to read Index Stream.
   -NumDiskAccess: 6    # The number of disks involved in this ScanNode.
   -NumScanners: 25 # The number of Scanners generated by this ScanNode.
   -PeakMemoryUsage: 0  # meaningless
   -PerReadThreadRawHdfsThroughput: 0.00 /sec   # meaningless
   -RawRowsRead: 141.71K    # The number of raw rows read in the storage engine. See below for details.
   -ReaderInitTime: 16.515ms    # OlapScanner time to initialize Reader. V1 includes the time to form MergeHeap. V2 includes the time to generate Iterators at all levels and read the first block.
   -RowsBitmapFiltered: 0   # Number of rows filtered by bitmap index
   -RowsBloomFilterFiltered: 0  # In V2 only, the number of rows filtered by the BloomFilter index.
   -RowsDelFiltered: 0  # V1 indicates the number of rows filtered according to the delete condition. V2 also includes the number of rows filtered by BloomFilter and some predicate conditions.
   -RowsPushedCondFiltered: 0   # Filter the conditions based on the predicate passed down, such as the condition passed from BuildTable to ProbeTable in Join calculation. This value is inaccurate because if the filtering effect is poor, it will not be filtered.
   -RowsRead: 132.78K   # The number of rows returned from the storage engine to the Scanner, excluding the number of rows filtered by the Scanner.
   -RowsReturned: 132.78K   # The number of rows returned from ScanNode to the upper node.
   -RowsReturnedRate: 32.78 M/sec   # RowsReturned/ActiveTime
   -RowsStatsFiltered: 0    # In V2, the number of rows filtered according to Zonemap with predicate conditions. V1 also contains the number of rows filtered by BloomFilter.
   -RowsVectorPredFiltered: 0   # The number of rows filtered by the vectorized conditional filtering operation.
   -ScanTime: 49.239ms # Time-consuming statistics of Scanner calling get_next() method.
   -ScannerThreadsInvoluntaryContextSwitches: 0 # meaningless
   -ScannerThreadsTotalWallClockTime: 0.000ns   # meaningless
     -MaterializeTupleTime(*): 0.000ns  # meaningless
     -ScannerThreadsSysTime: 0.000ns    # meaningless
     -ScannerThreadsUserTime: 0.000ns   # meaningless
   -ScannerThreadsVoluntaryContextSwitches: 0   # meaningless
   -ShowHintsTime: 0.000ns  # meaningless in V2. Part of the data is read in V1 to perform ScanRange segmentation.
   -TabletCount: 25 # The number of tablets involved in this ScanNode.
   -TotalPagesNum: 0    # In V2 only, the total number of pages read.
   -TotalRawReadTime(*): 0.000ns    # meaningless
   -TotalReadThroughput: 0.00 /sec  # meaningless
   -UncompressedBytesRead: 4.28 MB  # V1 is the decompressed size of the read data file (if the file does not need to be decompressed, the file size is directly counted). In V2, only the uncompressed size of the PageCache is counted (if the Page does not need to be decompressed, the Page size is directly counted)
   -VectorPredEvalTime: 0.000ns # Time consuming of vectorized conditional filtering operation.
```

* Some notes on the number of rows in Profile

    The metrics related to the number of rows in the Profile are:
    
    * RowsKeyRangeFiltered
    * RowsBitmapIndexFiltered
    * RowsBloomFilterFiltered
    * RowsStatsFiltered
    * RowsDelFiltered
    * RawRowsRead
    * RowsRead
    * RowsReturned

    The predicate conditions in a query are filtered in the storage engine and Scanner respectively. Among the above indicators, the group of metrics `Rows***Filtered` describes the number of rows filtered in the storage engine. The last three metrics describe the number of lines processed in Scanner.
    
    The following only describes the process of reading data in Segment V2 format. In the Segment V1 format, the meaning of these metrics are slightly different.

    When reading a V2 format segment, it will first filter based on the Key range (the query range composed of the prefix key), and the number of filtered lines is recorded in `RowsKeyRangeFiltered`. After that, the data is filtered using the Bitmap index, and the filtered rows are recorded in `RowsBitmapIndexFiltered`. After that, the data is filtered using the BloomFilter index and recorded in `RowsBloomFilterFiltered`. The value of `RowsBloomFilterFiltered` is the difference between the total number of rows in the Segment (not the number of rows after being filtered by the Bitmap index) and the number of remaining rows after BloomFilter filtering, so the data filtered by BloomFilter may overlap with the data filtered by Bitmap.

    `RowsStatsFiltered` records the number of rows filtered by other predicate conditions. This includes the predicate conditions pushed down to the storage engine and the Delete condition in the storage engine.
    
    `RowsDelFiltered` contains the number of filtered rows recorded by `RowsBloomFilterFiltered` and `RowsStatsFiltered`.
    
    `RawRowsRead` is the number of rows that need to be read after the above filtering. The `RowsRead` is the number of rows returned to the Scanner. `RowsRead` is usually smaller than `RawRowsRead`, because returning from the storage engine to the Scanner may go through a data aggregation.
    
    `RowsReturned` is the number of rows that ScanNode will eventually return to the upper node. `RowsReturned` will usually be less than
`RowsRead`. Because there will be some predicate conditions that are not pushed down to the storage engine on the Scanner, it will be filtered in Scanner.

    Through the above indicators, you can roughly analyze the number of rows processed by the storage engine and the final number of rows after filtering. Through the set of indicators of `Rows***Filtered`, you can also analyze whether the query condition is pushed down to the storage engine and the filtering effect of different indexes.
    
    If the gap between `RawRowsRead` and `RowsRead` is large, it means that a large number of rows are aggregated, and the aggregation may be time-consuming. If the gap between `RowsRead` and `RowsReturned` is large, it means that many lines are filtered in Scanner. This shows that many highly selected conditions are not pushed to the storage engine. The filtering efficiency in Scanner is worse than that in the storage engine.

* Simple analysis of Scan Node Profile

    OlapScanNode's Profile is usually used to analyze the efficiency of data scanning. In addition to the information about the number of rows that can be used to infer the predicate pushdown and index usage, the following aspects can also be used for simple analysis.
    
    * First of all, many indicators, such as `IOTimer`, `BlockFetchTime`, etc. are the accumulation of all Scanner thread indicators, so the value may be relatively large. And because the Scanner thread reads data asynchronously, these cumulative indicators can only reflect the cumulative working time of the Scanner, and do not directly represent the time cost of ScanNode. The proportion of time spent by ScanNode in the entire query plan is the value recorded in the `Active` field. Sometimes it appears that `IOTimer` has tens of seconds, while `Active` actually has only a few seconds. This situation is usually because: 1. `IOTimer` is the accumulated time of multiple Scanners, and there are many Scanners. 2. The upper nodes are more time-consuming. For example, the upper node takes 100 seconds, while the lower ScanNode only takes 10 seconds. The field reflected in `Active` may only be a few milliseconds. Because while the upper node is processing data, the ScanNode has asynchronously scanned the data and prepared the data. When the upper-layer node obtains data from ScanNode, it can obtain the prepared data, so the `Active` time is very short.
    * IOTimer is the IO time, which can directly reflect the time-consuming IO operation. Here is the accumulated IO time of all Scanner threads.
    * NumScanners indicates the number of Scanner threads. Too many or too few threads will affect query efficiency. At the same time, some aggregate indicators can be divided by the number of threads to roughly estimate the time spent by each thread.
    * TabletCount represents the number of tablets that need to be scanned. Excessive numbers may mean that a large number of random reads and data merge operations are required.
    * UncompressedBytesRead indirectly reflects the amount of data read. If the value is large, it indicates that there may be a large number of IO operations.
    * CachedPagesNum and TotalPagesNum. For V2 format, you can view the hit of PageCache. The higher the hit rate, the less time the IO and decompression operations take.

#### `Buffer pool`
 - AllocTime: Memory allocation time
 - CumulativeAllocationBytes: Cumulative amount of memory allocated
 - CumulativeAllocations: Cumulative number of memory allocations
 - PeakReservation: Peak of reservation
 - PeakUnpinnedBytes: Amount of memory data of unpin
 - PeakUsedReservation: Peak usage of reservation
 - ReservationLimit: Limit of reservation of bufferpool

