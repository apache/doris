---
{
    "title": "查询执行的统计",
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

# 查询执行的统计

本文档主要介绍Doris在查询执行的统计结果。利用这些统计的信息，可以更好的帮助我们了解Doris的执行情况，并有针对性的进行相应**Debug与调优工作**。


## 名词解释

* FE：Frontend，Doris 的前端节点。负责元数据管理和请求接入。
* BE：Backend，Doris 的后端节点。负责查询执行和数据存储。
* Fragment：FE会将具体的SQL语句的执行转化为对应的Fragment并下发到BE进行执行。BE上执行对应Fragment，并将结果汇聚返回给FE。

## 基本原理

FE将查询计划拆分成为Fragment下发到BE进行任务执行。BE在执行Fragment时记录了**运行状态时的统计值**，并将Fragment执行的统计信息输出到日志之中。 FE也可以通过开关将各个Fragment记录的这些统计值进行搜集，并在FE的Web页面上打印结果。

## 操作流程

通过Mysql命令，将FE上的Report的开关打开

```
mysql> set is_report_success=true; 
```

之后执行对应的SQL语句之后，在FE的Web页面就可以看到对应SQL语句执行的Report信息：
![image.png](https://upload-images.jianshu.io/upload_images/8552201-f5308be377dc4d90.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

这里会列出最新执行完成的**100条语句**，我们可以通过Profile查看详细的统计信息。
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
这里详尽的列出了**查询的ID，执行时间，执行语句**等等的总结信息。接下来内容是打印从BE收集到的各个Fragement的详细信息。
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
这里列出了Fragment的ID；```hostname```指的是执行Fragment的BE节点；```Active：10s270ms```表示该节点的执行总时间；```non-child: 0.14%```表示执行节点自身的执行时间，不包含子节点的执行时间。后续依次打印子节点的统计信息，**这里可以通过缩进区分节点之间的父子关系**。

## Profile参数解析
BE端收集的统计信息较多，下面列出了各个参数的对应含义：

#### `Fragment`
   - AverageThreadTokens: 执行Fragment使用线程数目，不包含线程池的使用情况
   - Buffer Pool PeakReservation: Buffer Pool使用的内存的峰值
   - MemoryLimit: 查询时的内存限制
   - PeakMemoryUsage: 内存使用的峰值  
   - RowsProduced: 处理列的行数
   - BytesReceived: 通过网络接收的Bytes大小
   - DeserializeRowBatchTimer: 反序列化的耗时

#### `BlockMgr`
  - BlocksCreated: 落盘时创建的Blocks数目
  - BlocksRecycled: 重用的Blocks数目
  - BytesWritten: 总的落盘写数据量
  - MaxBlockSize: 单个Block的大小
  - TotalReadBlockTime: 读Block的总耗时

#### `DataStreamSender`
   - BytesSent: 发送的总数据量 = 接受者 * 发送数据量
   - IgnoreRows: 过滤的行数
   - OverallThroughput: 总的吞吐量 = BytesSent / 时间
   - SerializeBatchTime: 发送数据序列化消耗的时间
   - UncompressedRowBatchSize: 发送数据压缩前的RowBatch的大小

#### `SORT_NODE`
  - InMemorySortTime: 内存之中的排序耗时
  - InitialRunsCreated: 初始化排序的趟数（如果内存排序的话，该数为1）
  - SortDataSize: 总的排序数据量
  - MergeGetNext: MergeSort从多个sort_run获取下一个batch的耗时 (仅在落盘时计时）
  - MergeGetNextBatch: MergeSort提取下一个sort_run的batch的耗时 (仅在落盘时计时）
  - TotalMergesPerformed: 进行外排merge的次数
 
#### `AGGREGATION_NODE`
  - PartitionsCreated: 聚合查询拆分成Partition的个数
  - GetResultsTime: 从各个partition之中获取聚合结果的时间
  - HTResizeTime:  HashTable进行resize消耗的时间
  - HTResize:  HashTable进行resize的次数
  - HashBuckets:  HashTable中Buckets的个数
  - HashBucketsWithDuplicate:  HashTable有DuplicateNode的Buckets的个数
  - HashCollisions:  HashTable产生哈希冲突的次数
  - HashDuplicateNodes:  HashTable出现Buckets相同DuplicateNode的个数
  - HashFailedProbe:  HashTable Probe操作失败的次数
  - HashFilledBuckets:  HashTable填入数据的Buckets数目
  - HashProbe:  HashTable查询的次数
  - HashTravelLength:  HashTable查询时移动的步数

#### `OLAP_SCAN_NODE`

`OLAP_SCAN_NODE` 节点负责具体的数据扫描任务。一个 `OLAP_SCAN_NODE` 会生成一个或多个 `OlapScanner` 线程。每个 Scanner 线程负责扫描部分数据。

查询中的部分或全部谓词条件会推送给 `OLAP_SCAN_NODE`。这些谓词条件中一部分会继续下推给存储引擎，以便利用存储引擎的索引进行数据过滤。另一部分会保留在 `OLAP_SCAN_NODE` 中，用于过滤从存储引擎中返回的数据。

一个典型的 `OLAP_SCAN_NODE` 节点的 Profile 如下。部分指标会因存储格式的不同（V1 或 V2）而有不同含义。

```
OLAP_SCAN_NODE (id=0):(Active: 4.050ms, non-child: 35.68%)
   - BitmapIndexFilterTimer: 0.000ns    # 利用 bitmap 索引过滤数据的耗时。
   - BlockConvertTime: 7.433ms  # 将向量化Block转换为行结构的 RowBlock 的耗时。向量化 Block 在 V1 中为 VectorizedRowBatch，V2中为 RowBlockV2。
   - BlockFetchTime: 36.934ms   # Rowset Reader 获取 Block 的时间。
   - BlockLoadTime: 23.368ms    # SegmentReader(V1) 或 SegmentIterator(V2) 获取 block 的时间。
   - BlockSeekCount: 0  # 读取 Segment 时进行 block seek 的次数。
   - BlockSeekTime: 3.062ms # 读取 Segment 时进行 block seek 的耗时。
   - BlocksLoad: 221    # 读取 Block 的数量
   - BytesRead: 6.59 MB # 从数据文件中读取到的数据量。假设读取到了是10个32位整型，则数据量为 10 * 4B = 40 Bytes。这个数据仅表示数据在内存中全展开的大小，并不代表实际的 IO 大小。
   - CachedPagesNum: 0  # 仅 V2 中，当开启 PageCache 后，命中 Cache 的 Page 数量。
   - CompressedBytesRead: 1.36 MB   # V1 中，从文件中读取的解压前的数据大小。V2 中，读取到的没有命中 PageCache 的 Page 的压缩前的大小。
   - DecompressorTimer: 4.194ms # 数据解压耗时。
   - IOTimer: 1.404ms   # 实际从操作系统读取数据的 IO 时间。
   - IndexLoadTime: 1.521ms # 仅 V1 中，读取 Index Stream 的耗时。
   - NumDiskAccess: 6   # 该 ScanNode 节点涉及到的磁盘数量。
   - NumScanners: 25    # 该 ScanNode 生成的 Scanner 数量。
   - PeakMemoryUsage: 0     # 无意义
   - PerReadThreadRawHdfsThroughput: 0.00 /sec  # 无意义
   - RawRowsRead: 141.71K   # 存储引擎中读取的原始行数。详情见下文。
   - ReaderInitTime: 16.515ms   # OlapScanner 初始化 Reader 的时间。V1 中包括组建 MergeHeap 的时间。V2 中包括生成各级 Iterator 并读取第一组Block的时间。
   - RowsBitmapFiltered: 0  # 利用 bitmap 索引过滤掉的行数。
   - RowsBloomFilterFiltered: 0 # 仅 V2 中，通过 BloomFilter 索引过滤掉的行数。
   - RowsDelFiltered: 0     # V1 中表示根据 delete 条件过滤掉的行数。V2 中还包括通过 BloomFilter 和部分谓词条件过滤掉的行数。
   - RowsPushedCondFiltered: 0  # 根据传递下推的谓词过滤掉的条件，比如 Join 计算中从 BuildTable 传递给 ProbeTable 的条件。该数值不准确，因为如果过滤效果差，就不再过滤了。
   - RowsRead: 132.78K  # 从存储引擎返回到 Scanner 的行数，不包括经 Scanner 过滤的行数。
   - RowsReturned: 132.78K  # 从 ScanNode 返回给上层节点的行数。
   - RowsReturnedRate: 32.78 M/sec  # RowsReturned/ActiveTime
   - RowsStatsFiltered: 0   # V2 中，包含谓词条件根据 Zonemap 过滤掉的行数。V1 中还包含通过 BloomFilter 过滤掉的行数。
   - RowsVectorPredFiltered: 0  # 通过向量化条件过滤操作过滤掉的行数。
   - ScanTime: 49.239ms：Scanner 调用 get_next() 方法的耗时统计。
   - ScannerThreadsInvoluntaryContextSwitches: 0    # 无意义
   - ScannerThreadsTotalWallClockTime: 0.000ns  # 无意义
     - MaterializeTupleTime(*): 0.000ns # 无意义
     - ScannerThreadsSysTime: 0.000ns   # 无意义
     - ScannerThreadsUserTime: 0.000ns  # 无意义
   - ScannerThreadsVoluntaryContextSwitches: 0  # 无意义
   - ShowHintsTime: 0.000ns # V2 中无意义。V1 中读取部分数据来进行 ScanRange 的切分。
   - TabletCount : 25   # 该 ScanNode 涉及的 Tablet 数量。
   - TotalPagesNum: 0   # 仅 V2 中，读取的总 Page 数量。
   - TotalRawReadTime(*): 0.000ns   # 无意义
   - TotalReadThroughput: 0.00 /sec # 无意义
   - UncompressedBytesRead: 4.28 MB # V1 中为读取的数据文件解压后的大小（如果文件无需解压，则直接统计文件大小）。V2 中，仅统计未命中 PageCache 的 Page 解压后的大小（如果Page无需解压，直接统计Page大小）
   - VectorPredEvalTime: 0.000ns    # 向量化条件过滤操作的耗时。
```

* Profile 中关于行数的一些说明

    在 Profile 中和行数相关的指标有：
    
    * RowsKeyRangeFiltered
    * RowsBitmapIndexFiltered
    * RowsBloomFilterFiltered
    * RowsStatsFiltered
    * RowsDelFiltered
    * RawRowsRead
    * RowsRead
    * RowsReturned

    一个查询中的谓词条件会分别在存储引擎和 Scanner 中进行过滤。以上指标中，`Rows***Filtered` 这组指标描述了在存储引擎中被过滤的行数。后三个指标描述了在 Scanner 中被处理的行数。

    以下仅针对 Segment V2 格式数据读取的流程进行说明。Segment V1 格式中，这些指标的含义略有不同。

    当读取一个 V2 格式的 Segment 时，首先会根据 Key range（前缀key组成的查询范围）进行一次过滤，过滤掉的行数记录在 `RowsKeyRangeFiltered` 中。之后，再利用 Bitmap 索引过滤数据，过滤掉的行数记录在 `RowsBitmapIndexFiltered` 中。之后，再利用 BloomFilter 索引过滤数据，记录在 `RowsBloomFilterFiltered` 中。`RowsBloomFilterFiltered` 的值是 Segment 的总行数（而不是Bitmap索引过滤后的行数）和经过 BloomFilter 过滤后剩余行数的差值，因此 BloomFilter 过滤掉的数据可能会和 Bitmap 过滤掉的数据有重叠。

    `RowsStatsFiltered` 中记录的是经过其他谓词条件过滤掉的行数，这里包括下推到存储引擎的谓词条件，以及存储引擎中的 Delete 条件。
    
    `RowsDelFiltered` 中包含了 `RowsBloomFilterFiltered` 和 `RowsStatsFiltered` 记录的过滤行数。
    
    `RawRowsRead` 是经过上述过滤后，最终需要读取的行数。而 `RowsRead` 是最终返回给 Scanner 的行数。`RowsRead` 通常小于 `RawRowsRead`，是因为从存储引擎返回到 Scanner，可能会经过一次数据聚合。
    
    `RowsReturned` 是 ScanNode 最终返回给上层节点的行数。`RowsReturned` 通常也会小于 
`RowsRead`。因为在 Scanner 上会有一些没有下推给存储引擎的谓词条件，会进行一次过滤。

    通过以上指标，可以大致分析出存储引擎处理的行数以及最终过滤后的结果行数大小。通过 `Rows***Filtered` 这组指标，也可以分析查询条件是否下推到了存储引擎，以及不同索引的过滤效果。
    
    如果 `RawRowsRead` 和 `RowsRead` 差距较大，则说明大量的行被聚合，而聚合可能比较耗时。如果 `RowsRead` 和 `RowsReturned` 差距较大，则说明很多行在 Scanner 中进行了过滤。这说明很多选择度高的谓词条件并没有推送给存储引擎。而在 Scanner 中的过滤效率会比在存储引擎中过滤效率差。
    
* Scan Node Profile 的简单分析

    OlapScanNode 的 Profile 通常用于分析数据扫描的效率。除了前面介绍的通过行数相关信息可以推断谓词条件下推和索引使用情况外，还可以通过以下几个方面进行简单的分析。
    
    * 首先，很多指标，如 `IOTimer`，`BlockFetchTime` 等都是所有 Scanner 线程指标的累加，因此数值可能会比较大。并且因为 Scanner 线程是异步读取数据的，所以这些累加指标只能反映 Scanner 累加的工作时间，并不直接代表 ScanNode 的耗时。ScanNode 在整个查询计划中的耗时占比为 `Active` 字段记录的值。有时会出现比如 `IOTimer` 有几十秒，而 `Active` 实际只有几秒钟。这种情况通常因为：1. `IOTimer` 为多个 Scanner 的累加时间，而 Scanner 数量较多。2. 上层节点比较耗时。比如上层节点耗时 100秒，而底层 ScanNode 只需 10秒。则反映在 `Active` 的字段可能只有几毫秒。因为在上层处理数据的同时，ScanNode 已经异步的进行了数据扫描并准备好了数据。当上层节点从 ScanNode 获取数据时，可以获取到已经准备好的数据，因此 Active 时间很短。
    * IOTimer 是 IO 时间，能够直接反映 IO 操作耗时。这里是所有 Scanner 线程累加的 IO 时间。
    * NumScanners 表示 Scanner 线程数。线程数过多或过少都会影响查询效率。同时可以用一些汇总指标除以线程数来大致的估算每个线程的耗时。
    * TabletCount 表示需要扫描的 tablet 数量。数量过多可能意味着需要大量的随机读取和数据合并操作。
    * UncompressedBytesRead 间接反映了读取的数据量。如果该数值较大，说明可能有大量的 IO 操作。
    * CachedPagesNum 和 TotalPagesNum。对于 V2 格式，可以查看命中 PageCache 的情况。命中率越高，说明 IO 和解压操作耗时越少。

#### `Buffer pool`
 - AllocTime: 内存分配耗时
 - CumulativeAllocationBytes: 累计内存分配的量
 - CumulativeAllocations: 累计的内存分配次数
 - PeakReservation: Reservation的峰值
 - PeakUnpinnedBytes: unpin的内存数据量
 - PeakUsedReservation: Reservation的内存使用量
 - ReservationLimit: BufferPool的Reservation的限制量

