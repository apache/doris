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

#### Fragment
   - AverageThreadTokens: 执行Fragment使用线程数目，不包含线程池的使用情况
   - Buffer Pool PeakReservation: Buffer Pool使用的内存的峰值
   - MemoryLimit: 查询时的内存限制
   - PeakMemoryUsage: 内存使用的峰值  
   - RowsProduced: 处理列的行数
   - BytesReceived: 通过网络接收的Bytes大小
   - DeserializeRowBatchTimer: 反序列化的耗时
#### BlockMgr:
  - BlocksCreated: 落盘时创建的Blocks数目
  - BlocksRecycled: 重用的Blocks数目
  - BytesWritten: 总的落盘写数据量
  - MaxBlockSize: 单个Block的大小
  - TotalReadBlockTime: 读Block的总耗时
#### DataStreamSender
   - BytesSent: 发送的总数据量 = 接受者 * 发送数据量
   - IgnoreRows: 过滤的行数
   - OverallThroughput: 总的吞吐量 = BytesSent / 时间
   - SerializeBatchTime: 发送数据序列化消耗的时间
   - UncompressedRowBatchSize: 发送数据压缩前的RowBatch的大小

 #### SORT_NODE
  - InMemorySortTime: 内存之中的排序耗时
  - InitialRunsCreated: 初始化排序的趟数（如果内存排序的话，该数为1）
  - SortDataSize: 总的排序数据量
  - MergeGetNext: MergeSort从多个sort_run获取下一个batch的耗时 (仅在落盘时计时）
  - MergeGetNextBatch: MergeSort提取下一个sort_run的batch的耗时 (仅在落盘时计时）
  - TotalMergesPerformed: 进行外排merge的次数
 
#### AGGREGATION_NODE：
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

#### OLAP_SCAN_NODE:
 - RowsProduced: 生成结果的行数
 - BytesRead: scan node 扫描数据的总量
 - TotalReadThroughput：吞吐量
 - TabletCount: tablet 的数目
 - RowsPushedCondFiltered：下推的过滤器
 - RawRowsRead: 实际读取的行数
 - RowsReturned: 该节点返回的行数
 - RowsReturnedRate: 返回行数的速率
 - PeakMemoryUsage 内存使用的峰值  

#### Buffer pool:
 - AllocTime: 内存分配耗时
 - CumulativeAllocationBytes: 累计内存分配的量
 - CumulativeAllocations: 累计的内存分配次数
 - PeakReservation: Reservation的峰值
 - PeakUnpinnedBytes: unpin的内存数据量
 - PeakUsedReservation: Reservation的内存使用量
 - ReservationLimit: BufferPool的Reservation的限制量
