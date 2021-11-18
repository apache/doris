---
{
     "title": "Introduction to Apache Doris Grafana Monitoring Indicators",
     "description": "This article mainly receives data indicators collected by Apache Doris through Promethous, visually displayed through Grafana, a description of each monitoring indicator, and also gives the key data indicator reception.",
     "date": "2021-11-17",
     "metaTitle": "Introduction to Apache Doris Grafana Monitoring Indicators",
     "isArticle": true,
     "language": "zh-CN",
     "author": "Zhang Jiafeng",
     "layout": "Article",
     "sidebar": false
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

Prometheus

Prometheus is an open source system monitoring and alarm suite. It can collect the monitored items of the monitored system through Pull or Push and store them in its own time series database. And through the rich multi-dimensional data query language, to meet the different data display needs of users.

Grafana

Grafana is an open source data analysis and display platform. Support multiple mainstream time series database sources including Prometheus. Obtain the display data from the data source through the corresponding database query statement. Through the flexible and configurable Dashboard, these data can be quickly displayed to users in the form of charts.

Several indicators that the entire cluster focuses on:

1. Cluster FE JVM heap statistics
2. Overview of cluster BE memory usage
3. Max Replayed journal id
4. BDBJE Write
5. Tablet scheduling situation
6. BE IO statistics
7. BE Compaction Score
8. Query Statistic This part of the query request number and response time
9. BE BC (Base Compaction) and CC (Compaction Cumulate)

## 1. Overview view

### 1.1 Doris FE status

If the FE node will be displayed as a colored dot, it means that the node is offline. If all front ends are alive, all dots should be green.

<img src="/images/grafana/image-20211105134818474.png" alt="image-20211105134818474" style="zoom:50%;" />

### 1.2 Doris BE status

BE nodes that are down will be displayed as colored dots. If all BEs are alive, all dots should be green.

<img src="/images/grafana/image-20211105135121869.png" alt="image-20211105135121869" style="zoom:50%;" />

### 1.3 Cluster FE JVM heap statistics

The percentage of JVM heap usage for each front end of each Doris cluster.

<img src="/images/grafana/image-20211105135356267.png" alt="image-20211105135356267" style="zoom:50%;" />

### 1.4 Cluster BE CPU Usage

An overview of the back-end CPU usage of each Doris cluster.

<img src="/images/grafana/image-20211105135528392.png" alt="image-20211105135528392" style="zoom:50%;" />

### 1.5 Overview of cluster BE memory usage

Overview of BE memory usage for each Doris cluster.

<img src="/images/grafana/image-20211105140134993.png" alt="image-20211105140134993" style="zoom:50%;" />

### 1.6 Cluster QPS Statistics

QPS statistics grouped by cluster.
The QPS of each cluster is the sum of all queries processed in all FEs.

<img src="/images/grafana/image-20211105140207465.png" alt="image-20211105140207465" style="zoom:50%;" />

### 1.7 Cluster Disk Status

Disk status. The green dot indicates that the disk is online. The red dot indicates that the disk is offline, and the disk that is processed offline indicates that the disk may be damaged and requires operation and maintenance repair or replacement of the disk for processing.

<img src="/images/grafana/image-20211105140450924.png" alt="image-20211105140450924" style="zoom:50%;" />

## 2. Cluster overview

### 2.1 Cluster overview

![image-20211105140551267](/images/grafana/image-20211105140551267.png)

1. FE Node: the total number of FE nodes
2. FE Alive: The current number of normal FE nodes
3. BE Node: the total number of BE nodes in the cluster
4. BE Alive: The number of BE nodes that are fully alive in the current cluster. If this number is inconsistent with the number of BE Nodes, it means that there are offline BE nodes in the cluster, and you need to check and deal with it.
5. Uesd Capacity: Disk space used by the current cluster
6. Total Capacity: the overall storage space of the cluster

### 2.2 Max Replayed journal id

**This is a core monitoring indicator**

The maximum replay metadata log ID of Doris FE. The journal id of the normal Master is the largest, and the value of other non-Master FE nodes is basically the same, which is less than the value of the Master node. If there is an FE node, this value is very different from other nodes, indicating that the metadata version of this node is too old, and the data will be There are inconsistencies. In this case, the node can be deleted from the cluster, and then added as a new FE node, so that this value will be consistent with other nodes under normal circumstances.

![image-20211105141350358](/images/grafana/image-20211105141350358.png)

This value can also be seen through the Web interface of Doris. From the figure below, the values of the two non-Master nodes are the same, and there will be inconsistencies, but the difference will be small and will quickly become Consistent.

<img src="/images/grafana/image-20211105141530244.png" alt="image-20211105141530244" style="zoom:50%;" />

### 2.3 Image counter

Doris Master FE metadata image generation counter. And the Image counter is successfully pushed to other non-Master nodes. These indicators are expected to increase at reasonable intervals. Usually, they should be equal.

![image-20211105141806836](/images/grafana/image-20211105141806836.png)

### 2.4 BDBJE Write

**This is an important monitoring indicator**

BDBJE writing conditions are normally in the millisecond level. If the writing speed of the second level occurs, be vigilant. Metadata writing delays may occur, and serious writing errors may occur.

BDBJE: [Oracle Berkeley DB Java Edition (opens new window)](http://www.oracle.com/technetwork/database/berkeleydb/overview/index-093405.html). In Doris, we use bdbje to complete metadata operation log persistence, FE high availability and other functions

The Y axis on the left shows the 99th write delay. The Y axis on the right shows the number of writes per second of the log.

![image-20211105141833066](/images/grafana/image-20211105141833066.png)

### 2.5 Tablet scheduling

The number of tablets to start scheduling to run. These tablets may be in the process of recovery or balance

![image-20211105142933550](/images/grafana/image-20211105142933550.png)

### 2.6 BE IO Statistics

**This is an important monitoring indicator**

For IO operations, there is currently no separate IO monitoring for Compaction operations. We can only make judgments based on the overall IO utilization of the cluster. We can view the monitoring graph Disk IO util:

<img src="/images/grafana/image-20211105143010357.png" alt="image-20211105143010357" style="zoom:50%;" />

This monitor shows the IO util indicators of the disks on each BE node. The higher the value, the busier the IO. Of course, in most cases, IO resources are consumed by query requests. This monitoring is mainly used to guide us whether we need to increase or decrease the number of Compaction tasks.

### 2.7 BE Compaction Score

**This is an important monitoring indicator**

Doris' data writing model uses a data structure similar to LSM-Tree. Data is written to disk in an append (Append) way. This data structure can change random writes into sequential writes. This is a write-optimized data structure, which can enhance the write throughput of the system, but in the read logic, it is necessary to merge the data written multiple times during reading by Merge-on-Read, so as to process the write Data changes at the time of entry.

Merge-on-Read will affect the efficiency of reading. In order to reduce the amount of data that needs to be merged during reading, LSM-Tree-based systems will introduce background data merging logic, and periodically merge data with a certain strategy. This mechanism in Doris is called Compaction

Normally, this value is considered normal within 100, but if it continues to be close to the value of 100, it means that your cluster may be at risk and you need to pay attention to it.

For the calculation method of this value and the principle of Compaction, please refer to: [Doris Compaction mechanism analysis](https://mp.weixin.qq.com/s?__biz=Mzg5MDEyODc1OA==&mid=2247485136&idx=1&sn=a10850a61f2cb6af42484ba8250566b5&chksm9aff79c9dcenef8adc8250566b16&sn=a10850a61f2cb6af42484ba8250566b5&chksm=cfe79c9d #wechat_redirect) this article

What is reflected here is the number of versions of the tablet with the most data version in all tablets for each BE node in the cluster, which can reflect the current version accumulation situation.

1. Observe the trend of the number of data versions. If the trend is stable, it means that the compaction and import speed are basically the same. If it shows an upward trend, it means that the speed of Compaction cannot keep up with the import speed. If it shows a downward trend, it means that the Compaction speed exceeds the import speed. If it is rising, or in a stable state but the value is high, you need to consider adjusting the Compaction parameters to speed up the progress of the Compaction. Here you need to refer to Section 7.10 of BE in Part 7: Base Compaction and Cumulative Compaction

2. Normally, the number of versions within 100 can be regarded as normal. In most batch import or low-frequency import scenarios, the number of versions is usually 10-20 or even lower.

![image-20211105143220540](/images/grafana/image-20211105143220540.png)

If the number of versions is on the rise or the value is high, you can optimize Compaction from the following two aspects:

1. Modify the number of Compaction threads so that more Compaction tasks can be executed at the same time.
2. Optimize the execution logic of a single Compaction to keep the number of data versions within a reasonable range.

## 3.Query Statistic

This part is mainly to monitor various request conditions of the entire cluster, Select query and response time

### 3.1 RPS

The number of requests per second per FE. The request includes all requests sent to the FE.

<img src="/images/grafana/image-20211105144113098.png" alt="image-20211105144113098" style="zoom:50%;" />

### 3.2 QPS

The number of queries per second per FE. The query only includes Select requests.

<img src="/images/grafana/image-20211105144247229.png" alt="image-20211105144247229" style="zoom:50%;" />

### 3.3 99th Latency

The 99th query latency of each FE.

<img src="/images/grafana/image-20211105144445785.png" alt="image-20211105144445785" style="zoom:50%;" />

### 3.4 Query efficiency, failed queries and number of connections

![image-20211105144629619](/images/grafana/image-20211105144629619.png)

1. Query Percentile: The left Y axis represents the 95th to 99th query latency of each FE. The Y-axis on the right represents the query rate per minute.
2. Query Error: The left Y axis represents the cumulative error query times. The Y axis on the right represents the error query rate per minute. Generally, the error query rate should be 0.
3. Connections: the number of connections per FE

## 4.Job job information

![image-20211105145149356](/images/grafana/image-20211105145149356.png)

### 4.1 Mini Load Job

Statistics of the number of Mini Load jobs in each load state. This has been slowly abandoned and is not in use

### 4.2 Hadoop Load Job

Statistics of the number of Hadoop Load jobs in each load state.

### 4.3 Broker Load Job

Statistics of the number of Broker Load jobs in each load state.

### 4.4 Insert Load Job

Statistics of the number of load jobs in each Load State generated by Insert Stmt.

### 4.5 Mini load tendency

Mini Load Job Trend Report

### 4.6 Hadoop load tendency

Hadoop Load job trend report

### 4.7 Broker load tendency

Broker Load job trend report

### 4.8 Insert Load tendency

Load job trend report generated by Insert Stmt

### 4.9 Load submit

Displays the submitted Load job and the counter of Load job completion. If the Load submission is a Routine operation, the two lines are displayed as parallel.
The Y axis on the right shows the submission rate of the load job

![image-20211105150120384](/images/grafana/image-20211105150120384.png)

### 4.10 SC Job

The number of schema change jobs that are running.

### 4.11 Rollup Job

Number of Rollup build jobs running

### 4.12 Report queue size

The queue size reported in the Master FE.

## 5.Transaction

### 5.2 Txn Begin/Success on FE

Shows the number and rate of Txn start and success

![image-20211105150458701](/images/grafana/image-20211105150458701.png)

### 5.2 Txn Failed/Reject on FE

Display failed txn requests. Including rejected requests and failed txn

![image-20211105150655205](/images/grafana/image-20211105150655205.png)

### 5.3 Publish Task on BE

The total number of requests and error rates for posting tasks.

![image-20211105151025773](/images/grafana/image-20211105151025773.png)

### 5.4 Txn Requset on BE

Show txn request on BE

This includes statistics for four requests: begin, exec, commit, and rollback

![image-20211105151137013](/images/grafana/image-20211105151137013.png)

### 5.5 Txn Load Bytes/Rows rate

The left Y axis represents the total number of received bytes of txn. The Y axis on the right represents the Row loading rate of txn.

![image-20211105151433128](/images/grafana/image-20211105151433128.png)

## 6.FE JVM

This content is mainly for statistical analysis of Doris FE JVM memory usage monitoring

### 6.1 FE JVM Heap

Specify the JVM heap usage of FE. The left Y axis shows the used/maximum heap size. The right Y axis shows the percentage used.

![image-20211105154044921](/images/grafana/image-20211105154044921.png)

### 6.2 JVM Non Heap

Specify the JVM non-heap usage of FE. The left Y-axis shows the used/committed non-heap size.

![image-20211105154356486](/images/grafana/image-20211105154356486.png)

### 6.3 JVM Direct Buffer

Specify the JVM direct buffer usage of FE. The left Y-axis shows the used/capacity direct buffer size.

![image-20211105154523690](/images/grafana/image-20211105154523690.png)

### 6.4 JVM Threads

Number of FE JVM threads in the cluster

![image-20211105154627738](/images/grafana/image-20211105154627738.png)

### 6.5 JVM Young

Specify the use of the FE's young generation of JVM. The left Y axis shows the used/maximum young generation size. The right Y axis shows the percentage used.

![image-20211105154916220](/images/grafana/image-20211105154916220.png)

### 6.6 JVM Old

Specify FE's JVM usage in the old age. The left Y axis shows the used/maximum old generation size. The right Y axis shows the percentage used.
Generally, the usage percentage should be less than 80%.

![image-20211105155026116](/images/grafana/image-20211105155026116.png)

### 6.7 JVM Young GC

Specify the FE's JVM young gc statistics. The left Y-axis shows the time of young gc. The right Y-axis shows the time cost of each young gc.

![image-20211105155258676](/images/grafana/image-20211105155258676.png)

### 6.8 JVM Old GC

Specify the complete gc statistics of the JVM of the FE. The left Y axis shows the number of complete gc. The right Y-axis shows the time cost of each complete gc.

![image-20211105155428879](/images/grafana/image-20211105155428879.png)

## 7.BE

This part of the content mainly monitors BE's CPU, memory, network, disk, tablet, compaction and other indicators

### 7.1 BE CPU Idle

The CPU idle state of BE. Low means the CPU is busy. Explain that the higher the CPU utilization

![image-20211105155723030](/images/grafana/image-20211105155723030.png)

### 7.2 BE Mem

Here is to monitor the memory usage of each BE in the cluster

![image-20211105155933278](/images/grafana/image-20211105155933278.png)

### 7.3 Net send/receive bytes

The network sending (left Y)/receiving (right Y) byte rate of each BE node, except for "IO"

![image-20211105160055020](/images/grafana/image-20211105160055020.png)

### 7.4 Disk Usage

Disk utilization of BE nodes

![image-20211105160212618](/images/grafana/image-20211105160212618.png)

### 7.5 Tablet Distribution

The tablet distribution on each BE node is in principle distributed and balanced. If the difference is particularly large, you need to analyze the reason

![image-20211105160321983](/images/grafana/image-20211105160321983.png)

### 7.6 BE FD count

BE's file descriptor (File Descriptor) usage. The left Y axis shows the number of FDs used. The Y axis on the right shows the number of open files with the soft limit.

FileDescriptor, as the name suggests, is `File Descriptor`, FileDescriptor can be used to represent open files, open sockets, etc. For example, using FileDescriptor to represent a file: When FileDescriptor represents a file, we can generally regard FileDescriptor as the file. However, we cannot directly manipulate the file through FileDescriptor.

If you need to operate the file through FileDescriptor, you need to create a new FileDescriptor corresponding to the `FileOutputStream` or `FileInputStream`, and then operate on the file, **applications should not create their own file descriptors**

![image-20211105161321505](/images/grafana/image-20211105161321505.png)

### 7.7 BE Thread Num

BE thread count

![image-20211105161412512](/images/grafana/image-20211105161412512.png)

### 7.9 Disk IO util

BE's IO util. High means I/O is busy.

**Refer to section 2.6**

![image-20211105162301906](/images/grafana/image-20211105162301906.png)

### 7.10 BE BC (Base Compaction) and CC (Compaction Cumulate)

**This is an important monitoring indicator**

This is related to the Base Compaction Score in section 2.7.

1. Base Compaction: BE full compression ratio. Normally, basic compression only runs between 20:00 and 4:00 and it is configurable. The right Y axis represents the total basic compressed bytes.
2. Compaction Cumulate: BE incremental compression rate, the right Y axis represents the total accumulated compressed bytes.

Doris' Compaction is divided into two types: base compaction and cumulative compaction. Among them, cumulative compaction is mainly responsible for merging multiple newly imported rows into a larger rowset, while base compaction merges the rows generated by cumulative compaction into the baseline data version (Base Rowset) with start version 0, which is a kind of The expensive compaction operation. The boundary of these two compactions is determined by the cumulative point. The base compaction will merge all rows before the cumulative point, and the cumulative compaction will select several adjacent rows after the cumulative point to merge.

![image-20211105162416773](/images/grafana/image-20211105162416773.png)

### 7.11 BE Scan / Push

1. Scan Bytes: BE scanning efficiency, which means the read rate when processing queries.
2. Push Rows: Load Rows efficiency of BE, which represents the rate of rows loaded in the LOADING state of the Load job. The Y axis on the right shows the total push rate of the cluster.

![image-20211105164301864](/images/grafana/image-20211105164301864.png)

### 7.12 Tablet Meta Write

 The Y axis shows the write rate of the tablet header stored in rocksdb. The Y axis on the right shows the duration of each write operation.

![image-20211105164155489](/images/grafana/image-20211105164155489.png)

### 7.13 BE Scan Rows

The row scan rate of BE, which represents the read row rate when processing the query.

![image-20211105164611167](/images/grafana/image-20211105164611167.png)

### 7.14 BE Scan Bytes

The scanning rate of BE, which represents the row reading rate when processing the query.

![image-20211105180737088](/images/grafana/image-20211105180737088.png)

### 7.15 Tablet Meta Read

The Y axis shows the reading rate of the tablet header stored in rocksdb. The Y axis on the right shows the duration of each read operation.

![image-20211105164715087](/images/grafana/image-20211105164715087.png)

## 8 BE Task

### 8.1 Tablet Report

The Y axis on the left represents the failure rate of the specified task. Normally, it should be 0.
The Y axis on the right represents the total number of specified tasks in all Backends.

![image-20211105165143354](/images/grafana/image-20211105165143354.png)

### 8.2 Single Tablet Report

The Y axis on the left represents the failure rate of the specified task. Normally, it should be 0.
The Y axis on the right represents the total number of specified tasks in all Backends.

![image-20211105165503807](/images/grafana/image-20211105165503807.png)

### 8.3 Finish task report

The Y axis on the left represents the failure rate of the specified task. Normally, it should be 0.
The Y axis on the right represents the total number of specified tasks in all Backends.

![image-20211105165534449](/images/grafana/image-20211105165534449.png)

### 8.4 Push Task

The Y axis on the left represents the failure rate of the specified task. Normally, it should be 0.
The Y axis on the right represents the total number of specified tasks in all Backends.

![image-20211105165813642](/images/grafana/image-20211105165813642.png)

### 8.5 Push Task Cost Time

The average elapsed time of each BE push task.

![image-20211105165943222](/images/grafana/image-20211105165943222.png)

### 8.6 Delete Task

The Y axis on the left represents the failure rate of the specified task. Normally, it should be 0.
The Y axis on the right represents the total number of specified tasks in all Backends.

![image-20211105170142166](/images/grafana/image-20211105170142166.png)

### 8.7 Base Compaction Task

The running status of the Base Compaction task

The Y axis on the left represents the failure rate of the specified task. Normally, it should be 0.
The Y axis on the right represents the total number of specified tasks in all Backends.

![image-20211105171751686](/images/grafana/image-20211105171751686.png)

### 8.8 Cumulative Compaction Task

Cumulative Compaction task running status

The Y axis on the left represents the failure rate of the specified task. Normally, it should be 0.
The Y axis on the right represents the total number of specified tasks in all Backends.

![image-20211105171939322](/images/grafana/image-20211105171939322.png)

### 8.9 Clone Task

The Y axis on the left represents the failure rate of the specified task. Normally, it should be 0.
The Y axis on the right represents the total number of specified tasks in all Backends.

![image-20211105172239985](/images/grafana/image-20211105172239985.png)

### 8.10 BE Compaction Base

The Base Compaction compression rate of BE. Normally, the basic compression only runs between 20:00 and 4:00 and it is configurable.
The Y axis on the right represents the total basic compressed bytes.

For specific parameter configuration, please refer to [BE configuration item] on the official website (http://doris.apache.org/master/zh-CN/administrator-guide/config/be_config.html# configuration item list)

![image-20211105172356629](/images/grafana/image-20211105172356629.png)

### 8.11 Create tablet task

Create tablet task statistics

The Y axis on the left represents the failure rate of the specified task. Normally, it should be 0.
The Y axis on the right represents the total number of specified tasks in all Backends.

![image-20211105172934521](/images/grafana/image-20211105172934521.png)

### 8.12 Create rollup task

 Create rollup task statistics

The Y axis on the left represents the failure rate of the specified task. Normally, it should be 0.
The Y axis on the right represents the total number of specified tasks in all Backends.

![image-20211105173047377](/images/grafana/image-20211105173047377.png)

### 8.13 Schema Change Task

Schema change task statistics

The Y axis on the left represents the failure rate of the specified task. Normally, it should be 0.
The Y axis on the right represents the total number of specified tasks in all Backends.

![image-20211105173209413](/images/grafana/image-20211105173209413.png)