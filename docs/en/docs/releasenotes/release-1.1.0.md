---
{
    "title": "Release 1.1.0",
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

In version 1.1, we realized the full vectorization of the computing layer and storage layer, and officially enabled the vectorized execution engine as a stable function. All queries are executed by the vectorized execution engine by default, and the performance is 3-5 times higher than the previous version. It increases the ability to access the external tables of Apache Iceberg and supports federated query of data in Doris and Iceberg, and expands the analysis capabilities of Apache Doris on the data lake; on the basis of the original LZ4, the ZSTD compression algorithm is added , further improves the data compression rate; fixed many performance and stability problems in previous versions, greatly improving system stability. Downloading and using is recommended.

## Upgrade Notes

### The vectorized execution engine is enabled by default

In version 1.0, we introduced the vectorized execution engine as an experimental feature and Users need to manually enable it when executing queries by configuring the session variables through `set batch_size = 4096` and `set enable_vectorized_engine = true` .

In version 1.1, we officially fully enabled the vectorized execution engine as a stable function. The session variable `enable_vectorized_engine` is set to true by default. All queries are executed by default through the vectorized execution engine.

### BE Binary File Renaming

BE binary file has been renamed from palo_be to doris_be . Please pay attention to modifying the relevant scripts if you used to rely on process names for cluster management and other operations.

### Segment storage format upgrade

The storage format of earlier versions of Apache Doris was Segment V1. In version 0.12, we had implemented Segment V2 as a new storage format, which introduced Bitmap indexes, memory tables, page cache, dictionary compression, delayed materialization and many other features. Starting from version 0.13, the default storage format for newly created tables is Segment V2, while maintaining compatibility with the Segment V1 format.

In order to ensure the maintainability of the code structure and reduce the additional learning and development costs caused by redundant historical codes, we have decided to no longer support the Segment v1 storage format from the next version. It is expected that this part of the code will be deleted in the Apache Doris 1.2 version.

### Normal Upgrade

For normal upgrade operations, you can perform rolling upgrades according to the cluster upgrade documentation on the official website.

[https://doris.apache.org//docs/admin-manual/cluster-management/upgrade](https://doris.apache.org//docs/admin-manual/cluster-management/upgrade)

## Features

### Support random distribution of data [experimental]

In some scenarios (such as log data analysis), users may not be able to find a suitable bucket key to avoid data skew, so the system needs to provide additional distribution methods to solve the problem.

Therefore, when creating a table you can set `DISTRIBUTED BY random BUCKETS number`to use random distribution, the data will be randomly written to a single tablet when importing to reduce the data fanout during the loading process. And reduce resource overhead and improve system stability.

### Support for creating Iceberg external tables[experimental]

Iceberg external tables provide Apache Doris with direct access to data stored in Iceberg. Through Iceberg external tables, federated queries on data stored in local storage and Iceberg can be implemented, which saves tedious data loading work, simplifies the system architecture for data analysis, and performs more complex analysis operations.

In version 1.1, Apache Doris supports creating Iceberg external tables and querying data, and supports automatic synchronization of all table schemas in the Iceberg database through the REFRESH command.

### Added ZSTD compression algorithm

At present, the data compression method in Apache Doris is uniformly specified by the system, and the default is LZ4. For some scenarios that are sensitive to data storage costs, the original data compression ratio requirements cannot be met.

In version 1.1, users can set "compression"="zstd" in the table properties to specify the compression method as ZSTD when creating a table. In the 25GB 110 million lines of text log test data, the highest compression rate is nearly 10 times, which is 53% higher than the original compression rate, and the speed of reading data from disk and decompressing it is increased by 30%.

## Improvements

### More comprehensive vectorization support

In version 1.1, we implemented full vectorization of the compute and storage layers, including:

Implemented vectorization of all built-in functions

The storage layer implements vectorization and supports dictionary optimization for low-cardinality string columns

Optimized and resolved numerous performance and stability issues with the vectorization engine.

We tested the performance of Apache Doris version 1.1 and version 0.15 on the SSB and TPC-H standard test datasets:

On all 13 SQLs in the SSB test data set, version 1.1 is better than version 0.15, and the overall performance is improved by about 3 times, which solves the problem of performance degradation in some scenarios in version 1.0;

On all 22 SQLs in the TPC-H test data set, version 1.1 is better than version 0.15, the overall performance is improved by about 4.5 times, and the performance of some scenarios is improved by more than ten times;

![](/images/release-note-1.1.0-SSB.png)

<p align='center'>SSB Benchmark</p>

![](/images/release-note-1.1.0-TPC-H.png)


<p align='center'>TPC-H Benchmark</p>

**Performance test report**

[https://doris.apache.org//docs/benchmark/ssb](https://doris.apache.org//docs/benchmark/ssb)

[https://doris.apache.org//docs/benchmark/tpch](https://doris.apache.org//docs/benchmark/tpch)

### Compaction logic optimization and real-time guarantee

In Apache Doris, each commit will generate a data version. In high concurrent write scenarios, -235 errors are prone to occur due to too many data versions and untimely compaction, and query performance will also decrease accordingly.

In version 1.1, we introduced QuickCompaction, which will actively trigger compaction when the data version increases. At the same time, by improving the ability to scan fragment metadata, it can quickly find fragments with too many data versions and trigger compaction. Through active triggering and passive scanning, the real-time problem of data merging is completely solved.

At the same time, for high-frequency small file cumulative compaction, the scheduling and isolation of compaction tasks is implemented to prevent the heavyweight base compaction from affecting the merging of new data.

Finally, for the merging of small files, the strategy of merging small files is optimized, and the method of gradient merging is adopted. Each time the files participating in the merging belong to the same data magnitude, it prevents versions with large differences in size from merging, and gradually merges hierarchically. , reducing the number of times a single file participates in merging, which can greatly save the CPU consumption of the system.

When the data upstream maintains a write frequency of 10w per second (20 concurrent write tasks, 5000 rows per job, and checkpoint interval of 1s), version 1.1 behaves as follows:

-   Quick data consolidation: Tablet version remains below 50 and compaction score is stable. Compared with the -235 problem that frequently occurred during high concurrent writing in the previous version, the compaction merge efficiency has been improved by more than 10 times.

-   Significantly reduced CPU resource consumption: The strategy has been optimized for small file Compaction. In the above scenario of high concurrent writing, CPU resource consumption is reduced by 25%;

-   Stable query time consumption: The overall orderliness of data is improved, and the fluctuation of query time consumption is greatly reduced. The query time consumption during high concurrent writing is the same as that of only querying, and the query performance is improved by 3-4 times compared with the previous version.

### Read efficiency optimization for Parquet and ORC files

By adjusting arrow parameters, arrow's multi-threaded read capability is used to speed up Arrow's reading of each row_group, and it is modified to SPSC model to reduce the cost of waiting for the network through prefetching. After optimization, the performance of Parquet file import is improved by 4 to 5 times.

### Safer metadata Checkpoint

By double-checking the image files generated after the metadata checkpoint and retaining the function of historical image files, the problem of metadata corruption caused by image file errors is solved.

## Bugfix

### Fix the problem that the data cannot be queried due to the missing data version.(Serious)

This issue was introduced in version 1.0 and may result in the loss of data versions for multiple replicas.

### Fix the problem that the resource isolation is invalid for the resource usage limit of loading tasks (Moderate)

In 1.1, the broker load and routine load will use Backends with specified resource tags to do the load.

### Use HTTP BRPC to transfer network data packets over 2GB (Moderate)

In the previous version, when the data transmitted between Backends through BRPC exceeded 2GB,
it may cause data transmission errors.

## Others

### Disabling Mini Load

The `/_load` interface is disabled by default, please use `the /_stream_load` interface uniformly.
Of course, you can re-enable it by turning off the FE configuration item `disable_mini_load`.

The Mini Load interface will be completely removed in version 1.2.

### Completely disable the SegmentV1 storage format

Data in SegmentV1 format is no longer allowed to be created. Existing data can continue to be accessed normally.
You can use the `ADMIN SHOW TABLET STORAGE FORMAT` statement to check whether the data in SegmentV1 format
still exists in the cluster. And convert to SegmentV2 through the data conversion command

Access to SegmentV1 data will no longer be supported in version 1.2.

### Limit the maximum length of String type

In previous versions, String types were allowed a maximum length of 2GB.
In version 1.1, we will limit the maximum length of the string type to 1MB. Strings longer than this length cannot be written anymore.
At the same time, using the String type as a partitioning or bucketing column of a table is no longer supported.

The String type that has been written can be accessed normally.

### Fix fastjson related vulnerabilities

Update to Canal version to fix fastjson security vulnerability.

### Added `ADMIN DIAGNOSE TABLET` command

Used to quickly diagnose problems with the specified tablet.

## Download to Use

### Download Link

[hhttps://doris.apache.org/download](https://doris.apache.org/download)

### Feedback

If you encounter any problems with use, please feel free to contact us through GitHub discussion forum or Dev e-mail group anytime.

GitHub Forum: [https://github.com/apache/doris/discussions](https://github.com/apache/doris/discussions)

Mailing list: [dev@doris.apache.org](dev@doris.apache.org)

## Thanks

Thanks to everyone who has contributed to this release:

```

@adonis0147

@airborne12

@amosbird

@aopangzi

@arthuryangcs

@awakeljw

@BePPPower

@BiteTheDDDDt

@bridgeDream

@caiconghui

@cambyzju

@ccoffline

@chenlinzhong

@daikon12

@DarvenDuan

@dataalive

@dataroaring

@deardeng

@Doris-Extras

@emerkfu

@EmmyMiao87

@englefly

@Gabriel39

@GoGoWen

@gtchaos

@HappenLee

@hello-stephen

@Henry2SS

@hewei-nju

@hf200012

@jacktengg

@jackwener

@Jibing-Li

@JNSimba

@kangshisen

@Kikyou1997

@kylinmac

@Lchangliang

@leo65535

@liaoxin01

@liutang123

@lovingfeel

@luozenglin

@luwei16

@luzhijing

@mklzl

@morningman

@morrySnow

@nextdreamblue

@Nivane

@pengxiangyu

@qidaye

@qzsee

@SaintBacchus

@SleepyBear96

@smallhibiscus

@spaces-X

@stalary

@starocean999

@steadyBoy

@SWJTU-ZhangLei

@Tanya-W

@tarepanda1024

@tianhui5

@Userwhite

@wangbo

@wangyf0555

@weizuo93

@whutpencil

@wsjz

@wunan1210

@xiaokang

@xinyiZzz

@xlwh

@xy720

@yangzhg

@Yankee24

@yiguolei

@yinzhijian

@yixiutt

@zbtzbtzbt

@zenoyang

@zhangstar333

@zhangyifan27

@zhannngchen

@zhengshengjun

@zhengshiJ

@zingdle

@zuochunwei

@zy-kkk
```
