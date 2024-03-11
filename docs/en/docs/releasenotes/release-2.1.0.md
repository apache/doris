---
{
    "title": "Release 2.1.0",
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

Dear community, we are pleased to share with you the official release of Apache Doris 2.1.0, now available for download and use as of March 8th. This latest version marks a significant milestone in our journey towards enhancing data analysis capabilities, particularly for handling massive and complex datasets.

With Doris 2.1.0, our primary focus has been on optimizing analysis performance, and the results speak for themselves. We have achieved an impressive performance improvement of over 100% on the TPC-DS 1TB test dataset, making Apache Doris more capable of challenging real-world business scenarios.

- **Quick Download：** [https://doris.apache.org/download/](https://doris.apache.org/download/)

- **GitHub：** [https://github.com/apache/doris/releases](https://github.com/apache/doris/releases)


## Performance improvement

### Smarter optimizer

On the basis of V2.0, the query optimizer in Doris V2.1 comes with enhanced statistics-based inference and enumeration framework. We have upgraded the cost model and expanded the optimization rules to serve the needs of more use cases

### Better heuristic optimization

For data analytics at scale or data lake scenarios, Doris V2.1 provides better heuristic query plans. Meanwhile, the RuntimeFilter is more self-adaptive to enable higher performance even without statistical information.

### Parallel adaptive scan

Doris V2.1 has adopted parallel adaptive scan to optimize scan I/O and thus improve query performance. It can avoid the negative impact of unreasonable numbers of buckets. (This feature is currently available on the Duplicate Key model and Merge-on-Write Unique Key model.)

### Local shuffle

We have introduced Local Shuffle to prevent uneven data distribution. Benchmark tests show that Local Shuffle in combination with Parallel Adaptive Scan can guarantee fast query performance in spite of unreasonable bucket number settings upon table creation.

### Faster INSERT INTO SELECT

To further improve the performance of INSERT INTO SELECT, which is a frequent operation in ETL, we have moved forward the MemTable execution-wise to reduce data ingestion overheads. Tests show that this can double the data ingestion speed in most cases compared to V2.0.
Improved data lake analytics capabilities

## Data lake analytic performance

### TPC-DS Benchmark

According to TPC-DS benchmark tests (1TB) of Doris V2.1 against Trino,

- Without caching, the total execution time of Doris is 56% of that of Trino V435. (717s VS 1296s)
- Enabling file cache can further increase the overall performance of Doris by 2.2 times. (323s)
  This is achieved by a series of optimizations in I/O, parquet/ORC file reading, predicate pushdown, caching, and scan task scheduling, etc.

### SQL dialects compatibility

To facilitate migration to Doris and increase its compatibility with other DBMS, we have enabled SQL dialect conversion in V2.1. ([read more](https://doris.apache.org/docs/lakehouse/sql-dialect/)) For example, by set sql_dialect = "trino" in Doris, you can use the Trino SQL dialect as you're used to, without modifying your current business logic, and Doris will execute the corresponding queries for you. Tests in user production environment show that Doris V2.1 is compatible with 99% of Trino SQL.

### Arrow Flight SQL protocol

As a column-oriented database compatible with MySQL 8.0 protocol, Doris V2.1 now supports the Arrow Flight SQL protocol as well so users can have fast access to Doris data via Pandas/Numpy without data serialization and deserialization. For most common data types, the Arrow Flight protocol enables tens of times faster performance than the MySQL protocol.

## Asynchronous materialized view

V2.1 allows creating a materialized view based on multiple tables. This feature currently supports:

- Transparent rewriting: supports transparent rewriting of common operators including Select, Where, Join, Group By, and Aggregation.
- Auto refresh: supports regular refresh, manual refresh, full refresh, incremental refresh, and partition-based refresh.
- Materialized view of external tables: supports materialized views based on external data tables such as those on Hive, Hudi, and Iceberg; supported synchronizing data from data lakes into Doris internal tables via materialized views.
- Direct query on materialized views: Materialized views can be regarded as the result set after ETL. In this sense, materialized views are data tables, so users can conduct queries on them directly.

## Enhanced storage

### Auto-increment column

V2.1 supports auto-increment columns, which can ensure data uniqueness of each row. This lays the foundation for efficient dictionary encoding and query pagination. For example, for precise UV calculation and customer grouping, users often apply the bitmap type in Doris, the process of which entails dictionary encoding. With V2.1, users can first create a dictionary table using the auto-increment column, and then simply load user data into it.

### Auto partition

To further release burden on operation and maintenance, V2.1 allows auto data partitioning. Upon data ingestion, it detects whether a partition exists for the data based on the partitioning column. If not, it automatically creates one and starts data ingestion.

### High-concurrency real-time data ingestion

For data writing, a back pressure mechanism is in place to avoid execessive data versions, so as to reduce resource consumption by data version merging. In addition, V2.1 supports group commit ([read more](https://doris.apache.org/docs/data-operate/import/import-way/group-commit-manual/)), which means to accumulate multiple writing and commit them as one. Benchmark tests on group commit with JDBC ingestion and the Stream Load method present great results.

## Semi-structured data analysis

### A new data type: Variant

V2.1 supports a new data type named Variant. It can accommodate semi-structured data such as JSON as well as compound data types that contain integers, strings, booleans, etcs. Users don't have to pre-define the exact data types for a Variant column in the table schema. The Variant type is handy when processing nested data structures.
You can include Variant columns and static columns with pre-defined data types in the same table. This will provide you with more flexibility in storage and queries.
Tests with ClickBench datasets prove that data in Variant columns takes up the same storage space as data in static columns, which is half of that in JSON format. In terms of query performance, the Variant type enables 8 times higher query speed than JSON in hot runs and even more in cold runs.

### IP types

Doris V2.1 provides native support for IPv4 and IPv6. It stores IP data in binary format, which cuts down storage space usage by 60% compared to IP string in plain texts. Along with these IP types, we have added over 20 functions for IP data processing.

### More powerful functions for compound data types

- explode_map: supports exploding rows into columns for the Map data type.
- Supports the STRUCT data type in the IN predicates

## Workload Management

### Hard isolation of resources

On the basis of the Workload Group mechanism, which imposes a soft limit on the resources that a workload group can use, Doris 2.1 introduces a hard limit on CPU resource consumption for workload groups as a way to ensure higher stability in query performance.

### TopSQL

V2.1 allows users to check the most resource-consuming SQL queries in the runtime. This can be a big help when handling cluster load spike caused by unexpected large queries.


## Others

### Decimal 256

For users in the financial sector or high-end manufacturing, V2.1 supports a high-precision data type: Decimal, which supports up to 76 significant digits (an experimental feature, please set enable_decimal256=true.)

### Job scheduler

V2.1 provides a good option for regular task scheduling: Doris Job Scheduler. It can trigger the pre-defined operations on schedule or at fixed intervals. The Doris Job Scheduler is accurate to the second. It provides consistency guarantee for data writing, high efficiency and flexibility, high-performance processing queues, retraceable scheduling records, and high availability of jobs.

### Support Docker fast start to experience the new version

Starting from version 2.1.0, we will provide a separate Docker Image to support the rapid creation of a 1FE, 1BE Docker container to experience the new version of Doris. The container will complete the initialization of FE and BE, BE registration and other steps by default. After creating the container, it can directly access and use the Doris cluster about 1 [minute.In](http://minute.in/) this image version, the default `max_map_count`, `ulimit`, `Swap` and other hard limits are removed. It supports X64 (avx2) machines and ARM machines for deployment. The default open ports are 8000, 8030, 8040, 9030.If you need to experience the Broker component, you can add the environment variable `--env BROKER=true` at startup to start the Broker process synchronously. After startup, it will automatically complete the registration. The Broker name is `test`.

Please note that this version is only suitable for quick experience and functional testing, not for production environment!

## Behavior changed

- The default data model is the Merge-on-Write Unique Key model. enable_unique_key_merge_on_write will be included as a default setting when a table is created in the Unique Key model.
- As inverted index has proven to be more performant than bitmap index, V2.1 stops supporting bitmap index. Existing bitmap indexes will remain effective but new creation is not allowed. We will remove bitmap index-related code in the future.
- cpu_resource_limit is no longer supported. It is to put a limit on the number of scanner threads on Doris BE. Since the workload group mechanism also supports such settings, the already configured cpu_resource_limit will be invalid.
- The default value of enable_segcompaction is true. This means Doris supports compaction of multiple segments in the same rowset.
- Audit log plug-in
  - Since V2.1.0, Doris has a built-in audit log plug-in. Users can simply enable or disable it by setting the enable_audit_plugin parameter.
  - If you have already installed your own audit log plug-in, you can either continue using it after upgrading to Doris V2.1, or uninstall it and use the one in Doris. Please note that the audit log table will be relocated after switching plug-in.
  - For more details, please see the [docs](https://doris.apache.org/docs/ecosystem/audit-plugin/).


## Credits
| Thanks all who contribute to this release:     |
| :---------------------------------------------- |
| morrySnow                                       |
| Gabriel39                                       |
| [BiteTheDDDDt](https://github.com/BiteTheDDDDt) |
| [kaijchen](https://github.com/kaijchen)         |
| starocean999                                    |
| morningman                                      |
| [jackwener](https://github.com/jackwener)       |
| zy-kkk                                          |
| englefly                                        |
| Jibing-Li                                       |
| [XieJiann](https://github.com/XieJiann)         |
| [yujun777](https://github.com/yujun777)         |
| Mryange                                         |
| HHoflittlefish777                               |
| LiDongyangLi                                    |
| HappenLee                                       |
| zhangstar333                                    |
| lihangyu                                        |
| zclllyybb                                       |
| amory                                           |
| bobhan1                                         |
| AKIRA                                           |
| zhangdong                                       |
| ZouXinyiZou                                     |
| HuJerryHu                                       |
| yiguolei                                        |
| airborne12                                      |
| wangbo                                          |
| jacktengg                                       |
| jacktengg                                       |
| TangSiyang2001                                  |
| BePPPower                                       |
| Yukang-Lian                                     |
| mymeiyi                                         |
| liugddx                                         |
| kaka11chen                                      |
| AshinGau                                        |
| DrogonJackDrogon                                |
| wsjz                                            |
| seuhezhiqiang                                   |
| zhannngchen                                     |
| shuke987                                        |
| KassieZ                                         |
| huanghaibin                                     |
| zzzxl1993                                       |
| Nitin-Kashyap                                   |
| AlexYue                                         |
| dataroaring                                     |
| seawinde                                        |
| walter                                          |
| xzj7019                                         |
| xiaokang                                        |
| SWJTU-ZhangLei                                  |
| [liaoxin01](https://github.com/liaoxin01)       |
| [dutyu](https://github.com/dutyu)               |
| wuwenchihdu                                     |
| LiBinfeng-01                                    |
| daidai                                          |
| qidaye                                          |
| mch_ucchi                                       |
| zhangguoqiang                                   |
| zhengyu                                         |
| plat1ko                                         |
| LemonLiTree                                     |
| ixzc                                            |
| deardeng                                        |
| yiguolei                                        |
| catpineapple                                    |
| LingAdonisLing                                  |
| DongLiang-0                                     |
| whuxingying                                     |
| Tanya-W                                         |
| Yulei-Yang                                      |
| zzzzzzzs                                        |
| caoliang-web                                    |
| xueweizhang                                     |
| yangshijie                                      |
| Luwei                                           |
| lsy3993                                         |
| xy720                                           |
| HowardQin                                       |
| DeadlineFen                                     |
| Petrichor                                       |
| caiconghui                                      |
| KirsCalvinKirs                                  |
| SunChenyangSun                                  |
| ChouGavinChou                                   |
| Luzhijing                                       |
| gnehil                                          |
| wudi                                            |
| zhiqqqq                                         |
| zfr95                                           |
| zxealous                                        |
| kkop                                            |
| yagagagaga                                      |
| Chester                                         |
| LuGuangmingLu                                   |
| Lightman                                        |
| Xiaocc                                          |
| taoxutao                                        |
| yuanyuan8983                                    |
| KirsCalvinKirs                                  |
| DuRipeng                                        |
| GoGoWen                                         |
| JingDas                                         |
| camby                                           |
| camby                                           |
| Euporia                                         |
| rohitrs1983                                     |
| felixwluo                                       |
| wudongliang                                     |
| FreeOnePlus                                     |
| PaiVallishPai                                   |
| XuJianxu                                        |
| seuhezhiqiang                                   |
| luozenglin                                      |
| 924060929                                       |
| HB                                              |
| LiuLijiaLiu                                     |
| Ma1oneZhang                                     |
| bingquanzhao                                    |
| chunping                                        |
| echo-dundun                                     |
| feiniaofeiafei                                  |
| walter                                          |
| yongjinhou                                      |
| zgxme                                           |
| zhangy5                                         |
| httpshirley                                     |
| ChenyangSunChenyang                             |
| ZenoYang                                        |
| ZhangYu0123                                     |
| hechao                                          |
| herry2038                                       |
| jayhua                                          |
| koarz                                           |
| nanfeng                                         |
| LiChuangLi                                      |
| LiuGuangdongLiu                                 |
| Jeffrey                                         |
| liuJiwenliu                                     |
| Stalary                                         |
| DuanXujianDuan                                  |
| HuZhiyuHu                                       |
| jiafeng.zhang                                   |
| nanfeng                                         |
| py023                                           |
| xiongjx                                         |
| yuxuan-luo                                      |
| zhaoshuo                                        |
| XiaoChangmingXiao                               |
| ElvinWei                                        |
| LiuHongLiu                                      |
| QiHouliangQi                                    |
| Hyman-zhao                                      |
| HelgeLarsHelge                                  |
| Uniqueyou                                       |
| YangYAN                                         |
| acnot                                           |
| amory                                           |
| feifeifeimoon                                   |
| flynn                                           |
| gohalo                                          |
| htyoung                                         |
| realize096                                      |
| shee                                            |
| wangqt                                          |
| xyfsjq                                          |
| zzwwhh                                          |
| songguangfan                                    |
| 467887319                                       |
| BirdAmosBird                                    |
| ZhuArmandoZhu                                   |
| CanGuan                                         |
| ChengDaqi2023                                   |
| ChinaYiGuan                                     |
| gitccl                                          |
| colagy                                          |
| DeadlineFen                                     |
| Doris-Extras                                    |
| HonestManXin                                    |
| q763562998                                      |
| guardcrystal                                    |
| Dragonliu2018                                   |
| ZhaoLongZhao                                    |
| LuoMetaLuo                                      |
| Miaohongkai                                     |
| YinShaowenYin                                   |
| Centurybbx                                      |
| hongkun-Shao                                    |
| Wanghuan                                        |
| Xinxing                                         |
| XueYuhai                                        |
| Yoko                                            |
| HeZhangJianHe                                   |
| ZhongJinHacker                                  |
| alan_rodriguez                                  |
| allenhooo                                       |
| beat4ocean                                      |
| bigben0204                                      |
| chen                                            |
| czzmmc                                          |
| dalong                                          |
| deadlinefen                                     |
| didiaode18                                      |
| dong-shuai                                      |
| feelshana                                       |
| fornaix                                         |
| hammer                                          |
| xuke-hat                                        |
| hqx871                                          |
| i78086                                          |
| irenesrl                                        |
| julic20s                                        |
| kindred77                                       |
| lihuigang                                       |
| wenluowen                                       |
| lxliyou001                                      |
| CSTGluigi                                       |
| ranxiang327                                     |
| shysnow                                         |
| sunny                                           |
| vhwzIs                                          |
| wangtao                                         |
| wangtianyi2004                                  |
| wyx123654                                       |
| xuefengze                                       |
| xiangran0327                                    |
| xy                                              |
| yimeng                                          |
| ytwp                                            |
| yujian                                          |
| zhangstar333                                    |
| figurant                                        |
| sdhzwc                                          |
| LHG41278                                        |
| zlw5307                                         |