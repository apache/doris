---
{
    "title": "Hudi",
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


# Hudi

## 使用限制

1. Hudi 表支持的查询类型如下，后续将支持 CDC。

|  表类型   | 支持的查询类型  |
|  ----  | ----  |
| Copy On Write  | Snapshot Query, Time Travel, Icremental Read |
| Merge On Read  | Snapshot Queries, Read Optimized Queries, Time Travel, Icremental Read |

2. 目前支持 Hive Metastore 和兼容 Hive Metastore 类型(例如[AWS Glue](./hive.md)/[Alibaba DLF](./dlf.md))的 Catalog。

## 创建 Catalog

和 Hive Catalog 基本一致，这里仅给出简单示例。其他示例可参阅 [Hive Catalog](./hive.md)。

```sql
CREATE CATALOG hudi PROPERTIES (
    'type'='hms',
    'hive.metastore.uris' = 'thrift://172.21.0.1:7004',
    'hadoop.username' = 'hive',
    'dfs.nameservices'='your-nameservice',
    'dfs.ha.namenodes.your-nameservice'='nn1,nn2',
    'dfs.namenode.rpc-address.your-nameservice.nn1'='172.21.0.2:4007',
    'dfs.namenode.rpc-address.your-nameservice.nn2'='172.21.0.3:4007',
    'dfs.client.failover.proxy.provider.your-nameservice'='org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider'
);
```

可选配置参数：

|参数名|说明|默认值|
|---|---|---|
|use_hive_sync_partition|使用hms已同步的分区数据|false|

## 列类型映射

和 Hive Catalog 一致，可参阅 [Hive Catalog](./hive.md) 中 **列类型映射** 一节。

## Skip Merge
Spark 在创建 hudi mor 表的时候，会创建 `_ro` 后缀的 read optimize 表，doris 读取 read optimize 表会跳过 log 文件的合并。doris 判定一个表是否为 read optimize 表并不是通过 `_ro` 后缀，而是通过 hive inputformat，用户可以通过 `SHOW CREATE TABLE` 命令观察 cow/mor/read optimize 表的 inputformat 是否相同。
此外 doris 支持在 catalog properties 添加 hoodie 相关的配置，配置项兼容 [Spark Datasource Configs](https://hudi.apache.org/docs/configurations/#Read-Options)。所以用户可以在 catalog properties 中添加 `hoodie.datasource.merge.type=skip_merge` 跳过合并 log 文件。

## 查询优化

Doris 使用 parquet native reader 读取 COW 表的数据文件，使用 Java SDK(通过JNI调用hudi-bundle) 读取 MOR 表的数据文件。在 upsert 场景下，MOR 依然会有数据文件没有被更新，这部分文件可以通过 parquet native reader读取，用户可以通过 [explain](../../advanced/best-practice/query-analysis.md) 命令查看 hudi scan 的执行计划，`hudiNativeReadSplits` 表示有多少 split 文件通过 parquet native reader 读取。
```
|0:VHUDI_SCAN_NODE                                                             |
|      table: minbatch_mor_rt                                                  |
|      predicates: `o_orderkey` = 100030752                                    |
|      inputSplitNum=810, totalFileSize=5645053056, scanRanges=810             |
|      partition=80/80                                                         |
|      numNodes=6                                                              |
|      hudiNativeReadSplits=717/810                                            |
```
用户可以通过 [profile](../../admin-manual/http-actions/fe/profile-action.md) 查看 Java SDK 的性能，例如:
```
-  HudiJniScanner:  0ns
  -  FillBlockTime:  31.29ms
  -  GetRecordReaderTime:  1m5s
  -  JavaScanTime:  35s991ms
  -  OpenScannerTime:  1m6s
```
1. `OpenScannerTime`: 创建并初始化 JNI Reader 的时间
2. `JavaScanTime`: Java SDK 读取数据的时间
3. `FillBlockTime`: Java 数据拷贝为 C++ 数据的时间
4. `GetRecordReaderTime`: 调用 Java SDK 并创建 Hudi Record Reader 的时间

## Time Travel

每一次对 Hudi 表的写操作都会产生一个新的快照，Time Travel 支持读取 Hudi 表指定的 Snapshot。默认情况下，查询请求只会读取最新版本的快照。

可以使用 `FOR TIME AS OF` 语句，根据快照的时间([时间格式](https://hudi.apache.org/docs/0.14.0/quick-start-guide/#timetravel)和Hudi官网保持一致)读取历史版本的数据。示例如下：
```
SELECT * FROM hudi_tbl FOR TIME AS OF "2022-10-07 17:20:37";
SELECT * FROM hudi_tbl FOR TIME AS OF "20221007172037";
SELECT * FROM hudi_tbl FOR TIME AS OF "2022-10-07";
```
Hudi 表不支持 `FOR VERSION AS OF` 语句，使用该语法查询 Hudi 表将抛错。

## Incremental Read
Incremental Read 可以查询在 startTime 和 endTime 之间变化的数据，返回的结果集是数据在 endTime 的最终状态。

Doris 提供了 `@incr` 语法支持 Incremental Read:
```
SELECT * from hudi_table@incr('beginTime'='xxx', ['endTime'='xxx'], ['hoodie.read.timeline.holes.resolution.policy'='FAIL'], ...);
```
`beginTime` 是必须的，时间格式和 hudi 官网 [hudi_table_changes](https://hudi.apache.org/docs/0.14.0/quick-start-guide/#incremental-query) 保持一致，支持 "earliest"。`endTime` 选填，默认最新commitTime。兼容 [Spark Read Options](https://hudi.apache.org/docs/0.14.0/configurations#Read-Options)。

支持 Incremental Read 需要开启[新优化器](../../query-acceleration/nereids.md)，新优化器默认打开。通过 `desc` 查看执行计划，可以发现 Doris 将 `@incr` 转化为 `predicates` 下推给 `VHUDI_SCAN_NODE`:
```
|   0:VHUDI_SCAN_NODE(113)                                                                                            |
|      table: lineitem_mor                                                                                            |
|      predicates: (_hoodie_commit_time[#0] >= '20240311151019723'), (_hoodie_commit_time[#0] <= '20240311151606605') |
|      inputSplitNum=1, totalFileSize=13099711, scanRanges=1                                                          |
```
