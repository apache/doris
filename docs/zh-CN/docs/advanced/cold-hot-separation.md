---
{
    "title": "冷热分层",
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

# 冷热分层

## 需求场景

未来一个很大的使用场景是类似于es日志存储，日志场景下数据会按照日期来切割数据，很多数据是冷数据，查询很少，需要降低这类数据的存储成本。从节约存储成本角度考虑
1. 各云厂商普通云盘的价格都比对象存储贵
2. 在doris集群实际线上使用中，普通云盘的利用率无法达到100%
3. 云盘不是按需付费，而对象存储可以做到按需付费
4. 基于普通云盘做高可用，需要实现多副本，某副本异常要做副本迁移。而将数据放到对象存储上则不存在此类问题，因为对象存储是共享的。

## 解决方案
在Partition级别上设置freeze time，表示多久这个Partition会被freeze，并且定义freeze之后存储的remote storage的位置。在be上daemon线程会周期性的判断表是否需要freeze，若freeze后会将数据上传到s3上。

冷热分层支持所有doris功能，只是把部分数据放到对象存储上，以节省成本，不牺牲功能。因此有如下特点：

- 冷数据放到对象存储上，用户无需担心数据一致性和数据安全性问题
- 灵活的freeze策略，冷却远程存储property可以应用到表和partition级别
- 用户查询数据，无需关注数据分布位置，若数据不在本地，会拉取对象上的数据，并cache到be本地
- 副本clone优化，若存储数据在对象上，则副本clone的时候不用去拉取存储数据到本地
- 远程对象空间回收recycler，若表、分区被删除，或者冷热分层过程中异常情况产生的空间浪费，则会有recycler线程周期性的回收，节约存储资源
- cache优化，将访问过的冷数据cache到be本地，达到非冷热分层的查询性能
- be线程池优化，区分数据来源是本地还是对象存储，防止读取对象延时影响查询性能

## Storage policy的使用

存储策略是使用冷热分层功能的入口，用户只需要在建表或使用doris过程中，给表或分区关联上storage policy，即可以使用冷热分层的功能。

<version since="dev"></version> 创建S3 RESOURCE的时候，会进行S3远端的链接校验，以保证RESOURCE创建的正确。

此外，需要新增fe配置：`enable_storage_policy=true`  

注意：这个属性不会被CCR同步，如果这个表是被CCR复制而来的，即PROPERTIES中包含`is_being_synced = true`时，这个属性将会在这个表中被擦除。

例如：

```
CREATE RESOURCE "remote_s3"
PROPERTIES
(
    "type" = "s3",
    "s3.endpoint" = "bj.s3.com",
    "s3.region" = "bj",
    "s3.bucket" = "test-bucket",
    "s3.root.path" = "path/to/root",
    "s3.access_key" = "bbb",
    "s3.secret_key" = "aaaa",
    "s3.connection.maximum" = "50",
    "s3.connection.request.timeout" = "3000",
    "s3.connection.timeout" = "1000"
);

CREATE STORAGE POLICY test_policy
PROPERTIES(
    "storage_resource" = "remote_s3",
    "cooldown_ttl" = "1d"
);

CREATE TABLE IF NOT EXISTS create_table_use_created_policy 
(
    k1 BIGINT,
    k2 LARGEINT,
    v1 VARCHAR(2048)
)
UNIQUE KEY(k1)
DISTRIBUTED BY HASH (k1) BUCKETS 3
PROPERTIES(
    "storage_policy" = "test_policy"
);
```
或者对一个已存在的表，关联storage policy
```
ALTER TABLE create_table_not_have_policy set ("storage_policy" = "test_policy");
```
或者对一个已存在的partition，关联storage policy
```
ALTER TABLE create_table_partition MODIFY PARTITION (*) SET("storage_policy"="test_policy");
```
**注意**，如果用户在建表时给整张table和部分partition指定了不同的storage policy，partition设置的storage policy会被无视，整张表的所有partition都会使用table的policy. 如果您需要让某个partition的policy和别的不同，则可以使用上文中对一个已存在的partition，关联storage policy的方式修改.
具体可以参考docs目录下[resource](../sql-manual/sql-reference/Data-Definition-Statements/Create/CREATE-RESOURCE.md)、 [policy](../sql-manual/sql-reference/Data-Definition-Statements/Create/CREATE-POLICY.md)、 [create table](../sql-manual/sql-reference/Data-Definition-Statements/Create/CREATE-TABLE.md)、 [alter table](../sql-manual/sql-reference/Data-Definition-Statements/Alter/ALTER-TABLE-COLUMN.md)等文档，里面有详细介绍

### 一些限制

- 单表或单partition只能关联一个storage policy，关联后不能drop掉storage policy，需要先解除二者的关联。
- storage policy关联的对象信息不支持修改数据存储path的信息，比如bucket、endpoint、root_path等信息
- storage policy支持创建和修改和支持删除，删除前需要先保证没有表引用此storage policy。
- Unique 模型在开启 Merge-On-Write 特性时，不支持设置 storage policy。

## 冷数据占用对象大小
方式一：
通过show proc '/backends'可以查看到每个be上传到对象的大小，RemoteUsedCapacity项，此方式略有延迟。

方式二：
通过show tablets from tableName可以查看到表的每个tablet占用的对象大小，RemoteDataSize项。

## 冷数据的cache
上文提到冷数据为了优化查询的性能和对象存储资源节省，引入了cache的概念。在冷却后首次命中，Doris会将已经冷却的数据又重新加载到be的本地磁盘，cache有以下特性：
- cache实际存储于be磁盘，不占用内存空间。
- cache可以限制膨胀，通过LRU进行数据的清理
- cache的实现和联邦查询catalog的cache是同一套实现，文档参考[此处](../lakehouse/filecache.md)

## 冷数据的compaction
冷数据传入的时间是数据rowset文件写入本地磁盘时刻起，加上冷却时间。由于数据并不是一次性写入和冷却的，因此避免在对象存储内的小文件问题，doris也会进行冷数据的compaction。
但是，冷数据的compaction的频次和资源占用的优先级并不是很高，也推荐本地热数据compaction后再执行冷却。具体可以通过以下be参数调整：
- be参数`cold_data_compaction_thread_num`可以设置执行冷数据的compaction的并发，默认是2。
- be参数`cold_data_compaction_interval_sec` 可以设置执行冷数据的compaction的时间间隔，默认是1800，单位：秒，即半个小时。

## 冷数据的schema change
数据冷却后支持schema change类型如下：
- 增加、删除列
- 修改列类型
- 调整列顺序
- 增加、修改 Bloom Filter
- 增加、删除 bitmap index

## 冷数据的垃圾回收
冷数据的垃圾数据是指没有被任何Replica使用的数据，对象存储上可能会有如下情况产生的垃圾数据：
1. 上传rowset失败但是有部分segment上传成功。
2. FE重新选CooldownReplica后，新旧CooldownReplica的rowset version不一致，FollowerReplica都去同步新CooldownReplica的CooldownMeta，旧CooldownReplica中version不一致的rowset没有Replica使用成为垃圾数据。
3. 冷数据Compaction后，合并前的rowset因为还可能被其他Replica使用不能立即删除，但是最终FollowerReplica都使用了最新的合并后的rowset，合并前的rowset成为垃圾数据。

另外，对象上的垃圾数据并不会立即清理掉。
be参数`remove_unused_remote_files_interval_sec` 可以设置冷数据的垃圾回收的时间间隔，默认是21600，单位：秒，即6个小时。


## 未尽事项

- 目前暂无方式查询特定storage policy 关联的表。
- 一些远端占用指标更新获取不够完善

## 常见问题

1. ERROR 1105 (HY000): errCode = 2, detailMessage = Failed to create repository: connect to s3 failed: Unable to marshall request to JSON: host must not be null.

S3 SDK 默认使用 virtual-hosted style 方式。但某些对象存储系统(如：minio)可能没开启或没支持 virtual-hosted style 方式的访问，此时我们可以添加 use_path_style 参数来强制使用 path style 方式：

```text
CREATE RESOURCE "remote_s3"
PROPERTIES
(
    "type" = "s3",
    "s3.endpoint" = "bj.s3.com",
    "s3.region" = "bj",
    "s3.bucket" = "test-bucket",
    "s3.root.path" = "path/to/root",
    "s3.access_key" = "bbb",
    "s3.secret_key" = "aaaa",
    "s3.connection.maximum" = "50",
    "s3.connection.request.timeout" = "3000",
    "s3.connection.timeout" = "1000",
    "use_path_style" = "true"
);
```
