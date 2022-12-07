---
{
    "title": "冷热分离",
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

# 冷热分离

## 需求场景

未来一个很大的使用场景是类似于es日志存储，日志场景下数据会按照日期来切割数据，很多数据是冷数据，查询很少，需要降低这类数据的存储成本。从节约存储成本角度考虑
1. 各云厂商普通云盘的价格都比对象存储贵
2. 在doris集群实际线上使用中，普通云盘的利用率无法达到100%
3. 云盘不是按需付费，而对象存储可以做到按需付费
4. 基于普通云盘做高可用，需要实现多副本，某副本异常要做副本迁移。而将数据放到对象存储上则不存在此类问题，因为对象存储是共享的。

## 解决方案
在Partition级别上设置freeze time，表示多久这个Partition会被freeze，并且定义freeze之后存储的remote storage的位置。在be上daemon线程会周期性的判断表是否需要freeze，若freeze后会将数据上传到s3上。

冷热分离支持所有doris功能，只是把部分数据放到对象存储上，以节省成本，不牺牲功能。因此有如下特点：

- 冷数据放到对象存储上，用户无需担心数据一致性和数据安全性问题
- 灵活的freeze策略，冷却远程存储property可以应用到表和partition级别
- 用户查询数据，无需关注数据分布位置，若数据不在本地，会拉取对象上的数据，并cache到be本地
- 副本clone优化，若存储数据在对象上，则副本clone的时候不用去拉取存储数据到本地
- 远程对象空间回收recycler，若表、分区被删除，或者冷热分离过程中异常情况产生的空间浪费，则会有recycler线程周期性的回收，节约存储资源
- cache优化，将访问过的冷数据cache到be本地，达到非冷热分离的查询性能
- be线程池优化，区分数据来源是本地还是对象存储，防止读取对象延时影响查询性能

## Storage policy的使用

存储策略是使用冷热分离功能的入口，用户只需要在建表或使用doris过程中，给表或分区关联上storage policy，即可以使用冷热分离的功能。

例如：

```
CREATE RESOURCE "remote_s3"
PROPERTIES
(
    "type" = "s3",
    "AWS_ENDPOINT" = "bj.s3.com",
    "AWS_REGION" = "bj",
    "AWS_BUCKET" = "test-bucket",
    "AWS_ROOT_PATH" = "path/to/root",
    "AWS_ACCESS_KEY" = "bbb",
    "AWS_SECRET_KEY" = "aaaa",
    "AWS_MAX_CONNECTIONS" = "50",
    "AWS_REQUEST_TIMEOUT_MS" = "3000",
    "AWS_CONNECTION_TIMEOUT_MS" = "1000"
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
具体可以参考docs目录下resource、policy、create table、alter等文档，里面有详细介绍

### 一些限制

- 单表或单partition只能关联一个storage policy，关联后不能drop掉storage policy
- storage policy关联的对象信息不支持修改数据存储path的信息，比如bucket、endpoint、root_path等信息
- storage policy目前只支持创建，不支持删除

## 冷数据占用对象大小
方式一：
通过show proc '/backends'可以查看到每个be上传到对象的大小，RemoteUsedCapacity项

方式二：
通过show tablets from tableName可以查看到表的每个tablet占用的对象大小，RemoteDataSize项


## 未尽事项

- 数据被cooldown后，又有新数据update或导入等，compaction目前没有处理
- 数据被cooldown后，schema change操作，目前不支持
