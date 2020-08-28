---
{
    "title": "QUERY CACHE",
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
# QUERY CACHE



## 一 需求

虽然在数据库层也做了对应的缓存，但这种数据库层次的缓存一般针对的是查询内容，而且粒度也太小，一般只有表中数据没有变更的时候，数据库对应的cache才发挥了作用。但这并不能减少业务系统对数据库产生的增、删、查、改的庞大IO压力。所以数据库缓存技术在此诞生，实现热点数据的高速缓存，提高应用的响应速度，极大缓解后端数据库的压力

- 高并发场景
  Doris可以较好的支持高并发，但单台服务器无法承载太高的QPS

- 复杂图表的看板
  复杂的Dashboard或者大屏类应用，数据来自多张表，每个页面有数十个查询，虽然每个查询只有数十毫秒，但是总体查询时间会在数秒

- 趋势分析
  给定日期范围的查询，指标按日显示，比如查询最近7天内的用户数的趋势，这类查询数据量大，查询范围广，查询时间往往需要数十秒

- 用户重复查询
  如果产品没有防重刷机制，用户因手误或其他原因重复刷新页面，导致提交大量的重复的SQL



以上四种场景，在应用层的解决方案，把查询结果放到Redis中，周期性的更新缓存或者用户手工刷新缓存，但是这个方案有如下问题：

- 数据不一致
  无法感知数据的更新，导致用户经常看到旧的数据

- 命中率低
  缓存整个查询结果，如果数据实时写入，缓存频繁失效，命中率低且系统负载较重

- 额外成本
  引入外部缓存组件，会带来系统复杂度，增加额外成本



## 二 解决方案

目前我们设计出结果缓存和分区缓存两个模块



## 三 名词解释

1. 结果缓存 result_cache

针对用户的sql直接缓存查询的结果集合



2. 分区缓存 partition_cache

在partition粒度做针对每个分区查询的结果缓存 



## 四 设计原理

### 1 结果缓存 `result_cache`

result_cache 分两层 第一层为 result_cache_ttl 第二层为 result_cache_version 



#### `result_cache_ttl`

result_cache_ttl 变量设置在用户Session中，用户可自定义是否开启,通过ttl时间来确定用户的sql是否使用缓存，`这里数据变更时不保证数据的正确性`

按照 用户 connectid,和查询的sql 来存储和获取缓存，超过缓存失效时间则命中不了缓存，该缓存也会被清理



#### ` result_cache_version`

result_cache_version 按SQL的签名、查询的表的分区ID、分区最新版本来存储和获取缓存。三者组合确定一个缓存数据集，任何一个变化了，如SQL有变化，如查询字段或条件不一样，或数据更新后版本变化了，会导致命中不了缓存。

如果多张表Join，使用最近更新的分区ID和最新的版本号，如果其中一张表更新了，会导致分区ID或版本号不一样，也一样命中不了缓存。



### 2 分区缓存 `partition_cache`

1. SQL可以并行拆分，Q = Q1 ∪ Q2 ... ∪ Qn，R= R1 ∪ R2 ... ∪ Rn，Q为查询语句，R为结果集

2. 拆分为只读分区和可更新分区，只读分区缓存，更新分区不缓存



## 五 参数

###  fe

- cache 开关

```
enable_result_cache_ttl

enable_result_cache_version

enable_partition_cache
```

- 每个查询是否缓存的限制

```
cache_per_query_max_row_count

cache_per_query_max_size_in_bytes
```

- result cache til

```
result_cache_ttl_in_milliseconds
```



### be

- BE最大分区数量cache_max_partition_count，指每个SQL对应的最大分区数，如果是按日期分区，能缓存2年多的数据，假如想保留更长时间的缓存，请把这个参数设置得更大，同时修改cache_result_max_row_count的参数。

```
cache_max_partition_count
```



- BE中缓存内存设置，有两个参数cache_max_size_in_mb和cache_elasticity_size_in_mb），内存超过cache_max_size_in_mb+cache_elasticity_size_in_mb会开始清理，并把内存控制到cache_max_size_in_mb以下。可以根据BE节点数量，节点内存大小，和缓存命中率来设置这两个参数。

```
cache_max_size_in_mb=256

cache_elasticity_size_in_mb=128
```





## 使用方法

- use enable_result_cache_ttl

```
set `global`  enable_result_cache_ttl =true
```

- use enable_result_cache_version

```
set `global` enable_result_cache_version = true
```

- use enable_partition_cache

```
enable_partition_cache
```

