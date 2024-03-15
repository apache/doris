---
{
    "title": "SQL Cache",
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

# SQL Cache

SQL 语句完全一致时将命中缓存。

## 需求场景 & 解决方案

见 query-cache.md。

## 设计原理

SQLCache按SQL的签名、查询的表的分区ID、分区最新版本来存储和获取缓存。三者组合确定一个缓存数据集，任何一个变化了，如SQL有变化，如查询字段或条件不一样，或数据更新后版本变化了，会导致命中不了缓存。

如果多张表Join，使用最近更新的分区ID和最新的版本号，如果其中一张表更新了，会导致分区ID或版本号不一样，也一样命中不了缓存。

SQLCache，更适合T+1更新的场景，凌晨数据更新，首次查询从BE中获取结果放入到缓存中，后续相同查询从缓存中获取。实时更新数据也可以使用，但是可能存在命中率低的问题。

当前支持 OlapTable内表 和 Hive外表。

## 使用方式

确保fe.conf的cache_enable_sql_mode=true（默认是true）

```text
vim fe/conf/fe.conf
cache_enable_sql_mode=true
```

在MySQL命令行中设置变量

```sql
MySQL [(none)]> set [global] enable_sql_cache=true;
```

注：global是全局变量，不加指当前会话变量

## 缓存条件

第一次查询后，如果满足下面三个条件，查询结果就会被缓存。

1. (当前时间 - 查询的分区最后更新时间) 大于 fe.conf 中的 cache_last_version_interval_second。

2. 查询结果行数 小于 fe.conf 中的 cache_result_max_row_count。

3. 查询结果bytes 小于 fe.conf 中的 cache_result_max_data_size。

具体参数介绍和未尽事项见 query-cache.md。

## 未尽事项

- SQL中包含产生随机值的函数，比如 random()，使用 QueryCache 会导致查询结果失去随机性，每次执行将得到相同的结果。

- 类似的SQL，之前查询了2个指标，现在查询3个指标，是否可以利用2个指标的缓存？ 目前不支持