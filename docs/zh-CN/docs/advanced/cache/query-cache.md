---
{
    "title": "Query Cache",
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

# Query Cache

## 需求场景

大部分数据分析场景是写少读多，数据写入一次，多次频繁读取，比如一张报表涉及的维度和指标，数据在凌晨一次性计算好，但每天有数百甚至数千次的页面访问，因此非常适合把结果集缓存起来。在数据分析或BI应用中，存在下面的业务场景：

- **高并发场景**，Doris可以较好的支持高并发，但单台服务器无法承载太高的QPS
- **复杂图表的看板**，复杂的Dashboard或者大屏类应用，数据来自多张表，每个页面有数十个查询，虽然每个查询只有数十毫秒，但是总体查询时间会在数秒
- **趋势分析**，给定日期范围的查询，指标按日显示，比如查询最近7天内的用户数的趋势，这类查询数据量大，查询范围广，查询时间往往需要数十秒
- **用户重复查询**，如果产品没有防重刷机制，用户因手误或其他原因重复刷新页面，导致提交大量的重复的SQL

以上四种场景，在应用层的解决方案，把查询结果放到Redis中，周期性的更新缓存或者用户手工刷新缓存，但是这个方案有如下问题：

- **数据不一致**，无法感知数据的更新，导致用户经常看到旧的数据
- **命中率低**，缓存整个查询结果，如果数据实时写入，缓存频繁失效，命中率低且系统负载较重
- **额外成本**，引入外部缓存组件，会带来系统复杂度，增加额外成本

## 解决方案

本分区缓存策略可以解决上面的问题，优先保证数据一致性，在此基础上细化缓存粒度，提升命中率，因此有如下特点：

- 用户无需担心数据一致性，通过版本来控制缓存失效，缓存的数据和从BE中查询的数据是一致的
- 没有额外的组件和成本，缓存结果存储在BE的内存中，用户可以根据需要调整缓存内存大小
- 实现了两种缓存策略，SQLCache和PartitionCache，后者缓存粒度更细
- 用一致性哈希解决BE节点上下线的问题，BE中的缓存算法是改进的LRU

## 使用场景

当前支持 SQL Cache 和 Partition Cache 两种方式，支持 OlapTable内表 和 Hive外表。

SQL Cache: 只有 SQL 语句完全一致才会命中缓存，详情见: sql-cache-manual.md

Partition Cache: 多个 SQL 使用相同的表分区即可命中缓存，所以相比 SQL Cache 有更高的命中率，详情见: partition-cache-manual.md

## 监控

FE的监控项：

```text
query_table            //Query中有表的数量
query_olap_table       //Query中有Olap表的数量
cache_mode_sql         //识别缓存模式为sql的Query数量
cache_hit_sql          //模式为sql的Query命中Cache的数量
query_mode_partition   //识别缓存模式为Partition的Query数量
cache_hit_partition    //通过Partition命中的Query数量
partition_all          //Query中扫描的所有分区
partition_hit          //通过Cache命中的分区数量

Cache命中率     = （cache_hit_sql + cache_hit_partition) / query_olap_table
Partition命中率 = partition_hit / partition_all
```

BE的监控项：

```text
query_cache_memory_total_byte       //Cache内存大小
query_query_cache_sql_total_count   //Cache的SQL的数量
query_cache_partition_total_count   //Cache分区数量

SQL平均数据大小       = cache_memory_total / cache_sql_total
Partition平均数据大小 = cache_memory_total / cache_partition_total
```

其他监控： 可以从Grafana中查看BE节点的CPU和内存指标，Query统计中的Query Percentile等指标，配合Cache参数的调整来达成业务目标。

## 相关参数

1. cache_result_max_row_count

查询结果集放入缓存的最大行数，默认 3000。

```text
vim fe/conf/fe.conf
cache_result_max_row_count=3000
```

2. cache_result_max_data_size

查询结果集放入缓存的最大数据大小，默认 30M，可以根据实际情况调整，但建议不要设置过大，避免过多占用内存，超过这个大小的结果集不会被缓存。

```text
vim fe/conf/fe.conf
cache_result_max_data_size=31457280
```

3. cache_last_version_interval_second

缓存的查询分区最新版本离现在的最小时间间隔，只有大于这个间隔没有被更新的分区的查询结果才会被缓存，默认 30，单位秒。

```text
vim fe/conf/fe.conf
cache_last_version_interval_second=30
```

4. query_cache_max_size_mb 和 query_cache_elasticity_size

query_cache_max_size_mb 缓存的内存上限，query_cache_elasticity_size 缓存可拉伸的内存大小，BE上的缓存总大小超过 query_cache_max_size + cache_elasticity_size 后会开始清理，并把内存控制到 query_cache_max_size 以下。

可以根据BE节点数量，节点内存大小，和缓存命中率来设置这两个参数。计算方法：假如缓存10000个Query，每个Query缓存1000行，每行是128个字节，分布在10台BE上，则每个BE需要约128M内存（10000 * 1000 * 128/10）。

```text
vim be/conf/be.conf
query_cache_max_size_mb=256
query_cache_elasticity_size_mb=128
```

5. cache_max_partition_count

Partition Cache 独有的参数。BE最大分区数量，指每个SQL对应的最大分区数，如果是按日期分区，能缓存2年多的数据，假如想保留更长时间的缓存，请把这个参数设置得更大，同时修改参数 cache_result_max_row_count 和 cache_result_max_data_size。

```text
vim be/conf/be.conf
cache_max_partition_count=1024
```
