---
{
    "title": "分区缓存",
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

# 分区缓存

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

## SQLCache

SQLCache按SQL的签名、查询的表的分区ID、分区最新版本来存储和获取缓存。三者组合确定一个缓存数据集，任何一个变化了，如SQL有变化，如查询字段或条件不一样，或数据更新后版本变化了，会导致命中不了缓存。

如果多张表Join，使用最近更新的分区ID和最新的版本号，如果其中一张表更新了，会导致分区ID或版本号不一样，也一样命中不了缓存。

SQLCache，更适合T+1更新的场景，凌晨数据更新，首次查询从BE中获取结果放入到缓存中，后续相同查询从缓存中获取。实时更新数据也可以使用，但是可能存在命中率低的问题，可以参考如下PartitionCache。

## PartitionCache

### 设计原理

1. SQL可以并行拆分，Q = Q1 ∪ Q2 ... ∪ Qn，R= R1 ∪ R2 ... ∪ Rn，Q为查询语句，R为结果集
2. 拆分为只读分区和可更新分区，只读分区缓存，更新分区不缓存

如上，查询最近7天的每天用户数，如按日期分区，数据只写当天分区，当天之外的其他分区的数据，都是固定不变的，在相同的查询SQL下，查询某个不更新分区的指标都是固定的。如下，在2020-03-09当天查询前7天的用户数，2020-03-03至2020-03-07的数据来自缓存，2020-03-08第一次查询来自分区，后续的查询来自缓存，2020-03-09因为当天在不停写入，所以来自分区。

因此，查询N天的数据，数据更新最近的D天，每天只是日期范围不一样相似的查询，只需要查询D个分区即可，其他部分都来自缓存，可以有效降低集群负载，减少查询时间。

```sql
MySQL [(none)]> SELECT eventdate,count(userid) FROM testdb.appevent WHERE eventdate>="2020-03-03" AND eventdate<="2020-03-09" GROUP BY eventdate ORDER BY eventdate;
+------------+-----------------+
| eventdate  | count(`userid`) |
+------------+-----------------+
| 2020-03-03 |              15 |
| 2020-03-04 |              20 |
| 2020-03-05 |              25 |
| 2020-03-06 |              30 |
| 2020-03-07 |              35 |
| 2020-03-08 |              40 | //第一次来自分区，后续来自缓存
| 2020-03-09 |              25 | //来自分区
+------------+-----------------+
7 rows in set (0.02 sec)
```

在PartitionCache中，缓存第一级Key是去掉了分区条件后的SQL的128位MD5签名，下面是改写后的待签名的SQL：

```sql
SELECT eventdate,count(userid) FROM testdb.appevent GROUP BY eventdate ORDER BY eventdate;
```

缓存的第二级Key是查询结果集的分区字段的内容，比如上面查询结果的eventdate列的内容，二级Key的附属信息是分区的版本号和版本更新时间。

下面演示上面SQL在2020-03-09当天第一次执行的流程：

1. 从缓存中获取数据

```text
+------------+-----------------+
| 2020-03-03 |              15 |
| 2020-03-04 |              20 |
| 2020-03-05 |              25 |
| 2020-03-06 |              30 |
| 2020-03-07 |              35 |
+------------+-----------------+
```

1. 从BE中获取数据的SQL和数据

```sql
SELECT eventdate,count(userid) FROM testdb.appevent WHERE eventdate>="2020-03-08" AND eventdate<="2020-03-09" GROUP BY eventdate ORDER BY eventdate;

+------------+-----------------+
| 2020-03-08 |              40 |
+------------+-----------------+
| 2020-03-09 |              25 | 
+------------+-----------------+
```

1. 最后发送给终端的数据

```text
+------------+-----------------+
| eventdate  | count(`userid`) |
+------------+-----------------+
| 2020-03-03 |              15 |
| 2020-03-04 |              20 |
| 2020-03-05 |              25 |
| 2020-03-06 |              30 |
| 2020-03-07 |              35 |
| 2020-03-08 |              40 |
| 2020-03-09 |              25 |
+------------+-----------------+
```

1. 发送给缓存的数据

```text
+------------+-----------------+
| 2020-03-08 |              40 |
+------------+-----------------+
```

Partition缓存，适合按日期分区，部分分区实时更新，查询SQL较为固定。

分区字段也可以是其他字段，但是需要保证只有少量分区更新。

### 一些限制

- 只支持OlapTable，其他存储如MySQL的表没有版本信息，无法感知数据是否更新
- 只支持按分区字段分组，不支持按其他字段分组，按其他字段分组，该分组数据都有可能被更新，会导致缓存都失效
- 只支持结果集的前半部分、后半部分以及全部命中缓存，不支持结果集被缓存数据分割成几个部分

## 使用方式

### 开启SQLCache

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

### 开启PartitionCache

确保fe.conf的cache_enable_partition_mode=true(默认是true)

```text
vim fe/conf/fe.conf
cache_enable_partition_mode=true
```

在MySQL命令行中设置变量

```sql
MySQL [(none)]> set [global] enable_partition_cache=true;
```

如果同时开启了两个缓存策略，下面的参数，需要注意一下:

```text
cache_last_version_interval_second=900
```

如果分区的最新版本的时间离现在的间隔，大于cache_last_version_interval_second，则会优先把整个查询结果缓存。如果小于这个间隔，如果符合PartitionCache的条件，则按PartitionCache数据。

### 监控

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

### 优化参数

FE的配置项cache_result_max_row_count，查询结果集放入缓存的最大行数，可以根据实际情况调整，但建议不要设置过大，避免过多占用内存，超过这个大小的结果集不会被缓存。

```text
vim fe/conf/fe.conf
cache_result_max_row_count=3000
```

BE最大分区数量cache_max_partition_count，指每个SQL对应的最大分区数，如果是按日期分区，能缓存2年多的数据，假如想保留更长时间的缓存，请把这个参数设置得更大，同时修改cache_result_max_row_count的参数。

```text
vim be/conf/be.conf
cache_max_partition_count=1024
```

BE中缓存内存设置，有两个参数query_cache_max_size和query_cache_elasticity_size两部分组成（单位MB），内存超过query_cache_max_size + cache_elasticity_size会开始清理，并把内存控制到query_cache_max_size以下。可以根据BE节点数量，节点内存大小，和缓存命中率来设置这两个参数。

```text
query_cache_max_size_mb=256
query_cache_elasticity_size_mb=128
```

计算方法：

假如缓存10K个Query，每个Query缓存1000行，每行是128个字节，分布在10台BE上，则每个BE需要128M内存（10K*1000*128/10）。

## 未尽事项

- T+1的数据，是否也可以用Partition缓存? 目前不支持
- 类似的SQL，之前查询了2个指标，现在查询3个指标，是否可以利用2个指标的缓存？ 目前不支持
- 按日期分区，但是需要按周维度汇总数据，是否可用PartitionCache？ 目前不支持