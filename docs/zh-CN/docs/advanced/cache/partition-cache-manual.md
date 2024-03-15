---
{
    "title": "Partition Cache",
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

# Partition Cache

多个 SQL 使用相同的表分区时可命中缓存。

```
**Partition Cache是个试验性功能，没有得到很好的维护，谨慎使用**
```

## 需求场景 & 解决方案

见 query-cache.md。

## 设计原理

1. SQL可以并行拆分，Q = Q1 ∪ Q2 ... ∪ Qn，R= R1 ∪ R2 ... ∪ Rn，Q为查询语句，R为结果集

2. SQL 只使用DATE、INT、BIGINT类型的分区字段聚合，且只扫描一个分区，因此不支持按天分区，只支持按年、月分区。

3. 将查询结果集中部分日期的结果缓存，然后缩减 SQL 中扫描的日期范围，本质 PartitionCache 并没有减少扫描的分区数量，而且缩减扫描的日期范围，从而减少扫描数据量。

此外一些限制：

- 只支持按分区字段分组，不支持按其他字段分组，按其他字段分组，该分组数据都有可能被更新，会导致缓存都失效

- 只支持结果集的前半部分、后半部分以及全部命中缓存，不支持结果集被缓存数据分割成几个部分，且结果集的日期必须连续，如果某一天在结果集中没有数据，那只有这一天之前的日期会被缓存。

- 如果 predicate 有分区之外的列，那么必须给分区 predicate 加上括号 `where k1 = 1 and (key >= "2023-10-18" and key <= "2021-12-01")`

- 查询的天数必须大于1，小于cache_result_max_row_count，否则无法使用partition cache。

- 分区字段的 predicate 只能是 key >= a and key <= b 或者 key = a or key = b 或者 key in(a,b,c)。

## 使用方式

确保 fe.conf 的 cache_enable_partition_mode=true (默认是true)

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
cache_last_version_interval_second=30
```

如果分区的最新版本的时间离现在的间隔，大于cache_last_version_interval_second，则会优先把整个查询结果缓存。如果小于这个间隔，如果符合PartitionCache的条件，则按PartitionCache数据。

具体参数介绍和未尽事项见 query-cache.md。

## 未尽事项

拆分为只读分区和可更新分区，只读分区缓存，更新分区不缓存

如查询最近7天的每天用户数，如按日期分区，数据只写当天分区，当天之外的其他分区的数据，都是固定不变的，在相同的查询SQL下，查询某个不更新分区的指标都是固定的。如下，在2020-03-09当天查询前7天的用户数，2020-03-03至2020-03-07的数据来自缓存，2020-03-08第一次查询来自分区，后续的查询来自缓存，2020-03-09因为当天在不停写入，所以来自分区。

因此，查询N天的数据，数据更新最近的D天，每天只是日期范围不一样相似的查询，只需要查询D个分区即可，其他部分都来自缓存，可以有效降低集群负载，减少查询时间。

实现原理示例:

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

2. 从BE中获取数据的SQL和数据

```sql
SELECT eventdate,count(userid) FROM testdb.appevent WHERE eventdate>="2020-03-08" AND eventdate<="2020-03-09" GROUP BY eventdate ORDER BY eventdate;

+------------+-----------------+
| 2020-03-08 |              40 |
+------------+-----------------+
| 2020-03-09 |              25 | 
+------------+-----------------+
```

3. 最后发送给终端的数据

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

4. 发送给缓存的数据

```text
+------------+-----------------+
| 2020-03-08 |              40 |
+------------+-----------------+
```

Partition缓存，适合按日期分区，部分分区实时更新，查询SQL较为固定。

分区字段也可以是其他字段，但是需要保证只有少量分区更新。
