---
{
    "title": "最佳实践",
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

# 最佳实践

## 建表

### 数据模型选择

Doris 数据模型上目前分为三类: AGGREGATE KEY, UNIQUE KEY, DUPLICATE KEY。三种模型中数据都是按KEY进行排序。

#### AGGREGATE KEY

AGGREGATE KEY相同时，新旧记录进行聚合，目前支持的聚合函数有SUM, MIN, MAX, REPLACE。

AGGREGATE KEY模型可以提前聚合数据, 适合报表和多维分析业务。

```sql
CREATE TABLE site_visit
(
    siteid      INT,
    city        SMALLINT,
    username    VARCHAR(32),
    pv BIGINT   SUM DEFAULT '0'
)
AGGREGATE KEY(siteid, city, username)
DISTRIBUTED BY HASH(siteid) BUCKETS 10;
```

#### UNIQUE KEY

UNIQUE KEY 相同时，新记录覆盖旧记录。在1.2版本之前，UNIQUE KEY 实现上和 AGGREGATE KEY 的 REPLACE 聚合方法一样，二者本质上相同，自1.2版本我们给UNIQUE KEY引入了merge on write实现，该实现有更好的聚合查询性能。适用于有更新需求的分析业务。

```sql
CREATE TABLE sales_order
(
    orderid     BIGINT,
    status      TINYINT,
    username    VARCHAR(32),
    amount      BIGINT DEFAULT '0'
)
UNIQUE KEY(orderid)
DISTRIBUTED BY HASH(orderid) BUCKETS 10;
```

#### DUPLICATE KEY

只指定排序列，相同的行不会合并。适用于数据无需提前聚合的分析业务。

```sql
CREATE TABLE session_data
(
    visitorid   SMALLINT,
    sessionid   BIGINT,
    visittime   DATETIME,
    city        CHAR(20),
    province    CHAR(20),
    ip          varchar(32),
    brower      CHAR(20),
    url         VARCHAR(1024)
)
DUPLICATE KEY(visitorid, sessionid)
DISTRIBUTED BY HASH(sessionid, visitorid) BUCKETS 10;
```

### 大宽表与 Star Schema

业务方建表时, 为了和前端业务适配, 往往不对维度信息和指标信息加以区分, 而将 Schema 定义成大宽表，这种操作对于数据库其实不是那么友好，我们更建议用户采用星型模型。

- Schema 中字段数比较多, 聚合模型中可能 key 列比较多, 导入过程中需要排序的列会增加。
- 维度信息更新会反应到整张表中，而更新的频率直接影响查询的效率。

使用过程中，建议用户尽量使用 Star Schema 区分维度表和指标表。频繁更新的维度表也可以放在 MySQL 外部表中。而如果只有少量更新, 可以直接放在 Doris 中。在 Doris 中存储维度表时，可对维度表设置更多的副本，提升 Join 的性能。

### 分区和分桶

Doris 支持两级分区存储, 第一层为分区(partition)，目前支持 RANGE 分区和 LIST 分区两种类型, 第二层为 HASH 分桶(bucket)。

#### 分区(partition)

分区用于将数据划分成不同区间, 逻辑上可以理解为将原始表划分成了多个子表。可以方便的按分区对数据进行管理，例如，删除数据时，更加迅速。

##### RANGE分区

业务上，多数用户会选择采用按时间进行partition, 让时间进行partition有以下好处：

* 可区分冷热数据
* 可用上Doris分级存储(SSD + SATA)的功能

##### LIST分区

业务上，用户可以选择城市或者其他枚举值进行partition。

#### HASH分桶(bucket)

根据hash值将数据划分成不同的 bucket。

* 建议采用区分度大的列做分桶, 避免出现数据倾斜
* 为方便数据恢复, 建议单个 bucket 的 size 不要太大, 保持在 10GB 以内, 所以建表或增加 partition 时请合理考虑 bucket 数目, 其中不同 partition 可指定不同的 buckets 数。

### 稀疏索引和 Bloom Filter

Doris对数据进行有序存储, 在数据有序的基础上为其建立稀疏索引,索引粒度为 block(1024行)。

稀疏索引选取 schema 中固定长度的前缀作为索引内容, 目前 Doris 选取 36 个字节的前缀作为索引。

- 建表时建议将查询中常见的过滤字段放在 Schema 的前面, 区分度越大，频次越高的查询字段越往前放。
- 这其中有一个特殊的地方,就是 varchar 类型的字段。varchar 类型字段只能作为稀疏索引的最后一个字段。索引会在 varchar 处截断, 因此 varchar 如果出现在前面，可能索引的长度可能不足 36 个字节。具体可以参阅 [数据模型](./data-model.md)、[ROLLUP 及查询](./hit-the-rollup.md)。
- 除稀疏索引之外, Doris还提供bloomfilter索引, bloomfilter索引对区分度比较大的列过滤效果明显。 如果考虑到varchar不能放在稀疏索引中, 可以建立bloomfilter索引。

### Rollup

Rollup 本质上可以理解为原始表(Base Table)的一个物化索引。建立 Rollup 时可只选取 Base Table 中的部分列作为 Schema。Schema 中的字段顺序也可与 Base Table 不同。

下列情形可以考虑建立 Rollup：

#### Base Table 中数据聚合度不高。

这一般是因 Base Table 有区分度比较大的字段而导致。此时可以考虑选取部分列，建立 Rollup。

如对于 `site_visit` 表：

```text
site_visit(siteid, city, username, pv)
```

siteid 可能导致数据聚合度不高，如果业务方经常根据城市统计pv需求，可以建立一个只有 city, pv 的 Rollup：

```sql
ALTER TABLE site_visit ADD ROLLUP rollup_city(city, pv);
```

#### Base Table 中的前缀索引无法命中

这一般是 Base Table 的建表方式无法覆盖所有的查询模式。此时可以考虑调整列顺序，建立 Rollup。

如对于 session_data 表：

```text
session_data(visitorid, sessionid, visittime, city, province, ip, brower, url)
```

如果除了通过 visitorid 分析访问情况外，还有通过 brower, province 分析的情形，可以单独建立 Rollup。

```sql
ALTER TABLE session_data ADD ROLLUP rollup_brower(brower,province,ip,url) DUPLICATE KEY(brower,province);
```

## Schema Change

用户可以通过 Schema Change 操作来修改已存在表的 Schema。目前 Doris 支持以下几种修改:

- 增加、删除列
- 修改列类型
- 调整列顺序
- 增加、修改 Bloom Filter
- 增加、删除 bitmap index

具体请参照 [Schema 变更](../advanced/alter-table/schema-change)

## 行存
用户可以在Olap表中开启行存模式，但是需要额外的空建来存储行存。目前的行存实现是将行存编码后存在单独的一列中，这样做是用于简化行存的实现。行存模式默认是关闭的，如果您想开启则可以在建表语句的property中指定如下属性
```
"store_row_column" = "true"
```

## 在merge-on-write模型下的点查优化
上诉的行存用于在merge-on-write模型下减少点查时的IO开销。当`enable_unique_key_merge_on_write`在建表开启时，对于主键的点查会走短路径来对SQL执行进行优化，仅需要执行一次RPC即可执行完成查询。下面是点查结合行存在merge-on-write下的一个例子:
```
CREATE TABLE `tbl_point_query` (
  `key` int(11) NULL,
  `v1` decimal(27, 9) NULL,
  `v2` varchar(30) NULL,
  `v3` varchar(30) NULL,
  `v4` date NULL,
  `v5` datetime NULL,
  `v6` float NULL,
  `v7` datev2 NULL
) ENGINE=OLAP
UNIQUE KEY(key)
COMMENT 'OLAP'
DISTRIBUTED BY HASH(key) BUCKETS 1
PROPERTIES (
"replication_allocation" = "tag.location.default: 1",
"enable_unique_key_merge_on_write" = "true",
"light_schema_change" = "true",
"store_row_column" = "true"
);
```
[NOTE]
1. `enable_unique_key_merge_on_write`应该被开启， 存储引擎需要根据主键来快速点查
2. 当条件只包含主键时，如 `select * from tbl_point_query where key = 123`，类似的查询会走短路径来优化查询
3. `light_schema_change` 应该被开启， 因为主键点查的优化依赖了轻量级schema change中的`column unique id`来定位列

### 使用 `PreparedStatement`
为了减少SQL解析和表达式计算的开销， 我们在FE端提供了与mysql协议完全兼容的`PreparedStatement`特性（目前只支持主键点查）。当`PreparedStatement`在FE开启，SQL和其表达式将被提前计算并缓存到session级别的内存缓存中，后续的查询直接使用缓存对象即可。当CPU成为主键点查的瓶颈， 在开启`PreparedStatement`后，将会有4倍+的性能提升。下面是在JDBC中使用`PreparedStatement`的例子
1. 设置JDB url并在server端开启prepared statement
```
url = jdbc:mysql://127.0.0.1:9137/ycsb?useServerPrepStmts=true
``

2. 使用 `PreparedStatement`
```java
// use `?` for placement holders, readStatement should be reused
PreparedStatement readStatement = conn.prepareStatement("select * from tbl_point_query where key = ?");
...
readStatement.setInt(1234);
ResultSet resultSet = readStatement.executeQuery();
...
readStatement.setInt(1235);
resultSet = readStatement.executeQuery();
...
```
