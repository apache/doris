---
{
    "title": "高并发点查",
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

# 高并发点查

<version since="2.0.0"></version>

## 背景 

Doris 基于列存格式引擎构建，在高并发服务场景中，用户总是希望从系统中获取整行数据。但是，当表宽时，列存格式将大大放大随机读取 IO。Doris 查询引擎和计划对于某些简单的查询（如点查询）来说太重了。需要一个在 FE 的查询规划中规划短路径来处理这样的查询。FE 是 SQL 查询的访问层服务，使用 Java 编写，分析和解析 SQL 也会导致高并发查询的高 CPU 开销。为了解决上述问题，我们在 Doris 中引入了行存、短查询路径、PreparedStatement 来解决上诉问题，下面是开启这些优化的指南。

## 行存

用户可以在 Olap 表中开启行存模式，但是需要额外的空间来存储行存。目前的行存实现是将行存编码后存在单独的一列中，这样做是用于简化行存的实现。行存模式仅支持在建表的时候开启，需要在建表语句的 property 中指定如下属性：

```
"store_row_column" = "true"
```

## 在 Unique 模型下的点查优化

上述的行存用于在 Unique 模型下开启 Merge-On-Write 策略是减少点查时的 IO 开销。当`enable_unique_key_merge_on_write`与`store_row_column`在创建 Unique 表开启时，对于主键的点查会走短路径来对 SQL 执行进行优化，仅需要执行一次 RPC 即可执行完成查询。下面是点查结合行存在 在 Unique 模型下开启 Merge-On-Write 策略的一个例子:

```sql
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
UNIQUE KEY(`key`)
COMMENT 'OLAP'
DISTRIBUTED BY HASH(`key`) BUCKETS 1
PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "enable_unique_key_merge_on_write" = "true",
    "light_schema_change" = "true",
    "store_row_column" = "true"
);
```

**注意：**
1. `enable_unique_key_merge_on_write`应该被开启， 存储引擎需要根据主键来快速点查
2. 当条件只包含主键时，如`select * from tbl_point_query where key = 123`，类似的查询会走短路径来优化查询
3. `light_schema_change`应该被开启， 因为主键点查的优化依赖了轻量级 Schema Change 中的`column unique id`来定位列
4. 只支持单表key列等值查询不支持join、嵌套子查询， **where条件里需要有且仅有key列的等值**， 可以认为是一种key value查询

## 使用 `PreparedStatement`

为了减少 SQL 解析和表达式计算的开销， 我们在 FE 端提供了与 MySQL 协议完全兼容的`PreparedStatement`特性（目前只支持主键点查）。当`PreparedStatement`在 FE 开启，SQL 和其表达式将被提前计算并缓存到 Session 级别的内存缓存中，后续的查询直接使用缓存对象即可。当 CPU 成为主键点查的瓶颈， 在开启 `PreparedStatement` 后，将会有 4 倍+的性能提升。下面是在 JDBC 中使用 `PreparedStatement` 的例子

1. 设置 JDBC url 并在 Server 端开启 prepared statement

```
url = jdbc:mysql://127.0.0.1:9030/ycsb?useServerPrepStmts=true
```

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

## 开启行缓存

Doris 中有针对 Page 级别的 Cache，每个 Page 中存的是某一列的数据, 所以 Page cache 是针对列的缓存，对于前面提到的行存，一行里包括了多列数据，缓存可能被大查询给刷掉，为了增加行缓存命中率，单独引入了行存缓存，行缓存复用了 Doris 中的 LRU Cache 机制来保障内存的使用，通过指定下面的的BE配置来开启

- `disable_storage_row_cache`是否开启行缓存， 默认不开启
- `row_cache_mem_limit`指定 Row cache 占用内存的百分比， 默认 20% 内存
