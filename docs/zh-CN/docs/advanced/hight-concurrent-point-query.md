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

# 基于主键的高并发点查询

<version since="1.2.1">
</version>

## 背景 
Doris 基于列存格式引擎构建，在高并发服务场景中，用户总是希望从系统中获取整行数据。但是，当表宽时，列存格式将大大放大随机读取 IO。Doris 查询引擎和计划对于某些简单的查询（如点查询）来说太重了。需要一个在FE的查询规划中规划短路径来处理这样的查询。FE 是 SQL 查询的访问层服务，使用 Java 编写，分析和解析 SQL 也会导致高并发查询的高 CPU 开销。为了解决上诉问题，我们在Doris中引入了行存、短查询路径、PreparedStatment来解决上诉问题， 下面是开启这些优化的指南。

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

## 使用 `PreparedStatement`
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
