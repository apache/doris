---
{
    "title": "索引概述",
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
# 索引概述

索引用于帮助快速过滤或查找数据。

目前 Doris 主要支持两类索引：
1. 内建的智能索引，包括前缀索引和 ZoneMap 索引。
2. 用户手动创建的二级索引，包括 [倒排索引](./inverted-index.md)、 [bloomfilter索引](./bloomfilter.md)、 [ngram bloomfilter索引](./ngram-bloomfilter-index.md) 和[bitmap索引](./bitmap-index.md)。

其中 ZoneMap 索引是在列存格式上，对每一列自动维护的索引信息，包括 Min/Max，Null 值个数等等。这种索引对用户透明。

## 前缀索引

不同于传统的数据库设计，Doris 不支持在任意列上创建索引。Doris 这类 MPP 架构的 OLAP 数据库，通常都是通过提高并发，来处理大量数据的。

本质上，Doris 的数据存储在类似 SSTable（Sorted String Table）的数据结构中。该结构是一种有序的数据结构，可以按照指定的列进行排序存储。在这种数据结构上，以排序列作为条件进行查找，会非常的高效。

在 Aggregate、Unique 和 Duplicate 三种数据模型中。底层的数据存储，是按照各自建表语句中，AGGREGATE KEY、UNIQUE KEY 和 DUPLICATE KEY 中指定的列进行排序存储的。

而前缀索引，即在排序的基础上，实现的一种根据给定前缀列，快速查询数据的索引方式。

## 示例

我们将一行数据的前 **36 个字节** 作为这行数据的前缀索引。当遇到 VARCHAR 类型时，前缀索引会直接截断。我们举例说明：

1. 以下表结构的前缀索引为 user_id(8 Bytes) + age(4 Bytes) + message(prefix 20 Bytes)。

   | ColumnName     | Type         |
   | -------------- | ------------ |
   | user_id        | BIGINT       |
   | age            | INT          |
   | message        | VARCHAR(100) |
   | max_dwell_time | DATETIME     |
   | min_dwell_time | DATETIME     |

2. 以下表结构的前缀索引为 user_name(20 Bytes)。即使没有达到 36 个字节，因为遇到 VARCHAR，所以直接截断，不再往后继续。

   | ColumnName     | Type         |
   | -------------- | ------------ |
   | user_name      | VARCHAR(20)  |
   | age            | INT          |
   | message        | VARCHAR(100) |
   | max_dwell_time | DATETIME     |
   | min_dwell_time | DATETIME     |

当我们的查询条件，是**前缀索引的前缀**时，可以极大的加快查询速度。比如在第一个例子中，我们执行如下查询：

```sql
SELECT * FROM table WHERE user_id=1829239 and age=20；
```

该查询的效率会**远高于**如下查询：

```sql
SELECT * FROM table WHERE age=20；
```

所以在建表时，**正确的选择列顺序，能够极大地提高查询效率**。

## 通过 Rollup 来调整前缀索引

因为建表时已经指定了列顺序，所以一个表只有一种前缀索引。这对于使用其他不能命中前缀索引的列作为条件进行的查询来说，效率上可能无法满足需求。因此，我们可以通过创建 ROLLUP 来人为的调整列顺序。详情可参考 [ROLLUP](../hit-the-rollup.md)。