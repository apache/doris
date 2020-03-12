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

# Colocate Join
## Description
Colocate/Local Join 在多节点 jion 时没有数据在节点间的传输耗时，每个节点都在本地进行join.
本地join的前提是将同一个join key 的数据按照相同的规则导入到固定的节点上

1 如何使用:

只需要在建表时加上 `colocate_with` 属性就可以了。 `colocate_with` 可以被设置在相同colocate 集合中的任何一个表上。
你需要保证 colocate_with 中表需要被首先创建
假设你需要创建Colocate Join 表 t1 和 t2,你可以参考如下语句：
```
CREATE TABLE `t1` (
`id` int(11) COMMENT "",
'value ` varchar (8) COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`id`)
DISTRIBUTED BY HASH(`id`) BUCKETS 10
PROPERTIES (
"colocate_with" = "t1"
);

CREATE TABLE `t2` (
`id` int(11) COMMENT "",
'value ` varchar (8) COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`id`)
DISTRIBUTED BY HASH(`id`) BUCKETS 10
PROPERTIES (
"colocate_with" = "t1"
);
```

2 Colocate Join 的限制:

1. Colcoate Table 必须是 OLAP 表
2. 相同`colocate_with` 属性的表的 BUCKET 数必须相同
3. 相同`colocate_with` 属性的表的副本数必须相同
4.  相同`colocate_with` 属性的表和DISTRIBUTED 列的数据类型必须相同

3 Colocate Join 适用场景:

Colocate Join 适用于 适用相同分桶列， 并且会在使用这些列进行高频join 的场景。

4 FAQ:

Q: 支持多张表进行Colocate Join 吗?

A: 支持

Q: Colocate 表和普通表 Join 吗?

A: 支持

Q: Colocate 支持和非分桶key join 吗?

A: 支持: 不符合Colocate join的时候会使用 Shuffle Join 或者 Broadcast Join

Q: 如何判断 Join 操作使用了 Colocate Join?

A: 在Hash Join 的explain 结果中，如果子节点是直接是OlapScanNode而没有 exchange node， 那么就使用了Colocate Join。

Q: 如何修改 colocate_with 属性?

A: ALTER TABLE example_db.my_table set ("colocate_with"="target_table");

Q: 如何禁用 colcoate join?

A: set disable_colocate_join = true; 就可以禁用Colocate Join，查询时就会使用Shuffle Join 和Broadcast Join

## keyword

COLOCATE, JOIN, CREATE TABLE
