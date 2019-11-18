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

#BITMAP

## description
### Syntax

`TO_BITMAP(expr)` : 将TINYINT,SMALLINT和INT类型的列转为Bitmap

`BITMAP_UNION(expr)` : 计算两个Bitmap的并集，返回值是序列化后的Bitmap值

`BITMAP_COUNT(expr)` : 计算Bitmap中不同值的个数

`BITMAP_UNION_INT(expr)` : 计算TINYINT,SMALLINT和INT类型的列中不同值的个数，返回值和
COUNT(DISTINCT expr)相同

`BITMAP_EMPTY()`: 生成空Bitmap列，用于insert或导入的时填充默认值


注意：

	1. TO_BITMAP 函数输入的类型必须是TINYINT,SMALLINT,INT
	2. BITMAP_UNION函数的参数目前仅支持： 
		- 聚合模型中聚合类型为BITMAP_UNION的列
		- TO_BITMAP 函数

## example

```
CREATE TABLE `bitmap_udaf` (
  `id` int(11) NULL COMMENT "",
  `id2` int(11)
) ENGINE=OLAP
DUPLICATE KEY(`id`)
DISTRIBUTED BY HASH(`id`) BUCKETS 10;


mysql> select bitmap_count(bitmap_union(to_bitmap(id2))) from bitmap_udaf;
+----------------------------------------------+
| bitmap_count(bitmap_union(to_bitmap(`id2`))) |
+----------------------------------------------+
|                                            6 |
+----------------------------------------------+

mysql> select bitmap_union_int (id2) from bitmap_udaf;
+-------------------------+
| bitmap_union_int(`id2`) |
+-------------------------+
|                       6 |
+-------------------------+



CREATE TABLE `bitmap_test` (
  `id` int(11) NULL COMMENT "",
  `id2` bitmap bitmap_union NULL
) ENGINE=OLAP
AGGREGATE KEY(`id`)
DISTRIBUTED BY HASH(`id`) BUCKETS 10;


mysql> select bitmap_count(bitmap_union(id2)) from bitmap_test;
+-----------------------------------+
| bitmap_count(bitmap_union(`id2`)) |
+-----------------------------------+
|                                 8 |
+-----------------------------------+

```

## keyword

BITMAP,BITMAP_COUNT,BITMAP_EMPTY,BITMAP_UNION,BITMAP_UNION_INT,TO_BITMAP
