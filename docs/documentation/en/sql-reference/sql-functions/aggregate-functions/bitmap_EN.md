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

`TO_BITMAP(expr)` : Convert TINYINT,SMALLINT,INT type column to Bitmap.

`BITMAP_UNION(expr)` : Calculate the union of Bitmap, return the serialized Bitmap value.

`BITMAP_COUNT(expr)` : Calculate the distinct value number of a Bitmap.

`BITMAP_UNION_INT(expr)` : Calculate the distinct value number of TINYINT,SMALLINT and INT type column. Same as COUNT(DISTINCT expr)

`BITMAP_EMPTY()`: Generate empty bitmap column for insert into or load data.

Notice：

	1. TO_BITMAP function only receives TINYINT,SMALLINT,INT.
	2. BITMAP_UNION only receives following types of parameter:
		- Column with BITMAP_UNION aggregate type in AGGREGATE KEY mode.
		- TO_BITMAP function.

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

    BITMAP,BITMAP_COUNT,BITMAP_UNION,BITMAP_UNION_INT,TO_BITMAP
