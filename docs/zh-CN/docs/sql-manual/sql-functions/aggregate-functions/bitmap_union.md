---
{
    "title": "BITMAP_UNION",
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

## BITMAP_UNION

### description

### example

#### Create table

建表时需要使用聚合模型，数据类型是 bitmap , 聚合函数是 bitmap_union

```
CREATE TABLE `pv_bitmap` (
  `dt` int(11) NULL COMMENT "",
  `page` varchar(10) NULL COMMENT "",
  `user_id` bitmap BITMAP_UNION NULL COMMENT ""
) ENGINE=OLAP
AGGREGATE KEY(`dt`, `page`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`dt`) BUCKETS 2;
```
注：当数据量很大时，最好为高频率的 bitmap_union 查询建立对应的 rollup 表

```
ALTER TABLE pv_bitmap ADD ROLLUP pv (page, user_id);
```

#### Data Load

`TO_BITMAP(expr)` : 将 0 ~ 18446744073709551615 的 unsigned bigint 转为 bitmap

`BITMAP_EMPTY()`: 生成空 bitmap 列，用于 insert 或导入的时填充默认值

`BITMAP_HASH(expr)`或者`BITMAP_HASH64(expr)`: 将任意类型的列通过 Hash 的方式转为 bitmap

##### Stream Load

``` 
cat data | curl --location-trusted -u user:passwd -T - -H "columns: dt,page,user_id, user_id=to_bitmap(user_id)"   http://host:8410/api/test/testDb/_stream_load
```

``` 
cat data | curl --location-trusted -u user:passwd -T - -H "columns: dt,page,user_id, user_id=bitmap_hash(user_id)"   http://host:8410/api/test/testDb/_stream_load
```

``` 
cat data | curl --location-trusted -u user:passwd -T - -H "columns: dt,page,user_id, user_id=bitmap_empty()"   http://host:8410/api/test/testDb/_stream_load
```

##### Insert Into

id2 的列类型是 bitmap
```
insert into bitmap_table1 select id, id2 from bitmap_table2;
```

id2 的列类型是 bitmap
```
INSERT INTO bitmap_table1 (id, id2) VALUES (1001, to_bitmap(1000)), (1001, to_bitmap(2000));
```

id2 的列类型是 bitmap
```
insert into bitmap_table1 select id, bitmap_union(id2) from bitmap_table2 group by id;
```

id2 的列类型是 int
```
insert into bitmap_table1 select id, to_bitmap(id2) from table;
```

id2 的列类型是 String
```
insert into bitmap_table1 select id, bitmap_hash(id_string) from table;
```

#### Data Query
##### Syntax


`BITMAP_UNION(expr)` : 计算输入 Bitmap 的并集，返回新的bitmap

`BITMAP_UNION_COUNT(expr)`: 计算输入 Bitmap 的并集，返回其基数，和 BITMAP_COUNT(BITMAP_UNION(expr)) 等价。目前推荐优先使用 BITMAP_UNION_COUNT ，其性能优于 BITMAP_COUNT(BITMAP_UNION(expr))

`BITMAP_UNION_INT(expr)` : 计算 TINYINT,SMALLINT 和 INT 类型的列中不同值的个数，返回值和
COUNT(DISTINCT expr) 相同

`INTERSECT_COUNT(bitmap_column_to_count, filter_column, filter_values ...)` : 计算满足
filter_column 过滤条件的多个 bitmap 的交集的基数值。
bitmap_column_to_count 是 bitmap 类型的列，filter_column 是变化的维度列，filter_values 是维度取值列表


##### Example

下面的 SQL 以上面的 pv_bitmap table 为例：

计算 user_id 的去重值：

```
select bitmap_union_count(user_id) from pv_bitmap;

select bitmap_count(bitmap_union(user_id)) from pv_bitmap;
```

计算 id 的去重值：

```
select bitmap_union_int(id) from pv_bitmap;
```

计算 user_id 的 留存:

```
select intersect_count(user_id, page, 'meituan') as meituan_uv,
intersect_count(user_id, page, 'waimai') as waimai_uv,
intersect_count(user_id, page, 'meituan', 'waimai') as retention //在 'meituan' 和 'waimai' 两个页面都出现的用户数
from pv_bitmap
where page in ('meituan', 'waimai');
```

### keywords

BITMAP,BITMAP_COUNT,BITMAP_EMPTY,BITMAP_UNION,BITMAP_UNION_INT,TO_BITMAP,BITMAP_UNION_COUNT,INTERSECT_COUNT
