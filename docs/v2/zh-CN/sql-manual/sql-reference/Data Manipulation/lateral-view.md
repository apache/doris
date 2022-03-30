---
{
    "title": "Lateral View",
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

# Lateral View

## description

Lateral view 语法可以搭配 Table Function，完成将一行数据扩展成多行（列转行）的需求。

语法：

```
...
FROM table_name
lateral_view_ref[ lateral_view_ref ...]

lateral_view_ref:

LATERAL VIEW table_function(...) view_alias as col_name
```
    
Lateral view 子句必须跟随在表名或子查询之后。可以包含多个 Lateral view 子句。`view_alias` 是对应 Lateral View 的名称。`col_name` 是表函数 `table_function` 产出的列名。

目前支持的表函数：

1. `explode_split`
2. `explode_bitmap`
3. `explode_json_array`

具体函数说明可参阅对应语法帮助文档。

table 中的数据会和各个 Lateral View 产生的结果集做笛卡尔积后返回上层。

## example

这里只给出 Lateral View 的语法示例，具体含义和产出的结果说明，可参阅对应表函数帮助文档。

1.

```
select k1, e1 from tbl1
lateral view explode_split(v1, ',') tmp1 as e1 where e1 = "abc";
```

2.

```
select k1, e1, e2 from tbl2
lateral view explode_split(v1, ',') tmp1 as e1
lateral view explode_bitmap(bitmap1) tmp2 as e2
where e2 > 3;
```

3.

```
select k1, e1, e2 from tbl3
lateral view explode_json_array_int("[1,2,3]") tmp1 as e1
lateral view explode_bitmap(bitmap_from_string("4,5,6")) tmp2 as e2;
```

4.

```
select k1, e1 from (select k1, bitmap_union(members) as x from tbl1 where k1=10000 group by k1)tmp1
lateral view explode_bitmap(x) tmp2 as e1;
```

## keyword

    LATERAL, VIEW
