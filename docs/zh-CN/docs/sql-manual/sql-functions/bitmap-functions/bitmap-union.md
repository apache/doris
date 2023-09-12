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

## bitmap_union function

### description

聚合函数，用于计算分组后的 bitmap 并集。常见使用场景如：计算PV，UV。

#### Syntax

`BITMAP BITMAP_UNION(BITMAP value)`

输入一组 bitmap 值，求这一组 bitmap 值的并集，并返回。

### example

```
mysql> select page_id, bitmap_union(user_id) from table group by page_id;
```

和 bitmap_count 函数组合使用可以求得网页的 UV 数据

```
mysql> select page_id, bitmap_count(bitmap_union(user_id)) from table group by page_id;
```

当 user_id 字段为 int 时，上面查询语义等同于

```
mysql> select page_id, count(distinct user_id) from table group by page_id;
```

### keywords

    BITMAP_UNION, BITMAP
