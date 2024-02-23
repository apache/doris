---
{
    "title": "SEQUENCE",
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

## sequence

<version since="dev">

sequence

</version>

### description
函数array_range的别称

#### Syntax

```sql
ARRAY<Int> sequence(Int end)
ARRAY<Int> sequence(Int start, Int end)
ARRAY<Int> sequence(Int start, Int end, Int step)
ARRAY<Datetime> sequence(Datetime start_datetime, Datetime end_datetime)
ARRAY<Datetime> sequence(Datetime start_datetime, Datetime end_datetime, INTERVAL Int interval_step UNIT)
```
1. 生成int数组：
参数均为正整数 start 默认为 0, step 默认为 1。
最终返回一个数组，从start 到 end - 1, 步长为 step。
2. 生成日期时间数组：
至少取两个参数。
前两个参数都是datetimev2，第三个是正整数。
如果缺少第三部分，则`INTERVAL 1 DAY`将为默认值。
UNIT 支持年/月/周/日/小时/分钟/秒。
返回 start_datetime 和最接近 end_datetime 之间的 datetimev2 数组（按 Interval_step UNIT 计算）。

### notice

`如果第三个参数 step/interval_step 为负数或者零, 函数结果将为NULL`

### example

```
mysql> select sequence(10);
+--------------------------------+
| sequence(10)                   |
+--------------------------------+
| [0, 1, 2, 3, 4, 5, 6, 7, 8, 9] |
+--------------------------------+

mysql> select sequence(10,20);
+------------------------------------------+
| sequence(10, 20)                         |
+------------------------------------------+
| [10, 11, 12, 13, 14, 15, 16, 17, 18, 19] |
+------------------------------------------+

mysql> select sequence(0,20,2);
+-------------------------------------+
| sequence(0, 20, 2)                  |
+-------------------------------------+
| [0, 2, 4, 6, 8, 10, 12, 14, 16, 18] |
+-------------------------------------+

mysql> select sequence(cast('2022-05-15 12:00:00' as datetimev2(0)), cast('2022-05-17 12:00:00' as datetimev2(0))) AS sequence_default;
+------------------------------------------------+
| sequence_default                               |
+------------------------------------------------+
| ["2022-05-15 12:00:00", "2022-05-16 12:00:00"] |
+------------------------------------------------+

mysql> select sequence(cast('2019-05-15 12:00:00' as datetimev2(0)), cast('2022-05-17 12:00:00' as datetimev2(0)), interval 2 year) as sequence_2_year;
+------------------------------------------------+
| sequence_2_year                                |
+------------------------------------------------+
| ["2019-05-15 12:00:00", "2021-05-15 12:00:00"] |
+------------------------------------------------+
```

### keywords

ARRAY, RANGE, SEQUENCE
