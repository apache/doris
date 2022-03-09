---
{
    "title": "yearweek",
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

# yearweek
## description
### Syntax

`INT YEARWEEK(DATE date)`
`INT YEARWEEK(DATE date, INT mode)`

返回指定日期的年份和星期数。mode的值默认为0。
当日期所在的星期属于上一年时，返回的是上一年的年份和星期数；
当日期所在的星期属于下一年时，返回的是下一年的年份，星期数为1。
参数mode的作用参见下面的表格：
|Mode |星期的第一天 |星期数的范围 |第一个星期的定义                             |
|:----|:------------|:------------|:--------------------------------------------|
|0    |星期日       |1-53         |这一年中的第一个星期日所在的星期             |
|1    |星期一       |1-53         |这一年的日期所占的天数大于等于4天的第一个星期|
|2    |星期日       |1-53         |这一年中的第一个星期日所在的星期             |
|3    |星期一       |1-53         |这一年的日期所占的天数大于等于4天的第一个星期|
|4    |星期日       |1-53         |这一年的日期所占的天数大于等于4天的第一个星期|
|5    |星期一       |1-53         |这一年中的第一个星期一所在的星期             |
|6    |星期日       |1-53         |这一年的日期所占的天数大于等于4天的第一个星期|
|7    |星期一       |1-53         |这一年中的第一个星期一所在的星期             |

参数为Date或者Datetime类型

`INT YEARWEEK(DATE date, INT week_start, INT day_in_first_week)`
返回指定日期的年份和星期数。

参数`week_start`表示新的星期的起始，取值范围为(1, 7)，参见下面的表格：
|week_start|含义|
|:-----|:-----|
|1|周一是星期的第一天|
|2|周二是星期的第一天|
|3|周三是星期的第一天|
|4|周四是星期的第一天|
|5|周五是星期的第一天|
|6|周六是星期的第一天|
|7|周日是星期的第一天|

参数`day_in_first_week`表示1月份的哪一天所在的星期作为一年的第一周，取值范围为(1, 7)，参见下面的表格：
|day_in_first_week|含义|
|:-----|:-----|
|1|1月1日所在的星期是一年的第一周|
|2|1月2日所在的星期是一年的第一周|
|3|1月3日所在的星期是一年的第一周|
|4|1月4日所在的星期是一年的第一周|
|5|1月5日所在的星期是一年的第一周|
|6|1月6日所在的星期是一年的第一周|
|7|1月7日所在的星期是一年的第一周|

## example

```
mysql> select yearweek('2021-1-1');
+----------------------+
| yearweek('2021-1-1') |
+----------------------+
|               202052 |
+----------------------+
```
```
mysql> select yearweek('2020-7-1');
+----------------------+
| yearweek('2020-7-1') |
+----------------------+
|               202026 |
+----------------------+
```
```
mysql> select yearweek('2024-12-30',1);
+------------------------------------+
| yearweek('2024-12-30 00:00:00', 1) |
+------------------------------------+
|                             202501 |
+------------------------------------+
```

```
mysql> select yearweek('2022-1-1', 1, 1);
+---------------------------------------+
| yearweek('2022-01-01 00:00:00', 1, 1) |
+---------------------------------------+
|                                202201 |
+---------------------------------------+
```

```
mysql> select yearweek('2022-1-1', 1, 3);
+---------------------------------------+
| yearweek('2022-01-01 00:00:00', 1, 3) |
+---------------------------------------+
|                                202153 |
+---------------------------------------+
```

```
mysql> select yearweek('2021-12-29', 1, 1);
+---------------------------------------+
| yearweek('2021-12-29 00:00:00', 1, 1) |
+---------------------------------------+
|                                202201 |
+---------------------------------------+
```
## keyword

    YEARWEEK
