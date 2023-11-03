---
{
    "title": "date_floor",
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

## date_floor
### description
#### Syntax

`DATETIME DATE_FLOOR(DATETIME datetime, INTERVAL period type)`


将日期转化为指定的时间间隔周期的最近下取整时刻。

datetime 参数是合法的日期表达式。

period 参数是指定每个周期有多少个单位组成，开始的时间起点为0001-01-01T00:00:00.

type 参数可以是下列值：YEAR, MONTH, DAY, HOUR, MINUTE, SECOND.

### example

```
mysql>select date_floor("0001-01-01 00:00:16",interval 5 second);
+---------------------------------------------------------------+
| second_floor('0001-01-01 00:00:16', 5, '0001-01-01 00:00:00') |
+---------------------------------------------------------------+
| 0001-01-01 00:00:15                                           |
+---------------------------------------------------------------+
1 row in set (0.00 sec)

mysql>select date_floor("0001-01-01 00:00:18",interval 5 second);
+---------------------------------------------------------------+
| second_floor('0001-01-01 00:00:18', 5, '0001-01-01 00:00:00') |
+---------------------------------------------------------------+
| 0001-01-01 00:00:15                                           |
+---------------------------------------------------------------+
1 row in set (0.01 sec)

mysql>select date_floor("2023-07-13 22:28:18",interval 5 minute);
+---------------------------------------------------------------+
| minute_floor('2023-07-13 22:28:18', 5, '0001-01-01 00:00:00') |
+---------------------------------------------------------------+
| 2023-07-13 22:25:00                                           |
+---------------------------------------------------------------+
1 row in set (0.00 sec)

mysql>select date_floor("2023-07-13 22:28:18",interval 5 hour);
+-------------------------------------------------------------+
| hour_floor('2023-07-13 22:28:18', 5, '0001-01-01 00:00:00') |
+-------------------------------------------------------------+
| 2023-07-13 18:00:00                                         |
+-------------------------------------------------------------+
1 row in set (0.01 sec)

mysql>select date_floor("2023-07-13 22:28:18",interval 5 day);
+------------------------------------------------------------+
| day_floor('2023-07-13 22:28:18', 5, '0001-01-01 00:00:00') |
+------------------------------------------------------------+
| 2023-07-10 00:00:00                                        |
+------------------------------------------------------------+
1 row in set (0.00 sec)

mysql>select date_floor("2023-07-13 22:28:18",interval 5 month);
+--------------------------------------------------------------+
| month_floor('2023-07-13 22:28:18', 5, '0001-01-01 00:00:00') |
+--------------------------------------------------------------+
| 2023-07-01 00:00:00                                          |
+--------------------------------------------------------------+
1 row in set (0.01 sec)

mysql>select date_floor("2023-07-13 22:28:18",interval 5 year);
+-------------------------------------------------------------+
| year_floor('2023-07-13 22:28:18', 5, '0001-01-01 00:00:00') |
+-------------------------------------------------------------+
| 2021-01-01 00:00:00                                         |
+-------------------------------------------------------------+

```

### keywords

    DATE_FLOOR,DATE,FLOOR
