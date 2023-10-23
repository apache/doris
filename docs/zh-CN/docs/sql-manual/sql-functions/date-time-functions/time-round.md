---
{
    "title": "TIME_ROUND",
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

## time_round
### description
#### Syntax

```sql
DATETIME TIME_ROUND(DATETIME expr)
DATETIME TIME_ROUND(DATETIME expr, INT period)
DATETIME TIME_ROUND(DATETIME expr, DATETIME origin)
DATETIME TIME_ROUND(DATETIME expr, INT period, DATETIME origin)
```

函数名 `TIME_ROUND` 由两部分组成，每部分由以下可选值组成
- `TIME`: `SECOND`, `MINUTE`, `HOUR`, `DAY`, `WEEK`, `MONTH`, `YEAR`
- `ROUND`: `FLOOR`, `CEIL`

返回 `expr` 的上/下界。

- `period` 指定每个周期有多少个 `TIME` 单位组成，默认为 `1`。
- `origin` 指定周期的开始时间，默认为 `1970-01-01T00:00:00`，`WEEK` 的默认开始时间为 `1970-01-04T00:00:00`，即周日。可以比 `expr` 大。
- 请尽量选择常见 `period`，如 3 `MONTH`，90 `MINUTE` 等，如设置了非常用 `period`，请同时指定 `origin`。

### example

```

MySQL> SELECT YEAR_FLOOR('20200202000000');
+------------------------------+
| year_floor('20200202000000') |
+------------------------------+
| 2020-01-01 00:00:00          |
+------------------------------+


MySQL> SELECT MONTH_CEIL(CAST('2020-02-02 13:09:20' AS DATETIME), 3); --quarter
+--------------------------------------------------------+
| month_ceil(CAST('2020-02-02 13:09:20' AS DATETIME), 3) |
+--------------------------------------------------------+
| 2020-04-01 00:00:00                                    |
+--------------------------------------------------------+


MySQL> SELECT WEEK_CEIL('2020-02-02 13:09:20', '2020-01-06'); --monday
+---------------------------------------------------------+
| week_ceil('2020-02-02 13:09:20', '2020-01-06 00:00:00') |
+---------------------------------------------------------+
| 2020-02-03 00:00:00                                     |
+---------------------------------------------------------+


MySQL> SELECT MONTH_CEIL(CAST('2020-02-02 13:09:20' AS DATETIME), 3, CAST('1970-01-09 00:00:00' AS DATETIME)); --next rent day
+-------------------------------------------------------------------------------------------------+
| month_ceil(CAST('2020-02-02 13:09:20' AS DATETIME), 3, CAST('1970-01-09 00:00:00' AS DATETIME)) |
+-------------------------------------------------------------------------------------------------+
| 2020-04-09 00:00:00                                                                             |
+-------------------------------------------------------------------------------------------------+

```
### keywords
    TIME_ROUND
