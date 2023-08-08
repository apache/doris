---
{
    "title": "WEEK",
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

## week
### description
#### Syntax
`INT WEEK(DATE date[, INT mode])`

返回指定日期的星期数。mode的值默认为0。
参数mode的作用参见下面的表格：

|Mode |星期的第一天 |星期数的范围 |第一个星期的定义                            |
|:---|:-------------|:-----------|:--------------------------------------------|
|0   |星期日        |0-53        |这一年中的第一个星期日所在的星期             |
|1   |星期一        |0-53        |这一年的日期所占的天数大于等于4天的第一个星期|
|2   |星期日        |1-53        |这一年中的第一个星期日所在的星期             |
|3   |星期一        |1-53        |这一年的日期所占的天数大于等于4天的第一个星期|
|4   |星期日        |0-53        |这一年的日期所占的天数大于等于4天的第一个星期|
|5   |星期一        |0-53        |这一年中的第一个星期一所在的星期             |
|6   |星期日        |1-53        |这一年的日期所占的天数大于等于4天的第一个星期|
|7   |星期一        |1-53        |这一年中的第一个星期一所在的星期             |

参数为Date或者Datetime类型

### example
```
mysql> select week('2020-1-1');
+------------------+
| week('2020-1-1') |
+------------------+
|                0 |
+------------------+
```
```
mysql> select week('2020-7-1',1);
+---------------------+
| week('2020-7-1', 1) |
+---------------------+
|                  27 |
+---------------------+
```

### keywords
    WEEK
