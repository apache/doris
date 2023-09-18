---
{
    "title": "YEAR_WEEK_FORMAT",
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

## year_week_format
### description
#### Syntax

`VARCHAR YEAR_WEEK_FORMAT(DATETIME date, 
[INT first_day_of_week, INT first_week_of_year_policy, VARCHAR format])`

计算某一个日期的年份和周号，并按照format的类型转化为字符串。

date 参数是合法的日期。后面三个参数说明如下，如果不指定则使用默认值（first_day_of_week=0，first_week_of_year_policy=0，format='%YWK%W'）
当前支持最大16字节的字符串，如果返回值长度超过16字节，则返回NULL。

1. first_day_of_week：一周的起始周号，其中0代表周一，6代表周日，以此类推
2. first_week_of_year_policy：一年的第一周如何确定，有三种策略
策略0：1月1日所在的周，是一年的第一周
策略1：1月1日之后，第一个周起始日所在的周，是一年的第一周
策略2：第一个超过四天的周，是一年的第一周
3. format：规定日期/时间的输出格式。

format可以使用的格式有：

%W | 周号，两位

%w | 周号，最小一位，最大两位

%Y | 年，4 位          
                           
%y | 年，2 位

%% | 用于表示 %

### example

```
mysql> select year_week_format(20240101);
+----------------------------+
| year_week_format(20240101) |
+----------------------------+
| 2024WK01                   |
+----------------------------+

mysql> select year_week_format(20240101,0,0,'%YWK%W');
+--------------------------------------------+
| year_week_format(20240101, 0, 0, '%YWK%W') |
+--------------------------------------------+
| 2024WK01                                   |
+--------------------------------------------+

mysql> select year_week_format(20240101,1,1,'%YWK%W');
+--------------------------------------------+
| year_week_format(20240101, 1, 1, '%YWK%W') |
+--------------------------------------------+
| 2023WK52                                   |
+--------------------------------------------+

mysql> select year_week_format(20240101,3,2,'%YWK%W');
+--------------------------------------------+
| year_week_format(20240101, 3, 2, '%YWK%W') |
+--------------------------------------------+
| 2023WK53                                   |
+--------------------------------------------+

```

### keywords

    YEAR_WEEK_FORMAT,DATE,FORMAT
