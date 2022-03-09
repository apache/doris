---
{
    "title": "yearweek",
    "language": "en"
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
## Description
### Syntax

`INT YEARWEEK(DATE date)`
`INT YEARWEEK(DATE date, INT mode)`

Returns year and week for a date.The value of the mode argument defaults to 0.
When the week of the date belongs to the previous year, the year and week of the previous year are returned; 
when the week of the date belongs to the next year, the year of the next year is returned and the week is 1.

The following table describes how the mode argument works.
|Mode |First day of week |Range   |Week 1 is the first week …    |
|:----|:-----------------|:-------|:-----------------------------|
|0    |Sunday            |1-53    |with a Sunday in this year    |
|1    |Monday            |1-53    |with 4 or more days this year |
|2    |Sunday            |1-53    |with a Sunday in this year    |
|3    |Monday            |1-53    |with 4 or more days this year |
|4    |Sunday            |1-53    |with 4 or more days this year |
|5    |Monday            |1-53    |with a Monday in this year    |
|6    |Sunday            |1-53    |with 4 or more days this year |
|7    |Monday            |1-53    |with a Monday in this year    |

The parameter is Date or Datetime type

`INT YEARWEEK(DATE date, INT week_start, INT day_in_first_week)`
Returns year and week for a date.

The parameter `week_start` means the start day of a week, which value is between 1 and 7，as is shown in following table：
|week_start|meaning|
|:-----|:-----|
|1|Monday is the first day in a week|
|2|Tuesday is the first day in a week|
|3|Wednesday is the first day in a week|
|4|Thursday is the first day in a week|
|5|Friday is the first day in a week|
|6|Saturday is the first day in a week|
|7|Sunday is the first day in a week|

The parameter `day_in_first_week` means which day in January belongs to first week，which value is between 1 and 7,
 as is shown in following table：
|day_in_first_week|meaning|
|:-----|:-----|
|1|The week including January 1st is the first week in the year|
|2|The week including January 2nd is the first week in the year|
|3|The week including January 3rd is the first week in the year|
|4|The week including January 4th is the first week in the year|
|5|The week including January 5th is the first week in the year|
|6|The week including January 6th is the first week in the year|
|7|The week including January 7th is the first week in the year|

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
