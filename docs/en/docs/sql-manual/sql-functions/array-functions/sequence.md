---
{
    "title": "SEQUENCE",
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

## sequence

<version since="dev">

sequence

</version>

### description
alias of array_range function

#### Syntax

```sql
ARRAY<Int> sequence(Int end)
ARRAY<Int> sequence(Int start, Int end)
ARRAY<Int> sequence(Int start, Int end, Int step)
ARRAY<Datetime> sequence(Datetime start_datetime, Datetime end_datetime)
ARRAY<Datetime> sequence(Datetime start_datetime, Datetime end_datetime, INTERVAL Int interval_step UNIT)
```
1. To generate array of int:
The parameters are all positive integers. 
start default value is 0, and step default value is 1.
Return the array which numbers from start to end - 1 by step.

2. To generate array of datetime:
At least taking two parameters. 
The first two parameters are all datetimev2, the third is positive integer.
If the third part is missing, `INTERVAL 1 DAY` will be default value.
UNIT supports YEAR/MONTH/WEEK/DAY/HOUR/MINUTE/SECOND.
Return the array of datetimev2 between start_datetime and closest to end_datetime by interval_step UNIT.

### notice

`if the 3rd parameter step/interval_step is negative or zero, the function will return NULL`

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
