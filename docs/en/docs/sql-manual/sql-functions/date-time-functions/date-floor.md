---
{
    "title": "date_floor",
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

## date_floor
### description
#### Syntax

`DATETIME DATE_FLOOR(DATETIME datetime, INTERVAL period type)`


Converts a date to the nearest rounding down time of a specified time interval period.

The datetime parameter is a valid date expression.

The period parameter specifies how many units each cycle consists of, starting from 0001-01-01T00:00:00

type ：YEAR, MONTH, DAY, HOUR, MINUTE, SECOND.

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
