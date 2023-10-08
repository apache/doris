---
{
    "title": "TIMEDIFF",
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

## timediff
### description
#### Syntax

`TIME TIMEDIFF(DATETIME expr1, DATETIME expr2)`


TIMEDIFF返回两个DATETIME之间的差值

TIMEDIFF函数返回表示为时间值的expr1 - expr2的结果，返回值为TIME类型

### example

```
mysql> SELECT TIMEDIFF(now(),utc_timestamp());
+----------------------------------+
| timediff(now(), utc_timestamp()) |
+----------------------------------+
| 08:00:00                         |
+----------------------------------+

mysql> SELECT TIMEDIFF('2019-07-11 16:59:30','2019-07-11 16:59:21');
+--------------------------------------------------------+
| timediff('2019-07-11 16:59:30', '2019-07-11 16:59:21') |
+--------------------------------------------------------+
| 00:00:09                                               |
+--------------------------------------------------------+

mysql> SELECT TIMEDIFF('2019-01-01 00:00:00', NULL);
+---------------------------------------+
| timediff('2019-01-01 00:00:00', NULL) |
+---------------------------------------+
| NULL                                  |
+---------------------------------------+
```

### keywords

    TIMEDIFF
