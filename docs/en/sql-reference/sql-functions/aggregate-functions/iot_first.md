---
{
    "title": "IOT_FIRST",
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

# IOT_FIRST
## description
### Syntax

`iot_first(bigint, double)`

After grouping, the first parameter is sorted from smallest to largest (ignoring the null value), and the value of the corresponding second parameter is returned. Similar to the first function of influxdb.

## example

```
mysql> select * from iot order by tag;
+------+------------+------+
| tag  | ts         | val  |
+------+------------+------+
| tag1 | 1641199000 |  1.1 |
| tag1 | 1641199001 |  2.2 |
| tag1 | 1641199002 |  3.3 |
| tag2 | 1641199001 |   11 |
| tag2 | 1641199000 |   10 |
| tag3 |       NULL | NULL |
| tag3 | 1641199000 |  100 |
| tag4 |       NULL | NULL |
+------+------------+------+
8 rows in set (0.01 sec)

mysql> select tag, iot_first(ts, val) from iot group by tag order by tag;
+------+------------------------+
| tag  | iot_first(`ts`, `val`) |
+------+------------------------+
| tag1 |                    1.1 |
| tag2 |                     10 |
| tag3 |                    100 |
| tag4 |                   NULL |
+------+------------------------+
4 rows in set (0.01 sec)
```

## keyword

    IOT_FIRST
