---
{
    "title": "SECOND_TIMESTAMP",
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

## second_timestamp
### description
#### Syntax

`BIGINT SECOND_TIMESTAMP(DATETIME date)`
`BIGINT MILLISECOND_TIMESTAMP(DATETIME date)`
`BIGINT MICROSECOND_TIMESTAMP(DATETIME date)`

将DATETIME类型转换成对应的时间戳

传入的是DATETIME类型，返回的是整型


### example

```
mysql> select from_millisecond(89417891234789),millisecond_timestamp(from_millisecond(89417891234789));
+----------------------------------+---------------------------------------------------------+
| from_millisecond(89417891234789) | millisecond_timestamp(from_millisecond(89417891234789)) |
+----------------------------------+---------------------------------------------------------+
| 4803-07-17 15:07:14.789          |                                          89417891234789 |
+----------------------------------+---------------------------------------------------------+

mysql> select from_second(89417891234),second_timestamp(from_second(89417891234));
+--------------------------+--------------------------------------------+
| from_second(89417891234) | second_timestamp(from_second(89417891234)) |
+--------------------------+--------------------------------------------+
| 4803-07-17 15:07:14      |                                89417891234 |
+--------------------------+--------------------------------------------+

mysql> select from_microsecond(89417891234),microsecond_timestamp(from_microsecond(89417891234));
+-------------------------------+------------------------------------------------------+
| from_microsecond(89417891234) | microsecond_timestamp(from_microsecond(89417891234)) |
+-------------------------------+------------------------------------------------------+
| 1970-01-02 08:50:17.891234    |                                          89417891234 |
+-------------------------------+------------------------------------------------------+
```

### keywords

    SECOND_TIMESTAMP
