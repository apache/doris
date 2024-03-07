---
{
    "title": "FROM_SECOND",
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

## from_second
### description
#### syntax

`DATETIME FROM_SECOND(BIGINT unix_timestamp)`
`DATETIME FROM_MILLISECOND(BIGINT unix_timestamp)`
`DATETIME FROM_MICROSECOND(BIGINT unix_timestamp)`

Converts a timestamp to its DATETIME represent, with argument as an integer and returned as a DATETIME type. Returns `NULL` if `unix_timestamp < 0` or if the function result is greater than `9999-12-31 23:59:59.999999`.

### example

```
mysql> set time_zone='Asia/Shanghai';

mysql> select from_second(-1);
+---------------------------+
| from_second(-1)           |
+---------------------------+
| NULL                      |
+---------------------------+

mysql> select from_millisecond(12345678);
+----------------------------+
| from_millisecond(12345678) |
+----------------------------+
| 1970-01-01 11:25:45.678    |
+----------------------------+

mysql> select from_microsecond(253402271999999999);
+--------------------------------------+
| from_microsecond(253402271999999999) |
+--------------------------------------+
| 9999-12-31 23:59:59.999999           |
+--------------------------------------+

mysql> select from_microsecond(253402272000000000);
+--------------------------------------+
| from_microsecond(253402272000000000) |
+--------------------------------------+
| NULL                                 |
+--------------------------------------+
```

### keywords

    FROM_SECOND,FROM,SECOND,MILLISECOND,MICROSECOND
