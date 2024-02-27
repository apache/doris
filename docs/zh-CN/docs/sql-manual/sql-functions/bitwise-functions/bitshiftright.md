---
{
"title": "BIT_SHIFT_RIGHT",
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
## bit_shift_right
### description
#### syntax

`BIT_SHIFT_RIGHT(BIGINT x, TINYINT c)`

返回对 BIGINT 类型 x 进行逻辑右移 c 位的结果。

### example
Normal case
```sql
select 1024 as x, number as c, bit_shift_right(1024, number) as bit_shift_right from numbers("number"="5")
--------------

+------+------+-----------------+
| x    | c    | bit_shift_right |
+------+------+-----------------+
| 1024 |    0 |            1024 |
| 1024 |    1 |             512 |
| 1024 |    2 |             256 |
| 1024 |    3 |             128 |
| 1024 |    4 |              64 |
+------+------+-----------------+
5 rows in set (0.03 sec)
```
BIGINT -1 逻辑右移一位得到的结果是 BIGINT_MAX

```sql
select bit_shift_right(-1, 1)
--------------

+------------------------+
| bit_shift_right(-1, 1) |
+------------------------+
|    9223372036854775807 |
+------------------------+
```
如果 c 小于 0 得到的结果始终为 0
```sql
select bit_shift_right(100, -1)
--------------

+--------------------------+
| bit_shift_right(100, -1) |
+--------------------------+
|                        0 |
+--------------------------+
1 row in set (0.04 sec)
```

### keywords

    BITSHIFT, BITSHIFTRIGHT
