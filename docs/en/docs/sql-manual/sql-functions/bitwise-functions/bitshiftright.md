---
{
"title": "BIT_SHIFT_RIGHT",
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

## bit_shift_right
### description
#### syntax

`BIT_SHIFT_RIGHT(BIGINT x, TINYINT c)`

Return result of logical right shift of `BIGINT` type x by c bits.

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
Logical right shift `BIGINT` -1 by 1 bits gets `BIGINT_MAX`
```sql
select bit_shift_right(-1, 1)
--------------

+------------------------+
| bit_shift_right(-1, 1) |
+------------------------+
|    9223372036854775807 |
+------------------------+
```
Return zero if `c` is less than 0
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
