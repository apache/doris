---
{
"title": "BIT_SHIFT_LEFT",
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

## bit_shift_left
### description
#### syntax

`BIT_SHIFT_LEFT(BIGINT x, TINYINT c)`

Do logical left shift to `BIGINT` type x by c bits, and return result as a `BIGINT`.
Return zero if `c` is less than 0.

### example
Normal case
```sql
select 8 as x, number as c, bit_shift_left(8, number) as bit_shift_left from numbers("number"="5")
--------------

+------+------+----------------+
| x    | c    | bit_shift_left |
+------+------+----------------+
|    8 |    0 |              8 |
|    8 |    1 |             16 |
|    8 |    2 |             32 |
|    8 |    3 |             64 |
|    8 |    4 |            128 |
+------+------+----------------+
5 rows in set (0.04 sec)
```
Left shift result of `9223372036854775807` which is `BIGINT_MAX` by 1 bit will get -2.
```sql
WITH tbl AS (
  SELECT 9223372036854775807 AS BIGINT_MAX
)
SELECT BIGINT_MAX, bit_shift_left(BIGINT_MAX, 1)
FROM tbl
--------------

+---------------------+-------------------------------+
| BIGINT_MAX          | bit_shift_left(BIGINT_MAX, 1) |
+---------------------+-------------------------------+
| 9223372036854775807 |                            -2 |
+---------------------+-------------------------------+
1 row in set (0.05 sec)
```
### keywords

    BITSHIFT, BITSHIFTLEFT
