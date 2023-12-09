---
{
    "title": "RANDOM",
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

## random

### description
#### Syntax

`DOUBLE random()`
返回0-1之间的随机数。

`DOUBLE random(DOUBLE seed)`
返回0-1之间的随机数，以`seed`作为种子。

`BIGINT random(BIGINT a, BIGINT b)`
返回a-b之间的随机数，a必须小于b。

别名：`rand`

### example

```sql
mysql> select random();
+---------------------+
| random()            |
+---------------------+
| 0.35446706030596947 |
+---------------------+

mysql> select rand(1.2);
+---------------------+
| rand(1)             |
+---------------------+
| 0.13387664401253274 |
+---------------------+
1 row in set (0.13 sec)

mysql> select rand(1.2);
+---------------------+
| rand(1)             |
+---------------------+
| 0.13387664401253274 |
+---------------------+
1 row in set (0.11 sec)

mysql> select rand(-20, -10);
+------------------+
| random(-20, -10) |
+------------------+
|              -13 |
+------------------+
1 row in set (0.10 sec)
```

### keywords
	RANDOM, RAND
