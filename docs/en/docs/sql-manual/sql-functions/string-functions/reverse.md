---
{
    "title": "REVERSE",
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

## reverse
### description
#### Syntax

```sql
VARCHAR reverse(VARCHAR str)
ARRAY<T> reverse(ARRAY<T> arr)
```

The REVERSE() function reverses a string or array and returns the result.

### notice

`For the array type, only supported in vectorized engine`

### example

```
mysql> SELECT REVERSE('hello');
+------------------+
| REVERSE('hello') |
+------------------+
| olleh            |
+------------------+
1 row in set (0.00 sec)

mysql> SELECT REVERSE('你好');
+------------------+
| REVERSE('你好')   |
+------------------+
| 好你              |
+------------------+
1 row in set (0.00 sec)

mysql> set enable_vectorized_engine=true;

mysql> select k1, k2, reverse(k2) from array_test order by k1;
+------+-----------------------------+-----------------------------+
| k1   | k2                          | reverse(`k2`)               |
+------+-----------------------------+-----------------------------+
|  1   | [1, 2, 3, 4, 5]             | [5, 4, 3, 2, 1]             |
|  2   | [6, 7, 8]                   | [8, 7, 6]                   |
|  3   | []                          | []                          |
|  4   | NULL                        | NULL                        |
|  5   | [1, 2, 3, 4, 5, 4, 3, 2, 1] | [1, 2, 3, 4, 5, 4, 3, 2, 1] |
|  6   | [1, 2, 3, NULL]             | [NULL, 3, 2, 1]             |
|  7   | [4, 5, 6, NULL, NULL]       | [NULL, NULL, 6, 5, 4]       |
+------+-----------------------------+-----------------------------+

mysql> select k1, k2, reverse(k2) from array_test01 order by k1;
+------+-----------------------------------+-----------------------------------+
| k1   | k2                                | reverse(`k2`)                     |
+------+-----------------------------------+-----------------------------------+
|  1   | ['a', 'b', 'c', 'd']              | ['d', 'c', 'b', 'a']              |
|  2   | ['e', 'f', 'g', 'h']              | ['h', 'g', 'f', 'e']              |
|  3   | [NULL, 'a', NULL, 'b', NULL, 'c'] | ['c', NULL, 'b', NULL, 'a', NULL] |
|  4   | ['d', 'e', NULL, ' ']             | [' ', NULL, 'e', 'd']             |
|  5   | [' ', NULL, 'f', 'g']             | ['g', 'f', NULL, ' ']             |
+------+-----------------------------------+-----------------------------------+
```
### keywords
    REVERSE, ARRAY
