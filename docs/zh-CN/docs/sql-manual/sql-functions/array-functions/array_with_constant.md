---
{
    "title": "array_with_constant",
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

## array_with_constant

### description

#### Syntax

```
ARRAY<T> array_with_constant(n, T)
```
返回一个数组, 包含n个重复的T常量

### notice

`Only supported in vectorized engine`

### example

```
mysql> set enable_vectorized_engine=true;

mysql> select array_with_constant(2, "hello");
+---------------------------------+
| array_with_constant(2, 'hello') |
+---------------------------------+
| ['hello', 'hello']              |
+---------------------------------+
1 row in set (0.04 sec)

mysql> select array_with_constant(3, 12345);
+-------------------------------+
| array_with_constant(3, 12345) |
+-------------------------------+
| [12345, 12345, 12345]         |
+-------------------------------+
1 row in set (0.01 sec)

mysql> select array_with_constant(3, null);
+------------------------------+
| array_with_constant(3, NULL) |
+------------------------------+
| [NULL, NULL, NULL]           |
+------------------------------+
1 row in set (0.01 sec)
```

### keywords

ARRAY,WITH_CONSTANT,ARRAY_WITH_CONSTANT
