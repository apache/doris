---
{
    "title": "IN",
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

## IN
### description
#### Syntax

`expr IN (value, ...)`

`expr IN (subquery)`

If expr is equal to any value in the IN list, return true; otherwise, return false.

Subquery can only return one column, and the column types returned by subquery must be compatible with expr types.

If subquery returns a bitmap data type column, expr must be an integer.

#### notice

- Currently, bitmap columns are only returned to in subqueries supported in the vectorized engine.

### example

```
mysql> select id from cost where id in (1, 2);
+------+
| id   |
+------+
|    2 |
|    1 |
+------+
```
```
mysql> select id from tbl1 where id in (select id from tbl2);
+------+
| id   |
+------+
|    1 |
|    4 |
|    5 |
+------+
```
```
mysql> select id from tbl1 where id in (select bitmap_col from tbl3);
+------+
| id   |
+------+
|    1 |
|    3 |
+------+
```

### keywords

    IN
