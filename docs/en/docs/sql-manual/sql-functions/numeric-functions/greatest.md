---
{
    "title": "GREATEST",
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

## greatest

### description
#### Syntax

`greatest(col_a, col_b, …, col_n)`  

`column` supports the following types: `TINYINT` `SMALLINT` `INT` `BIGINT` `LARGEINT` `FLOAT` `DOUBLE` `STRING` `DATETIME` `DECIMAL`

Compares the size of `n columns` and returns the largest among them. If there is `NULL` in `column`, it returns `NULL`.

### example

```
mysql> select greatest(-1, 0, 5, 8);
+-----------------------+
| greatest(-1, 0, 5, 8) |
+-----------------------+
|                     8 |
+-----------------------+
mysql> select greatest(-1, 0, 5, NULL);
+--------------------------+
| greatest(-1, 0, 5, NULL) |
+--------------------------+
| NULL                     |
+--------------------------+
mysql> select greatest(6.3, 4.29, 7.6876);
+-----------------------------+
| greatest(6.3, 4.29, 7.6876) |
+-----------------------------+
|                      7.6876 |
+-----------------------------+
mysql> select greatest("2022-02-26 20:02:11","2020-01-23 20:02:11","2020-06-22 20:02:11");
+-------------------------------------------------------------------------------+
| greatest('2022-02-26 20:02:11', '2020-01-23 20:02:11', '2020-06-22 20:02:11') |
+-------------------------------------------------------------------------------+
| 2022-02-26 20:02:11                                                           |
+-------------------------------------------------------------------------------+
```

### keywords
	GREATEST
