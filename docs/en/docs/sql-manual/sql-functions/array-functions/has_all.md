---
{
    "title": "has_all",
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

## arrays_overlap

### description

#### Syntax

`BOOLEAN has_all(ARRAY<T> set, ARRAY<T> subset)`
Checks whether one array is a subset of another.

#### Arguments

`set` – Array of any type with a set of elements.
`subset` – Array of any type with elements that should be tested to be a subset of set.

#### Returned value

`1`- if set contains all of the elements from subset.
`0`- otherwise.
`NULL` - set or subset is NULL

### example

```
 SELECT has_all([1,2,3],[1,2]);
+--------------------------------------+
| has_all(ARRAY(1, 2, 3), ARRAY(1, 2)) |
+--------------------------------------+
|                                    1 |
+--------------------------------------+

SELECT has_all([1.2,2.4,3.5],[]);
+----------------------------------------+
| has_all(ARRAY(1.2, 2.4, 3.5), ARRAY()) |
+----------------------------------------+
|                                      1 |
+----------------------------------------+

SELECT has_all([1,2,3],NULL);
+-------------------------------+
| has_all(ARRAY(1, 2, 3), NULL) |
+-------------------------------+
| NULL                          |
+-------------------------------+

SELECT has_all([100,200,NULL],[100,NULL]);
+--------------------------------------------------+
| has_all(ARRAY(100, 200, NULL), ARRAY(100, NULL)) |
+--------------------------------------------------+
|                                                1 |
+--------------------------------------------------+
```

### keywords

has_all