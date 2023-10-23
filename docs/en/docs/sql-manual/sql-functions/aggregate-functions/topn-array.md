---
{
    "title": "TOPN_ARRAY",
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

## TOPN_ARRAY
### description
#### Syntax

`ARRAY<T> topn_array(expr, INT top_num[, INT space_expand_rate])`

The topn function uses the Space-Saving algorithm to calculate the top_num frequent items in expr, 
and return an array about the top n nums, which is an approximation

The space_expand_rate parameter is optional and is used to set the number of counters used in the Space-Saving algorithm
```
counter numbers = top_num * space_expand_rate
```
The higher value of space_expand_rate, the more accurate result will be. The default value is 50

### example
```
mysql> select topn_array(k3,3) from baseall;
+--------------------------+
| topn_array(`k3`, 3)      |
+--------------------------+
| [3021, 2147483647, 5014] |
+--------------------------+
1 row in set (0.02 sec)

mysql> select topn_array(k3,3,100) from baseall;
+--------------------------+
| topn_array(`k3`, 3, 100) |
+--------------------------+
| [3021, 2147483647, 5014] |
+--------------------------+
1 row in set (0.02 sec)
```
### keywords
TOPN, TOPN_ARRAY