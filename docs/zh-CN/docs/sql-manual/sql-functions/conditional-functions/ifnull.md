---
{
    "title": "IFNULL",
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

## ifnull
### description
#### Syntax

`ifnull(expr1, expr2)`


如果 expr1 的值不为 NULL 则返回 expr1，否则返回 expr2

### example

```
mysql> select ifnull(1,0);
+--------------+
| ifnull(1, 0) |
+--------------+
|            1 |
+--------------+

mysql> select ifnull(null,10);
+------------------+
| ifnull(NULL, 10) |
+------------------+
|               10 |
+------------------+
```
### keywords
IFNULL
