---
{
    "title": "COS",
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

## cos

### description
#### Syntax

`DOUBLE cos(DOUBLE x)`
返回`x`的余弦值，`x` 为弧度值.

### example

```
mysql> select cos(1);
+---------------------+
| cos(1.0)            |
+---------------------+
| 0.54030230586813977 |
+---------------------+
mysql> select cos(0);
+----------+
| cos(0.0) |
+----------+
|        1 |
+----------+
mysql> select cos(Pi());
+-----------+
| cos(pi()) |
+-----------+
|        -1 |
+-----------+
```

### keywords
	COS
