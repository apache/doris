---
{
    "title": "DEGREES",
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

## degrees

### description
#### Syntax

`DOUBLE degrees(DOUBLE x)`
返回`x`的度, 从弧度转换为度.

### example

```
mysql> select degrees(0);
+--------------+
| degrees(0.0) |
+--------------+
|            0 |
+--------------+
mysql> select degrees(2);
+--------------------+
| degrees(2.0)       |
+--------------------+
| 114.59155902616465 |
+--------------------+
mysql> select degrees(Pi());
+---------------+
| degrees(pi()) |
+---------------+
|           180 |
+---------------+
```

### keywords
	DEGREES
