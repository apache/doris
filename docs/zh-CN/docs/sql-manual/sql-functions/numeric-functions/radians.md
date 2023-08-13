---
{
    "title": "RADIANS",
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

## radians

### description
#### Syntax

`DOUBLE radians(DOUBLE x)`
返回`x`的弧度值, 从度转换为弧度.

### example

```
mysql> select radians(0);
+--------------+
| radians(0.0) |
+--------------+
|            0 |
+--------------+
mysql> select radians(30);
+---------------------+
| radians(30.0)       |
+---------------------+
| 0.52359877559829882 |
+---------------------+
mysql> select radians(90);
+--------------------+
| radians(90.0)      |
+--------------------+
| 1.5707963267948966 |
+--------------------+
```

### keywords
	RADIANS
