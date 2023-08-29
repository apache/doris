---
{
    "title": "l2_distance",
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

## l2_distance

### description
#### Syntax

```sql
DOUBLE l2_distance(ARRAY<T> array1, ARRAY<T> array2)
```

计算欧几里得空间中两点（向量值为坐标）之间的距离
如果输入array为NULL，或者array中任何元素为NULL，则返回NULL

#### Notice
* 输入数组的子类型支持：TINYINT、SMALLINT、INT、BIGINT、LARGEINT、FLOAT、DOUBLE
* 输入数组array1和array2，元素数量需保持一致

### example

```
sql> SELECT l2_distance([1, 2], [2, 3]);
+---------------------------------------+
| l2_distance(ARRAY(1, 2), ARRAY(2, 3)) |
+---------------------------------------+
|                    1.4142135623730951 |
+---------------------------------------+
```

### keywords
	L2_DISTANCE,DISTANCE,L2,ARRAY
