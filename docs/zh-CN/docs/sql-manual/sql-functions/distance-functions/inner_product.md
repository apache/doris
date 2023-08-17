---
{
    "title": "inner_product",
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

## inner_product

### description
#### Syntax

```sql
DOUBLE inner_product(vector1, vector2)
```

计算两个大小相同的向量的标量积

### example

```
sql> SELECT cosine_distance([1, 2], [2, 3]);
+-------------------------------------------+
| cosine_distance(ARRAY(1, 2), ARRAY(2, 3)) |
+-------------------------------------------+
|                     0.0077221232863322609 |
+-------------------------------------------+
```

### keywords
	INNER_PRODUCT,DISTANCE,ARRAY
