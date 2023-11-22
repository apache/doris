---
{
    "title": "BITMAP_HAS_ANY",
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

## bitmap_has_any
### description
#### Syntax

`BOOLEAN BITMAP_HAS_ANY(BITMAP lhs, BITMAP rhs)`

计算两个Bitmap列是否存在相交元素，返回值是Boolean值. 

### example

```
mysql> select bitmap_has_any(to_bitmap(1),to_bitmap(2));
+--------------------------------------------+
| bitmap_has_any(to_bitmap(1), to_bitmap(2)) |
+--------------------------------------------+
|                                          0 |
+--------------------------------------------+

mysql> select bitmap_has_any(to_bitmap(1),to_bitmap(1));
+--------------------------------------------+
| bitmap_has_any(to_bitmap(1), to_bitmap(1)) |
+--------------------------------------------+
|                                          1 |
+--------------------------------------------+
```

### keywords

    BITMAP_HAS_ANY,BITMAP
