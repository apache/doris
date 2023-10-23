---
{
"title": "ORTHOGONAL_BITMAP_UNION_COUNT",
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

## orthogonal_bitmap_union_count
### description
#### Syntax

`BITMAP ORTHOGONAL_BITMAP_UNION_COUNT(bitmap_column, column_to_filter, filter_values)`
求bitmap并集大小的函数, 参数类型是bitmap，是待求并集count的列


### example

```
mysql> select orthogonal_bitmap_union_count(members) from tag_map where  tag_group in ( 1150000, 1150001, 390006);
+------------------------------------------+
| orthogonal_bitmap_union_count(`members`) |
+------------------------------------------+
|                                286957811 |
+------------------------------------------+
1 row in set (2.645 sec)
```

### keywords

    ORTHOGONAL_BITMAP_UNION_COUNT,BITMAP
