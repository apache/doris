---
{
"title": "ORTHOGONAL_BITMAP_INTERSECT",
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

## orthogonal_bitmap_intersect
### description
#### Syntax

`BITMAP ORTHOGONAL_BITMAP_INTERSECT(bitmap_column, column_to_filter, filter_values)`
The bitmap intersection function, the first parameter is the bitmap column, the second parameter is the dimension column for filtering, and the third parameter is the variable length parameter, which means different values of the filter dimension column

### example

```
mysql> select orthogonal_bitmap_intersect(members, tag_group, 1150000, 1150001, 390006) from tag_map where  tag_group in ( 1150000, 1150001, 390006);
+-------------------------------------------------------------------------------+
| orthogonal_bitmap_intersect(`members`, `tag_group`, 1150000, 1150001, 390006) |
+-------------------------------------------------------------------------------+
| NULL                                                                          |
+-------------------------------------------------------------------------------+
1 row in set (3.505 sec)

```

### keywords

    ORTHOGONAL_BITMAP_INTERSECT,BITMAP
