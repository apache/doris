---
{
    "title": "bitmap_intersect",
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

# bitmap_intersect
## description

Aggregation function, used to calculate the bitmap intersection after grouping. Common usage scenarios such as: calculating user retention rate.

### Syntax

`BITMAP BITMAP_INTERSECT(BITMAP value)`

Enter a set of bitmap values, find the intersection of the set of bitmap values, and return.

## example

```
Find the intersection of users for each tag
mysql> select tag, bitmap_intersect(user_id) from table group by tag;
```

Used in combination with the bitmap_to_string function to obtain the specific data of the intersection

```
Who are the intersection users of each tag
mysql> select tag, bitmap_to_string(bitmap_intersect(user_id)) from table group by tag;
```

## keyword

    BITMAP_INTERSECT, BITMAP
