---
{
    "title": "BITMAP_UNION",
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

## bitmap_union function

### description

Aggregate function, used to calculate the grouped bitmap union. Common usage scenarios such as: calculating PV, UV.

#### Syntax

`BITMAP BITMAP_UNION(BITMAP value)`

Enter a set of bitmap values, find the union of this set of bitmap values, and return.

### example

```
mysql> select page_id, bitmap_union(user_id) from table group by page_id;
```

Combined with the bitmap_count function, the PV data of the web page can be obtained

```
mysql> select page_id, bitmap_count(bitmap_union(user_id)) from table group by page_id;
```

When the user_id field is int, the above query semantics is equivalent to

```
mysql> select page_id, count(distinct user_id) from table group by page_id;
```

### keywords

    BITMAP_UNION, BITMAP
